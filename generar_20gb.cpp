#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <random>
#include <chrono>
#include <sstream>
#include <thread>
#include <mutex>
#include <atomic>

using namespace std;
constexpr size_t TARGET_SIZE = 20ULL * 1024 * 1024 * 1024; // 20 GB


mutex file_mutex;
atomic<size_t> bytes_written(0);

// Cargar palabras separadas por línea o por "|"
vector<string> load_words(const string& filepath) {
    ifstream file(filepath);
    vector<string> words;
    string line;
    while (getline(file, line)) {
        size_t start = 0, end;
        while ((end = line.find('|', start)) != string::npos) {
            words.push_back(line.substr(start, end - start));
            start = end + 1;
        }
        words.push_back(line.substr(start));
    }
    return words;
}

// Generar oración con estructura simulada y terminación
string generate_sentence(const vector<string>& words, mt19937& rng) {
    uniform_int_distribution<int> length_dist(6, 14); // palabras por oración
    uniform_int_distribution<size_t> word_dist(0, words.size() - 1);

    ostringstream oss;
    int sentence_length = length_dist(rng);

    for (int i = 0; i < sentence_length; ++i) {
        string word = words[word_dist(rng)];
        oss << word;
        if (i < sentence_length - 1)
            oss << " ";
    }
    oss << ".\n";
    return oss.str();
}

// Función para generar oraciones y escribirlas al archivo compartido
void worker(const vector<string>& words, const string& output_file) {
    mt19937 rng(random_device{}());
    ostringstream buffer;

    while (true) {
        string sentence = generate_sentence(words, rng);
        size_t len = sentence.size();

        if (bytes_written + len >= TARGET_SIZE)
            break;

        buffer << sentence;
        bytes_written += len;

        // Escribir en archivo por lotes de ~1MB
        if (buffer.tellp() > 1 * 1024 * 1024) {
            lock_guard<mutex> lock(file_mutex);
            ofstream out(output_file, ios::app);
            out << buffer.str();
            buffer.str("");
            buffer.clear();
        }
    }

    // Escribir lo que quedó
    if (buffer.tellp() > 0) {
        lock_guard<mutex> lock(file_mutex);
        ofstream out(output_file, ios::app);
        out << buffer.str();
    }
}

int main() {
    const string input_file = "most-common-spanish-words-v2.txt";
    const string output_file = "20gb.txt";

    unsigned int THREAD_COUNT = thread::hardware_concurrency();
    if (THREAD_COUNT == 0) THREAD_COUNT = 4; // valor por defecto seguro

    auto start_time = chrono::high_resolution_clock::now();

    cout << "Cargando palabras desde '" << input_file << "'...\n";
    vector<string> words = load_words(input_file);
    if (words.empty()) {
        cerr << "No se encontraron palabras.\n";
        return 1;
    }

    ofstream clear(output_file, ios::trunc);
    clear.close();

    cout << "Iniciando generación con " << THREAD_COUNT << " hilos...\n";
    vector<thread> threads;

    for (unsigned int i = 0; i < THREAD_COUNT; ++i) {
        threads.emplace_back(worker, cref(words), cref(output_file));
    }

    for (auto& t : threads) t.join();

    auto end_time = chrono::high_resolution_clock::now();
    chrono::duration<double> duration_seconds = end_time - start_time;

    cout << "Archivo generado: " << output_file << "\n";
    cout << "Tamaño final: " << (bytes_written / (1024 * 1024.0)) << " MB\n";
    cout << "Tiempo total: " << duration_seconds.count() << " segundos.\n";

    return 0;
}
