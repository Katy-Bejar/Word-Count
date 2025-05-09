#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <filesystem>
#include <algorithm>
#include <cctype>
#include <chrono>

using namespace std;

constexpr size_t BLOCK_SIZE = 64 * 1024 * 1024; // 64 MB
constexpr size_t OVERLAP_SIZE = 200; // bytes para corregir cortes de palabras

shared_mutex global_mutex; // Mutex para sincronizar acceso al mapa global

string clean_word(const string& word) {
    string cleaned;
    for (char c : word) {
        if (isalnum(static_cast<unsigned char>(c))) { // Solo caracteres alfanuméricos
            cleaned += tolower(static_cast<unsigned char>(c)); // Convertir a minúscula
        }
    }
    return cleaned;
}

void process_block(string block, bool is_first_block, unordered_map<string, size_t>& local_map) {
    // Saltar palabra incompleta al inicio si no es el primer bloque
    if (!is_first_block) {
        size_t skip_pos = block.find_first_of(" \n\r\t");
        if (skip_pos != string::npos) { //si encuentra el espacio (existe espacio)  npos:algo q no existe
            block = block.substr(skip_pos + 1);
        }
    }

    istringstream iss(block); // Tratar el bloque como stream
    string word;
    while (iss >> word) { // Extraer palabra por palabra
        string cleaned = clean_word(word);
        if (!cleaned.empty()) { //si esta limpio  si esq sigue viendo palabras
            ++local_map[cleaned]; // Contar en mapa local
        }
    }
}

string read_block(ifstream& file, streampos start, size_t size) {
    file.seekg(start); // Posicionar el puntero de lectura
    string buffer(size, '\0'); // Crear buffer del tamaño requerido
    file.read(&buffer[0], size); // Leer datos al buffer
    buffer.resize(file.gcount()); // Ajustar tamaño real leído
    return buffer;
}

void merge_maps(const unordered_map<string, size_t>& local_map,
                unordered_map<string, size_t>& global_map) {
    unique_lock lock(global_mutex);
    for (const auto& [word, count] : local_map) {
        global_map[word] += count;
    }
}

int main() {
    const string input_file = "20gb.txt";
    const string output_file = "wordcount_20gb.txt";

    //unsigned int num_threads = thread::hardware_concurrency();
    unsigned int num_threads = 2;
    cout << "Usando " << num_threads << " hilos disponibles.\n";
    if (num_threads == 0) num_threads = 4;

    auto start_time = chrono::high_resolution_clock::now();

    ifstream infile(input_file, ios::binary);
    if (!infile) {
        cerr << "No se pudo abrir el archivo: " << input_file << endl;
        return 1;
    }

    auto file_size = filesystem::file_size(input_file);
    unordered_map<string, size_t> global_wordcount;
    vector<thread> threads;
    vector<unordered_map<string, size_t>> local_maps(num_threads);

    size_t num_blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;

    for (size_t i = 0; i < num_blocks; ++i) {
        streampos block_start = i * BLOCK_SIZE;
        bool is_first_block = (i == 0);
    
        streamoff offset = static_cast<streamoff>(BLOCK_SIZE + OVERLAP_SIZE);
        size_t effective_block_size;
    
        if (block_start + offset <= static_cast<streamoff>(file_size)) {
            effective_block_size = BLOCK_SIZE + OVERLAP_SIZE;
        } else {
            effective_block_size = static_cast<size_t>(file_size - block_start);
        }
    
        size_t thread_id = i % num_threads;
        if (threads.size() < num_threads) {
            threads.emplace_back([&, block_start, effective_block_size, thread_id, is_first_block]() {
                ifstream thread_file(input_file, ios::binary);
                auto block = read_block(thread_file, block_start, effective_block_size);
                process_block(block, is_first_block, local_maps[thread_id]);
            });
        }
    
        if (threads.size() == num_threads || i == num_blocks - 1) {
            for (auto& t : threads) t.join();
            threads.clear();
    
            for (auto& local_map : local_maps) {
                merge_maps(local_map, global_wordcount);
                local_map.clear();
            }
        }
    }
    

    // Ordenar por frecuencia
    vector<pair<string, size_t>> sorted_words(global_wordcount.begin(), global_wordcount.end());
    sort(sorted_words.begin(), sorted_words.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    // Guardar resultados
    ofstream outfile(output_file);
    if (!outfile) {
        cerr << "No se pudo escribir en el archivo de salida." << endl;
        return 1;
    }

    for (const auto& [word, count] : sorted_words) {
        if (word.length() >= 3 && count > 1) {
            outfile << word << " " << count << "\n";
        }
    }

    // Mostrar top 10
    cout << "\nTop 10 palabras mas frecuentes:\n";
    for (size_t i = 0; i < min<size_t>(10, sorted_words.size()); ++i) {
        cout << sorted_words[i].first << " => " << sorted_words[i].second << "\n";
    }

    auto end_time = chrono::high_resolution_clock::now();
    chrono::duration<double> duration_seconds = end_time - start_time;

    cout << "\nConteo completado. Resultado en: " << output_file << "\n";
    cout << "Tiempo de ejecucion: " << duration_seconds.count() << " segundos.\n";

    return 0;
}