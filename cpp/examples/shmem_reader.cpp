/// Example: read from a KrabiBuffer in Windows shared memory.
///
/// Usage: shmem_reader [link_file_path]
///
/// The link file contains the OS-level shared memory object name.

#ifdef _WIN32

#include <Windows.h>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "../include/krabibuffer.hpp"

int main(int argc, char** argv)
{
    std::string link_file_path = "my_queue_object";
    if (argc > 1) {
        link_file_path = argv[1];
    }

    std::ifstream ifs(link_file_path);
    if (!ifs.is_open()) {
        std::cerr << "Failed to open link file: " << link_file_path << std::endl;
        return 1;
    }
    std::string shm_name;
    if (!std::getline(ifs, shm_name)) {
        std::cerr << "Failed to read from link file" << std::endl;
        return 1;
    }
    ifs.close();

    HANDLE hMap = OpenFileMappingA(FILE_MAP_ALL_ACCESS, FALSE, shm_name.c_str());
    if (!hMap) {
        std::cerr << "OpenFileMappingA failed: " << GetLastError() << std::endl;
        return 1;
    }

    void* addr = MapViewOfFile(hMap, FILE_MAP_ALL_ACCESS, 0, 0, 0);
    if (!addr) {
        std::cerr << "MapViewOfFile failed: " << GetLastError() << std::endl;
        CloseHandle(hMap);
        return 1;
    }

    auto* kb = static_cast<KrabiBuffer*>(addr);

    std::cout << "magic      = 0x" << std::hex << (int)kb->magic << std::dec << std::endl;
    std::cout << "slot_count = " << kb->slot_count << std::endl;
    std::cout << "stride     = " << kb->stride << std::endl;
    std::cout << "head       = " << kb->head.load(std::memory_order_relaxed) << std::endl;
    std::cout << "tail       = " << kb->tail.load(std::memory_order_relaxed) << std::endl;

    std::vector<uint8_t> frame(kb->stride);
    if (krabibuffer_dequeue(kb, frame.data())) {
        std::cout << "Dequeued: ";
        for (uint64_t i = 0; i < kb->stride; ++i) {
            std::cout << (int)frame[i] << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "Queue is empty" << std::endl;
    }

    UnmapViewOfFile(addr);
    CloseHandle(hMap);
    return 0;
}

#else

#include <iostream>

int main()
{
    std::cerr << "This example requires Windows shared memory APIs." << std::endl;
    return 1;
}

#endif
