// =============================================================================
// client/main.cpp — CLI entry point
//
// Usage:
//   ./client put   <local_file>
//   ./client get   <filename> <output_path>
//   ./client delete <filename>
//   ./client list
//
// The metadata server address defaults to "localhost:50050" but can be
// overridden with the METADATA_ADDR environment variable.
// =============================================================================

#include "client.h"
#include "../common/config.h"

#include <iostream>
#include <string>
#include <cstdlib>  // std::getenv

static void printUsage(const char* program_name) {
    std::cout << "\n";
    std::cout << "Fault-Tolerant Distributed Storage System — Client\n";
    std::cout << std::string(50, '─') << "\n\n";
    std::cout << "Usage:\n";
    std::cout << "  " << program_name << " put    <local_file_path>\n";
    std::cout << "  " << program_name << " get    <filename> <output_path>\n";
    std::cout << "  " << program_name << " delete <filename>\n";
    std::cout << "  " << program_name << " list\n\n";
    std::cout << "Environment variables:\n";
    std::cout << "  METADATA_ADDR  Metadata server address (default: localhost:50050)\n\n";
    std::cout << "Examples:\n";
    std::cout << "  " << program_name << " put    ./myfile.pdf\n";
    std::cout << "  " << program_name << " get    myfile.pdf ./downloaded.pdf\n";
    std::cout << "  " << program_name << " delete myfile.pdf\n";
    std::cout << "  " << program_name << " list\n\n";
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printUsage(argv[0]);
        return 1;
    }

    // Read metadata server address from environment (Docker sets this).
    const char* meta_env = std::getenv("METADATA_ADDR");
    std::string metadata_addr = meta_env
        ? meta_env
        : ("localhost:" + std::to_string(config::METADATA_SERVER_PORT));

    std::cout << "[Client] Connecting to metadata server: " << metadata_addr << "\n";

    StorageClient client(metadata_addr);

    std::string command = argv[1];

    if (command == "put") {
        if (argc < 3) {
            std::cerr << "Usage: " << argv[0] << " put <local_file_path>\n";
            return 1;
        }
        client.put(argv[2]);

    } else if (command == "get") {
        if (argc < 4) {
            std::cerr << "Usage: " << argv[0] << " get <filename> <output_path>\n";
            return 1;
        }
        client.get(argv[2], argv[3]);

    } else if (command == "delete") {
        if (argc < 3) {
            std::cerr << "Usage: " << argv[0] << " delete <filename>\n";
            return 1;
        }
        client.deleteFile(argv[2]);

    } else if (command == "list") {
        client.listFiles();

    } else {
        std::cerr << "Unknown command: " << command << "\n";
        printUsage(argv[0]);
        return 1;
    }

    return 0;
}
