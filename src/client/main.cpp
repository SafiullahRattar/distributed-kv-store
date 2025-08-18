#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"

namespace {

class KVClient {
public:
    explicit KVClient(const std::string& server_address)
        : channel_(grpc::CreateChannel(server_address,
                                       grpc::InsecureChannelCredentials()))
        , stub_(kvstore::KVStore::NewStub(channel_))
    {}

    bool put(const std::string& key, const std::string& value) {
        kvstore::PutRequest request;
        request.set_key(key);
        request.set_value(value);

        kvstore::PutResponse response;
        grpc::ClientContext ctx;
        auto status = stub_->Put(&ctx, request, &response);

        if (!status.ok()) {
            std::cerr << "Error: " << status.error_message() << "\n";
            return false;
        }
        return response.success();
    }

    std::pair<bool, std::string> get(const std::string& key) {
        kvstore::GetRequest request;
        request.set_key(key);

        kvstore::GetResponse response;
        grpc::ClientContext ctx;
        auto status = stub_->Get(&ctx, request, &response);

        if (!status.ok()) {
            std::cerr << "Error: " << status.error_message() << "\n";
            return {false, ""};
        }
        return {response.found(), response.value()};
    }

    bool del(const std::string& key) {
        kvstore::DeleteRequest request;
        request.set_key(key);

        kvstore::DeleteResponse response;
        grpc::ClientContext ctx;
        auto status = stub_->Delete(&ctx, request, &response);

        if (!status.ok()) {
            std::cerr << "Error: " << status.error_message() << "\n";
            return false;
        }
        return response.success();
    }

private:
    std::shared_ptr<grpc::Channel>                 channel_;
    std::unique_ptr<kvstore::KVStore::Stub>        stub_;
};

void print_usage() {
    std::cout
        << "Distributed KV Store CLI\n"
        << "========================\n"
        << "\n"
        << "Commands:\n"
        << "  put <key> <value>   Store a key-value pair\n"
        << "  get <key>           Retrieve the value for a key\n"
        << "  del <key>           Delete a key\n"
        << "  help                Show this message\n"
        << "  quit / exit         Exit the client\n"
        << "\n";
}

void run_repl(KVClient& client) {
    print_usage();

    std::string line;
    std::cout << "kv> ";
    while (std::getline(std::cin, line)) {
        std::istringstream iss(line);
        std::string cmd;
        if (!(iss >> cmd)) {
            std::cout << "kv> ";
            continue;
        }

        if (cmd == "quit" || cmd == "exit") {
            break;
        }

        if (cmd == "help") {
            print_usage();
        } else if (cmd == "put") {
            std::string key, value;
            if (!(iss >> key >> value)) {
                std::cerr << "Usage: put <key> <value>\n";
            } else {
                // Consume the rest of the line as value (supports spaces)
                std::string rest;
                if (std::getline(iss, rest) && !rest.empty()) {
                    value += rest;  // rest starts with a space if present
                }
                if (client.put(key, value)) {
                    std::cout << "OK\n";
                } else {
                    std::cout << "FAILED\n";
                }
            }
        } else if (cmd == "get") {
            std::string key;
            if (!(iss >> key)) {
                std::cerr << "Usage: get <key>\n";
            } else {
                auto [found, val] = client.get(key);
                if (found) {
                    std::cout << val << "\n";
                } else {
                    std::cout << "(nil)\n";
                }
            }
        } else if (cmd == "del") {
            std::string key;
            if (!(iss >> key)) {
                std::cerr << "Usage: del <key>\n";
            } else {
                if (client.del(key)) {
                    std::cout << "OK\n";
                } else {
                    std::cout << "FAILED\n";
                }
            }
        } else {
            std::cerr << "Unknown command: " << cmd
                      << " (type 'help' for usage)\n";
        }
        std::cout << "kv> ";
    }
    std::cout << "Bye.\n";
}

}  // anonymous namespace

int main(int argc, char* argv[]) {
    std::string server_address = "localhost:50051";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "--server" || arg == "-s") && i + 1 < argc) {
            server_address = argv[++i];
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: " << argv[0]
                      << " [--server <host:port>]\n"
                      << "  Default server: localhost:50051\n";
            return 0;
        }
    }

    std::cout << "Connecting to " << server_address << "...\n";
    KVClient client(server_address);
    run_repl(client);
    return 0;
}
