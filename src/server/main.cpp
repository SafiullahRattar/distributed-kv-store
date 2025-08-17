#include "node.h"

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {

kvstore::Node* g_node = nullptr;

void signal_handler(int /*sig*/) {
    std::cout << "\n[server] Shutting down...\n";
    if (g_node) {
        g_node->shutdown();
    }
}

void print_usage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0 << " [options]\n"
        << "\n"
        << "Options:\n"
        << "  --id <node-id>          Node identifier (default: node-1)\n"
        << "  --listen <host:port>    Listen address (default: 0.0.0.0:50051)\n"
        << "  --wal <path>            WAL file path (default: /tmp/kv_node.wal)\n"
        << "  --role <leader|follower> Node role (default: leader)\n"
        << "  --peer <host:port>      Peer address (repeatable)\n"
        << "  --help                  Show this message\n"
        << "\n"
        << "Examples:\n"
        << "  # Start a single-node leader\n"
        << "  " << argv0 << " --id node-1 --listen 0.0.0.0:50051\n"
        << "\n"
        << "  # Start a 3-node cluster\n"
        << "  " << argv0 << " --id node-1 --listen 0.0.0.0:50051 --role leader "
           "--peer localhost:50052 --peer localhost:50053\n"
        << "  " << argv0 << " --id node-2 --listen 0.0.0.0:50052 --role follower "
           "--peer localhost:50051\n"
        << "  " << argv0 << " --id node-3 --listen 0.0.0.0:50053 --role follower "
           "--peer localhost:50051\n";
}

}  // anonymous namespace

int main(int argc, char* argv[]) {
    kvstore::NodeConfig config;
    config.node_id        = "node-1";
    config.listen_address = "0.0.0.0:50051";
    config.wal_path       = "/tmp/kv_node.wal";
    config.role           = kvstore::NodeRole::LEADER;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }

        if (i + 1 >= argc) {
            std::cerr << "Error: " << arg << " requires a value\n";
            return 1;
        }

        std::string val = argv[++i];

        if (arg == "--id") {
            config.node_id = val;
        } else if (arg == "--listen") {
            config.listen_address = val;
        } else if (arg == "--wal") {
            config.wal_path = val;
        } else if (arg == "--role") {
            if (val == "leader") {
                config.role = kvstore::NodeRole::LEADER;
            } else if (val == "follower") {
                config.role = kvstore::NodeRole::FOLLOWER;
            } else {
                std::cerr << "Error: unknown role '" << val
                          << "' (use leader or follower)\n";
                return 1;
            }
        } else if (arg == "--peer") {
            config.peers.push_back(val);
        } else {
            std::cerr << "Error: unknown option '" << arg << "'\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    try {
        kvstore::Node node(config);
        g_node = &node;
        node.run();
    } catch (const std::exception& ex) {
        std::cerr << "[server] Fatal: " << ex.what() << "\n";
        return 1;
    }

    return 0;
}
