#include "config.h"
#include "coordinator.h"
#include <fstream>
using namespace std;

inline void clear_data() {
    system((std::string("rm -rf ") + nvm_dir + "part.data").c_str());
    system((std::string("rm -rf ") + nvm_dir + "fast_fair.data").c_str());
    system((std::string("rm -rf ") + nvm_dir + "skiplist.data").c_str());
}

int main(int argc, char **argv) {
    clear_data();
    Config conf;
    parse_arguments(argc, argv, conf);

    Coordinator<long long, long long, 64> coordinator(conf);
    coordinator.run();
}
