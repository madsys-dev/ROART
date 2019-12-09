#include "config.h"
#include "coordinator.h"
#include <fstream>

using namespace std;

inline void clear_data() {
    system("rm -rf /mnt/pmem0/matianmao/part.data");
    system("rm -rf /mnt/pmem0/matianmao/fast_fair.data");
    system("rm -rf /mnt/pmem0/matianmao/skiplist.data");
}

int main(int argc, char **argv) {
    clear_data();
    Config conf;
    parse_arguments(argc, argv, conf);

    Coordinator<long long, long long, 64> coordinator(conf);
    coordinator.run();
}
