#include "config.h"
#include "coordinator.h"
#include <fstream>

using namespace std;

int main(int argc, char **argv) {
    Config conf;
    parse_arguments(argc, argv, conf);

    nvindex::Coordinator<long long, long long, 64> coordinator(conf);
    coordinator.run();
}
