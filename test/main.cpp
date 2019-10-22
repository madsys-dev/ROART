#include <fstream>
#include "config.h"
#include "coordinator.h"

using namespace std;

int main(int argc, char** argv) {
    Config conf;
    parse_arguments(argc, argv, conf);

    nvindex::Coordinator<long long, long long, 64> coordinator(conf);
    coordinator.run();
}
