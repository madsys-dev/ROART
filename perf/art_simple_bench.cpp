#include "tbb/tbb.h"
#include <chrono>
#include <iostream>
#include <random>
#include <thread>

using namespace std;

#include "Tree.h"

using namespace PART_ns;

inline void clear_data() { system("rm -rf /mnt/pmem_pxf/part.data"); }
void run(char **argv) {
    clear_data();
    std::cout << "Simple Example of P-ART" << std::endl;

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];
    std::vector<Key *> Keys;

    Keys.reserve(n);
    Tree *tree = new Tree();
    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = i;
        // Keys[i] = Keys[i]->make_leaf(i, sizeof(uint64_t), i);
        Keys[i] = new Key(keys[i], sizeof(uint64_t), keys[i]);
        tree->insert(Keys[i]);
    }

    const int num_thread = atoi(argv[2]);
    tbb::task_scheduler_init init(num_thread);
    // std::thread *tid[num_thread];
    // int every_thread_num = n / num_thread + 1;

    // simple benchmark, not precise
    printf("operation,n,ops/s\n");
    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(
            tbb::blocked_range<uint64_t>(0, n),
            [&](const tbb::blocked_range<uint64_t> &range) {
                for (uint64_t i = range.begin(); i != range.end(); i++) {
                    Keys[i]->Init(2 * keys[i], sizeof(uint64_t), keys[i]);
                    tree->insert(Keys[i]);
                }
            });

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n,
               (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n,
               duration.count() / 1000000.0);
    }

    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n),
                          [&](const tbb::blocked_range<uint64_t> &range) {
                              for (uint64_t i = range.begin(); i != range.end();
                                   i++) {
                                  //   Keys[i] = Keys[i]->make_leaf(i,
                                  //   sizeof(i), i);
                                  // Keys[i]->Init(keys[i], sizeof(uint64_t),
                                  // keys[i]);
                                  tree->lookup(Keys[i]);
                              }
                          });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n,
               (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n,
               duration.count() / 1000000.0);
    }

    delete[] keys;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        printf(
            "usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: "
            "number of threads (integer)\n",
            argv[0]);
        return 1;
    }

    run(argv);
    return 0;
}