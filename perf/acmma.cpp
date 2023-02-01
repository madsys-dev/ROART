#include "nvm_mgr.h"
#include "threadinfo.h"
#include <boost/thread/barrier.hpp>
#include <chrono>
#include <iostream>
#include <libpmemobj.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

using namespace std;

void usage() {
    cout << "lack of parameters, please input ./acmma [t] [num]\n"
         << "[t] is the type of test\n"
         << "0: pmemobj_alloc, 1: dcmm， 2: nvm_malloc\n"
         << "[num] is the number of worker threads\n";
}

POBJ_LAYOUT_BEGIN(allocator);
POBJ_LAYOUT_ROOT(allocator, struct my_root);
POBJ_LAYOUT_TOID(allocator, char);
POBJ_LAYOUT_END(allocator);

struct my_root {
    PMEMoid ptr;
};

const int max_addr = 1000000;
uint64_t addr[max_addr + 5];

const int thread_to_core[36] = {
        0,  4,  8,  12, 16, 20, 24, 28, 32,
        36, 40, 44, 48, 52, 56, 60, 64, 68, // numa node 0
        1,  5,  9,  13, 17, 21, 25, 29, 33,
        37, 41, 45, 49, 53, 57, 61, 65, 69 // numa node 1
};

int stick_this_thread_to_core(int core_id) {
    int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    if (core_id < 0 || core_id >= num_cores)
        return EINVAL;

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(thread_to_core[core_id], &cpuset);

    pthread_t current_thread = pthread_self();
    return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t),
                                  &cpuset);
}


int main(int argc, char **argv) {
    if (argc != 3) {
        usage();
        return 0;
    }
    int type = atoi(argv[1]);
    int threads_num = atoi(argv[2]);

    system("rm -rf /mnt/pmem_pxf/acmma.data");
    system("rm -rf /mnt/pmem_pxf/part.data");

    const char *pool_name = "/mnt/pmem_pxf/acmma.data";
    const char *layout_name = "acmma";
    size_t pool_size = 64ull * 1024 * 1024 * 1024;

    PMEMobjpool *tmp_pool;
    if (access(pool_name, 0)) {
        tmp_pool = pmemobj_create(pool_name, layout_name, pool_size, 0666);
    } else {
        tmp_pool = pmemobj_open(pool_name, layout_name);
    }

    std::thread *tid[threads_num];

    int *done = new int;
    *done = 0;
    boost::barrier *bar = new boost::barrier(threads_num + 1);
    uint64_t result[threads_num];

    int nsize = 64;
    std::string name = "";
    if (type == 2) {
        // TODO: nvm_malloc
        name = "nvm_malloc";
    } else if (type == 1) {
        // acmma
        name = "DCMM";
        std::cout << "dcmm\n";
        NVMMgr_ns::init_nvm_mgr();
        NVMMgr_ns::NVMMgr *mgr = NVMMgr_ns::get_nvm_mgr();
        for (int i = 0; i < threads_num; i++) {
            result[i] = 0;
            tid[i] = new std::thread(
                [&](int id) {
                    stick_this_thread_to_core(id);
                    NVMMgr_ns::register_threadinfo();
//                    int s = 0;
                    int tx = 0;
                    bar->wait();
                    while ((*done) == 0) {
                        addr[result[id] % max_addr] =
                            (uint64_t)NVMMgr_ns::alloc_new_node_from_size(
                                nsize);
                        tx++;
//                        s++;
//                        if (s >= 10)
//                            s = 0;
                    }
                    result[id] = tx;
                },
                i);
        }
    } else if (type == 0) {
        name = "PMDK";
        std::cout << "pmalloc\n";
        for (int i = 0; i < threads_num; i++) {
            result[i] = 0;
            tid[i] = new std::thread(
                [&](int id) {
                    stick_this_thread_to_core(id);
//                    int s = 0;
                    int tx = 0;
                    bar->wait();
                    PMEMoid ptr;
                    while ((*done) == 0) {
                        //                    std::cout<<"alloc\n";
                        pmemobj_zalloc(tmp_pool, &ptr, nsize,
                                       TOID_TYPE_NUM(char));
                        void *addr = (void *)pmemobj_direct(ptr);
                        tx++;
//                        s++;
//                        if (s >= 10)
//                            s = 0;
                    }
                    result[id] = tx;
                },
                i);
        }
    }

    bar->wait();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    *done = 1;
    uint64_t res = 0;
    for (int i = 0; i < threads_num; i++) {
        tid[i]->join();
        res += result[i];
    }

    std::cout << name<< ", threads: "<< threads_num <<", throughput " << res/1000000.0 << " Mtps\n";
    return 0;
}