#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <libpmemobj.h>
#include <chrono>
#include "nvm_mgr.h"
#include "threadinfo.h"
#include <boost/thread/barrier.hpp>

using namespace std;

void usage(){
    cout<<"lack of parameters, please input ./acmma [t] [num]\n"\
        <<"[t] is the type of test\n"\
        <<"0: new/libvmmalloc, 1: pmemobj_alloc, 2: pmdk transactional allocator, 3: acmma\n"\
        <<"[num] is the number of worker threads\n";
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


int main(int argc, char **argv){
    if(argc != 3){
        usage();
        return 0;
    }
    int type = atoi(argv[1]);
    int threads_num = atoi(argv[2]);
    int nsize = 64;

    system("rm -rf /mnt/pmem0/matianmao/acmma.data");
    const char *pool_name = "/mnt/pmem0/matianmao/acmma.data";
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

    if(type == 3){
        // acmma
        std::cout<<"acmma\n";
        NVMMgr_ns::init_nvm_mgr();
        NVMMgr_ns::NVMMgr *mgr = NVMMgr_ns::get_nvm_mgr();
        for(int i = 0; i < threads_num; i++){
            result[i] = 0;
            tid[i] = new std::thread([&](int id){
                NVMMgr_ns::register_threadinfo();
                bar->wait();
                while((*done) == 0) {
                    addr[result[id] % max_addr] = (uint64_t)NVMMgr_ns::alloc_new_node_from_size(nsize);
                    result[id]++;
                }
                }, i);
        }
    }else if(type == 2){
        std::cout<<"transactional pmdk\n";
        for(int i = 0; i < threads_num; i++){
            result[i] = 0;
            tid[i] = new std::thread([&](int id){
                bar->wait();
                TOID(struct my_root) root = POBJ_ROOT(tmp_pool, struct my_root);
                while((*done) == 0) {
                    TX_BEGIN(tmp_pool) {
                        TX_ADD(root);
                        D_RW(root)->ptr = pmemobj_tx_alloc(nsize, TOID_TYPE_NUM(char));
                        void *addr = (void *)pmemobj_direct(D_RW(root)->ptr);
                    }
                    TX_END
                    result[id]++;
                }
            }, i);
        }
    }else if(type == 1){
        std::cout<<"pmalloc\n";
        for(int i = 0; i < threads_num; i++){
            result[i] = 0;
            tid[i] = new std::thread([&](int id){
                bar->wait();
                PMEMoid ptr;
                while((*done) == 0) {
//                    std::cout<<"alloc\n";
                    pmemobj_zalloc(tmp_pool, &ptr, nsize, TOID_TYPE_NUM(char));
                    void *addr = (void *)pmemobj_direct(ptr);
                    result[id]++;
                }
            }, i);
        }
    }else if(type == 0){
        std::cout<<"libvmmalloc\n";
        for(int i = 0; i < threads_num; i++){
            result[i] = 0;
            tid[i] = new std::thread([&](int id){
                bar->wait();
                while((*done) == 0) {
                    addr[result[id] % max_addr] = (uint64_t)malloc(nsize);
                    result[id]++;
                }
            }, i);
        }
    }

    bar->wait();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    *done = 1;
    uint64_t res= 0;
    for(int i = 0; i < threads_num; i++){
        tid[i]->join();
        res += result[i];
    }

    std::cout<<"throughput "<<res<<" tps\n";
    return 0;
}