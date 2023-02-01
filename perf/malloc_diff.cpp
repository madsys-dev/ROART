#include "util.h"
#include <chrono>
#include <iostream>
#include <libpmemobj.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

using namespace std;

POBJ_LAYOUT_BEGIN(allocator);
POBJ_LAYOUT_ROOT(allocator, struct my_root);
POBJ_LAYOUT_TOID(allocator, char);
POBJ_LAYOUT_END(allocator);

struct my_root {
    PMEMoid ptr;
};
uint64_t addr[1000005];

// test malloc, libvmmalloc, pmemobj_alloc, pmdk transactional allocator
void usage() {
    cout << "lack of parameters, please input ./malloc_diff [t] [size]\n"
         << "[t] is the type of test\n"
         << "0: new/libvmmalloc, 1: pmemobj_alloc, 2: tx+pmemobj_alloc, 3: "
            "pmdk transactional allocator\n"
         << "[size] is the size of allocated block\n";
}

int main(int argc, char **argv) {
    cout << "[ALLOC]test different allocate\n";

    if (argc < 3) {
        usage();
    }
    int type = atoi(argv[1]);
    size_t nsize = atoi(argv[2]);
    int test_iter = 1000000;

    system("rm -rf /mnt/pmem_pxf/test_alloc.data");
    const char *pool_name = "/mnt/pmem_pxf/test_alloc.data";
    const char *layout_name = "pmdk-alloc";
    size_t pool_size = 1024 * 1024 * 256;

    PMEMobjpool *tmp_pool;
    if (access(pool_name, 0)) {
        tmp_pool = pmemobj_create(pool_name, layout_name, pool_size, 0666);
    } else {
        tmp_pool = pmemobj_open(pool_name, layout_name);
    }

    char value[nsize + 5];
    memset(value, 'a', nsize);
    auto starttime = chrono::system_clock::now();
    if (type == 0) {
        cout << "test new/libvmmalloc\n";
        for (int i = 0; i < test_iter; i++) {
            addr[i] = (uint64_t)malloc(nsize);
            //            memcpy((char *)addr[i], value, nsize);
            //            flush_data((void*)addr[i], nsize);
        }
    } else {
        if (type == 1) {
            cout << "test pmemobj_alloc\n";
            TOID(struct my_root) root = POBJ_ROOT(tmp_pool, struct my_root);
            for (int i = 0; i < test_iter; i++) {
                int ret =
                    pmemobj_zalloc(tmp_pool, &D_RW(root)->ptr,
                                   sizeof(char) * nsize, TOID_TYPE_NUM(char));
                if (ret) {
                    cout << "pmemobj_zalloc error\n";
                    return 1;
                }
                char *ad = (char *)pmemobj_direct(D_RW(root)->ptr);
                memcpy(ad, value, nsize);
                flush_data(ad, nsize);
            }
        } else if (type == 2) {
            cout << "test tx+pmemobj_alloc\n";
            PMEMoid ptr;
            for (int i = 0; i < test_iter; i++) {
                TX_BEGIN(tmp_pool) {
                    ptr = pmemobj_tx_zalloc(sizeof(char) * nsize,
                                            TOID_TYPE_NUM(char));
                }
                TX_END
                char *ad = (char *)pmemobj_direct(ptr);
                memcpy(ad, value, nsize);
                flush_data(ad, nsize);
            }
        } else {
            cout << "test pmdk transactional allocator\n";
            TOID(struct my_root) root = POBJ_ROOT(tmp_pool, struct my_root);
            for (int i = 0; i < 1000000; i++) {
                TX_BEGIN(tmp_pool) {
                    TX_ADD(root);
                    D_RW(root)->ptr = pmemobj_tx_zalloc(sizeof(char) * nsize,
                                                        TOID_TYPE_NUM(char));
                }
                TX_END
                char *ad = (char *)pmemobj_direct(D_RW(root)->ptr);
                memcpy(ad, value, nsize);
                flush_data(ad, nsize);
            }
        }
    }
    auto end = chrono::system_clock::now();
    auto duration =
        chrono::duration_cast<std::chrono::microseconds>(end - starttime);

    cout << "type is: " << type << " size is: " << nsize << "duration time "
         << duration.count() << "us\n";
    return 0;
}