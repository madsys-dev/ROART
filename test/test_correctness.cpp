#include "Tree.h"
#include "threadinfo.h"
#include <gtest/gtest.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <thread>
#include <vector>

using namespace PART_ns;

inline void clear_data() { system("rm -rf /mnt/pmem0/matianmao/part.data"); }

TEST(TestCorrectness, PM_ART) {
    std::cout << "[TEST]\tstart to test correctness\n";
    clear_data();

    const int nthreads = 24;
    const int test_iter = 20000;

    std::vector<Key *> Keys;
    Keys.reserve(nthreads * test_iter);

    uint64_t *keys = new uint64_t[nthreads * test_iter + 1];

    Tree *art = new Tree();
    std::thread *tid[nthreads];

    // Generate keys
    std::cout << "[TEST]\tstart to build tree\n";
    for (uint64_t i = 0; i < nthreads * test_iter; i++) {
        keys[i] = i;
        Keys[i] = new Key(keys[i], sizeof(uint64_t), i + 1);
        // Keys[i] = Keys[i]->make_leaf(i, sizeof(uint64_t), i + 1);
        //        printf("insert start %d\n", i);
        Tree::OperationResults res = art->insert(Keys[i]);
        //        printf("insert success\n");
        ASSERT_EQ(res, Tree::OperationResults::Success)
            << "insert failed on key " << i;
    }

    std::cout << "initialization finish.....\n";

    for (int i = 0; i < nthreads; i++) {
        tid[i] = new std::thread(
            [&](int id) {
                NVMMgr_ns::register_threadinfo();
                // read
                for (int j = 0; j < test_iter; j++) {
                    uint64_t kk = j * nthreads + id;
                    void *ret = art->lookup(Keys[kk]);

                    ASSERT_TRUE(ret) << "lookup not find the key" << kk;

                    uint64_t val = *((uint64_t *)ret);
                    ASSERT_EQ(val, kk + 1)
                        << "lookup fail in thread " << id << ", insert " << kk
                        << ", lookup " << val;
                }
                // remove
                for (int j = 0; j < test_iter; j++) {
                    uint64_t kk = j * nthreads + id;

                    Tree::OperationResults res = art->remove(Keys[kk]);
                    ASSERT_EQ(res, Tree::OperationResults::Success)
                        << "fail to remove key " << kk;
                }
                // read
                for (int j = 0; j < test_iter; j++) {
                    uint64_t kk = j * nthreads + id;

                    void *ret = art->lookup(Keys[kk]);
                    ASSERT_FALSE(ret)
                        << "find key " << kk << ", but it should be removed";
                }
                // insert
                for (int j = 0; j < test_iter; j++) {
                    uint64_t kk = j * nthreads + id;
                    Tree::OperationResults res = art->insert(Keys[kk]);

                    ASSERT_EQ(res, Tree::OperationResults::Success)
                        << "insert failed on key " << kk;
                }

                // read
                for (int j = 0; j < test_iter; j++) {
                    uint64_t kk = j * nthreads + id;
                    void *ret = art->lookup(Keys[kk]);

                    ASSERT_TRUE(ret) << "lookup not find the key" << kk;

                    uint64_t val = *((uint64_t *)ret);
                    ASSERT_EQ(val, kk + 1)
                        << "lookup fail in thread " << id << ", insert " << kk
                        << ", lookup " << val;
                }
                NVMMgr_ns::unregister_threadinfo();
            },
            i);
    }

    for (int i = 0; i < nthreads; i++) {
        tid[i]->join();
    }
    for (int i = 0; i < nthreads * test_iter; i++) {
        void *ret = art->lookup(Keys[i]);
        ASSERT_TRUE(ret) << "not find key " << i << "but it has been inserted";
    }

    std::cout << "passed test.....\n";

    delete art;
}