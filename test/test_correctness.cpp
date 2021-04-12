#include "Tree.h"
#include "generator.h"
#include "threadinfo.h"
#include <gtest/gtest.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <thread>
#include <vector>

using namespace PART_ns;

inline void clear_data() { system("rm -rf /mnt/pmem0/jzc/part.data"); }

TEST(TestCorrectness, PM_ART) {
    std::cout << "[TEST]\tstart to test correctness\n";
    clear_data();

    const int nthreads = 1;
    const int test_iter = 1000000;

    std::vector<std::string> key_vec;
    std::set<std::string> key_set;

    Tree *art = new Tree();
    std::thread *tid[nthreads];

    RandomGenerator rdm;

    // Generate keys
    std::cout << "[TEST]\tstart to build tree\n";
    for (int i = 0; i < nthreads * test_iter; i++) {
        //        std::string key = std::to_string(i);
        ////        key = key + "x";
        std::string key = rdm.RandomStr();
        key = "msn" + key + "msn";
        Key *k = new Key();
        k->Init((char *)key.c_str(), key.size(), (char *)key.c_str(),
                key.size());
        if (key_set.count(key)) {
            Tree::OperationResults res = art->insert(k);
            ASSERT_EQ(res, Tree::OperationResults::Existed);
            i--;
        } else {

            Tree::OperationResults res = art->insert(k);
            ASSERT_EQ(res, Tree::OperationResults::Success);

            Leaf *ret = art->lookup(k);
            ASSERT_TRUE(ret);
            ASSERT_EQ(ret->key_len, key.size());
            ASSERT_EQ(ret->val_len, key.size());

            key_set.insert(key);
            key_vec.push_back(key);
        }
    }

    for (uint64_t i = 0; i < nthreads * test_iter; i++) {
        std::string key = key_vec[i];
        Key *k = new Key();
        k->Init((char *)key.c_str(), key.size(), (char *)key.c_str(),
                key.size());
        //        std::cout<<k->fkey<<" "<<k->key_len<<"\n";
        Leaf *ret = art->lookup(k);

        ASSERT_TRUE(ret) << "key: " << key << " " << k->fkey << " "
                         << k->key_len << "\n";
        ASSERT_EQ(ret->key_len, key.size());
        ASSERT_EQ(ret->val_len, key.size());
        ASSERT_EQ(memcmp(ret->kv, key.c_str(), key.size()), 0);
        ASSERT_EQ(memcmp(ret->kv + key.size(), key.c_str(), key.size()), 0);
        //        std::cout<<(char *)ret<<"\n";
    }

    std::cout << "initialization finish.....\n";

    for (int i = 0; i < nthreads; i++) {
        tid[i] = new std::thread(
            [&](int id) {
                std::cout << "thread " << id << "\n";
                NVMMgr_ns::register_threadinfo();
                Key *str_key = new Key();
                Tree::OperationResults res;
                // read update read
                for (int j = 0; j < test_iter; j++) {
                    // read
                    std::string kk = key_vec[j * nthreads + id];
                    str_key->Init((char *)kk.c_str(), kk.size(),
                                  (char *)kk.c_str(), kk.size());
                    Leaf *ret = art->lookup(str_key);
                    ASSERT_TRUE(ret);
                    ASSERT_EQ(ret->key_len, kk.size());
                    ASSERT_EQ(ret->val_len, kk.size())
                        << "test_iter:" << j << " kk:" << kk
                        << " ret:" << ret->GetKey()
                        << " val:" << ret->GetValue() << std::endl;
                    ASSERT_EQ(memcmp(ret->kv, kk.c_str(), kk.size()), 0);
                    ASSERT_EQ(
                        memcmp(ret->kv + kk.size(), kk.c_str(), kk.size()), 0);

                    // update
                    std::string newval = "0" + kk + "0";
                    str_key->Init((char *)kk.c_str(), kk.size(),
                                  (char *)newval.c_str(), newval.size());
                    res = art->update(str_key);
                    ASSERT_EQ(res, Tree::OperationResults::Success);

                    // read
                    ret = art->lookup(str_key);
                    ASSERT_TRUE(ret);
                    ASSERT_EQ(ret->val_len, kk.size() + 2);
                    std::string old_ret(kk);
                    old_ret = "0" + old_ret + "0";
                    ASSERT_EQ(ret->val_len, old_ret.size());
                    ASSERT_EQ(memcmp(ret->kv + kk.size(), old_ret.c_str(),
                                     ret->val_len),
                              0);
                }

                std::cout << "finish read update read\n";
                for (int j = 0; j < test_iter; j++) {
                    std::string kk = key_vec[j * nthreads + id];
                    str_key->Init((char *)kk.c_str(), kk.size(),
                                  (char *)kk.c_str(), kk.size());
                    Leaf *ret = art->lookup(str_key);

                    ASSERT_TRUE(ret);
                    ASSERT_EQ(ret->val_len, kk.size() + 2);
                    std::string old_ret(kk);
                    old_ret = "0" + old_ret + "0";
                    ASSERT_EQ(ret->val_len, old_ret.size());
                    ASSERT_EQ(memcmp(ret->kv + kk.size(), old_ret.c_str(),
                                     ret->val_len),
                              0);

                    std::string newval = "madsys" + kk + "aaa";
                    str_key->Init((char *)kk.c_str(), kk.size(),
                                  (char *)newval.c_str(), newval.size());
                    res = art->update(str_key);
                    ASSERT_EQ(res, Tree::OperationResults::Success);
                }

                for (int j = 0; j < test_iter; j++) {
                    std::string kk = key_vec[j * nthreads + id];
                    str_key->Init((char *)kk.c_str(), kk.size(),
                                  (char *)kk.c_str(), kk.size());
                    Leaf *ret = art->lookup(str_key);

                    ASSERT_TRUE(ret);
                    ASSERT_EQ(ret->val_len, kk.size() + 9);
                    std::string old_ret(kk);
                    old_ret = "madsys" + old_ret + "aaa";
                    ASSERT_EQ(ret->val_len, old_ret.size());
                    ASSERT_EQ(memcmp(ret->kv + kk.size(), old_ret.c_str(),
                                     ret->val_len),
                              0);
                }
                std::cout << "finish check update\n";
                // remove
                for (int j = 0; j < test_iter; j++) {
                    // remove read
                    std::string kk = key_vec[j * nthreads + id];
                    str_key->Init((char *)kk.c_str(), kk.size(),
                                  (char *)kk.c_str(), kk.size());

                    res = art->remove(str_key);
                    ASSERT_EQ(res, Tree::OperationResults::Success);

                    Leaf *ret = art->lookup(str_key);
                    ASSERT_FALSE(ret);
                }
                std::cout << "finish remove read\n";

                // insert read
                for (int j = 0; j < test_iter; j++) {
                    // insert
                    std::string kk = key_vec[j * nthreads + id];

                    str_key->Init((char *)kk.c_str(), kk.size(),
                                  (char *)kk.c_str(), kk.size());
                    res = art->insert(str_key);
                    ASSERT_EQ(res, Tree::OperationResults::Success);

                    Leaf *ret = art->lookup(str_key);
                    ASSERT_TRUE(ret);
                    ASSERT_EQ(ret->val_len, kk.size());
                    ASSERT_EQ(memcmp(ret->kv + kk.size(), (char *)kk.c_str(),
                                     kk.size()),
                              0);
                }
                std::cout << "finish insert read\n";
                NVMMgr_ns::unregister_threadinfo();
            },
            i);
    }

    for (int i = 0; i < nthreads; i++) {
        tid[i]->join();
    }

    std::cout << "passed test.....\n";

    delete art;
}