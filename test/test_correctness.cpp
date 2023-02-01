#include "generator.h"
#include "threadinfo.h"
#include <boost/thread/barrier.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <thread>
#include <vector>

using namespace PART_ns;

inline void clear_data() { system("rm -rf /mnt/pmem_pxf/part.data"); }

TEST(TestCorrectness, PM_ART) {

    std::cout << "[TEST]\tstart to test correctness\n";
    clear_data();

    const int nthreads = 4;
    const int test_iter = 1000;
    const int total_key_cnt = nthreads * test_iter;
    const int scan_iter = 100;

    std::vector<std::string> key_vec;
    std::set<std::string> key_set;

    Tree *art = new Tree();
    std::thread *tid[nthreads];

    srand(time(nullptr));
    RandomGenerator rdm;
    std::vector<unsigned short> s1 = {1, 2, 3}, s2 = {4, 5, 6};
    rdm.setSeed(s1.data(), s2.data());
    // Generate keys
    std::cout << "[TEST]\tstart to build tree\n";
    for (int i = 0; i < nthreads * test_iter; i++) {
        //        std::string key = std::to_string(i);
        ////        key = key + "x";
        std::string key = rdm.RandomStr();
        key = "ebc" + key + "ebc";
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
            //            art->graphviz_debug();
            Leaf *ret = art->lookup(k);
            //            assert(ret != nullptr);
            //            if (i == 3)
            //                return;
            ASSERT_TRUE(ret) << "i: " << i << " key: " << key << std::endl;
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
    std::cout << key_vec[123] << std::endl;
    auto *bar = new boost::barrier(nthreads);
    // single thread scan
    int end_limit_cnt = 0, size_limit_cnt = 0;
    for (int i = 0; i < scan_iter; i++) {
        int scan_length = (rdm.randomInt() % total_key_cnt / 4) + 1;
        auto start_key = new Key(), end_key = new Key();
        std::string start_string = rdm.RandomStr();
        start_string = "ebc" + start_string + "ebc";
        start_key->Init((char *)start_string.c_str(), start_string.size(),
                        (char *)start_string.c_str(), start_string.size());
        std::string end_string = rdm.RandomStr();
        end_string = "ebc" + end_string + "ebc";
        end_key->Init((char *)end_string.c_str(), end_string.size(),
                      (char *)end_string.c_str(), end_string.size());

        Key *toContinue = nullptr;
        std::vector<Leaf *> result(scan_length);
        //        Leaf *result[scan_length];
        size_t result_count = 0;
        auto re = art->lookupRange(start_key, end_key, toContinue,
                                   result.data(), scan_length, result_count);
        if (start_string < end_string == false) {
            ASSERT_EQ(re, false);
            i--;
            continue;
        } else {
            int cnt = 0;
#ifdef SORT_LEAVES
            //            for (auto iterator =
            //            key_set.lower_bound(start_string);
            //                 iterator != key_set.lower_bound(end_string) &&
            //                 cnt < scan_length;
            //                 iterator++) {
            //                std::cout << "right:\t" << *iterator << std::endl;
            //                if (cnt < result_count) {
            //                    std::cout << "art:\t"
            //                              <<
            //                              std::string(result[cnt]->GetKey(),
            //                                             result[cnt]->key_len)
            //                              << std::endl
            //                              << std::endl;
            //                }
            //                cnt++;
            //            }
            //            std::cout << cnt << std::endl;
            //            cnt = 0;

            for (auto iterator = key_set.lower_bound(start_string);
                 iterator != key_set.lower_bound(end_string) &&
                 cnt < scan_length;
                 iterator++) {
                //                ASSERT_TRUE(cnt < result_count);
                ASSERT_EQ(memcmp(iterator->c_str(), result[cnt]->GetKey(),
                                 iterator->size()),
                          0)
                    << "test-iter:" << i << " cnt:" << cnt << std::endl
                    << "right:\t" << *iterator << std::endl
                    << "art:\t"
                    << std::string(result[cnt]->GetKey(), result[cnt]->key_len)
                    << std::endl
                    << "start:\t" << start_string << std::endl
                    << "end:\t" << end_string << std::endl;

                cnt++;
            }
#else

            for (cnt = 0; cnt < result_count; cnt++) {
                ASSERT_TRUE(N::leaf_key_lt(result[cnt], end_key, 0));
                ASSERT_FALSE(N::leaf_key_lt(result[cnt], start_key, 0))
                    << "start key:\t" << start_string << std::endl
                    << "art found:\t"
                    << std::string(result[cnt]->GetKey(), result[cnt]->key_len);
            }
#endif
            ASSERT_EQ(cnt, result_count);
            if (result_count == scan_length) {
                size_limit_cnt++;
            } else {
                end_limit_cnt++;
            }
        }
    }
    std::cout << size_limit_cnt << " scans end for size limit" << std::endl
              << end_limit_cnt << " scans end for end limit" << std::endl;
    std::cout << "single thread scan finish......" << std::endl;
    //    return;

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
                //                if (id % 2 == 1)
                //                bar->wait();

                // insert read
                for (int j = 0; j < test_iter; j++) {
                    // insert
                    std::string kk = key_vec[j * nthreads + id];

                    str_key->Init((char *)kk.c_str(), kk.size(),
                                  (char *)kk.c_str(), kk.size());
                    res = art->insert(str_key);
                    ASSERT_EQ(res, Tree::OperationResults::Success);

                    Leaf *ret = art->lookup(str_key);
                    ASSERT_TRUE(ret) << "thread: " << id << std::endl
                                     << "j: " << j << std::endl
                                     << "inserted:\t" << kk << std::endl;
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

TEST(TestCorrectness, PM_ART_INSERT_AND_READ) {

    std::cout << "[TEST]\tstart to test correctness\n";
    clear_data();

    const int nthreads = 4;
    const int test_iter = 5;
    const int total_key_cnt = nthreads * test_iter;

    std::vector<std::string> key_vec;
    std::set<std::string> key_set;

    Tree *art = new Tree();
    std::thread *tid[nthreads];

    RandomGenerator rdm;
    std::vector<unsigned short> s1 = {1, 2, 3}, s2 = {4, 5, 6};
    rdm.setSeed(s1.data(), s2.data());
    // Generate keys
    std::cout << "[TEST]\tstart to build tree\n";
    for (int i = 0; i < nthreads * test_iter; i++) {
        std::string key = rdm.RandomStr();
        key = "ebc" + key + "ebc";
        Key *k = new Key();
        k->Init((char *)key.c_str(), key.size(), (char *)key.c_str(),
                key.size());
        if (key_set.count(key)) {
            i--;
        } else {
            key_set.insert(key);
            key_vec.push_back(key);
        }
    }
    std::sort(key_vec.begin(), key_vec.end());
    for (auto s : key_vec) {
        std::cout << s << std::endl;
    }
    auto *bar = new boost::barrier(nthreads);

    for (int i = 0; i < nthreads; i++) {
        tid[i] = new std::thread(
            [&](int id) {
                std::cout << "thread " << id << "\n";
                NVMMgr_ns::register_threadinfo();
                Key *str_key = new Key();
                Tree::OperationResults res;

                // insert read
                for (int j = 0; j < test_iter; j++) {
                    // insert
                    std::string kk = key_vec[j * nthreads + id];

                    str_key->Init((char *)kk.c_str(), kk.size(),
                                  (char *)kk.c_str(), kk.size());
                    res = art->insert(str_key);
                    ASSERT_EQ(res, Tree::OperationResults::Success);

                    Leaf *ret = art->lookup(str_key);
                    ASSERT_TRUE(ret) << "thread: " << id << std::endl
                                     << "j: " << j << std::endl
                                     << "inserted:\t" << kk << std::endl;
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
    //        art->graphviz_debug();
    std::cout << "passed test.....\n";

    delete art;
}