//#include "lf-skiplist-alloc.h"
//#include <gtest/gtest.h>
//#include <thread>
//
// using namespace skiplist;
//
// inline void clear_data() {
//    system("rm -rf /mnt/pmem_pxf/skiplist.data");
//}
//
// TEST(TestSkipList, skiplist) {
//    std::cout << "start test skiplist\n";
//    clear_data();
//    init_pmem();
//    skiplist_t *sl = new_skiplist();
//    std::cout << "skiplist create\n";
//
//    const int test_num = 1000000;
//    const int threads_num = 4;
//    const int num_per_thread = test_num / threads_num;
//
//    for (int i = 0; i < test_num; i++) {
//        std::string key = std::to_string(i);
//
//        const char *k = key.c_str();
//        ASSERT_EQ(strlen(k), key.size());
//
//        skiplist_insert(sl, (char *)key.c_str(), (char *)key.c_str());
//        char *value = skiplist_find(sl, (char *)key.c_str());
//        ASSERT_TRUE(value);
//        ASSERT_EQ(strlen(value), key.size());
//        ASSERT_EQ(memcmp(value, (char *)key.c_str(), strlen(value)), 0);
//        //        std::cout<<"insert "<<i<<"\n";
//    }
//
//    std::cout << "init insert finish\n";
//
//    std::thread *tid[threads_num];
//    for (int i = 0; i < threads_num; i++) {
//        tid[i] = new std::thread(
//            [&](int id) {
//                register_thread();
//
//                // find
//                for (int j = 0; j < num_per_thread; j++) {
//                    std::string key = std::to_string(j * threads_num + id);
//
//                    char *value = skiplist_find(sl, (char *)key.c_str());
//                    ASSERT_TRUE(value);
//                    ASSERT_EQ(strlen(value), key.size());
//                    //                std::cout<<strlen(value)<<"
//                    //                "<<value<<"\n";
//                    ASSERT_EQ(memcmp(value, (char *)key.c_str(),
//                    strlen(value)),
//                              0);
//
//                    char *delete_val = skiplist_remove(sl, (char
//                    *)key.c_str()); ASSERT_TRUE(delete_val);
//                    ASSERT_EQ(strlen(delete_val), strlen(value));
//                    ASSERT_EQ(memcmp(delete_val, value, strlen(value)), 0);
//
//                    value = skiplist_find(sl, (char *)key.c_str());
//                    ASSERT_FALSE(value);
//                }
//
//                std::cout << "thread " << id << " read delete read finish\n";
//
//                for (int j = 0; j < num_per_thread; j++) {
//                    std::string key = std::to_string(j * threads_num + id);
//                    skiplist_insert(sl, (char *)key.c_str(),
//                                    (char *)key.c_str());
//                    char *value = skiplist_find(sl, (char *)key.c_str());
//                    ASSERT_TRUE(value);
//                    ASSERT_EQ(strlen(value), key.size());
//                    ASSERT_EQ(memcmp(value, (char *)key.c_str(),
//                    strlen(value)),
//                              0);
//
//                    std::string newval = "0" + key + "0";
//                    skiplist_update(sl, (char *)key.c_str(),
//                                    (char *)newval.c_str());
//                    char *newvalue = skiplist_find(sl, (char *)key.c_str());
//                    ASSERT_TRUE(newvalue);
//                    ASSERT_EQ(strlen(newvalue), newval.size());
//                    ASSERT_EQ(strlen(newvalue), strlen(value) + 2);
//                    ASSERT_EQ(memcmp(newvalue + 1, value, strlen(value)), 0);
//                    std::string s1(newvalue);
//                    std::string s2(value);
//                    s2 = "0" + s2 + "0";
//                    ASSERT_EQ(s1, s2);
//                }
//
//                std::cout << "thread " << id
//                          << " insert read update read check\n";
//            },
//            i);
//    }
//
//    for (int i = 0; i < threads_num; i++) {
//        tid[i]->join();
//    }
//}