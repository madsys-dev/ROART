#include "lf-skiplist.h"
#include <gtest/gtest.h>

using namespace skiplist;

inline void clear_data() { system("rm -rf /mnt/pmem0/matianmao/skiplist.data"); }

TEST(TestSkipList, skiplist){
    std::cout<<"start test skiplist\n";
    clear_data();
    init_pmem();
    skiplist_t *sl = new_skiplist();
    std::cout<<"skiplist create\n";

    const int test_num = 1000000;

    for(int i = 0; i < test_num; i++){
        std::string key = std::to_string(i);

        const char *k = key.c_str();
        ASSERT_EQ(strlen(k), key.size());

        skiplist_insert(sl, (char *)key.c_str(), (char *)key.c_str());
        char *value = skiplist_find(sl, (char *)key.c_str());
        ASSERT_TRUE(value);
        ASSERT_EQ(strlen(value), key.size());
        ASSERT_EQ(memcmp(value, (char *)key.c_str(), strlen(value)), 0);
//        std::cout<<"insert "<<i<<"\n";
    }
}