//
// Created by 谢威宇 on 2021/4/1.
//
#include "N.h"
#include "Tree.h"

#include <atomic>
#include <gtest/gtest.h>
#include <iostream>
#include <string>
inline void clear_data() {
    system((std::string("rm -rf ") + nvm_dir + "part.data").c_str());
}

TEST(LeafArrayTest, bitmap_test) {
    PART_ns::LeafArray *n = new PART_ns::LeafArray(1, {});
    for (int i = 0; i < PART_ns::LeafArrayLength; i++) {
        auto x = n->getRightmostSetBit();
        //        std::cout << x << std::endl;
        ASSERT_EQ(i, x);
        n->setBit(x, false);
    }
    n->leaf[3].store(0);
    //    std::cout << n->getFingerPrint(3) << std::endl;
    ASSERT_EQ(n->getFingerPrint(3), (1 << 16) - 1);

    std::bitset<64> b = 11;
    //    std::cout << b << std::endl;
    auto i = b[0] ? 0 : 1;
    while (i < 64) {
        //        std::cout << i << std::endl;
        i = b._Find_next(i);
    }
}

TEST(LeafArrayTest, finger_print_test) { PART_ns::Key k(); }