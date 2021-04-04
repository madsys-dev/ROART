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

TEST(BasicTest, test_z) {
    std::cout << static_cast<int>('\0') << std::endl;
    std::cout << sizeof(size_t) << std::endl;
    std::cout << "Size of Leaf: " << sizeof(PART_ns::Leaf) << std::endl;
    std::cout << "Size of N: " << sizeof(PART_ns::N) << std::endl;
    std::cout << "Size of N4: " << sizeof(PART_ns::N4) << std::endl;
    std::cout << "Size of N16: " << sizeof(PART_ns::N16) << std::endl;
    std::cout << "Size of N48: " << sizeof(PART_ns::N48) << std::endl;
    std::cout << "Size of N256: " << sizeof(PART_ns::N256) << std::endl;
    std::cout << "Size of std::atomic<uint64_t>: "
              << sizeof(std::atomic<uint64_t>) << std::endl;
    std::cout << "Size of std::atomic<std::bitset<64>: "
              << sizeof(std::atomic<std::bitset<64>>) << std::endl;
    std::cout
        << "Size of "
           "std::pair<std::atomic<uint16_t>,std::atomic<std::bitset<48>>>: "
        << sizeof(
               std::pair<std::atomic<uint16_t>, std::atomic<std::bitset<48>>>)
        << std::endl;
}

TEST(TreeTest, tree_build) {
    //    clear_data();
    auto art = new PART_ns::Tree();
    //    std::cout << "finish" << std::endl;
}
