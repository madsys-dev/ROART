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

    std::cout << "Int value of nullptr: "
              << reinterpret_cast<uintptr_t>(nullptr) << std::endl;

    int a = 432;
    auto pa = &a;
    std::cout << "The address of a(pa): " << std::hex
              << reinterpret_cast<uintptr_t>(pa)
              << ", deref it and get: " << std::dec << *pa << std::endl;
    pa = reinterpret_cast<int *>(reinterpret_cast<uintptr_t>(pa) ^
                                 (0xffffLL << 48));
    std::cout << "The modified address of a(pa): " << std::hex
              << reinterpret_cast<uintptr_t>(pa) << std::endl;
}

TEST(TreeTest, graph_viz) {
    clear_data();
    auto art = new PART_ns::Tree();
    auto *k = new PART_ns::Key();
    std::vector<std::string> keys = {"1",    "12",    "123",
                                     "1234", "12345", "123456"};
    for (auto &s : keys) {
        k->Init(const_cast<char *>(s.c_str()), s.length(), "123", 3);
        art->insert(k);
    }
    art->graphviz_debug();
    std::cout << "finish3 " << std::endl;
}

TEST(TreeTest, test_insert_and_lookup) {
    clear_data();
    auto art = new PART_ns::Tree();
    auto *k = new PART_ns::Key();
    std::vector<std::string> keys = {"111", "123", "211", "222"};

    for (auto &s : keys) {
        k->Init(const_cast<char *>(s.c_str()), s.length(),
                const_cast<char *>(s.c_str()), s.length());
        std::cout << s << std::endl;

        art->insert(k);
    }
    std::cout << "insertok" << std::endl;
    art->graphviz_debug();
    for (auto &s : keys) {
        k->Init(const_cast<char *>(s.c_str()), s.length(),
                const_cast<char *>(s.c_str()), s.length());
        auto l = art->lookup(k);

        std::cout << std::string(l->GetKey()) << std::endl;
        ASSERT_TRUE(l->checkKey(k));
    }

    std::cout << "finish2 " << std::endl;
}
