#include <gtest/gtest.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <thread>
#include <vector>

#include "N.h"
#include "nvm_mgr.h"
#include "pmalloc_wrap.h"
#include "threadinfo.h"

using namespace NVMMgr_ns;

inline void clear_data() { system("rm -rf /mnt/dax/matianmao/part.data"); }

TEST(TestNVMMgr, nvm_mgr) {
    std::cout << "[TEST]\tstart to test nvm_mgr\n";
    clear_data();

    NVMMgr *mgr = new NVMMgr();
    int bl_num = mgr->free_page_list.size();
    uint64_t start = NVMMgr::data_block_start;
    int type = 1;
    for (int i = 0; i < bl_num; i++) {
        // alloc some blocks in the free_page_list
        void *addr = mgr->alloc_block(type);
        ASSERT_NE((uint64_t)addr, (uint64_t)NULL);
        ASSERT_EQ((uint64_t)addr, start + i * NVMMgr::PGSIZE);
    }

    ASSERT_EQ(mgr->free_bit_offset, bl_num);
    ASSERT_EQ((int)mgr->free_page_list.size(), 0);

    std::cout << "[TEST]\talloc block successfully\n";

    start = NVMMgr::thread_local_start;
    int thread_num = 4;
    for (int i = 0; i < thread_num; i++) {
        void *addr = mgr->alloc_thread_info();
        ASSERT_EQ((size_t)addr, start + i * NVMMgr::PGSIZE);
    }

    std::cout << "[TEST]\talloc thread info successfully\n";

    // check the meta_data
    NVMMgr::Head *head =
        reinterpret_cast<NVMMgr::Head *>(mgr->alloc_tree_root());
    ASSERT_EQ(head->threads, thread_num);
    for (int i = 0; i < bl_num; i++) {
        ASSERT_EQ(head->bitmap[i], type);
    }
    std::cout << "[TEST]\tcheck meta data successfully\n";

    delete mgr;
}

TEST(TestNVMMgr, PMBlockAllocator) {
    std::cout << "[TEST]\tstart to test PMBlockAllocator\n";
    clear_data();

    NVMMgr *mgr = new NVMMgr();
    PMBlockAllocator *pmb = new PMBlockAllocator(mgr);

    int bl_num = 10;
    int type = 2;
    uint64_t start = NVMMgr::data_block_start;
    for (int i = 0; i < bl_num; i++) {
        // alloc some blocks using interface of pmblockalloctor
        void *addr = pmb->alloc_block(type);
        ASSERT_EQ((uint64_t)addr, start + i * NVMMgr::PGSIZE);
    }
    std::cout << "[TEST]\tpmb alloc block successfully\n";

    std::cout << "[TEST]\tnvm_mgr's free page list size is "
              << mgr->free_page_list.size() << "\n";
    ASSERT_EQ((int)mgr->free_page_list.size(), 6);

    delete mgr;
}

TEST(TestNVMMgr, PMFreeList) {
    std::cout << "[TEST]\tstart to test PMFreeList\n";
    clear_data();

    NVMMgr *mgr = new NVMMgr();
    PMBlockAllocator *pmb = new PMBlockAllocator(mgr);
    PMFreeList *pf = new PMFreeList(pmb);

    int node_num = 10;
    PART_ns::NTypes type = PART_ns::NTypes::N4;
    size_t node_size = sizeof(PART_ns::N4);
    uint64_t start = NVMMgr::data_block_start;
    for (int i = 0; i < node_num; i++) {
        // alloc some node from a block using interface of pmfreelist
        void *addr = pf->alloc_node(type);
        ASSERT_EQ((uint64_t)addr, start + i * node_size);
    }
    std::cout << "[TEST]\tpf alloc node successfully\n";
    std::cout << "[TEST]\tnode size is " << node_size << ", freelist size is "
              << pf->get_freelist_size() << "\n";
    ASSERT_EQ(pf->get_freelist_size(), NVMMgr::PGSIZE / node_size - node_num);

    std::cout << "[TEST]\tnvm_mgr's free page list size is "
              << mgr->free_page_list.size() << "\n";
    ASSERT_EQ((int)mgr->free_page_list.size(), 7);

    delete mgr;
}

TEST(TestNVMMgr, thread_info) {
    std::cout << "[TEST]\tstart to test PMFreeList\n";
    clear_data();

    // initialize a global nvm_mgr
    init_nvm_mgr();

    // initialize a thread and global pmblockallocator
    register_threadinfo();

    //    ASSERT_EQ(get_thread_id(), 0);
    void *n4 = alloc_new_node(PART_ns::NTypes::N4);
    void *n16 = alloc_new_node(PART_ns::NTypes::N16);
    void *n48 = alloc_new_node(PART_ns::NTypes::N48);
    void *n256 = alloc_new_node(PART_ns::NTypes::N256);
    void *leaf = alloc_new_node(PART_ns::NTypes::Leaf);

    std::cout << "[TEST]\talloc different nodes\n";

    NVMMgr *mgr = get_nvm_mgr();
    uint64_t start = NVMMgr::data_block_start;
    ASSERT_EQ((int)mgr->free_page_list.size(), 3);
    ASSERT_EQ((uint64_t)n4, start);
    ASSERT_EQ((uint64_t)n16, start + 1 * NVMMgr::PGSIZE);
    ASSERT_EQ((uint64_t)n48, start + 2 * NVMMgr::PGSIZE);
    ASSERT_EQ((uint64_t)n256, start + 3 * NVMMgr::PGSIZE);
    ASSERT_EQ((uint64_t)leaf, start + 4 * NVMMgr::PGSIZE);

    std::cout << "[TEST]\tcheck every node's address successfully\n";

    thread_info *ti = reinterpret_cast<thread_info *>(get_threadinfo());
    ASSERT_EQ(ti->node4_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / sizeof(PART_ns::N4) - 1);
    ASSERT_EQ(ti->node16_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / sizeof(PART_ns::N16) - 1);
    ASSERT_EQ(ti->node48_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / sizeof(PART_ns::N48) - 1);
    ASSERT_EQ(ti->node256_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / sizeof(PART_ns::N256) - 1);
    ASSERT_EQ(ti->leaf_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / sizeof(PART_ns::Leaf) - 1);

    std::cout << "[TEST]\tcheck every freelist's size successfully\n";

    free_node(PART_ns::NTypes::N4, n4);
    free_node(PART_ns::NTypes::N16, n16);
    free_node(PART_ns::NTypes::N48, n48);
    free_node(PART_ns::NTypes::N256, n256);
    free_node(PART_ns::NTypes::Leaf, leaf);

    std::cout << "[TEST]\tfree nodes successfully\n";

    ASSERT_EQ(ti->node4_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / sizeof(PART_ns::N4));
    ASSERT_EQ(ti->node16_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / sizeof(PART_ns::N16));
    ASSERT_EQ(ti->node48_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / sizeof(PART_ns::N48));
    ASSERT_EQ(ti->node256_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / sizeof(PART_ns::N256));
    ASSERT_EQ(ti->leaf_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / sizeof(PART_ns::Leaf));

    std::cout << "[TEST]]\tfreelist's size correct\n";

    unregister_threadinfo();
    close_nvm_mgr();
}