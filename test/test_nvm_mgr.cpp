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

inline void clear_data() { system("rm -rf /mnt/pmem_pxf/part.data"); }

TEST(TestNVMMgr, nvm_mgr) {
    std::cout << "[TEST]\tstart to test nvm_mgr\n";
    clear_data();

    NVMMgr *mgr = new NVMMgr();

    ASSERT_EQ(mgr->meta_data->free_bit_offset, 0);

    std::cout << "[TEST]\talloc block successfully\n";

    uint64_t start = NVMMgr::thread_local_start;
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
    std::cout << "[TEST]\tcheck meta data successfully\n";

    delete mgr;
}

TEST(TestNVMMgr, PMBlockAllocator) {
    std::cout << "[TEST]\tstart to test PMBlockAllocator\n";
    clear_data();

    init_nvm_mgr();
    PMBlockAllocator *pmb = new PMBlockAllocator(get_nvm_mgr());
    register_threadinfo();

    int bl_num = 10;
    int tid = 2;
    uint64_t start = NVMMgr::data_block_start;
    NVMMgr *mgr = get_nvm_mgr();
    for (int i = 0; i < bl_num; i++) {
        // alloc some blocks using interface of pmblockalloctor
        void *addr = pmb->alloc_block(tid);
        ASSERT_EQ((uint64_t)addr, start + i * NVMMgr::PGSIZE);
        ASSERT_EQ(mgr->meta_data->free_bit_offset, i + 1);
        ASSERT_EQ(mgr->meta_data->bitmap[i], tid);
    }
    std::cout << "[TEST]\tpmb alloc block successfully\n";

    unregister_threadinfo();
    delete pmb;
    close_nvm_mgr();
}

// TEST(TestNVMMgr, PMFreeList) {
//    std::cout << "[TEST]\tstart to test PMFreeList\n";
//    clear_data();
//
//    init_nvm_mgr();
//    PMBlockAllocator *pmb = new PMBlockAllocator(get_nvm_mgr());
//    PMFreeList *pf = new PMFreeList(pmb);
//
//    register_threadinfo();
//
//    int node_num = 10;
//    PART_ns::NTypes type = PART_ns::NTypes::N4;
//    size_t node_size = sizeof(PART_ns::N4);
//    uint64_t start = NVMMgr::data_block_start;
//    for (int i = 0; i < node_num; i++) {
//        // alloc some node from a block using interface of pmfreelist
//        void *addr = pf->alloc_node(type);
//        ASSERT_EQ((uint64_t)addr, start + i * node_size);
//    }
//    std::cout << "[TEST]\tpf alloc node successfully\n";
//    std::cout << "[TEST]\tnode size is " << node_size << ", freelist size is "
//              << pf->get_freelist_size() << "\n";
//    ASSERT_EQ(pf->get_freelist_size(), NVMMgr::PGSIZE / node_size - node_num);
//
//    unregister_threadinfo();
//    delete pf;
//    delete pmb;
//    close_nvm_mgr();
//}

TEST(TestNVMMgr, buddy_allocator) {
    std::cout << "[TEST]\tstart to test buddy allocator\n";
    clear_data();

    init_nvm_mgr();
    NVMMgr *mgr = get_nvm_mgr();
    PMBlockAllocator *pmb = new PMBlockAllocator(get_nvm_mgr());
    buddy_allocator *ba = new buddy_allocator(pmb);
    register_threadinfo();
    thread_info *ti = (thread_info *)get_threadinfo();

    void *addr = ba->alloc_node(4096);
    uint64_t start = NVMMgr::data_block_start;
    ASSERT_EQ(ba->get_freelist_size(free_list_number - 1),
              (NVMMgr::PGSIZE / 4096) - 1);
    ASSERT_EQ((uint64_t)addr, start);
    ASSERT_EQ(mgr->meta_data->free_bit_offset, 1);
    ASSERT_EQ(mgr->meta_data->bitmap[0], ti->id);

    std::cout << "[TEST]\tstart to test reclaim\n";
    ba->insert_into_freelist(0, 64 + 32);
    ASSERT_EQ(ba->get_freelist_size(2), 1);
    ASSERT_EQ(ba->get_freelist_size(3), 1);
    ba->insert_into_freelist(0, 1024 + 256 + 128 + 32 + 8);
    ASSERT_EQ(ba->get_freelist_size(7), 1);
    ASSERT_EQ(ba->get_freelist_size(5), 1);
    ASSERT_EQ(ba->get_freelist_size(4), 1);
    ASSERT_EQ(ba->get_freelist_size(2), 2);
    ASSERT_EQ(ba->get_freelist_size(0), 1);
    ba->insert_into_freelist(0, 256 + 256 + 64 + 16);
    ASSERT_EQ(ba->get_freelist_size(6), 1);
    ASSERT_EQ(ba->get_freelist_size(3), 2);
    ASSERT_EQ(ba->get_freelist_size(1), 1);
    std::cout << "[TEST]\ttest reclaim successfully\n";

    unregister_threadinfo();
    delete ba;
    delete pmb;
    close_nvm_mgr();
}

TEST(TestNVMMgr, thread_info) {
    std::cout << "[TEST]\tstart to test thread_info\n";
    clear_data();

    // initialize a global nvm_mgr
    init_nvm_mgr();

    // initialize a thread and global pmblockallocator
    register_threadinfo();
    NVMMgr *mgr = get_nvm_mgr();
    thread_info *ti = (thread_info *)get_threadinfo();

    //    ASSERT_EQ(get_thread_id(), 0);
    void *n4 = alloc_new_node_from_type(PART_ns::NTypes::N4);
    void *n16 = alloc_new_node_from_type(PART_ns::NTypes::N16);
    void *n48 = alloc_new_node_from_type(PART_ns::NTypes::N48);
    void *n256 = alloc_new_node_from_type(PART_ns::NTypes::N256);
    void *leaf = alloc_new_node_from_type(PART_ns::NTypes::Leaf);

    std::cout << "[TEST]\talloc different nodes\n";

    uint64_t start = NVMMgr::data_block_start;

    for (int i = 0; i < free_list_number; i++) {
        std::cout << ti->free_list->get_freelist_size(i) << "\n";
    }

    ASSERT_EQ(ti->free_list->get_freelist_size(free_list_number - 1),
              (NVMMgr::PGSIZE / 4096) - 2);
    ASSERT_EQ((uint64_t)n4, start);
    //    std::cout<<"n4 addr "<<(uint64_t)n4<<"\n";
    //    std::cout<<"meta data addr "<< (uint64_t)(mgr->meta_data)<<"\n";
    //    std::cout<<"mgr addr" <<(uint64_t)mgr<<"\n";
    ASSERT_EQ(mgr->meta_data->free_bit_offset, 1);
    ASSERT_EQ(mgr->meta_data->bitmap[0], ti->id);

    int currnum = 0;
    for (int i = 0; i < free_list_number; i++) {
        currnum += ti->free_list->get_freelist_size(i);
    }

    std::cout << "[TEST]\tcheck every node's address successfully\n";

    free_node_from_type((uint64_t)n4, PART_ns::NTypes::N4);
    free_node_from_type((uint64_t)n16, PART_ns::NTypes::N16);
    free_node_from_type((uint64_t)n48, PART_ns::NTypes::N48);
    free_node_from_type((uint64_t)n256, PART_ns::NTypes::N256);
    free_node_from_type((uint64_t)leaf, PART_ns::NTypes::Leaf);

    std::cout << "[TEST]\tfree nodes successfully\n";

    int nownum = 0;
    for (int i = 0; i < free_list_number; i++) {
        nownum += ti->free_list->get_freelist_size(i);
    }
    ASSERT_EQ(nownum, currnum + 5);

    std::cout << "[TEST]\tfreelist's size correct\n";

    unregister_threadinfo();
    close_nvm_mgr();
}