#include <gtest/gtest.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <thread>
#include <vector>

#include "N.h"
#include "nvm_mgr.h"
#include "threadinfo.h"
#include "pmalloc_wrap.h"

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

TEST(TestNVMMgr, PMBlockAllocator){
    std::cout << "[TEST]\tstart to test PMBlockAllocator\n";
    clear_data();

    NVMMgr *mgr = new NVMMgr();
    PMBlockAllocator *pmb = new PMBlockAllocator(mgr);

    int bl_num = 10;
    int type = 2;
    uint64_t start = NVMMgr::data_block_start;
    for(int i = 0; i < bl_num; i++){
        //alloc some blocks using interface of pmblockalloctor
        void *addr = pmb->alloc_block(type);
        ASSERT_EQ((uint64_t)addr, start + i * NVMMgr::PGSIZE);
    }
    std::cout<<"[TEST]\tpmb alloc block successfully\n";

    std::cout<<"[TEST]\tnvm_mgr's free page list size is "<<mgr->free_page_list.size() <<"\n";
    ASSERT_EQ((int)mgr->free_page_list.size(), 6);

    delete mgr;
}

TEST(TestNVMMgr, PMFreeList){
    std::cout << "[TEST]\tstart to test PMFreeList\n";
    clear_data();

    NVMMgr *mgr = new NVMMgr();
    PMBlockAllocator *pmb = new PMBlockAllocator(mgr);
    PMFreeList *pf = new PMFreeList(pmb);

    int bl_num = 10;
    PART_ns::NTypes type = PART_ns::NTypes::N4;
    size_t node_size = sizeof(PART_ns::N4);
    uint64_t start = NVMMgr::data_block_start;
    for(int i = 0; i < bl_num; i++){
        //alloc some blocks using interface of pmfreelist
        void *addr = pf->alloc_node(type);
        ASSERT_EQ((uint64_t)addr, start + i * node_size);
    }
    std::cout<<"[TEST]\tpmb alloc block successfully\n";

    std::cout<<"[TEST]\tnvm_mgr's free page list size is "<<mgr->free_page_list.size() <<"\n";
    ASSERT_EQ((int)mgr->free_page_list.size(), 6);

    delete mgr;
}