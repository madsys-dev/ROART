#include <gtest/gtest.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <thread>
#include <vector>

#include "nvm_mgr.h"

using namespace NVMMgr_ns;

TEST(TestNVMMgr, nvm_mgr) {
    NVMMgr *mgr = new NVMMgr();
    int num = mgr->free_page_list.size();
    for(int i = 0; i < num; i++){
        // alloc some blocks in the free_page_list
        void * addr = mgr->alloc_block(1);
        ASSERT_NE(addr, NULL);
    }

    ASSERT_EQ(mgr->free_bit_offset, num);
    ASSERT_EQ((int)mgr->free_page_list.size(), 0);

    size_t start = NVMMgr::thread_local_start;
    for(int i = 0; i < 4;i++){
        void *addr = mgr->alloc_thread_info();
        ASSERT_EQ((size_t)addr, start + i * NVMMgr::PGSIZE);
    }

}