//#include <gtest/gtest.h>
//#include <iostream>
//#include <stdio.h>
//#include <stdlib.h>
//#include <thread>
//#include <vector>
//
//#include "N.h"
//#include "Tree.h"
//#include "nvm_mgr.h"
//#include "pmalloc_wrap.h"
//#include "threadinfo.h"
//
// using namespace NVMMgr_ns;
// inline void clear_data() { system("rm -rf /mnt/pmem_pxf/part.data"); }
//
// TEST(TestRecovery, recovery1) {
//    clear_data();
//    std::cout << "[TEST]\tstart to test recovery1\n";
//
//    init_nvm_mgr();
//    register_threadinfo();
//    NVMMgr *mgr = get_nvm_mgr();
//    thread_info *ti = (thread_info *)get_threadinfo();
//
//    int block_num = 3;
//    mgr->meta_data->free_bit_offset = block_num;
//    for (int i = 0; i < block_num; i++) {
//        mgr->meta_data->bitmap[i] = ti->id;
//    }
//    mgr->recovery_free_memory();
//    std::cout << "[TEST]\treclaim memory successfully\n";
//
//    std::cout << ti->free_list->get_freelist_size(free_list_number - 1) <<
//    "\n"; ASSERT_EQ(ti->free_list->get_freelist_size(free_list_number - 1),
//              NVMMgr::PGSIZE * block_num / 4096);
//
//    unregister_threadinfo();
//    close_nvm_mgr();
//}
//
// TEST(TestRecovery, recovery2) {
//    clear_data();
//    std::cout << "[TEST]\tstart to test recovery1\n";
//
//    init_nvm_mgr();
//    register_threadinfo();
//    NVMMgr *mgr = get_nvm_mgr();
//    thread_info *ti = (thread_info *)get_threadinfo();
//
//    int block_num = 1;
//    mgr->meta_data->free_bit_offset = block_num;
//    for (int i = 0; i < block_num; i++) {
//        mgr->meta_data->bitmap[i] = ti->id;
//    }
//    uint64_t start = NVMMgr::data_block_start;
//    mgr->recovery_set.insert(std::make_pair(start, 4096));
//    mgr->recovery_set.insert(std::make_pair(start + 4096, 8));
//    mgr->recovery_set.insert(std::make_pair(start + 4096 + 16, 8));
//    mgr->recovery_set.insert(std::make_pair(start + 4096 + 64, 16));
//
//    mgr->recovery_free_memory();
//    int num[free_list_number] = {2, 1, 2, 0, 1, 1, 1, 1, 1, 62};
//    for (int i = 0; i < free_list_number; i++) {
//        ASSERT_EQ(ti->free_list->get_freelist_size(i), num[i]);
//        std::cout << ti->free_list->get_freelist_size(i) << "\n";
//    }
//
//    unregister_threadinfo();
//    close_nvm_mgr();
//}
