#include <gtest/gtest.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <thread>
#include <vector>

#include "N.h"
#include "Tree.h"
#include "nvm_mgr.h"
#include "pmalloc_wrap.h"
#include "threadinfo.h"

using namespace NVMMgr_ns;



inline void clear_data() { system("rm -rf /mnt/pmem_pxf/part.data"); }



TEST(TestEpoch, Epoch_Mgr) {
    clear_data();
    std::cout << "[TEST]\ttest epoch_mgr\n";
    uint64_t e = Epoch_Mgr::GetGlobalEpoch();
    Epoch_Mgr *epoch_mgr = new Epoch_Mgr();
    ASSERT_EQ(Epoch_Mgr::GetGlobalEpoch(), e);
    epoch_mgr->IncreaseEpoch();
    epoch_mgr->IncreaseEpoch();
    epoch_mgr->IncreaseEpoch();
    ASSERT_EQ(Epoch_Mgr::GetGlobalEpoch(), e + 3);

    epoch_mgr->StartThread();
    e = Epoch_Mgr::GetGlobalEpoch();

    //    for (int i = 0; i < 2; i++) {
    //        uint64_t curr_e = Epoch_Mgr::GetGlobalEpoch();
    //        ASSERT_EQ(e, curr_e) << "not equal, e: " << e << ", curr_e: " <<
    //        curr_e;
    //
    //        std::chrono::milliseconds duration1(GC_INTERVAL);
    //        std::this_thread::sleep_for(duration1);
    //        e++;
    //    }
    //    std::cout << "[TEST]\ttest epoch increase successfully\n";

    delete epoch_mgr;
    std::chrono::milliseconds duration2(GC_INTERVAL);
    std::this_thread::sleep_for(duration2);

    e = Epoch_Mgr::GetGlobalEpoch();
    std::cout << "[TEST]\tdelete epoch_mgr and now epoch is " << e << "\n";

    std::chrono::milliseconds duration3(GC_INTERVAL * 10);
    std::this_thread::sleep_for(duration3);
    ASSERT_EQ(e, Epoch_Mgr::GetGlobalEpoch());
    std::cout << "[TEST]\ttest exit global epoch thread successfully\n";

    init_nvm_mgr();
    register_threadinfo();
    std::chrono::milliseconds duration4(GC_INTERVAL * 5);
    std::this_thread::sleep_for(duration4);
    std::cout << "[TEST]\trestart epoch mgr again, now epoch is "
              << Epoch_Mgr::GetGlobalEpoch() << "\n";
    ASSERT_GT(Epoch_Mgr::GetGlobalEpoch(), e);

    unregister_threadinfo();
    std::chrono::milliseconds duration1(GC_INTERVAL);
    std::this_thread::sleep_for(duration1);
    close_nvm_mgr();
}

TEST(TestEpoch, epoch_based_gc) {
    clear_data();
    std::cout << "[TEST]\ttest epoch_based_gc\n";

    init_nvm_mgr();
    register_threadinfo();

    // initialize different types node
    uint8_t *prefix = new uint8_t[4];
    uint32_t level = 0;
    uint32_t prefixlen = 4;
    memcpy(prefix, "abc", 3);

    PART_ns::N4 *n4 = new (alloc_new_node_from_type(PART_ns::NTypes::N4))
        PART_ns::N4(level, prefix, prefixlen);
    PART_ns::N16 *n16 = new (alloc_new_node_from_type(PART_ns::NTypes::N16))
        PART_ns::N16(level, prefix, prefixlen);
    PART_ns::N48 *n48 = new (alloc_new_node_from_type(PART_ns::NTypes::N48))
        PART_ns::N48(level, prefix, prefixlen);
    PART_ns::N256 *n256 = new (alloc_new_node_from_type(PART_ns::NTypes::N256))
        PART_ns::N256(level, prefix, prefixlen);
    PART_ns::Leaf *leaf =
        new (alloc_new_node_from_type(PART_ns::NTypes::Leaf)) PART_ns::Leaf();

    //    PART_ns::BaseNode *ll = (PART_ns::BaseNode *)leaf;
    //
    //    PART_ns::BaseNode *aa = (PART_ns::BaseNode *)((void *)leaf);
    //
    //    PART_ns::BaseNode *bb = reinterpret_cast<PART_ns::BaseNode *>(leaf);
    //
    //    printf("virtual different type %d %d %d %d\n", (int)(leaf->type),
    //           (int)(ll->type), (int)(aa->type), (int)(bb->type));

    std::cout << "[TEST]\talloc different nodes\n";

    NVMMgr *mgr = get_nvm_mgr();
    thread_info *ti = (thread_info *)get_threadinfo();
    ASSERT_EQ(mgr->meta_data->free_bit_offset, 1);
    printf("[TEST]\taddr: %x %x %x %x %x\n", n4, n16, n48, n256, leaf);
    int num[free_list_number];
    for (int i = 0; i < free_list_number; i++) {
        num[i] = ti->free_list->get_freelist_size(i);
        printf("[TEST]\t2^%d byte freelist size is %d\n", i, num[i]);
    }

    std::cout << "[TEST]\tcheck every node's address successfully\n";

    MarkNodeGarbage(n4); // delete at an active epoch
    MarkNodeGarbage(n16);
    MarkNodeGarbage(n48);
    MarkNodeGarbage(n256);
    MarkNodeGarbage(leaf);

    // GC for different types nodes
    ti->PerformGC();
    for (int i = 0; i < free_list_number; i++) {
        num[i] = ti->free_list->get_freelist_size(i);
        printf("[TEST]\t2^%d byte freelist size is %d\n", i, num[i]);
    }
    std::cout << "[TEST]\tperform GC finish\n";

    unregister_threadinfo();
    std::chrono::milliseconds duration1(GC_INTERVAL);
    std::this_thread::sleep_for(duration1);
    close_nvm_mgr();
}
