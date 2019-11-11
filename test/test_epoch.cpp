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

inline void clear_data() { system("rm -rf /mnt/pmem0/matianmao/part.data"); }

TEST(TestEpoch, Epoch_Mgr) {
    clear_data();
    std::cout << "[TEST]\ttest epoch_mgr\n";

    Epoch_Mgr *epoch_mgr = new Epoch_Mgr();
    ASSERT_EQ(Epoch_Mgr::GetGlobalEpoch(), 0);
    epoch_mgr->IncreaseEpoch();
    epoch_mgr->IncreaseEpoch();
    epoch_mgr->IncreaseEpoch();
    ASSERT_EQ(Epoch_Mgr::GetGlobalEpoch(), 3);

    epoch_mgr->StartThread();
    uint64_t e = Epoch_Mgr::GetGlobalEpoch();

    for (int i = 0; i < 2; i++) {
        uint64_t curr_e = Epoch_Mgr::GetGlobalEpoch();
        ASSERT_EQ(e, curr_e) << "not equal, e: " << e << ", curr_e: " << curr_e;

        std::chrono::milliseconds duration1(GC_INTERVAL);
        std::this_thread::sleep_for(duration1);
        e++;
    }
    std::cout << "[TEST]\ttest epoch increase successfully\n";

    delete epoch_mgr;
    std::chrono::milliseconds duration2(GC_INTERVAL);
    std::this_thread::sleep_for(duration2);

    e = Epoch_Mgr::GetGlobalEpoch();
    std::cout << "[TEST]\tdelete epoch_mgr and now epoch is " << e << "\n";

    std::chrono::milliseconds duration3(GC_INTERVAL * 10);
    std::this_thread::sleep_for(duration3);
    ASSERT_EQ(e, Epoch_Mgr::GetGlobalEpoch());
    std::cout << "[TEST]\ttest exit global epoch thread successfully\n";
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

    PART_ns::N4 *n4 = new (alloc_new_node(PART_ns::NTypes::N4))
        PART_ns::N4(level, prefix, prefixlen);
    PART_ns::N16 *n16 = new (alloc_new_node(PART_ns::NTypes::N16))
        PART_ns::N16(level, prefix, prefixlen);
    PART_ns::N48 *n48 = new (alloc_new_node(PART_ns::NTypes::N48))
        PART_ns::N48(level, prefix, prefixlen);
    PART_ns::N256 *n256 = new (alloc_new_node(PART_ns::NTypes::N256))
        PART_ns::N256(level, prefix, prefixlen);
    PART_ns::Leaf *leaf =
        new (alloc_new_node(PART_ns::NTypes::Leaf)) PART_ns::Leaf();

    PART_ns::BaseNode *ll = (PART_ns::BaseNode *)leaf;

    PART_ns::BaseNode *aa = (PART_ns::BaseNode *)((void *)leaf);

    PART_ns::BaseNode *bb = reinterpret_cast<PART_ns::BaseNode *>(leaf);

    printf("virtual different type %d %d %d %d\n", (int)(leaf->type),
           (int)(ll->type), (int)(aa->type), (int)(bb->type));

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

    thread_info *ti = (thread_info *)get_threadinfo();
    ASSERT_EQ(ti->node4_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / size_align(sizeof(PART_ns::N4), 64) - 1);
    ASSERT_EQ(ti->node16_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / size_align(sizeof(PART_ns::N16), 64) - 1);
    ASSERT_EQ(ti->node48_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / size_align(sizeof(PART_ns::N48), 64) - 1);
    ASSERT_EQ(ti->node256_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / size_align(sizeof(PART_ns::N256), 64) - 1);
    ASSERT_EQ(ti->leaf_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / size_align(sizeof(PART_ns::Leaf), 64) - 1);

    std::cout << "[TEST]\tcheck every freelist's size successfully\n";

    MarkNodeGarbage(n4); // delete at an active epoch
    MarkNodeGarbage(n16);
    MarkNodeGarbage(n48);
    MarkNodeGarbage(n256);
    MarkNodeGarbage(leaf);

    // GC for different types nodes
    ti->PerformGC();
    std::cout << "[TEST]\tperform GC finish\n";

    ASSERT_EQ(ti->node4_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / size_align(sizeof(PART_ns::N4), 64));
    ASSERT_EQ(ti->node16_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / size_align(sizeof(PART_ns::N16), 64));
    ASSERT_EQ(ti->node48_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / size_align(sizeof(PART_ns::N48), 64));
    ASSERT_EQ(ti->node256_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / size_align(sizeof(PART_ns::N256), 64));
    ASSERT_EQ(ti->leaf_free_list->get_freelist_size(),
              NVMMgr::PGSIZE / size_align(sizeof(PART_ns::Leaf), 64));

    std::cout << "[TEST]\tcheck GC successfully\n";

    unregister_threadinfo();
    close_nvm_mgr();
}
