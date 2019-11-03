#ifndef thread_info_h
#define thread_info_h

#include "N.h"

namespace NVMMgr_ns {
/*
 * Persistent leaf management
 * NOTE: PMFreeList is not thread-safe, only used for thread_info
 *
 * TODO: support leaf node recycling by add freed node to the free list.
 */
class PMFreeList {
    void** pm_free_list;
    void* pm_block;
    int list_cursor;
    int list_capacity;
    int block_cursor;

   public:
    PMFreeList();

    void* alloc_node(PART_ns::NTypes nt);
    void free_node(void* n);
};

struct thread_info {
    int id;
    volatile int _lock;
    struct thread_info* next;

    PMFreeList* node4_free_list;
    PMFreeList* node16_free_list;
    PMFreeList* node48_free_list;
    PMFreeList* node256_free_list;
    PMFreeList* leaf_free_list;

    char padding[8];
    char static_log[4032];  // 整个 thread_info的长度为 4096，所以剩下的内存
                            // 4096-64 = 4032 都可以用来做 static log。
} __attribute__((aligned(64)));

void register_threadinfo();
void unregister_threadinfo();

void* alloc_leaf();
void set_leaf_size(int);

void* static_leaf();

}  // namespace NVMMgr_ns
#endif
