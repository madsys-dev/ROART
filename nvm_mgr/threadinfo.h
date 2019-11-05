#ifndef thread_info_h
#define thread_info_h

#include "N.h"
#include "N16.h"
#include "N256.h"
#include "N4.h"
#include "N48.h"
#include "pmalloc_wrap.h"
#include <list>

namespace NVMMgr_ns {
/*
 * Persistent leaf management
 * NOTE: PMFreeList is not thread-safe, only used for thread_info
 *
 * TODO: support leaf node recycling by add freed node to the free list.
 */
class PMFreeList {
    std::list<uint64_t> free_node_list;
    PMBlockAllocator *pmb;

  public:
    PMFreeList(PMBlockAllocator *pmb_);

    void *alloc_node(PART_ns::NTypes nt);
    void free_node(void *addr);

    int get_freelist_size() { return free_node_list.size(); }
};

struct thread_info {
    int id;
    volatile int _lock;
    struct thread_info *next;

    PMFreeList *node4_free_list;
    PMFreeList *node16_free_list;
    PMFreeList *node48_free_list;
    PMFreeList *node256_free_list;
    PMFreeList *leaf_free_list;

    char padding[8];
    char static_log[4032]; // 整个 thread_info的长度为 4096，所以剩下的内存
                           // 4096-64 = 4032 都可以用来做 static log。
} __attribute__((aligned(64)));

void register_threadinfo();
void unregister_threadinfo();

int get_thread_id();
void *get_threadinfo();

void *alloc_new_node(PART_ns::NTypes type);
void free_node(PART_ns::NTypes type, void *addr);

void *get_static_log();

} // namespace NVMMgr_ns
#endif
