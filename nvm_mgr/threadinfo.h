#ifndef thread_info_h
#define thread_info_h

#include "Epoch.h"
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
} __attribute__((aligned(64)));

class thread_info {
  public:
    int id;
    volatile int _lock;
    struct thread_info *next;

    // node free list
    PMFreeList *node4_free_list;
    PMFreeList *node16_free_list;
    PMFreeList *node48_free_list;
    PMFreeList *node256_free_list;
    PMFreeList *leaf_free_list;

    // epoch based GC metadata
    GCMetaData *md;

    char static_log[4032]; // 整个 thread_info的长度为 4096，所以剩下的内存
                           // 4096-64 = 4032 都可以用来做 static log。
  public:
    // interface
    thread_info();
    ~thread_info();

    void *get_static_log() { return (void *)static_log; }
    int get_thread_id() { return id; }
    inline void JoinEpoch() { md->last_active_epoch = GetGlobalEpoch(); }

    inline void LeaveEpoch() {
        // This will make ie never be counted as active for GC
        md->last_active_epoch = static_cast<uint64_t>(-1);
    }

    /*
     * AddGarbageNode() - Adds a garbage node into the thread-local GC context
     *
     * Since the thread local GC context is only accessed by this thread, this
     * process does not require any atomicity
     *
     * This is always called by the thread owning thread local data, so we
     * do not have to worry about thread identity issues
     */

    void AddGarbageNode(void *node_p);

    /*
     * PerformGC() - This function performs GC on the current thread's garbage
     *               chain using the call back function
     *
     * Note that this function only collects for the current thread. Therefore
     * this function does not have to be atomic since its
     *
     * Note that this function should be used with thread_id, since it will
     * also be called inside the destructor - so we could not rely on
     * GetCurrentGCMetaData()
     */
    void PerformGC();
    void FreeEpochDeltaChain(void *node_p);

} __attribute__((aligned(64)));

void register_threadinfo();
void unregister_threadinfo();

void *get_threadinfo();

void JoinNewEpoch();
void LeaveThisEpoch();
void MarkNodeGarbage(void *node);
uint64_t SummarizeGCEpoch();

void *alloc_new_node(PART_ns::NTypes type);
void free_node(PART_ns::NTypes type, void *addr);

} // namespace NVMMgr_ns
#endif
