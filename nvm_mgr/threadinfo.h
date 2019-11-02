#ifndef thread_info_h
#define thread_info_h

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

    void* alloc_leaf();
    void free_leaf(void* leaf);
};

struct thread_info {
    int id;
    volatile int _lock;
    struct thread_info* next;
    PMFreeList* pm_free_list;
    char padding[8];
    char static_log[4072];  // 整个 thread_info的长度为 4096，所以剩下的内存
                            // 4096-32 = 4064 都可以用来做 static log。
} __attribute__((aligned(64)));

void register_threadinfo();
void unregister_threadinfo();

void* alloc_leaf();
void set_leaf_size(int);

void* static_leaf();

}  // namespace NVMMgr_ns
#endif
