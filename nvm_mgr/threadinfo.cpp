#include "threadinfo.h"
#include <assert.h>
#include "pmalloc_wrap.h"
#include <iostream>
#include <list>
#include <mutex>
#include "nvm_mgr.h"

namespace NVMMgr_ns {

    PMBlockAllocator *pmblock = NULL;

#define BLK_SIZE (1024)
    int leaf_size;

    __thread thread_info *ti = NULL;
    std::mutex ti_lock;
    thread_info *ti_list_head = NULL;

    PMFreeList::PMFreeList() {
        list_cursor = 0;
        block_cursor = BLK_SIZE;
        list_capacity = BLK_SIZE;

        pm_free_list = (void **) aligned_alloc(64, sizeof(void *) * BLK_SIZE);
        assert(pm_free_list);
    }

    void *PMFreeList::alloc_leaf() {
        if (block_cursor == BLK_SIZE) {
            pm_block = pmblock->alloc_block(BLK_SIZE * leaf_size);
            assert(pm_block != NULL);
            block_cursor = 0;
        }
        assert(block_cursor < BLK_SIZE);
        return (void *) ((size_t) pm_block + (block_cursor++) * leaf_size);
    }

    void PMFreeList::free_leaf(void *leaf) {
        // TODO: free leaf
        assert(0);
    }

    int id;

    void set_leaf_size(int size) { leaf_size = size; }

    void *alloc_leaf() { return ti->pm_free_list->alloc_leaf(); }

    void *static_leaf() { return ti->static_log; }

    void register_threadinfo() {
        ti_lock.lock();
        if (pmblock == NULL) {
            pmblock = new PMBlockAllocator();
        }
        if (ti == NULL) {
            NVMMgr *mgr = get_nvm_mgr();
            // ti =  new thread_info();
            ti = (thread_info *) mgr->alloc_thread_info();
            // printf("[THREAD INFO]\tti %p\n", ti);
            ti->pm_free_list = new PMFreeList;
            ti->next = ti_list_head;
            ti->_lock = 0;
            ti_list_head = ti;
            ti->id = id++;
        }
        ti_lock.unlock();
    }

    void unregister_threadinfo() {
        ti_lock.lock();
        thread_info *cti = ti_list_head;
        if (cti == ti) {
            ti_list_head = cti->next;
        } else {
            thread_info *next = cti->next;
            while (true) {
                assert(next);
                if (next == ti) {
                    cti->next = next->next;
                    break;
                }
                cti = next;
                next = next->next;
            }
        }
        if (ti_list_head == NULL) {
            // last one leave
            close_nvm_mgr();
        }
        ti = NULL;
        ti_lock.unlock();
    }

}  // namespace NVMMgr_ns
