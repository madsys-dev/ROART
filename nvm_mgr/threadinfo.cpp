#include "threadinfo.h"
#include "nvm_mgr.h"
#include "pmalloc_wrap.h"
#include <assert.h>
#include <iostream>
#include <list>
#include <mutex>

namespace NVMMgr_ns {

// global block allocator
PMBlockAllocator *pmblock = NULL;

// global threadinfo lock to protect alloc thread info
std::mutex ti_lock;

// global threadinfo list hread
thread_info *ti_list_head = NULL;

// thread local info
__thread thread_info *ti = NULL;

// global thread id
int tid = 0;

// global Epoch_Mgr
Epoch_Mgr *epoch_mgr = NULL;

size_t get_node_size(PART_ns::NTypes type) {
    switch (type) {
    case PART_ns::NTypes::N4:
        return sizeof(PART_ns::N4);
    case PART_ns::NTypes::N16:
        return sizeof(PART_ns::N16);
    case PART_ns::NTypes::N48:
        return sizeof(PART_ns::N48);
    case PART_ns::NTypes::N256:
        return sizeof(PART_ns::N256);
    case PART_ns::NTypes::Leaf:
        return sizeof(PART_ns::Leaf);
    default:
        std::cout << "[ALLOC NODE]\twrong type\n";
        assert(0);
    }
}

size_t size_align(size_t s, int align) {
    return ((s + align - 1) / align) * align;
}

size_t convert_power_two(size_t s) {
    return ti->free_list->get_power_two_size(s);
}

/**************************PMFreeList interface*****************************/

// PMFreeList::PMFreeList(PMBlockAllocator *pmb_) : pmb(pmb_) {
//    free_node_list.clear();
//}

// void *PMFreeList::alloc_node(PART_ns::NTypes type) {
//
//    if (free_node_list.empty()) {
//        size_t node_size = size_align(get_node_size(type), 64);
//        //        std::cout << "[ALLOC NODE]\tnode type " << (int)type << ",
//        //        node size "
//        //                  << node_size << "\n";
//        void *addr = pmb->alloc_block((int)type);
//
//        for (int i = 0; i + node_size <= NVMMgr::PGSIZE; i += node_size) {
//            free_node_list.push_back((uint64_t)addr + i);
//        }
//    }
//    uint64_t pos = free_node_list.front();
//    free_node_list.pop_front();
//
//    return (void *)pos;
//}
//
// void PMFreeList::free_node(void *addr) {
//    free_node_list.push_back((uint64_t)addr);
//}

/*************************buddy_allocator interface**************************/

void buddy_allocator::insert_into_freelist(uint64_t addr, size_t size) {
    uint64_t curr_addr = addr;
    size_t curr_size = size;
    while (curr_size) {
        for (int curr_id = free_list_number - 1; curr_id >= 0; curr_id--) {
            if (curr_addr % power_two[curr_id] == 0 &&
                curr_size >= power_two[curr_id]) {
                free_list[curr_id].push(curr_addr);
                curr_addr += power_two[curr_id];
                curr_size -= power_two[curr_id];
                break;
            }
        }
    }
    assert(curr_size == 0);
}

uint64_t buddy_allocator::get_addr(int id) {
    uint64_t addr;
    if (id == free_list_number - 1) {
        if (!free_list[id].try_pop(addr)) {
            // empty, allocate block from nvm_mgr
            thread_info *ti = (thread_info *)get_threadinfo();
            addr = (uint64_t)pmb->alloc_block(ti->id);
            for (int i = power_two[id]; i < NVMMgr::PGSIZE;
                 i += power_two[id]) {
                free_list[id].push(addr + (uint64_t)i);
            }
        }
        return addr;
    }

    // pop successfully
    if (free_list[id].try_pop(addr)) {
        return addr;
    } else { // empty
        addr = get_addr(id + 1);
        // get a bigger page and split half into free_list
        free_list[id].push(addr + power_two[id]);
        return addr;
    }
}

// alloc size smaller than 4k
void *buddy_allocator::alloc_node(size_t size) {
    int id;
    for (int i = 0; i < free_list_number; i++) {
        if (power_two[i] >= size) {
            id = i;
            break;
        }
    }
    return (void *)get_addr(id);
}

size_t buddy_allocator::get_power_two_size(size_t s) {
    int id = free_list_number;
    for (int i = 0; i < free_list_number; i++) {
        if (power_two[i] >= s) {
            id = i;
            break;
        }
    }
    assert(id < free_list_number);
    return power_two[id];
}

/*************************thread_info interface**************************/

thread_info::thread_info() {
    //    node4_free_list = new PMFreeList(pmblock);
    //    node16_free_list = new PMFreeList(pmblock);
    //    node48_free_list = new PMFreeList(pmblock);
    //    node256_free_list = new PMFreeList(pmblock);
    //    leaf_free_list = new PMFreeList(pmblock);

    free_list = new buddy_allocator(pmblock);

    md = new GCMetaData();
    _lock = 0;
    id = tid++;
}

thread_info::~thread_info() {
    //    delete node4_free_list;
    //    delete node16_free_list;
    //    delete node48_free_list;
    //    delete node256_free_list;
    //    delete leaf_free_list;

    delete free_list;
    delete md;
}

void thread_info::AddGarbageNode(void *node_p) {
    GarbageNode *garbage_node_p =
        new GarbageNode(Epoch_Mgr::GetGlobalEpoch(), node_p);
    assert(garbage_node_p != nullptr);

    // Link this new node to the end of the linked list
    // and then update last_p
    md->last_p->next_p = garbage_node_p;
    md->last_p = garbage_node_p;
    //    PART_ns::BaseNode *n = (PART_ns::BaseNode *)node_p;
    //    std::cout << "[TEST]\tgarbage node type " << (int)(n->type) << "\n";
    // Update the counter
    md->node_count++;

    // It is possible that we could not free enough number of nodes to
    // make it less than this threshold
    // So it is important to let the epoch counter be constantly increased
    // to guarantee progress
    if (md->node_count > GC_NODE_COUNT_THREADHOLD) {
        // Use current thread's gc id to perform GC
        PerformGC();
    }

    return;
}

void thread_info::PerformGC() {
    // First of all get the minimum epoch of all active threads
    // This is the upper bound for deleted epoch in garbage node
    uint64_t min_epoch = SummarizeGCEpoch();

    // This is the pointer we use to perform GC
    // Note that we only fetch the metadata using the current thread-local id
    GarbageNode *header_p = &(md->header);
    GarbageNode *first_p = header_p->next_p;

    // Then traverse the linked list
    // Only reclaim memory when the deleted epoch < min epoch
    while (first_p != nullptr && first_p->delete_epoch < min_epoch) {
        // First unlink the current node from the linked list
        // This could set it to nullptr
        header_p->next_p = first_p->next_p;

        // Then free memory
        FreeEpochNode(first_p->node_p);

        delete first_p;
        assert(md->node_count != 0UL);
        md->node_count--;

        first_p = header_p->next_p;
    }

    // If we have freed all nodes in the linked list we should
    // reset last_p to the header
    if (first_p == nullptr) {
        md->last_p = header_p;
    }

    return;
}

void thread_info::FreeEpochNode(void *node_p) {
    PART_ns::BaseNode *n = reinterpret_cast<PART_ns::BaseNode *>(node_p);

    if (n->type == PART_ns::NTypes::Leaf) {
        // reclaim leaf key
        PART_ns::Leaf *leaf = (PART_ns::Leaf *)n;
        free_node_from_size((uint64_t)(leaf->fkey), leaf->key_len);
        free_node_from_size((uint64_t)(leaf->value), leaf->val_len);
    }

    // reclaim the node
    free_node_from_type((uint64_t)n, n->type);
}

void *alloc_new_node_from_type(PART_ns::NTypes type) {
    size_t node_size = size_align(get_node_size(type), 64);
    return ti->free_list->alloc_node(node_size);
}

void *alloc_new_node_from_size(size_t size) {
    return ti->free_list->alloc_node(size);
}

void free_node_from_type(uint64_t addr, PART_ns::NTypes type) {
    size_t node_size = size_align(get_node_size(type), 64);
    node_size = ti->free_list->get_power_two_size(node_size);
    ti->free_list->insert_into_freelist(addr, node_size);
}

void free_node_from_size(uint64_t addr, size_t size) {
    size_t node_size = ti->free_list->get_power_two_size(size);
    ti->free_list->insert_into_freelist(addr, node_size);
}

void register_threadinfo() {
    std::lock_guard<std::mutex> lock_guard(ti_lock);

    if (pmblock == NULL) {
        pmblock = new PMBlockAllocator(get_nvm_mgr());
        std::cout << "[THREAD]\tfirst new pmblock\n";
        //        std::cout<<"PPPPP meta data addr "<<
        //        get_nvm_mgr()->meta_data<<"\n";
    }
    if (epoch_mgr == NULL) {
        epoch_mgr = new Epoch_Mgr();

        // need to call function to create a new thread to increase epoch
        epoch_mgr->StartThread();
        std::cout << "[THREAD]\tfirst new epoch_mgr and add global epoch\n";
    }
    if (ti == NULL) {
        if (tid == NVMMgr::max_threads) {
            std::cout << "[THREAD]\tno available threadinfo to allocate\n";
            assert(0);
        }
        NVMMgr *mgr = get_nvm_mgr();
        //        std::cout<<"in thread get mgr meta data addr
        //        "<<mgr->meta_data<<"\n";

        ti = new (mgr->alloc_thread_info()) thread_info();
        ti->next = ti_list_head;
        ti_list_head = ti;

        // persist thread info
        flush_data((void *)ti, 128);
        std::cout << "[THREAD]\talloc thread info " << ti->id << "\n";
    }
}

void unregister_threadinfo() {
    std::lock_guard<std::mutex> lock_guard(ti_lock);
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
    std::cout << "[THREAD]\tunregister thread\n";
    //    delete ti;
    ti = NULL;
    if (ti_list_head == NULL) {
        // reset all, only use for gtest
        delete epoch_mgr;
        epoch_mgr = NULL;
        delete pmblock;
        pmblock = NULL;
        tid = 0;
    }
}

void *get_threadinfo() { return (void *)ti; }

void JoinNewEpoch() { ti->JoinEpoch(); }

void LeaveThisEpoch() { ti->LeaveEpoch(); }

void MarkNodeGarbage(void *node) { ti->AddGarbageNode(node); }

uint64_t SummarizeGCEpoch() {
    assert(ti_list_head);

    // Use the first metadata's epoch as min and update it on the fly
    thread_info *tmp = ti_list_head;
    uint64_t min_epoch = tmp->md->last_active_epoch;

    // This might not be executed if there is only one thread
    while (tmp->next) {
        tmp = tmp->next;
        min_epoch = std::min(min_epoch, tmp->md->last_active_epoch);
    }

    return min_epoch;
}

} // namespace NVMMgr_ns
