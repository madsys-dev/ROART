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

PMFreeList::PMFreeList(PMBlockAllocator *pmb_) : pmb(pmb_) {
    free_node_list.clear();
}

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

void *PMFreeList::alloc_node(PART_ns::NTypes type) {
    // TODO: according to nt, alloc different node
    if (free_node_list.empty()) {
        size_t node_size = size_align(get_node_size(type), 64);
        std::cout << "[ALLOC NODE]\tnode type " << (int)type << ", node size "
                  << node_size << "\n";
        void *addr = pmb->alloc_block((int)type);
        for (int i = 0; i + node_size <= NVMMgr::PGSIZE; i += node_size) {
            free_node_list.push_back((uint64_t)addr + i);
        }
    }
    uint64_t pos = free_node_list.front();
    free_node_list.pop_front();

    return (void *)pos;
}

void PMFreeList::free_node(void *addr) {
    free_node_list.push_back((uint64_t)addr);
}

thread_info::thread_info() {
    node4_free_list = new PMFreeList(pmblock);
    node16_free_list = new PMFreeList(pmblock);
    node48_free_list = new PMFreeList(pmblock);
    node256_free_list = new PMFreeList(pmblock);
    leaf_free_list = new PMFreeList(pmblock);

    md = new GCMetaData();
    _lock = 0;
    id = tid++;
}

thread_info::~thread_info() {
    delete node4_free_list;
    delete node16_free_list;
    delete node48_free_list;
    delete node256_free_list;
    delete leaf_free_list;
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
        FreeEpochDeltaChain(first_p->node_p);

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

void thread_info::FreeEpochDeltaChain(void *node_p) {
    // TODO: free node to the allocation thread's free list
    // TODO: free whole block
    PART_ns::BaseNode *n = reinterpret_cast<PART_ns::BaseNode *>(node_p);
    switch (n->type) {
    case PART_ns::NTypes::N4:
        ti->node4_free_list->free_node(node_p);
        break;
    case PART_ns::NTypes::N16:
        ti->node16_free_list->free_node(node_p);
        break;
    case PART_ns::NTypes::N48:
        ti->node48_free_list->free_node(node_p);
        break;
    case PART_ns::NTypes::N256:
        ti->node256_free_list->free_node(node_p);
        break;
    case PART_ns::NTypes::Leaf:
        ti->leaf_free_list->free_node(node_p);
        break;
    default:
//        std::cout << "[TEST]\tnode type is " << (int)n->type << "\n";
        std::cout << "[FREE GC NODE]\twrong type\n";
        assert(0);
    }
}

void *alloc_new_node(PART_ns::NTypes type) {
    switch (type) {
    case PART_ns::NTypes::N4:
        return ti->node4_free_list->alloc_node(PART_ns::NTypes::N4);
    case PART_ns::NTypes::N16:
        return ti->node16_free_list->alloc_node(PART_ns::NTypes::N16);
    case PART_ns::NTypes::N48:
        return ti->node48_free_list->alloc_node(PART_ns::NTypes::N48);
    case PART_ns::NTypes::N256:
        return ti->node256_free_list->alloc_node(PART_ns::NTypes::N256);
    case PART_ns::NTypes::Leaf:
        return ti->leaf_free_list->alloc_node(PART_ns::NTypes::Leaf);
    default:
        std::cout << "[ALLOC NODE]\twrong type\n";
        assert(0);
    }
}

void free_node(PART_ns::NTypes type, void *addr) {
    switch (type) {
    case PART_ns::NTypes::N4:
        ti->node4_free_list->free_node(addr);
        break;
    case PART_ns::NTypes::N16:
        ti->node16_free_list->free_node(addr);
        break;
    case PART_ns::NTypes::N48:
        ti->node48_free_list->free_node(addr);
        break;
    case PART_ns::NTypes::N256:
        ti->node256_free_list->free_node(addr);
        break;
    case PART_ns::NTypes::Leaf:
        ti->leaf_free_list->free_node(addr);
        break;
    default:
        std::cout << "[FREE NODE]\twrong type\n";
        assert(0);
    }
}

void register_threadinfo() {
    std::lock_guard<std::mutex> lock_guard(ti_lock);

    if (pmblock == NULL) {
        pmblock = new PMBlockAllocator();
        std::cout << "[THREAD]\tfirst new pmblock\n";
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

        ti = new (mgr->alloc_thread_info()) thread_info();
        ti->next = ti_list_head;
        ti_list_head = ti;
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
    if(ti_list_head == NULL){
        delete epoch_mgr;
        epoch_mgr = NULL;
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
