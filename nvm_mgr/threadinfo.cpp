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
int id = 0;

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

void *PMFreeList::alloc_node(PART_ns::NTypes type) {
    // TODO: according to nt, alloc different node
    if (free_node_list.empty()) {
        size_t node_size = get_node_size(type);
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

void free_node(PART_ns::NTypes type, void *addr){
    switch (type) {
        case PART_ns::NTypes::N4:
             ti->node4_free_list->free_node(addr);
        case PART_ns::NTypes::N16:
             ti->node16_free_list->free_node(addr);
        case PART_ns::NTypes::N48:
             ti->node48_free_list->free_node(addr);
        case PART_ns::NTypes::N256:
             ti->node256_free_list->free_node(addr);
        case PART_ns::NTypes::Leaf:
             ti->leaf_free_list->free_node(addr);
        default:
            std::cout << "[FREE NODE]\twrong type\n";
            assert(0);
    }
}

void *get_static_log() { return ti->static_log; }

void register_threadinfo() {
    std::lock_guard<std::mutex> lock_guard(ti_lock);

    if (pmblock == NULL) {
        pmblock = new PMBlockAllocator();
        std::cout << "[THREAD]\tfirst new pmblock\n";
    }
    if (ti == NULL) {
        NVMMgr *mgr = get_nvm_mgr();
        ti = (thread_info *)mgr->alloc_thread_info();
        ti->node4_free_list = new PMFreeList(pmblock);
        ti->node16_free_list = new PMFreeList(pmblock);
        ti->node48_free_list = new PMFreeList(pmblock);
        ti->node256_free_list = new PMFreeList(pmblock);
        ti->leaf_free_list = new PMFreeList(pmblock);

        ti->next = ti_list_head;
        ti->_lock = 0;
        ti_list_head = ti;
        ti->id = id++;
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
    if (ti_list_head == NULL) {
        // last one leave
        close_nvm_mgr();
    }
    ti = NULL;
}

int get_thread_id() { return ti->id; }

void *get_threadinfo() { return (void *)ti; }

} // namespace NVMMgr_ns
