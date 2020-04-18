#include "N256.h"
#include "N.h"
#include <algorithm>
#include <assert.h>

namespace PART_ns {
#ifdef LOG_FREE
void N256::deleteChildren() {
    for (uint64_t i = 0; i < 256; ++i) {
        N *child = N::clearDirty(children[i].load());
        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

bool N256::insert(uint8_t key, N *val, bool flush) {
    children[key].store(N::setDirty(val), std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&children[key], sizeof(std::atomic<N *>));
    //        clflush((char *)&children[key], sizeof(N *), false, true);
    children[key].store(val, std::memory_order_seq_cst);
    count++;
    return true;
}

void N256::change(uint8_t key, N *n) {
    children[key].store(N::setDirty(n), std::memory_order_seq_cst);
    flush_data((void *)&children[key], sizeof(std::atomic<N *>));
    //    clflush((char *)&children[key], sizeof(N *), false, true);
    children[key].store(n, std::memory_order_seq_cst);
}

std::atomic<N *> *N256::getChild(const uint8_t k) { return &children[k]; }

bool N256::remove(uint8_t k, bool force, bool flush) {
    if (count <= 37 && !force) {
        return false;
    }
    children[k].store(N::setDirty(nullptr), std::memory_order_seq_cst);
    flush_data((void *)&children[k], sizeof(std::atomic<N *>));
    //    clflush((char *)&children[k], sizeof(N *), false, true);
    children[k].store(nullptr, std::memory_order_seq_cst);
    count--;
    return true;
}

N *N256::getAnyChild() const {
    N *anyChild = nullptr;
    for (uint64_t i = 0; i < 256; ++i) {
        N *child = N::clearDirty(children[i].load());
        if (child != nullptr) {
            if (N::isLeaf(child)) {
                return child;
            } else {
                anyChild = child;
            }
        }
    }
    return anyChild;
}

void N256::getChildren(uint8_t start, uint8_t end,
                       std::tuple<uint8_t, std::atomic<N *> *> children[],
                       uint32_t &childrenCount) {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        N *child = this->children[i].load();
        if (child != nullptr) {
            children[childrenCount] = std::make_tuple(i, &(this->children[i]));
            childrenCount++;
        }
    }
}

uint32_t N256::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < 256 && cnt < 3; i++) {
        N *child = children[i].load();
        if (child != nullptr)
            cnt++;
    }
    return cnt;
}
#else
void N256::deleteChildren() {
    for (uint64_t i = 0; i < 256; ++i) {
        N *child = N::clearDirty(children[i].load());
        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

bool N256::insert(uint8_t key, N *val, bool flush) {
    if(flush){
        uint64_t oldp = (1ull << 56) | ((uint64_t)key << 48);
        old_pointer.store(oldp); // store the old version
    }

    children[key].store(val, std::memory_order_seq_cst);
    if (flush){
        flush_data((void *)&children[key], sizeof(std::atomic<N *>));
        old_pointer.store(0);
    }

    count++;
    return true;
}

void N256::change(uint8_t key, N *n) {
    uint64_t oldp = (1ull << 56) | ((uint64_t)key << 48) |
                    ((uint64_t)children[key].load() & ((1ull << 48) - 1));
    old_pointer.store(oldp); // store the old version

    children[key].store(n, std::memory_order_seq_cst);
    flush_data((void *)&children[key], sizeof(std::atomic<N *>));
    old_pointer.store(0);
}

N *N256::getChild(const uint8_t k) {
    N *child = children[k].load();
    uint64_t oldp = old_pointer.load();
    int valid = (oldp >> 56) & 1;
    int index = (oldp >> 48) & ((1 << 8) - 1);
    uint64_t p = oldp & ((1ull << 48) - 1);
    if(valid && k == index){
        // guarantee the p is persistent
        return (N*)p;
    }
    else{
        // guarantee child is not being modified
        return child;
    }
}

bool N256::remove(uint8_t k, bool force, bool flush) {
    if (count <= 37 && !force) {
        return false;
    }

    uint64_t oldp = (1ull << 56) | ((uint64_t)k << 48) |
                    ((uint64_t)children[k].load() & ((1ull << 48) - 1));
    old_pointer.store(oldp); // store the old version

    children[k].store(nullptr, std::memory_order_seq_cst);
    flush_data((void *)&children[k], sizeof(std::atomic<N *>));

    old_pointer.store(0);

    count--;
    return true;
}

//TODO
N *N256::getAnyChild() const {
    N *anyChild = nullptr;
    for (uint64_t i = 0; i < 256; ++i) {
        N *child = N::clearDirty(children[i].load());
        if (child != nullptr) {
            if (N::isLeaf(child)) {
                return child;
            } else {
                anyChild = child;
            }
        }
    }
    return anyChild;
}

//TODO
void N256::getChildren(uint8_t start, uint8_t end,
                       std::tuple<uint8_t, std::atomic<N *> *> children[],
                       uint32_t &childrenCount) {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        N *child = this->children[i].load();
        if (child != nullptr) {
            children[childrenCount] = std::make_tuple(i, &(this->children[i]));
            childrenCount++;
        }
    }
}

uint32_t N256::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < 256 && cnt < 3; i++) {
        N *child = children[i].load();
        if (child != nullptr)
            cnt++;
    }
    return cnt;
}
#endif
} // namespace PART_ns