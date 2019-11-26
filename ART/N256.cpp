#include "N256.h"
#include "N.h"
#include <algorithm>
#include <assert.h>

namespace PART_ns {

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
        flush_data((void *)&children[key], sizeof(N *));
    //        clflush((char *)&children[key], sizeof(N *), false, true);
    children[key].store(val, std::memory_order_seq_cst);
    count++;
    return true;
}

void N256::change(uint8_t key, N *n) {
    children[key].store(N::setDirty(n), std::memory_order_seq_cst);
    flush_data((void *)&children[key], sizeof(N *));
    //    clflush((char *)&children[key], sizeof(N *), false, true);
    children[key].store(n, std::memory_order_seq_cst);
}

std::atomic<N *> *N256::getChild(const uint8_t k) { return &children[k]; }

bool N256::remove(uint8_t k, bool force, bool flush) {
    if (count <= 37 && !force) {
        return false;
    }
    children[k].store(N::setDirty(nullptr), std::memory_order_seq_cst);
    flush_data((void *)&children[k], sizeof(N *));
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
} // namespace PART_ns