#include "N.h"
#include <algorithm>
#include <assert.h>

namespace PART_ns {

void N4::deleteChildren() {
    for (uint32_t i = 0; i < compactCount; ++i) {
        N *child = N::clearDirty(children[i].load());
        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

inline bool N4::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 4) {
        return false;
    }
    keys[compactCount].store(key, std::memory_order_seq_cst);
    if(flush) clflush((char *)&keys[compactCount], sizeof(uint8_t), true, true);

    children[compactCount].store(N::setDirty(n), std::memory_order_seq_cst);
    if(flush) clflush((char *)&children[compactCount], sizeof(N *), true, true);
    children[compactCount].store(n, std::memory_order_seq_cst);
    compactCount++;
    count++;
    // As the size of node4 is lower than cache line size (64bytes),
    // only one clflush is required to atomically synchronize its updates
    // if (flush) clflush((char *)this, sizeof(N4), true, true);
    return true;
}

template <class NODE> void N4::copyTo(NODE *n) const {
    for (uint32_t i = 0; i < compactCount; ++i) {
        N *child = children[i].load();
        if (child != nullptr) {
            // not flush
            n->insert(keys[i].load(), child, false);
        }
    }
}

void N4::change(uint8_t key, N *val) {
    for (uint32_t i = 0; i < compactCount; ++i) {
        N *child = children[i].load();
        if (child != nullptr && keys[i].load() == key) {
            children[i].store(N::setDirty(val), std::memory_order_seq_cst);
            clflush((char *)&children[i], sizeof(N *), false, true);
            children[i].store(val, std::memory_order_seq_cst);
            return;
        }
    }
    assert(false);
    __builtin_unreachable();
}

std::atomic<N *> *N4::getChild(const uint8_t k) {
    for (uint32_t i = 0; i < 4; ++i) {
        N *child = children[i].load();
        if (child != nullptr && keys[i].load() == k) {
            return &children[i];
        }
    }
    return nullptr;
}

bool N4::remove(uint8_t k, bool force, bool flush) {
    for (uint32_t i = 0; i < compactCount; ++i) {
        if (children[i] != nullptr && keys[i].load() == k) {
            children[i].store(N::setDirty(nullptr), std::memory_order_seq_cst);
            clflush((char *)&children[i], sizeof(N *), false, true);
            children[i].store(nullptr, std::memory_order_seq_cst);
            count--;
            return true;
        }
    }
    assert(false);
    __builtin_unreachable();
}

N *N4::getAnyChild() const {
    N *anyChild = nullptr;
    for (uint32_t i = 0; i < 4; ++i) {
        N *child = N::clearDirty(children[i].load());
        if (child != nullptr) {
            if (N::isLeaf(child)) {
                return child;
            }
            anyChild = child;
        }
    }
    return anyChild;
}

std::tuple<N *, uint8_t> N4::getSecondChild(const uint8_t key) const {
    for (uint32_t i = 0; i < compactCount; ++i) {
        N *child = children[i].load();
        if (child != nullptr) {
            uint8_t k = keys[i].load();
            if (k != key) {
                return std::make_tuple(child, k);
            }
        }
    }
    return std::make_tuple(nullptr, 0);
}

void N4::getChildren(uint8_t start, uint8_t end,
                     std::tuple<uint8_t, N *> *&children,
                     uint32_t &childrenCount) const {
    childrenCount = 0;
    for (uint32_t i = 0; i < 4; ++i) {
        uint8_t key = this->keys[i].load();
        if (key >= start && key <= end) {
            N *child = this->children[i].load();
            if (child != nullptr) {
                children[childrenCount] = std::make_tuple(key, child);
                childrenCount++;
            }
        }
    }
    std::sort(children, children + childrenCount,
              [](auto &first, auto &second) {
                  return std::get<0>(first) < std::get<0>(second);
              });
}

uint32_t N4::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < compactCount && cnt < 3; i++) {
        N *child = children[i].load();
        if (child != nullptr)
            cnt++;
    }
    return cnt;
}
} // namespace PART_ns