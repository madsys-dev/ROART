#include "N4.h"
#include "N.h"
#include <algorithm>
#include <assert.h>

namespace PART_ns {
#ifdef LOG_FREE
void N4::deleteChildren() {
    for (uint32_t i = 0; i < compactCount; ++i) {
        N *child = N::clearDirty(children[i].load());
        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

bool N4::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 4) {
        return false;
    }
    keys[compactCount].store(key, std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&keys[compactCount], sizeof(std::atomic<uint8_t>));
    //        clflush((char *)&keys[compactCount], sizeof(uint8_t), true, true);

    children[compactCount].store(N::setDirty(n), std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&children[compactCount], sizeof(std::atomic<N *>));
    //        clflush((char *)&children[compactCount], sizeof(N *), true, true);
    children[compactCount].store(n, std::memory_order_seq_cst);
    compactCount++;
    count++;
    // As the size of node4 is lower than cache line size (64bytes),
    // only one clflush is required to atomically synchronize its updates
    // if (flush) clflush((char *)this, sizeof(N4), true, true);
    return true;
}

void N4::change(uint8_t key, N *val) {
    for (uint32_t i = 0; i < compactCount; ++i) {
        N *child = children[i].load();
        if (child != nullptr && keys[i].load() == key) {
            children[i].store(N::setDirty(val), std::memory_order_seq_cst);
            flush_data((void *)&children[i], sizeof(std::atomic<N *>));
            //            clflush((char *)&children[i], sizeof(N *), false,
            //            true);
            children[i].store(val, std::memory_order_seq_cst);
            return;
        }
    }
    return;
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
            flush_data((void *)&children[i], sizeof(std::atomic<N *>));
            //            clflush((char *)&children[i], sizeof(N *), false,
            //            true);
            children[i].store(nullptr, std::memory_order_seq_cst);
            count--;
            return true;
        }
    }
    return false;
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
                     std::tuple<uint8_t, std::atomic<N *> *> children[],
                     uint32_t &childrenCount) {
    childrenCount = 0;
    for (uint32_t i = 0; i < 4; ++i) {
        uint8_t key = this->keys[i].load();
        if (key >= start && key <= end) {
            N *child = this->children[i].load();
            if (child != nullptr) {
                children[childrenCount] =
                    std::make_tuple(key, &(this->children[i]));
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
#else
void N4::deleteChildren() {
    for (uint32_t i = 0; i < compactCount; ++i) {
        N *child = N::clearDirty(children[i].load());
        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

bool N4::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 4) {
        return false;
    }
    keys[compactCount].store(key, std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&keys[compactCount], sizeof(std::atomic<uint8_t>));
    //        clflush((char *)&keys[compactCount], sizeof(uint8_t), true, true);

    // record the old version and index
    if (flush) {
        uint64_t oldp = (1ull << 56) | ((uint64_t)compactCount << 48);
        old_pointer.store(oldp); // store the old version
    }

    // modify the pointer
    children[compactCount].store(n, std::memory_order_seq_cst);

    // flush the new pointer and clear the old version
    if (flush) {
        flush_data((void *)&children[compactCount], sizeof(std::atomic<N *>));
        old_pointer.store(0); // after persisting, clear the old version
    }

    compactCount++;
    count++;
    return true;
}

void N4::change(uint8_t key, N *val) {
    for (uint32_t i = 0; i < compactCount; ++i) {
        N *child = children[i].load();
        if (child != nullptr && keys[i].load() == key) {

            uint64_t oldp = (1ull << 56) | ((uint64_t)i << 48) |
                            ((uint64_t)children[i].load() & ((1ull << 48) - 1));
            old_pointer.store(oldp); // store the old version

            children[i].store(val, std::memory_order_seq_cst);
            flush_data((void *)&children[i], sizeof(std::atomic<N *>));

            old_pointer.store(0);

            return;
        }
    }
    return;
}

N * N4::getChild(const uint8_t k) {
    for (uint32_t i = 0; i < 4; ++i) {
        N *child = children[i].load();
        if (child != nullptr && keys[i].load() == k) {
            uint64_t oldp = old_pointer.load();
            int valid = (oldp >> 56) & 1;
            int index = (oldp >> 48) & ((1 << 8) - 1);
            uint64_t p = oldp & ((1ull << 48) - 1);
            if(valid && i == index){
                // guarantee the p is persistent
                return (N*)p;
            }
            else{
                // guarantee child is not being modified
                return child;
            }
        }
    }
    // we can check from old_pointer
    // weather val of key is being modified or not
    uint64_t oldp = old_pointer.load();
    int valid = (oldp >> 56) & 1;
    int index = (oldp >> 48) & ((1 << 8) - 1);
    uint64_t p = oldp & ((1ull << 48) - 1);
    if(valid && keys[index].load() == k){
        // guarantee the p is persistent
        return (N*)p;
    }
    return nullptr;
}

bool N4::remove(uint8_t k, bool force, bool flush) {
    for (uint32_t i = 0; i < compactCount; ++i) {
        if (children[i] != nullptr && keys[i].load() == k) {

            uint64_t oldp = (1ull << 56) | ((uint64_t)i << 48) |
                            ((uint64_t)children[i].load() & ((1ull << 48) - 1));
            old_pointer.store(oldp); // store the old version

            children[i].store(nullptr, std::memory_order_seq_cst);
            flush_data((void *)&children[i], sizeof(std::atomic<N *>));

            old_pointer.store(0);

            count--;
            return true;
        }
    }
    return false;
}

// TODO: need to modify
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

// TODO
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

// TODO
void N4::getChildren(uint8_t start, uint8_t end,
                     std::tuple<uint8_t, std::atomic<N *> *> children[],
                     uint32_t &childrenCount) {
    childrenCount = 0;
    for (uint32_t i = 0; i < 4; ++i) {
        uint8_t key = this->keys[i].load();
        if (key >= start && key <= end) {
            N *child = this->children[i].load();
            if (child != nullptr) {
                children[childrenCount] =
                    std::make_tuple(key, &(this->children[i]));
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
#endif
} // namespace PART_ns