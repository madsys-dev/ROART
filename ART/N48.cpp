#include "N48.h"
#include "N.h"
#include <algorithm>
#include <assert.h>

namespace PART_ns {
#ifdef LOG_FREE
bool N48::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 48) {
        return false;
    }
    childIndex[key].store(compactCount, std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&childIndex[key], sizeof(std::atomic<uint8_t>));
    //        clflush((char *)&childIndex[key], sizeof(uint8_t), false, true);

    children[compactCount].store(N::setDirty(n), std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&children[compactCount], sizeof(std::atomic<N *>));
    //        clflush((char *)&children[compactCount], sizeof(N *), false,
    //        true);
    children[compactCount].store(n, std::memory_order_seq_cst);

    compactCount++;
    count++;
    return true;
}

void N48::change(uint8_t key, N *val) {
    uint8_t index = childIndex[key].load();
    assert(index != emptyMarker);
    children[index].store(N::setDirty(val), std::memory_order_seq_cst);
    flush_data((void *)&children[index], sizeof(std::atomic<N *>));
    //    clflush((char *)&children[index], sizeof(N *), false, true);
    children[index].store(val, std::memory_order_seq_cst);
}

std::atomic<N *> *N48::getChild(const uint8_t k) {
    uint8_t index = childIndex[k].load();
    if (index == emptyMarker) {
        return nullptr;
    } else {
        return &children[index];
    }
}

bool N48::remove(uint8_t k, bool force, bool flush) {
    if (count <= 12 && !force) {
        return false;
    }
    uint8_t index = childIndex[k].load();
    assert(index != emptyMarker);
    children[index].store(N::setDirty(nullptr), std::memory_order_seq_cst);
    flush_data((void *)&children[index], sizeof(std::atomic<N *>));
    //    clflush((char *)&children[index], sizeof(N *), false, true);
    children[index].store(nullptr, std::memory_order_seq_cst);
    count--;
    assert(getChild(k) == nullptr);
    return true;
}

N *N48::getAnyChild() const {
    N *anyChild = nullptr;
    for (unsigned i = 0; i < 48; i++) {
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

void N48::deleteChildren() {
    for (unsigned i = 0; i < 256; i++) {
        uint8_t index = childIndex[i].load();
        if (index != emptyMarker && children[index].load() != nullptr) {
            N *child = N::clearDirty(children[index].load());
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

void N48::getChildren(uint8_t start, uint8_t end,
                      std::tuple<uint8_t, std::atomic<N *> *> children[],
                      uint32_t &childrenCount) {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        uint8_t index = this->childIndex[i].load();
        if (index != emptyMarker && this->children[index] != nullptr) {
            N *child = this->children[index].load();
            if (child != nullptr) {
                children[childrenCount] =
                    std::make_tuple(i, &(this->children[index]));
                childrenCount++;
            }
        }
    }
}

uint32_t N48::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < 256 && cnt < 3; i++) {
        uint8_t index = childIndex[i].load();
        if (index != emptyMarker && children[index].load() != nullptr)
            cnt++;
    }
    return cnt;
}
#else
bool N48::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 48) {
        return false;
    }
    childIndex[key].store(compactCount, std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&childIndex[key], sizeof(std::atomic<uint8_t>));

    // record the old version and index
    if (flush) {
        uint64_t oldp = (1ull << 56) | ((uint64_t)compactCount << 48);
        old_pointer.store(oldp,
                          std::memory_order_seq_cst); // store the old version
    }

    // modify the pointer
    children[compactCount].store(n, std::memory_order_seq_cst);

    // flush the new pointer and clear the old version
    if (flush) {
        flush_data((void *)&children[compactCount], sizeof(std::atomic<N *>));
        old_pointer.store(0,
                          std::memory_order_seq_cst); // after persisting, clear
                                                      // the old version
    }

    compactCount++;
    count++;
    return true;
}

void N48::change(uint8_t key, N *val) {
    uint8_t index = childIndex[key].load();
    assert(index != emptyMarker);

    uint64_t oldp = (1ull << 56) | ((uint64_t)index << 48) |
                    ((uint64_t)children[index].load() & ((1ull << 48) - 1));
    old_pointer.store(oldp, std::memory_order_seq_cst); // store the old version

    children[index].store(val, std::memory_order_seq_cst);
    flush_data((void *)&children[index], sizeof(std::atomic<N *>));

    old_pointer.store(0, std::memory_order_seq_cst);
}

N *N48::getChild(const uint8_t k) {
    uint8_t index = childIndex[k].load();
    if (index == emptyMarker) {
        return nullptr;
    } else {
        N *child = children[index].load();
        uint64_t oldp = old_pointer.load();
        uint8_t valid = (oldp >> 56) & 1;
        uint8_t id = (oldp >> 48) & ((1 << 8) - 1);
        uint64_t p = oldp & ((1ull << 48) - 1);
        if (valid && id == index) {
            // guarantee the p is persistent
            return (N *)p;
        } else {
            // guarantee child is not being modified
            return child;
        }
    }
}

bool N48::remove(uint8_t k, bool force, bool flush) {
    if (count <= 12 && !force) {
        return false;
    }
    uint8_t index = childIndex[k].load();
    assert(index != emptyMarker);

    uint64_t oldp = (1ull << 56) | ((uint64_t)index << 48) |
                    ((uint64_t)children[index].load() & ((1ull << 48) - 1));
    old_pointer.store(oldp, std::memory_order_seq_cst); // store the old version

    children[index].store(nullptr, std::memory_order_seq_cst);
    flush_data((void *)&children[index], sizeof(std::atomic<N *>));

    old_pointer.store(0, std::memory_order_seq_cst);

    count--;
    assert(getChild(k) == nullptr);
    return true;
}

N *N48::getAnyChild() const {
    N *anyChild = nullptr;
    for (unsigned i = 0; i < 48; i++) {
        N *child = children[i].load();

        // check old pointer
        uint64_t oldp = old_pointer.load();
        uint8_t valid = (oldp >> 56) & 1;
        uint8_t index = (oldp >> 48) & ((1 << 8) - 1);
        uint64_t p = oldp & ((1ull << 48) - 1);
        if (valid && index == i) {
            if ((N *)p != nullptr) {
                if (N::isLeaf((N *)p)) {
                    return (N *)p;
                }
                anyChild = (N *)p;
            }
        } else {
            if (child != nullptr) {
                if (N::isLeaf(child)) {
                    return child;
                }
                anyChild = child;
            }
        }
    }
    return anyChild;
}

void N48::deleteChildren() {
    for (unsigned i = 0; i < 256; i++) {
        uint8_t index = childIndex[i].load();
        if (index != emptyMarker && children[index].load() != nullptr) {
            N *child = N::clearDirty(children[index].load());
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

void N48::getChildren(uint8_t start, uint8_t end,
                      std::tuple<uint8_t, N *> children[],
                      uint32_t &childrenCount) {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        uint8_t index = this->childIndex[i].load();
        if (index != emptyMarker && this->children[index] != nullptr) {
            N *child = this->children[index].load();

            // check old pointer
            uint64_t oldp = old_pointer.load();
            uint8_t valid = (oldp >> 56) & 1;
            uint8_t ind = (oldp >> 48) & ((1 << 8) - 1);
            uint64_t p = oldp & ((1ull << 48) - 1);

            if (valid && ind == index) {
                if ((N *)p != nullptr) {
                    children[childrenCount] = std::make_tuple(i, (N *)p);
                    childrenCount++;
                }
            } else {
                if (child != nullptr) {
                    children[childrenCount] = std::make_tuple(i, child);
                    childrenCount++;
                }
            }
        }
    }
}

uint32_t N48::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < 256 && cnt < 3; i++) {
        uint8_t index = childIndex[i].load();
        if (index != emptyMarker && children[index].load() != nullptr)
            cnt++;
    }
    return cnt;
}
#endif
} // namespace PART_ns