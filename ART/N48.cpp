#include "N48.h"
#include "N.h"
#include <algorithm>
#include <assert.h>

namespace PART_ns {

bool N48::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 48) {
        return false;
    }
    childIndex[key].store(compactCount, std::memory_order_seq_cst);
    if (flush)
        clflush((char *)&childIndex[key], sizeof(uint8_t), false, true);

    children[compactCount].store(N::setDirty(n), std::memory_order_seq_cst);
    if (flush)
        clflush((char *)&children[compactCount], sizeof(N *), false, true);
    children[compactCount].store(n, std::memory_order_seq_cst);

    compactCount++;
    count++;
    return true;
}

void N48::change(uint8_t key, N *val) {
    uint8_t index = childIndex[key].load();
    assert(index != emptyMarker);
    children[index].store(N::setDirty(val), std::memory_order_seq_cst);
    clflush((char *)&children[index], sizeof(N *), false, true);
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
    clflush((char *)&children[index], sizeof(N *), false, true);
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
} // namespace PART_ns