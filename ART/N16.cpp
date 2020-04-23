#include "N16.h"
#include "N.h"
#include <algorithm>
#include <assert.h>
#include <emmintrin.h> // x86 SSE intrinsics

namespace PART_ns {
#ifdef LOG_FREE
bool N16::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 16) {
        return false;
    }
    keys[compactCount].store(flipSign(key), std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&keys[compactCount], sizeof(std::atomic<uint8_t>));
    //        clflush((char *)&keys[compactCount], sizeof(N *), false, true);

    children[compactCount].store(N::setDirty(n), std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&children[compactCount], sizeof(std::atomic<N *>));
    //        clflush((char *)&children[compactCount], sizeof(N *), false,
    //        true);
    children[compactCount].store(n, std::memory_order_seq_cst);
    compactCount++;
    count++;
    // this clflush will atomically flush the cache line including counters and
    // entire key entries
    // if (flush) clflush((char *)this, sizeof(uintptr_t), true, true);
    return true;
}

void N16::change(uint8_t key, N *val) {
    auto childPos = getChildPos(key);
    assert(childPos != nullptr);
    childPos->store(N::setDirty(val), std::memory_order_seq_cst);
    flush_data((void *)childPos, sizeof(std::atomic<N *>));
    //    clflush((char *)childPos, sizeof(N *), false, true);
    childPos->store(val, std::memory_order_seq_cst);
}

std::atomic<N *> *N16::getChildPos(const uint8_t k) {
    __m128i cmp = _mm_cmpeq_epi8(
        _mm_set1_epi8(flipSign(k)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << compactCount) - 1);
    while (bitfield) {
        uint8_t pos = ctz(bitfield);

        if (children[pos].load() != nullptr) {
            return &children[pos];
        }
        bitfield = bitfield ^ (1 << pos);
    }
    return nullptr;
}

std::atomic<N *> *N16::getChild(const uint8_t k) {
    __m128i cmp = _mm_cmpeq_epi8(
        _mm_set1_epi8(flipSign(k)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
    while (bitfield) {
        uint8_t pos = ctz(bitfield);

        N *child = children[pos].load();
        if (child != nullptr && keys[pos].load() == flipSign(k)) {
            return &children[pos];
        }
        bitfield = bitfield ^ (1 << pos);
    }
    return nullptr;
}

bool N16::remove(uint8_t k, bool force, bool flush) {
    if (count <= 3 && !force) {
        return false;
    }
    auto leafPlace = getChildPos(k);
    assert(leafPlace != nullptr);
    leafPlace->store(N::setDirty(nullptr), std::memory_order_seq_cst);
    flush_data((void *)leafPlace, sizeof(std::atomic<N *>));
    //    clflush((char *)leafPlace, sizeof(N *), false, true);
    leafPlace->store(nullptr, std::memory_order_seq_cst);
    count--;
    assert(getChild(k) == nullptr);
    return true;
}

N *N16::getAnyChild() const {
    N *anyChild = nullptr;
    for (int i = 0; i < 16; ++i) {
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

void N16::deleteChildren() {
    for (std::size_t i = 0; i < compactCount; ++i) {
        N *child = N::clearDirty(children[i].load());
        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

void N16::getChildren(uint8_t start, uint8_t end,
                      std::tuple<uint8_t, std::atomic<N *> *> children[],
                      uint32_t &childrenCount) {
    childrenCount = 0;
    for (int i = 0; i < compactCount; ++i) {
        uint8_t key = flipSign(this->keys[i].load());
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

uint32_t N16::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < compactCount && cnt < 3; i++) {
        N *child = children[i].load();
        if (child != nullptr)
            ++cnt;
    }
    return cnt;
}
#else
bool N16::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 16) {
        return false;
    }
    keys[compactCount].store(flipSign(key), std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&keys[compactCount], sizeof(std::atomic<uint8_t>));

    if (flush) {
        uint64_t oldp = (1ull << 56) | ((uint64_t)compactCount << 48);
        old_pointer.store(oldp,
                          std::memory_order_seq_cst); // store the old version
    }

    children[compactCount].store(n, std::memory_order_seq_cst);

    if (flush) {
        flush_data((void *)&children[compactCount], sizeof(std::atomic<N *>));
        old_pointer.store(0, std::memory_order_seq_cst);
    }

    compactCount++;
    count++;
    return true;
}

void N16::change(uint8_t key, N *val) {
    auto childPos = getChildPos(key);
    assert(childPos != -1);

    uint64_t oldp = (1ull << 56) | ((uint64_t)childPos << 48) |
                    ((uint64_t)children[childPos].load() & ((1ull << 48) - 1));
    old_pointer.store(oldp, std::memory_order_seq_cst); // store the old version

    children[childPos].store(val, std::memory_order_seq_cst);
    flush_data((void *)&children[childPos], sizeof(std::atomic<N *>));

    old_pointer.store(0, std::memory_order_seq_cst);
}

int N16::getChildPos(const uint8_t k) {
    __m128i cmp = _mm_cmpeq_epi8(
        _mm_set1_epi8(flipSign(k)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << compactCount) - 1);
    while (bitfield) {
        uint8_t pos = ctz(bitfield);

        if (children[pos].load() != nullptr) {
            return pos;
        }
        bitfield = bitfield ^ (1 << pos);
    }
    return -1;
}

N *N16::getChild(const uint8_t k) {
    __m128i cmp = _mm_cmpeq_epi8(
        _mm_set1_epi8(flipSign(k)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
    while (bitfield) {
        uint8_t pos = ctz(bitfield);

        N *child = children[pos].load();
        if (child != nullptr && keys[pos].load() == flipSign(k)) {
            uint64_t oldp = old_pointer.load();
            uint8_t valid = (oldp >> 56) & 1;
            uint8_t index = (oldp >> 48) & ((1 << 8) - 1);
            uint64_t p = oldp & ((1ull << 48) - 1);
            if (valid && pos == index) {
                // guarantee the p is persistent
                return (N *)p;
            } else {
                // guarantee child is not being modified
                return child;
            }
        }
        bitfield = bitfield ^ (1 << pos);
    }
    // we can check from old_pointer
    // weather val of key is being modified or not
    uint64_t oldp = old_pointer.load();
    uint8_t valid = (oldp >> 56) & 1;
    uint8_t index = (oldp >> 48) & ((1 << 8) - 1);
    uint64_t p = oldp & ((1ull << 48) - 1);
    if (valid && keys[index].load() == k) {
        // guarantee the p is persistent
        return (N *)p;
    }
    return nullptr;
}

bool N16::remove(uint8_t k, bool force, bool flush) {
    if (count <= 3 && !force) {
        return false;
    }
    auto leafPlace = getChildPos(k);
    assert(leafPlace != -1);

    uint64_t oldp = (1ull << 56) | ((uint64_t)leafPlace << 48) |
                    ((uint64_t)children[leafPlace].load() & ((1ull << 48) - 1));
    old_pointer.store(oldp, std::memory_order_seq_cst); // store the old version

    children[leafPlace].store(nullptr, std::memory_order_seq_cst);
    flush_data((void *)&children[leafPlace], sizeof(std::atomic<N *>));

    old_pointer.store(0, std::memory_order_seq_cst);

    count--;
    assert(getChild(k) == nullptr);
    return true;
}

N *N16::getAnyChild() const {
    N *anyChild = nullptr;
    for (int i = 0; i < 16; ++i) {
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

void N16::deleteChildren() {
    for (std::size_t i = 0; i < compactCount; ++i) {
        N *child = N::clearDirty(children[i].load());
        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

void N16::getChildren(uint8_t start, uint8_t end,
                      std::tuple<uint8_t, N *> children[],
                      uint32_t &childrenCount) {
    childrenCount = 0;
    for (int i = 0; i < compactCount; ++i) {
        uint8_t key = flipSign(this->keys[i].load());
        if (key >= start && key <= end) {
            N *child = this->children[i].load();

            // check old pointer
            uint64_t oldp = old_pointer.load();
            uint8_t valid = (oldp >> 56) & 1;
            uint8_t index = (oldp >> 48) & ((1 << 8) - 1);
            uint64_t p = oldp & ((1ull << 48) - 1);

            if (valid && index == i) {
                if ((N *)p != nullptr) {
                    children[childrenCount] = std::make_tuple(key, (N *)p);
                    childrenCount++;
                }
            } else {
                if (child != nullptr) {
                    children[childrenCount] = std::make_tuple(key, child);
                    childrenCount++;
                }
            }
        }
    }
    std::sort(children, children + childrenCount,
              [](auto &first, auto &second) {
                  return std::get<0>(first) < std::get<0>(second);
              });
}

uint32_t N16::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < compactCount && cnt < 3; i++) {
        N *child = children[i].load();
        if (child != nullptr)
            ++cnt;
    }
    return cnt;
}

#endif
} // namespace PART_ns