#include "N16.h"
#include "LeafArray.h"
#include "N.h"
#include <algorithm>
#include <assert.h>
#include <emmintrin.h> // x86 SSE intrinsics

namespace PART_ns {

bool N16::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 16) {
        return false;
    }

#ifdef ZENTRY
    zens[compactCount].store(makeZentry(flipSign(key), n));
    if (flush)
        flush_data(&zens[compactCount], sizeof(std::atomic<uintptr_t>));
#else
    keys[compactCount].store(flipSign(key), std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&keys[compactCount], sizeof(std::atomic<uint8_t>));

    children[compactCount].store(n, std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&children[compactCount], sizeof(std::atomic<N *>));
#endif
    compactCount++;
    count++;
    return true;
}

void N16::change(uint8_t key, N *val) {
    auto childPos = getChildPos(key);
    assert(childPos != -1);
#ifdef ZENTRY
    zens[childPos].store(makeZentry(flipSign(key), val));
    flush_data(&zens[childPos], sizeof(std::atomic<uintptr_t>));
#else
    children[childPos].store(val, std::memory_order_seq_cst);
    flush_data(&children[childPos], sizeof(std::atomic<N *>));
#endif
}

int N16::getChildPos(const uint8_t k) {
#ifdef ZENTRY
    uint8_t keys[16] = {};
    for (int i = 0; i < 16; i++) {
        keys[i] = getZentryKey(zens[i].load());
    }
    __m128i cmp = _mm_cmpeq_epi8(
        _mm_set1_epi8(flipSign(k)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << compactCount) - 1);
    while (bitfield) {
        uint8_t pos = ctz(bitfield);
        if (getZentryPtr(zens[pos]) != nullptr) {
            return pos;
        }
        bitfield = bitfield ^ (1 << pos);
    }
    return -1;
#else
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
#endif
}

N *N16::getChild(const uint8_t k) {
#ifdef ZENTRY
    uint8_t keys[16] = {};
    for (int i = 0; i < 16; i++) {
        keys[i] = getZentryKey(zens[i].load());
    }
    __m128i cmp = _mm_cmpeq_epi8(
        _mm_set1_epi8(flipSign(k)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
    while (bitfield) {
        uint8_t pos = ctz(bitfield);
        auto p = getZentryKeyPtr(zens[pos]);

        if (p.second != nullptr && p.first == flipSign(k)) {
            return p.second;
        }
        bitfield = bitfield ^ (1 << pos);
    }
    return nullptr;
#else
    __m128i cmp = _mm_cmpeq_epi8(
        _mm_set1_epi8(flipSign(k)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
    unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
    while (bitfield) {
        uint8_t pos = ctz(bitfield);

        N *child = children[pos].load();
        if (child != nullptr && keys[pos].load() == flipSign(k)) {
            return child;
        }
        bitfield = bitfield ^ (1 << pos);
    }
    return nullptr;
#endif
}

bool N16::remove(uint8_t k, bool force, bool flush) {
    if (count <= 3 && !force) {
        return false;
    }
    auto leafPlace = getChildPos(k);
    assert(leafPlace != -1);
#ifdef ZENTRY
    zens[leafPlace].store(0);
    flush_data(&zens[leafPlace], sizeof(std::atomic<uintptr_t>));
#else
    children[leafPlace].store(nullptr, std::memory_order_seq_cst);
    flush_data((void *)&children[leafPlace], sizeof(std::atomic<N *>));
#endif
    count--;
    assert(getChild(k) == nullptr);
    return true;
}

N *N16::getAnyChild() const {
    N *anyChild = nullptr;
    for (int i = 0; i < 16; ++i) {
#ifdef ZENTRY
        auto child = getZentryPtr(zens[i].load());
#else
        N *child = children[i].load();
#endif
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
#ifdef ZENTRY
        N *child = N::clearDirty(getZentryPtr(zens[i].load()));
#else
        N *child = N::clearDirty(children[i].load());
#endif
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
#ifdef ZENTRY
        auto p = getZentryKeyPtr(zens[i].load());
        p.first = flipSign(p.first);
        if (p.first >= start && p.first <= end) {
            if (p.second != nullptr) {
                children[childrenCount] = std::make_tuple(p.first, p.second);
                childrenCount++;
            }
        }
#else
        uint8_t key = flipSign(this->keys[i].load());
        if (key >= start && key <= end) {
            N *child = this->children[i].load();
            if (child != nullptr) {
                children[childrenCount] = std::make_tuple(key, child);
                childrenCount++;
            }
        }
#endif
    }
    std::sort(children, children + childrenCount,
              [](auto &first, auto &second) {
                  return std::get<0>(first) < std::get<0>(second);
              });
}

uint32_t N16::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < compactCount && cnt < 3; i++) {
#ifdef ZENTRY
        auto child = getZentryPtr(zens[i].load());
#else
        N *child = children[i].load();
#endif
        if (child != nullptr)
            ++cnt;
    }
    return cnt;
}
void N16::graphviz_debug(std::ofstream &f) {
    char buf[10000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "N16 %d\n", level);
    auto pre = this->getPrefi();
    sprintf(buf + strlen(buf), "Prefix Len: %d\n", pre.prefixCount);
    sprintf(buf + strlen(buf), "Prefix: ");
    for (int i = 0; i < std::min(pre.prefixCount, maxStoredPrefixLength); i++) {
        sprintf(buf + strlen(buf), "%c ", pre.prefix[i]);
    }
    sprintf(buf + strlen(buf), "\n");
    sprintf(buf + strlen(buf), "count: %d\n", count);
    sprintf(buf + strlen(buf), "compact: %d\n", compactCount);
    sprintf(buf + strlen(buf), "\"]\n");

    for (int i = 0; i < compactCount; i++) {
#ifdef ZENTRY
        auto pp = getZentryKeyPtr(zens[i].load());
        auto p = pp.second;
        auto x = pp.first;
#else
        auto p = children[i].load();
        auto x = keys[i].load();
#endif
        if (p != nullptr) {
            x = flipSign(x);
            auto addr = reinterpret_cast<uintptr_t>(p);
            if (isLeaf(p)) {
                addr = reinterpret_cast<uintptr_t>(getLeaf(p));
            }
            sprintf(buf + strlen(buf), "node%lx -- node%lx [label=\"%c\"]\n",
                    reinterpret_cast<uintptr_t>(this), addr, x);
        }
    }
    f << buf;

    for (int i = 0; i < compactCount; i++) {
#ifdef ZENTRY
        auto p = getZentryPtr(zens[i].load());
#else
        auto p = children[i].load();
#endif
        if (p != nullptr) {
            if (isLeaf(p)) {
#ifdef LEAF_ARRAY
                auto la = getLeafArray(p);
                la->graphviz_debug(f);
#else
                auto l = getLeaf(p);
                l->graphviz_debug(f);
#endif
            } else {
                N::graphviz_debug(f, p);
            }
        }
    }
}

} // namespace PART_ns