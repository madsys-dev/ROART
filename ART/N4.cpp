#include "N4.h"
#include "LeafArray.h"
#include "N.h"
#include <algorithm>
#include <assert.h>

namespace PART_ns {

void N4::deleteChildren() {
    for (uint32_t i = 0; i < compactCount; ++i) {
#ifdef ZENTRY
        N *child = N::clearDirty(getZentryPtr(zens[i]));
#else
        N *child = N::clearDirty(children[i].load());
#endif
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
#ifdef ZENTRY
    zens[compactCount].store(makeZentry(key, n));
    if (flush)
        flush_data(&zens[compactCount], sizeof(std::atomic<uintptr_t>));

#else
    keys[compactCount].store(key, std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&keys[compactCount], sizeof(std::atomic<uint8_t>));

    children[compactCount].store(n, std::memory_order_seq_cst);
    if (flush) {
        flush_data((void *)&children[compactCount], sizeof(std::atomic<N *>));
    }
#endif
    compactCount++;
    count++;
    return true;
}

void N4::change(uint8_t key, N *val) {
    for (uint32_t i = 0; i < compactCount; ++i) {
#ifdef ZENTRY
        auto p = getZentryKeyPtr(zens[i].load());
        if (p.second != nullptr && p.first == key) {
            zens[i].store(makeZentry(key, val));
            flush_data((void *)&zens[i], sizeof(std::atomic<uintptr_t>));
            return;
        }
#else
        N *child = children[i].load();
        if (child != nullptr && keys[i].load() == key) {
            children[i].store(val, std::memory_order_seq_cst);
            flush_data((void *)&children[i], sizeof(std::atomic<N *>));
            return;
        }
#endif
    }
}

N *N4::getChild(const uint8_t k) {
    for (uint32_t i = 0; i < 4; ++i) {
#ifdef ZENTRY
        auto p = getZentryKeyPtr(zens[i].load());
        if (p.second != nullptr && p.first == k) {
            return p.second;
        }
#else
        N *child = children[i].load();
        if (child != nullptr && keys[i].load() == k) {
            return child;
        }
#endif
    }
    return nullptr;
}

bool N4::remove(uint8_t k, bool force, bool flush) {
    for (uint32_t i = 0; i < compactCount; ++i) {
#ifdef ZENTRY
        auto p = getZentryKeyPtr(zens[i].load());
        if (p.second != nullptr && p.first == k) {
            zens[i].store(0);
            flush_data(&zens[i], sizeof(std::atomic<uintptr_t>));
            count--;
            return true;
        }
#else
        if (children[i] != nullptr && keys[i].load() == k) {
            children[i].store(nullptr, std::memory_order_seq_cst);
            flush_data((void *)&children[i], sizeof(std::atomic<N *>));
            count--;
            return true;
        }
#endif
    }
    return false;
}

N *N4::getAnyChild() const {
    N *anyChild = nullptr;
    for (uint32_t i = 0; i < 4; ++i) {
#ifdef ZENTRY
        N *child = getZentryPtr(zens[i].load());
        if (child != nullptr) {
            if (N::isLeaf(child)) {
                return child;
            }
            anyChild = child;
        }
#else
        N *child = children[i].load();
        if (child != nullptr) {
            if (N::isLeaf(child)) {
                return child;
            }
            anyChild = child;
        }
#endif
    }
    return anyChild;
}

// in the critical section
std::tuple<N *, uint8_t> N4::getSecondChild(const uint8_t key) const {
    for (uint32_t i = 0; i < compactCount; ++i) {
#ifdef ZENTRY
        auto p = getZentryKeyPtr(zens[i].load());
        if (p.second != nullptr) {
            if (p.first != key) {
                return std::make_tuple(p.second, p.first);
            }
        }
#else
        N *child = children[i].load();
        if (child != nullptr) {
            uint8_t k = keys[i].load();
            if (k != key) {
                return std::make_tuple(child, k);
            }
        }
#endif
    }
    return std::make_tuple(nullptr, 0);
}

// must read persisted child
void N4::getChildren(uint8_t start, uint8_t end,
                     std::tuple<uint8_t, N *> children[],
                     uint32_t &childrenCount) {
    childrenCount = 0;
    for (uint32_t i = 0; i < 4; ++i) {
#ifdef ZENTRY
        auto p = getZentryKeyPtr(zens[i]);
        if (p.first >= start && p.first <= end) {
            if (p.second != nullptr) {
                children[childrenCount] = std::make_tuple(p.first, p.second);
                childrenCount++;
            }
        }
#else
        uint8_t key = this->keys[i].load();
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

uint32_t N4::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < compactCount && cnt < 3; i++) {
#ifdef ZENTRY
        N *child = getZentryPtr(zens[i].load());
#else
        N *child = children[i].load();
#endif
        if (child != nullptr)
            cnt++;
    }
    return cnt;
}
void N4::graphviz_debug(std::ofstream &f) {
    char buf[1000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "N4 %d\n", level);
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