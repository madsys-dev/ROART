#include "N48.h"
#include "LeafArray.h"
#include "N.h"
#include <algorithm>
#include <assert.h>

namespace PART_ns {

bool N48::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 48) {
        return false;
    }

#ifdef ZENTRY
    childIndex[key].store(compactCount, std::memory_order_seq_cst);

    zens[compactCount].store(makeZentry(key, n));
    if (flush) {
        flush_data(&zens[compactCount], sizeof(std::atomic<uintptr_t>));
    }
#else
    childIndex[key].store(compactCount, std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&childIndex[key], sizeof(std::atomic<uint8_t>));

    children[compactCount].store(n, std::memory_order_seq_cst);
    if (flush) {
        flush_data((void *)&children[compactCount], sizeof(std::atomic<N *>));
    }
#endif

    compactCount++;
    count++;
    return true;
}

void N48::change(uint8_t key, N *val) {
    uint8_t index = childIndex[key].load();
    assert(index != emptyMarker);
#ifdef ZENTRY
    zens[index].store(makeZentry(key, val));
    flush_data(&zens[index], sizeof(std::atomic<uintptr_t>));
#else
    children[index].store(val, std::memory_order_seq_cst);
    flush_data((void *)&children[index], sizeof(std::atomic<N *>));
#endif
}

N *N48::getChild(const uint8_t k) {
    uint8_t index = childIndex[k].load();
    if (index == emptyMarker) {
        return nullptr;
    } else {
#ifdef ZENTRY
        auto child = getZentryPtr(zens[index].load());
#else
        N *child = children[index].load();
#endif
        return child;
    }
}

bool N48::remove(uint8_t k, bool force, bool flush) {
    if (count <= 12 && !force) {
        return false;
    }
    uint8_t index = childIndex[k].load();
    assert(index != emptyMarker);
#ifdef ZENTRY
    zens[index].store(0);
    flush_data(&zens[index], sizeof(std::atomic<uintptr_t>));
#else
    children[index].store(nullptr, std::memory_order_seq_cst);
    flush_data((void *)&children[index], sizeof(std::atomic<N *>));
#endif
    count--;
    assert(getChild(k) == nullptr);
    return true;
}

N *N48::getAnyChild() const {
    N *anyChild = nullptr;
    for (unsigned i = 0; i < 48; i++) {
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

void N48::deleteChildren() {
    for (unsigned i = 0; i < 256; i++) {
        uint8_t index = childIndex[i].load();
#ifdef ZENTRY
        auto child = getZentryPtr(zens[index].load());
        if (index != emptyMarker && child != nullptr) {
            N *child = N::clearDirty(child);
            N::deleteChildren(child);
            N::deleteNode(child);
        }
#else
        if (index != emptyMarker && children[index].load() != nullptr) {
            N *child = N::clearDirty(children[index].load());
            N::deleteChildren(child);
            N::deleteNode(child);
        }
#endif
    }
}

void N48::getChildren(uint8_t start, uint8_t end,
                      std::tuple<uint8_t, N *> children[],
                      uint32_t &childrenCount) {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        uint8_t index = this->childIndex[i].load();
#ifdef ZENTRY
        auto child = getZentryPtr(zens[index].load());
        if (index != emptyMarker && child != nullptr) {
            if (child != nullptr) {
                children[childrenCount] = std::make_tuple(i, child);
                childrenCount++;
            }
        }
#else
        if (index != emptyMarker && this->children[index] != nullptr) {
            N *child = this->children[index].load();

            if (child != nullptr) {
                children[childrenCount] = std::make_tuple(i, child);
                childrenCount++;
            }
        }
#endif
    }
}

uint32_t N48::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < 256 && cnt < 3; i++) {
        uint8_t index = childIndex[i].load();
#ifdef ZENTRY
        if (index != emptyMarker && getZentryPtr(zens[index].load()) != nullptr)
            cnt++;
#else
        if (index != emptyMarker && children[index].load() != nullptr)
            cnt++;
#endif
    }
    return cnt;
}
void N48::graphviz_debug(std::ofstream &f) {
    char buf[10000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "N48 %d\n", level);
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

    for (auto &i : childIndex) {
        auto ci = i.load();
        if (ci != emptyMarker) {
#ifdef ZENTRY
            auto pp = getZentryKeyPtr(zens[ci].load());
            auto p = pp.second;
            auto x = pp.first;
#else
            auto p = children[i].load();
            auto x = ci;
#endif
            if (p != nullptr) {
                auto addr = reinterpret_cast<uintptr_t>(p);
                if (isLeaf(p)) {
                    addr = reinterpret_cast<uintptr_t>(getLeaf(p));
                }
                sprintf(buf + strlen(buf),
                        "node%lx -- node%lx [label=\"%c\"]\n",
                        reinterpret_cast<uintptr_t>(this), addr, x);
            }
        }
    }
    f << buf;

    for (auto &i : childIndex) {
        auto ci = i.load();
        if (ci != emptyMarker) {
#ifdef ZENTRY
            auto p = getZentryPtr(zens[ci].load());
#else
            auto p = children[ci].load();
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
}
} // namespace PART_ns