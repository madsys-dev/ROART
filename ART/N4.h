#pragma once

#include "N.h"

namespace PART_ns {

class N4 : public N {
  public:
#ifdef ZENTRY
    std::atomic<uintptr_t> zens[4];
#else
    std::atomic<uint8_t> keys[4];
    std::atomic<N *> children[4];
#endif
  public:
    N4(uint32_t level, const uint8_t *prefix, uint32_t prefixLength)
        : N(NTypes::N4, level, prefix, prefixLength) {
#ifdef ZENTRY
        memset(zens, 0, sizeof(zens));
#else
        memset(keys, 0, sizeof(keys));
        memset(children, 0, sizeof(children));
#endif
    }

    N4(uint32_t level, const Prefix &prefi) : N(NTypes::N4, level, prefi) {
#ifdef ZENTRY
        memset(zens, 0, sizeof(zens));
#else
        memset(keys, 0, sizeof(keys));
        memset(children, 0, sizeof(children));
#endif
    }

    virtual ~N4() {}

    bool insert(uint8_t key, N *n, bool flush);

    template <class NODE> void copyTo(NODE *n) const {
        for (uint32_t i = 0; i < compactCount; ++i) {
#ifdef ZENTRY
            auto z = zens[i].load();
            N *child = getZentryPtr(z);
            if (child != nullptr) {
                // not flush
                n->insert(getZentryKey(z), child, false);
            }
#else
            N *child = children[i].load();
            if (child != nullptr) {
                // not flush
                n->insert(keys[i].load(), child, false);
            }
#endif
        }
    }

    void change(uint8_t key, N *val);

    N *getChild(const uint8_t k);

    bool remove(uint8_t k, bool force, bool flush);

    N *getAnyChild() const;

    std::tuple<N *, uint8_t> getSecondChild(const uint8_t key) const;

    void deleteChildren();

    void getChildren(uint8_t start, uint8_t end,
                     std::tuple<uint8_t, N *> children[],
                     uint32_t &childrenCount);

    uint32_t getCount() const;

    void graphviz_debug(std::ofstream &f);
} __attribute__((aligned(64)));

} // namespace PART_ns