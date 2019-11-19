#pragma once

#include "N.h"

namespace PART_ns {
class N4 : public N {
  public:
    std::atomic<uint8_t> keys[4];
    std::atomic<N *> children[4];

  public:
    N4(uint32_t level, const uint8_t *prefix, uint32_t prefixLength)
        : N(NTypes::N4, level, prefix, prefixLength) {
        memset(keys, 0, sizeof(keys));
        memset(children, 0, sizeof(children));
    }

    N4(uint32_t level, const Prefix &prefi) : N(NTypes::N4, level, prefi) {
        memset(keys, 0, sizeof(keys));
        memset(children, 0, sizeof(children));
    }

    virtual ~N4() {}

    bool insert(uint8_t key, N *n, bool flush);

    template <class NODE> void copyTo(NODE *n) const {
        for (uint32_t i = 0; i < compactCount; ++i) {
            N *child = children[i].load();
            if (child != nullptr) {
                // not flush
                n->insert(keys[i].load(), child, false);
            }
        }
    }

    void change(uint8_t key, N *val);

    std::atomic<N *> *getChild(const uint8_t k);

    bool remove(uint8_t k, bool force, bool flush);

    N *getAnyChild() const;

    std::tuple<N *, uint8_t> getSecondChild(const uint8_t key) const;

    void deleteChildren();

    void getChildren(uint8_t start, uint8_t end,
                     std::tuple<uint8_t, std::atomic<N *> *> children[],
                     uint32_t &childrenCount);

    uint32_t getCount() const;
} __attribute__((aligned(64)));

} // namespace PART_ns