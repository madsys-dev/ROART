#pragma once

#include "N.h"

namespace PART_ns {

class N256 : public N {
  public:
    std::atomic<N *> children[256];

  public:
    N256(uint32_t level, const uint8_t *prefix, uint32_t prefixLength)
        : N(NTypes::N256, level, prefix, prefixLength) {
        memset(children, '\0', sizeof(children));
    }

    N256(uint32_t level, const Prefix &prefi) : N(NTypes::N256, level, prefi) {
        memset(children, '\0', sizeof(children));
    }

    virtual ~N256() {}

    bool insert(uint8_t key, N *val, bool flush);

    template <class NODE> void copyTo(NODE *n) const {
        for (int i = 0; i < 256; ++i) {
            N *child = children[i].load();
            if (child != nullptr) {
                // not flush
                n->insert(i, child, false);
            }
        }
    }

    void change(uint8_t key, N *n);

    N *getChild(const uint8_t k);

    bool remove(uint8_t k, bool force, bool flush);

    N *getAnyChild() const;

    void deleteChildren();

    void getChildren(uint8_t start, uint8_t end,
                     std::tuple<uint8_t, N *> children[],
                     uint32_t &childrenCount);

    uint32_t getCount() const;
} __attribute__((aligned(64)));

} // namespace PART_ns