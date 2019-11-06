#pragma once

#include "N.h"

namespace PART_ns {
class N48 : public N {
  public:
    std::atomic<uint8_t> childIndex[256];
    std::atomic<N *> children[48];

  public:
    static const uint8_t emptyMarker = 48;

    N48(uint32_t level, const uint8_t *prefix, uint32_t prefixLength)
        : N(NTypes::N48, level, prefix, prefixLength) {
        memset(childIndex, emptyMarker, sizeof(childIndex));
        memset(children, 0, sizeof(children));
    }

    N48(uint32_t level, const Prefix &prefi) : N(NTypes::N48, level, prefi) {
        memset(childIndex, emptyMarker, sizeof(childIndex));
        memset(children, 0, sizeof(children));
    }

    bool insert(uint8_t key, N *n, bool flush);

    template <class NODE> void copyTo(NODE *n) const {
        for (unsigned i = 0; i < 256; i++) {
            uint8_t index = childIndex[i].load();
            if (index != emptyMarker && children[index].load() != nullptr) {
                // not flush
                n->insert(i, children[index].load(), false);
            }
        }
    }

    void change(uint8_t key, N *val);

    std::atomic<N *> *getChild(const uint8_t k);

    bool remove(uint8_t k, bool force, bool flush);

    N *getAnyChild() const;

    void deleteChildren();

    void getChildren(uint8_t start, uint8_t end,
                     std::tuple<uint8_t, std::atomic<N *> *> children[],
                     uint32_t &childrenCount);

    uint32_t getCount() const;
}__attribute__((aligned(64)));

} // namespace PART_ns