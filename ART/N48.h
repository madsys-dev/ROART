//
// Created by 潘许飞 on 2022/5.
//

#pragma once

#include "N.h"

namespace PART_ns {

class N48 : public N {
  public:
    std::atomic<uint8_t> childIndex[256];   //根据Key作为Index，从childIndex中找到子节点在children中的Index

    std::atomic<N *> children[48];

  public:
    static const uint8_t emptyMarker = 48;  //填充empty entry的默认数值（因为N48节点，只有children[0-47]存储子节点位置）

    N48(uint32_t level, const uint8_t *prefix, uint32_t prefixLength)
        : N(NTypes::N48, level, prefix, prefixLength) {
        memset(childIndex, emptyMarker, sizeof(childIndex));    //填充默认值

        memset(children, 0, sizeof(children));

    }

    N48(uint32_t level, const Prefix &prefi) : N(NTypes::N48, level, prefi) {
        memset(childIndex, emptyMarker, sizeof(childIndex));    //填充默认值

        memset(children, 0, sizeof(children));

    }

    virtual ~N48() {}

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

    N *getChild(const uint8_t k);

    N *checkKeyRange(uint8_t k,bool& hasSmaller,bool& hasBigger) const;

    N *getMaxChild() const;
    
    N *getMinChild() const;

    bool remove(uint8_t k, bool force, bool flush);

    N *getAnyChild() const;

    N *getMaxSmallerChild(uint8_t k) const;

    void deleteChildren();

    void getChildren(uint8_t start, uint8_t end,
                     std::tuple<uint8_t, N *> children[],
                     uint32_t &childrenCount);

    uint32_t getCount() const;

    void graphviz_debug(std::ofstream &f);
} __attribute__((aligned(64)));

} // namespace PART_ns