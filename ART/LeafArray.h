//
// Created by 谢威宇 on 2021/4/3.
//

#ifndef P_ART_LEAFARRAY_H
#define P_ART_LEAFARRAY_H
#include "N.h"
#include <atomic>
#include <bitset>
#include <functional>

namespace PART_ns {

const size_t LeafArrayLength = 64;
const size_t FingerPrintShift = 48;

class LeafArray : public N {
  public:
    std::atomic<uintptr_t> leaf[LeafArrayLength];
    std::atomic<std::bitset<LeafArrayLength>>
        bitmap; // 0 means used slot; 1 means empty slot

  public:
    LeafArray(uint32_t level = -1) : N(NTypes::LeafArray, level, {}, 0) {
        bitmap.store(std::bitset<LeafArrayLength>{}.reset());
        memset(leaf, 0, sizeof(leaf));
    }

    virtual ~LeafArray() {}

    size_t getRightmostSetBit() const;

    void setBit(size_t bit_pos, bool to = true);

    uint16_t getFingerPrint(size_t pos) const;

    Leaf *getLeafAt(size_t pos);

    Leaf *lookup(const Key *k) const;

    bool insert(Leaf *l, bool flush);

    bool remove(const Key *k);

    void reload();

    uint32_t getCount() const;

    bool isFull() const;

    void splitAndUnlock(N *parentNode, uint8_t parentKey, bool &need_restart);

    std::vector<Leaf *> getSortedLeaf(const Key *start, const Key *end);

    void graphviz_debug(std::ofstream &f);



} __attribute__((aligned(64)));
} // namespace PART_ns
#endif // P_ART_LEAFARRAY_H
