//
// Created by 谢威宇 on 2021/4/3.
//

#ifndef P_ART_LEAFARRAY_H
#define P_ART_LEAFARRAY_H
#include "N.h"
#include <atomic>
#include <bitset>

namespace PART_ns {

const size_t LeafArrayLength = 64;
const size_t FingerPrintShift = 48;

class LeafArray : public N {
  public:
    std::atomic<N *> leaf[LeafArrayLength];
    std::atomic<std::bitset<LeafArrayLength>> bitmap;

  public:
    LeafArray(uint32_t level, const uint8_t *prefix, uint32_t prefixLength)
        : N(NTypes::N256, level, prefix, prefixLength) {
        bitmap.store(std::bitset<LeafArrayLength>{}.set());
        memset(leaf, 0, sizeof(leaf));
    }

    LeafArray(uint32_t level, const Prefix &prefi)
        : N(NTypes::N256, level, prefi) {
        bitmap.store(std::bitset<LeafArrayLength>{}.set());
        memset(leaf, 0, sizeof(leaf));
    }

    virtual ~LeafArray() {}

    size_t getRightmostSetBit() const;

    void setBit(size_t bit_pos, bool to = true);

    uint16_t getFingerPrint(size_t pos);


    N *getChild(const uint8_t k);



} __attribute__((aligned(64)));
} // namespace PART_ns
#endif // P_ART_LEAFARRAY_H

