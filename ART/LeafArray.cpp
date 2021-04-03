//
// Created by 谢威宇 on 2021/4/3.
//

#include "LeafArray.h"
size_t PART_ns::LeafArray::getRightmostSetBit() const {
    auto b = bitmap.load();
    auto pos = b._Find_first();
    assert(pos < LeafArrayLength);
    return pos;
}
void PART_ns::LeafArray::setBit(size_t bit_pos, bool to) {
    auto b = bitmap.load();
    b[bit_pos] = to;
    bitmap.store(b);
}
uint16_t PART_ns::LeafArray::getFingerPrint(size_t pos) {
    auto x = reinterpret_cast<uint64_t>(leaf[pos].load());
    uint16_t re = x >> FingerPrintShift;
    return re;
}
