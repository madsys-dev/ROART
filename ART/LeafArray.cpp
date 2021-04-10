//
// Created by 谢威宇 on 2021/4/3.
//

#include "LeafArray.h"

namespace PART_ns {
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
uint16_t PART_ns::LeafArray::getFingerPrint(size_t pos) const {
    auto x = reinterpret_cast<uint64_t>(leaf[pos].load());
    uint16_t re = x >> FingerPrintShift;
    return re;
}
Leaf *LeafArray::lookup(const Key *k) const {
    uint16_t finger_print = k->getFingerPrint();
    auto b = bitmap.load();
    auto i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<Leaf *>(
                fingerprint_ptr ^
                (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            if (finger_print == thisfp && ptr->checkKey(k)) {
                return ptr;
            }
        }
        i = b._Find_next(i);
    }

    return nullptr;
}
bool LeafArray::insert(Leaf *l, bool flush) {

    auto b = bitmap.load();
    b.flip();
    auto pos = b._Find_first();
    if (pos < LeafArrayLength) {
        b.flip();
        b[pos] = true;
        bitmap.store(b);
        auto s =
            (static_cast<uintptr_t>(l->getFingerPrint()) << FingerPrintShift) |
            (reinterpret_cast<uintptr_t>(l));
        leaf[pos].store(s);
        if (flush)
            flush_data((void *)&leaf[pos], sizeof(leaf[pos]));

        return true;
    } else {
        return false;
    }
}
bool LeafArray::remove(const Key *k) {
    uint16_t finger_print = k->getFingerPrint();
    auto b = bitmap.load();
    auto i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<Leaf *>(
                fingerprint_ptr ^
                (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            if (finger_print == thisfp && ptr->checkKey(k)) {
                leaf[i].store(0);
                flush_data(&leaf[i], sizeof(leaf[i]));
                b[i] = false;
                bitmap.store(b);
                return true;
            }
        }
        i = b._Find_next(i);
    }
    return false;
}
void LeafArray::reload() {
    auto b = bitmap.load();
    for (int i = 0; i < LeafArrayLength; i++) {
        if (leaf[i].load() != 0) {
            b[i] = true;
        } else {
            b[i] = false;
        }
    }
    bitmap.store(b);
}
void LeafArray::graphviz_debug(std::ofstream &f) {
    char buf[1000] = {};
    sprintf(buf + strlen(buf), "%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "LeafArray\n");
    sprintf(buf + strlen(buf), "count: %zu\n", bitmap.load().count());
    sprintf(buf + strlen(buf), "\"]\n");

    auto b = bitmap.load();
    auto i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<Leaf *>(
                fingerprint_ptr ^
                (static_cast<uintptr_t>(thisfp) << FingerPrintShift));

            sprintf(buf + strlen(buf), "%lx -- %lx \n",
                    reinterpret_cast<uintptr_t>(this),
                    reinterpret_cast<uintptr_t>(ptr));
        }
        i = b._Find_next(i);
    }

    f << buf;

    b = bitmap.load();
    i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<Leaf *>(
                fingerprint_ptr ^
                (static_cast<uintptr_t>(thisfp) << FingerPrintShift));

            ptr->graphviz_debug(f);
        }
        i = b._Find_next(i);
    }
}

} // namespace PART_ns