#pragma once

#include "N.h"

namespace PART_ns {
class N16 : public N {
  public:
    std::atomic<uint8_t> keys[16];
    std::atomic<N *> children[16];

    static uint8_t flipSign(uint8_t keyByte) {
        // Flip the sign bit, enables signed SSE comparison of unsigned values,
        // used by Node16
        return keyByte ^ 128;
    }

    static inline unsigned ctz(uint16_t x) {
        // Count trailing zeros, only defined for x>0
#ifdef __GNUC__
        return __builtin_ctz(x);
#else
        // Adapted from Hacker's Delight
        unsigned n = 1;
        if ((x & 0xFF) == 0) {
            n += 8;
            x = x >> 8;
        }
        if ((x & 0x0F) == 0) {
            n += 4;
            x = x >> 4;
        }
        if ((x & 0x03) == 0) {
            n += 2;
            x = x >> 2;
        }
        return n - (x & 1);
#endif
    }

    std::atomic<N *> *getChildPos(const uint8_t k);

  public:
    N16(uint32_t level, const uint8_t *prefix, uint32_t prefixLength)
        : N(NTypes::N16, level, prefix, prefixLength) {
        memset(keys, 0, sizeof(keys));
        memset(children, 0, sizeof(children));
    }

    N16(uint32_t level, const Prefix &prefi) : N(NTypes::N16, level, prefi) {
        memset(keys, 0, sizeof(keys));
        memset(children, 0, sizeof(children));
    }

    virtual ~N16(){}

    bool insert(uint8_t key, N *n, bool flush);

    template <class NODE> void copyTo(NODE *n) const {
        for (unsigned i = 0; i < compactCount; i++) {
            N *child = children[i].load();
            if (child != nullptr) {
                // not flush
                n->insert(flipSign(keys[i].load()), child, false);
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
} __attribute__((aligned(64)));

} // namespace PART_ns