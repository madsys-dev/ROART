#ifndef ART_KEY_H
#define ART_KEY_H

#include <assert.h>
#include <cstring>
#include <memory>
#include <stdint.h>

namespace PART_ns {

struct Key {
    uint64_t value;
    size_t key_len;
    size_t val_len;
    uint64_t key;
    uint8_t *fkey;

    Key() {}

    Key(uint64_t key_, size_t key_len_, uint64_t value_) {
        value = value_;
        key_len = key_len_;
        val_len = sizeof(uint64_t);
        key = key_;
        fkey = (uint8_t *)&key;
    }

    void Init(uint64_t key_, size_t key_len_, uint64_t value_) {
        value = value_;
        key_len = key_len_;
        val_len = sizeof(uint64_t);
        key = key_;
        fkey = (uint8_t *)&key;
    }

    void Init(char *key_, size_t key_len_, char *value_, size_t val_len_) {
        val_len = val_len_;
        value = (uint64_t)value_;
        key_len = key_len_;
        fkey = (uint8_t *)key_;
    }

    inline Key *make_leaf(char *key, size_t key_len, uint64_t value);

    inline Key *make_leaf(uint64_t key, size_t key_len, uint64_t value);

    inline size_t getKeyLen() const;

    uint16_t getFingerPrint();
} __attribute__((aligned(64)));

inline Key *Key::make_leaf(char *key, size_t key_len, uint64_t value) {
    void *aligned_alloc;
    posix_memalign(&aligned_alloc, 64, sizeof(Key) + key_len);
    Key *k = reinterpret_cast<Key *>(aligned_alloc);

    k->value = value;
    k->key_len = key_len;
    memcpy(k->fkey, key, key_len);

    return k;
}

inline Key *Key::make_leaf(uint64_t key, size_t key_len, uint64_t value) {
    void *aligned_alloc;
    posix_memalign(&aligned_alloc, 64, sizeof(Key) + key_len);
    Key *k = reinterpret_cast<Key *>(aligned_alloc);

    k->value = value;
    k->key_len = key_len;
    reinterpret_cast<uint64_t *>(&k->fkey[0])[0] = __builtin_bswap64(key);

    return k;
}

inline size_t Key::getKeyLen() const {
    return key_len;
}

inline uint16_t Key::getFingerPrint() {
    uint16_t re = 0;
    for (int i = 0; i < key_len; i++) {
        re = re * 131 + this->fkey[i];
    }
    return re;
}
} // namespace PART_ns

#endif // ART_KEY_H