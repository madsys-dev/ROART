#ifndef ART_KEY_H
#define ART_KEY_H

#include <assert.h>
#include <stdint.h>
#include <cstring>
#include <memory>

typedef struct Key {
    uint64_t value;
    size_t key_len;
    uint64_t key;
    uint8_t *fkey;

    Key(uint64_t key_ = 0, size_t key_len_ = 0, uint64_t value_ = 0) {
        value = value_;
        key_len = key_len_;
        key = key_;
        fkey = (uint8_t *)&key;
    }

    void Init(uint64_t key_, size_t key_len_, uint64_t value_) {
        value = value_;
        key_len = key_len_;
        key = key_;
        fkey = (uint8_t *)&key;
    }

    void Init(char *key_, size_t key_len_, char * value_){
        value = (uint64_t)value_;
        key_len = key_len_;
        fkey = (uint8_t *)key_;
    }

    inline Key *make_leaf(char *key, size_t key_len, uint64_t value);

    inline Key *make_leaf(uint64_t key, size_t key_len, uint64_t value);

    inline size_t getKeyLen() const;
} Key;

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

inline size_t Key::getKeyLen() const { return key_len; }

#endif  // ART_KEY_H