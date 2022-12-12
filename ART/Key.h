//
// Created by 潘许飞 on 2022/5.
//

#ifndef ART_KEY_H
#define ART_KEY_H

#include <assert.h>
#include <cstring>
#include <memory>
#include <stdint.h>

namespace PART_ns {

// 存储键值对的类
struct Key {
    uint64_t value;     // 值Value是8字节无符号整型
    size_t key_len;     // 记录主键Key的长度。
    size_t val_len;     // 记录值Value的长度
    uint64_t key;       // 使用8字节无符号整型存储Key
    uint8_t *fkey;      // 使用指针记录存储Key的地址
    bool DelFlag;       // 本方案实现中，将删除操作认为是一种特殊的数据插入操作，需要将Key类型设置为Deleted。

    Key() {}

    Key(uint64_t key_, size_t key_len_, uint64_t value_) {
        value = value_;
        key_len = key_len_;
        val_len = sizeof(uint64_t);
        key = key_;
        fkey = (uint8_t *)&key;
        DelFlag = false;
    }

    // 删除操作的Key类型，无Value，DelFlag标记为true
    Key(uint64_t key_, size_t key_len_, bool DelFlag) {
        //value = value_;
        key_len = key_len_;
        val_len = 0;
        key = key_;
        fkey = (uint8_t *)&key;
        DelFlag = true;
    }

    void Init(uint64_t key_, size_t key_len_, uint64_t value_) {
        value = value_;
        key_len = key_len_;
        val_len = sizeof(uint64_t);
        key = key_;
        fkey = (uint8_t *)&key;
        DelFlag = false;
    }

    void Init(char *key_, size_t key_len_, char *value_, size_t val_len_) {
        val_len = val_len_;
        value = (uint64_t)value_;
        key_len = key_len_;
        fkey = (uint8_t *)key_;
        DelFlag = false;
    }

    // 将该Key标记为 删除操作的Key
    inline void setDelKey();

    inline Key *make_leaf(char *key, size_t key_len, uint64_t value);

    inline Key *make_leaf(uint64_t key, size_t key_len, uint64_t value);

    inline size_t getKeyLen() const;

    inline uint16_t getFingerPrint() const;
} __attribute__((aligned(64)));

// 将Key标记为 删除操作的Key
inline void setDelKey(){
    this->DelFlag = true ;
    // 由于删除操作，所以无需Value
    this->val_len = 0;
}

// 生成叶子节点，并赋值
inline Key *Key::make_leaf(char *key, size_t key_len, uint64_t value) {
    void *aligned_alloc;
    posix_memalign(&aligned_alloc, 64, sizeof(Key) + key_len);
    Key *k = reinterpret_cast<Key *>(aligned_alloc);

    k->value = value;
    k->key_len = key_len;
    memcpy(k->fkey, key, key_len);

    return k;
}

// 生成叶子节点，并赋值
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

// 根据fkey中存储的Key获取指纹
inline uint16_t Key::getFingerPrint() const {
    uint16_t re = 0;
    for (int i = 0; i < key_len; i++) {
        re = re * 131 + this->fkey[i];
    }
    return re;
}

// 根据fkey中存储的Key获取哈希值（主要用于：LeafArray快速寻找Expected_Pos时，发生哈希冲突的二次哈希）
inline uint16_t Key::getHash() const {
    uint16_t re = 0;
    for (int i = 0; i < key_len; i++) {
        re = re * 173 + this->fkey[i];
    }
    return re;
}
} // namespace PART_ns

#endif // ART_KEY_H