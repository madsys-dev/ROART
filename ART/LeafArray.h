//
// Created by 潘许飞 on 2022/5.
//

#ifndef P_ART_LEAFARRAY_H
#define P_ART_LEAFARRAY_H
#include "N.h"
#include <atomic>
#include <bitset>
#include <functional>

namespace PART_ns {
//leafarray的参数
const size_t LeafArrayLength = 64;      // 叶数组的容量
const size_t FingerPrintShift = 48;     // 指纹的偏移量
//leaf array类的定义
class LeafArray : public N {
  public:
    std::atomic<uintptr_t> leaf[LeafArrayLength];   // Leaf用于记录子节点地址
    std::atomic<std::bitset<LeafArrayLength>>
        bitmap;   // 位图用于存储使用情况
    // 新添加的双向指针
    std::atomic<LeafArray*> prev;
    std::atomic<LeafArray*> next;
    // 考虑到排序的开销较大，实际上只需要在Flush的时候进行一次排序即可，因此槽数组暂时未使用
    // 新添加的用于隐式排序的槽数组. uint8_t:2^8=256
    std::atomic<uint8_t> slot[LeafArrayLength];    // slot[i]记录第i小的键，在叶数组中所处的位置

  public:
    LeafArray(uint32_t level = -1) : N(NTypes::LeafArray, level, {}, 0) {
        bitmap.store(std::bitset<LeafArrayLength>{}.reset());
        memset(leaf, 0, sizeof(leaf));
        
        // 初始化双向指针
        prev.store(0);
        next.store(0);
        // 初始化槽数组
        memset(slot,0,sizeof(slot));
    }

    virtual ~LeafArray() {}

    //设置本leafarray节点的prev和next指针。默认会持久化该部分内容至NVM中。
    void setLinkedList(LeafArray* prev,LeafArray* next) {
      this->prev.store(prev);
      this->next.store(next);
      // 默认将双向指针持久化Flush到NVM中
      flush_data(&prev, sizeof(std::atomic<LeafArray*>));
      flush_data(&next, sizeof(std::atomic<LeafArray*>));
    }

    //设置本leafarray节点的prev和next指针
    void setLinkedList(LeafArray* prev,LeafArray* next ,bool flush) {
      this->prev.store(prev);
      this->next.store(next);
      // 如果参数flush为真，将双向指针持久化Flush到NVM中
      if(flush){
        flush_data(&prev, sizeof(std::atomic<LeafArray*>));
        flush_data(&next, sizeof(std::atomic<LeafArray*>));
      }
    }

    size_t getRightmostSetBit() const;

    void setBit(size_t bit_pos, bool to = true);

    uint16_t getFingerPrint(size_t pos) const;

    Leaf *getLeafAt(size_t pos) const;

    N *getAnyChild() const;

    static uintptr_t fingerPrintLeaf(uint16_t fingerPrint, Leaf *l);

    Leaf *lookup(const Key *k) const;

    bool update(const Key *k, Leaf *l);

    bool insert(Leaf *l, bool flush);
    bool radixLSMInsert(Leaf *l, bool flush);

    bool remove(const Key *k);

    void reload();

    uint32_t getCount() const;

    bool isFull() const;

    void splitAndUnlock(N *parentNode, uint8_t parentKey, bool &need_restart);

    void splitAndUnlock(N *parentNode, uint8_t parentKey, bool &need_restart, LeafArray* prev,LeafArray* next);

    std::vector<Leaf *> getSortedLeaf(const Key *start, const Key *end,
                                      int start_level, bool compare_start,
                                      bool compare_end);

    void graphviz_debug(std::ofstream &f);

} __attribute__((aligned(64)));
} // namespace PART_ns
#endif // P_ART_LEAFARRAY_H
