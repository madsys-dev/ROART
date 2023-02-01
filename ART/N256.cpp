//
// Created by 潘许飞 on 2022/5.
//

#include "N256.h"
#include "LeafArray.h"
#include "N.h"
#include <algorithm>
#include <assert.h>

namespace PART_ns {

// 删除所有子节点
void N256::deleteChildren() {
    for (uint64_t i = 0; i < 256; ++i) {
        N *child = N::clearDirty(children[i].load());
        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
    count=0;
    compactCount=0;
}

//插入数据
bool N256::insert(uint8_t key, N *val, bool flush) {
    // if (flush) {
    //     uint64_t oldp = (1ull << 56) | ((uint64_t)key << 48);
    // }

    children[key].store(val, std::memory_order_seq_cst);
    if (flush) {
        flush_data((void *)&children[key], sizeof(std::atomic<N *>));
    }

    count++;
    return true;
}

//修改Key对应的子节点。
void N256::change(uint8_t key, N *n) {

    children[key].store(n, std::memory_order_seq_cst);
    flush_data((void *)&children[key], sizeof(std::atomic<N *>));
}

//根据key获取子节点地址
N *N256::getChild(const uint8_t k) {
    N *child = children[k].load();
    return child;
}

// 判断某个key在该节点内的范围（最大、最小、两者之间），若在2者之间，则返回小于该key的最大child
N *N256::checkKeyRange(uint8_t k,bool& hasSmaller,bool& hasBigger) const{
    hasSmaller = false;
    hasBigger = false;
    N* res = nullptr;
    for(int i=k-1;i>=0;i--){
        res=children[i].load();
        if(res!=nullptr){
            hasSmaller = true;
            break;
        }
    }
    for(int i=k+1;i<255;i++){
        N* tmp=children[i].load();
        if(tmp!=nullptr){
            hasBigger = true;
            break;
        }
    }
    return res;
}

// 获取最大的子节点
N *N256::getMaxChild() const {
    N *maxChild=nullptr;
    for(uint8_t i=0;i<256;i++){
        maxChild=children[i].load();
        if(maxChild!=nullptr){
            return maxChild;
        }
    }
    
    return nullptr;
}

// 获取最小的子节点
N *N256::getMinChild() const {
    N *minChild=nullptr;
    for(uint8_t i=0;i<256;i++){
        minChild=children[i].load();
        if(minChild!=nullptr){
            return minChild;
        }
    }
    
    return nullptr;
}

// 获取小于k的 最大的子节点
N *N256::getMaxSmallerChild(uint8_t k) const{
    if(count==1){
        return getAnyChild();
    }
    
    for(uint8_t i=k-1;i!=0;i--){
        N* tmp = children[i].load();
        if(tmp!=nullptr){
            return tmp;
        }
    }
    return getAnyChild();
}

//根据key，将对应项的数据清空
bool N256::remove(uint8_t k, bool force, bool flush) {
    //不理解这部分的作用
    if (count <= 37 && !force) {
        return false;
    }

    children[k].store(nullptr, std::memory_order_seq_cst);
    flush_data((void *)&children[k], sizeof(std::atomic<N *>));
    count--;
    return true;
}

//获取任意子节点。
//优先返回Leaf节点，否则返回最后一个位置的子节点
N *N256::getAnyChild() const {
    N *anyChild = nullptr;
    for (uint64_t i = 0; i < 256; ++i) {
        N *child = children[i].load();

        if (child != nullptr) {
            if (N::isLeaf(child)) {
                return child;
            }
            anyChild = child;
        }
    }
    return anyChild;
}

// 根据start与end作为起始位置，获取所有子节点
void N256::getChildren(uint8_t start, uint8_t end,
                       std::tuple<uint8_t, N *> children[],
                       uint32_t &childrenCount) {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        N *child = this->children[i].load();

        if (child != nullptr) {
            children[childrenCount] = std::make_tuple(i, child);
            childrenCount++;
        }
    }
}

//返回子节点数目
uint32_t N256::getCount() const {
    // uint32_t cnt = 0;
    // for (uint32_t i = 0; i < 256 && cnt < 3; i++) {
    //     N *child = children[i].load();
    //     if (child != nullptr)
    //         cnt++;
    // }
    // return cnt;
    return count;
}
//图形化Debug
void N256::graphviz_debug(std::ofstream &f) {
    char buf[10000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "N256 %d\n",level);
    auto pre = this->getPrefi();
    sprintf(buf + strlen(buf), "Prefix Len: %d\n", pre.prefixCount);
    sprintf(buf + strlen(buf), "Prefix: ");
    for (int i = 0; i < std::min(pre.prefixCount, maxStoredPrefixLength); i++) {
        sprintf(buf + strlen(buf), "%c ", pre.prefix[i]);
    }
    sprintf(buf + strlen(buf), "\n");
    sprintf(buf + strlen(buf), "count: %d\n", count);
    sprintf(buf + strlen(buf), "compact: %d\n", compactCount);
    sprintf(buf + strlen(buf), "\"]\n");

    for (auto i = 0; i < 256; i++) {
        auto p = children[i].load();
        if (p != nullptr) {
            auto x = i;
            auto addr = reinterpret_cast<uintptr_t>(p);
            if (isLeaf(p)) {
                addr = reinterpret_cast<uintptr_t>(getLeaf(p));
            }
            sprintf(buf + strlen(buf), "node%lx -- node%lx [label=\"%c\"]\n",
                    reinterpret_cast<uintptr_t>(this), addr, x);
        }
    }
    f << buf;

    for (auto &i : children) {
        auto p = i.load();
        if (p != nullptr) {
            if (isLeaf(p)) {
#ifdef LEAF_ARRAY
                auto la = getLeafArray(p);
                la->graphviz_debug(f);
#else
                auto l = getLeaf(p);
                l->graphviz_debug(f);
#endif
            } else {
                N::graphviz_debug(f, p);
            }
        }
    }
}
} // namespace PART_ns