//
// Created by 潘许飞 on 2022/5.
//

#include "N4.h"
#include "LeafArray.h"
#include "N.h"
#include <algorithm>
#include <assert.h>

namespace PART_ns {

// 删除全部子节点
void N4::deleteChildren() {
    for (uint32_t i = 0; i < count; ++i) {
        //获取子节点地址，判断其是否为dirty
        N *child = N::clearDirty(children[i].load());
        //对子节点递归调用deleteChildren函数，删除全部子节点
        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
    count=0;
    compactCount=0;
}

//插入数据
//TIPS:key需要与现有Key不同，否则直接认为insert成功，不做实际的写入操作
bool N4::insert(uint8_t key, N *n, bool flush) {
    if (count==4) {
        return false;
    }

    // 从小到大的排序插入
    uint8_t tmpKey;
    N* tmpChild;
    int pos;
    for(pos=count;pos>=1;pos--){
        tmpKey = keys[pos-1].load();
        tmpChild = children[pos-1].load();
        if(tmpKey == key){
            //若已存在，则该node无需实际写入
            printf("N4::insert():Insert the same uint8_t key\n");
            return true;
        }else if(tmpKey > key){
            //将更大的Key往后移动
            keys[pos].store(tmpKey, std::memory_order_seq_cst);
            children[pos].store(tmpChild,std::memory_order_seq_cst);
            if (flush){
                flush_data((void *)&keys[pos], sizeof(std::atomic<uint8_t>));
                flush_data((void *)&children[pos], sizeof(std::atomic<N *>));
            }
        }else{
            keys[pos].store(key, std::memory_order_seq_cst);
            children[pos].store(n,std::memory_order_seq_cst);
            if (flush){
                flush_data((void *)&keys[pos], sizeof(std::atomic<uint8_t>));
                flush_data((void *)&children[pos], sizeof(std::atomic<N *>));
            }
            count++;
            compactCount++;
            return true;
        }
    }

    //到此处说明，新插入的Key是最小的Key
    keys[0].store(key, std::memory_order_seq_cst);
    children[0].store(n,std::memory_order_seq_cst);
    if (flush){
        flush_data((void *)&keys[0], sizeof(std::atomic<uint8_t>));
        flush_data((void *)&children[0], sizeof(std::atomic<N *>));
    }
        
    count++;
    compactCount++;
    return true;

}

//修改Key对应的子节点。（二分法）
void N4::change(uint8_t key, N *val) {
    int left=0;
    int right=count-1;
    while(left<=right){
        int middle = left + ((right-left)/2);
        N *child = children[middle].load();
        uint8_t num = keys[middle].load();
        if(num >key){
            right=middle-1;
        }else if(num<key){
            left=middle+1;
        }else{
            children[middle].store(val, std::memory_order_seq_cst);
            flush_data((void *)&children[middle], sizeof(std::atomic<N *>));
            return;
        }
    }
}

//根据key获取子节点地址(二分法)
N *N4::getChild(const uint8_t k) {
    int left=0;
    int right=count-1;
    while(left<=right){
        int middle = left + ((right-left)/2);
        N *child = children[middle].load();
        uint8_t num = keys[middle].load();
        if(num >k){
            right=middle-1;
        }else if(num<k){
            left=middle+1;
        }else{
            return child;
        }
    }
}

// 判断某个key在该节点内的范围（最大、最小、两者之间），若在2者之间，则返回小于该key的最大child
N *N4::checkKeyRange(uint8_t k,bool& hasSmaller,bool& hasBigger) const{
    if(keys[0].load() > k){
        hasSmaller = false;
        hasBigger = true;
        return nullptr;
    }else if(keys[count-1].load() < k){
        hasSmaller = true;
        hasBigger = false;
        return nullptr;
    }else{
        hasBigger = true;
        hasSmaller =true;
        return getMaxSmallerChild(k);
    }
}

// 获取最大的子节点
N *N4::getMaxChild() const {
    N *maxChild=children[count-1].load();
    return maxChild;
}

// 获取最小的子节点
N *N4::getMinChild() const {
    N *minChild=children[0].load();
    return minChild;
}

// 获取小于k的 最大的子节点
N *N4::getMaxSmallerChild(uint8_t k) const{
    uint8_t tmpKey;
    if(count==1){
        return children[0].load();
    }
    for(int i=1;i<count;i++){
        tmpKey = keys[i].load();
        if(tmpKey >= k){
            return children[i-1].load();
        }
    }
    return children[count-1].load();
}

//根据key，将对应项的数据清空
//Tips:为了保证数据连续且有序，需要移动之后的数据
bool N4::remove(uint8_t k, bool force, bool flush) {
    int left=0;
    int right=count-1;
    while(left<=right){
        int middle = left + ((right-left)/2);
        N *child = children[middle].load();
        uint8_t num = keys[middle].load();
        if(num >k){
            right=middle-1;
        }else if(num<k){
            left=middle+1;
        }else{
            children[middle].store(nullptr, std::memory_order_seq_cst);
            flush_data((void *)&children[middle], sizeof(std::atomic<N *>));
            count--;
            compactCount--;
            return true;
        }
    }
    return false;
}

//获取任意子节点。
//优先返回Leaf节点，否则返回最后一个位置的子节点
N *N4::getAnyChild() const {
    N *anyChild = nullptr;
    for (uint32_t i = 0; i < count; ++i) {
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

// in the critical section
// 遍历，返回 第一个 key与输入参数key不同的子节点的 地址和子节点key。
std::tuple<N *, uint8_t> N4::getSecondChild(const uint8_t key) const {
    for (uint32_t i = 0; i < count; ++i) {
        N *child = children[i].load();
        if (child != nullptr) {
            uint8_t k = keys[i].load();
            if (k != key) {
                return std::make_tuple(child, k);
            }
        }
    }
    return std::make_tuple(nullptr, 0);
}

// must read persisted child
// 根据start与end作为起始位置，获取所有子节点
void N4::getChildren(uint8_t start, uint8_t end,
                     std::tuple<uint8_t, N *> children[],
                     uint32_t &childrenCount) {
    childrenCount = 0;
    for (uint32_t i = 0; i < count; ++i) {
        uint8_t key = this->keys[i].load();
        if (key >= start && key <= end) {
            N *child = this->children[i].load();
            if (child != nullptr) {
                children[childrenCount] = std::make_tuple(key, child);
                childrenCount++;
            }
        }
    }
    // std::sort(children, children + childrenCount,
    //           [](auto &first, auto &second) {
    //               return std::get<0>(first) < std::get<0>(second);
    //           });
}

//返回子节点数目
uint32_t N4::getCount() const {
    // uint32_t cnt = 0;
    // for (uint32_t i = 0; i < compactCount && cnt < 3; i++) {

    //     N *child = children[i].load();

    //     if (child != nullptr)
    //         cnt++;
    // }
    // return cnt;
    return count;
}
//图形化Debug
void N4::graphviz_debug(std::ofstream &f) {
    char buf[1000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "N4 %d\n", level);
    auto pre = this->getPrefi();
    sprintf(buf + strlen(buf), "Prefix Len: %d\n", pre.prefixCount);
    sprintf(buf + strlen(buf), "Prefix: ");
    for (int i = 0; i < std::min(pre.prefixCount, maxStoredPrefixLength); i++) {
        sprintf(buf + strlen(buf), "%c ", pre.prefix[i]);
    }
    sprintf(buf + strlen(buf), "\n");
    sprintf(buf + strlen(buf), "count: %d\n", count);
    //sprintf(buf + strlen(buf), "compact: %d\n", compactCount);
    sprintf(buf + strlen(buf), "\"]\n");

    for (int i = 0; i < count; i++) {

        auto p = children[i].load();
        auto x = keys[i].load();

        if (p != nullptr) {

            auto addr = reinterpret_cast<uintptr_t>(p);
            if (isLeaf(p)) {
                addr = reinterpret_cast<uintptr_t>(getLeaf(p));
            }
            sprintf(buf + strlen(buf), "node%lx -- node%lx [label=\"%c\"]\n",
                    reinterpret_cast<uintptr_t>(this), addr, x);
        }
    }
    f << buf;

    for (int i = 0; i < count; i++) {

        auto p = children[i].load();

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