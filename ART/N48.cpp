//
// Created by 潘许飞 on 2022/5.
//

#include "N48.h"
#include "LeafArray.h"
#include "N.h"
#include <algorithm>
#include <assert.h>

namespace PART_ns {

//插入子节点
bool N48::insert(uint8_t key, N *n, bool flush) {
    if (compactCount == 48) {
        return false;
    }

    childIndex[key].store(compactCount, std::memory_order_seq_cst);
    if (flush)
        flush_data((void *)&childIndex[key], sizeof(std::atomic<uint8_t>));

    children[compactCount].store(n, std::memory_order_seq_cst);
    if (flush) {
        flush_data((void *)&children[compactCount], sizeof(std::atomic<N *>));
    }


    compactCount++;
    count++;
    return true;
}

// 根据uint8_t key修改其对应的子节点
void N48::change(uint8_t key, N *val) {
    uint8_t index = childIndex[key].load();
    assert(index != emptyMarker);

    if(index!=emptyMarker){
        children[index].store(val, std::memory_order_seq_cst);
        flush_data((void *)&children[index], sizeof(std::atomic<N *>));
    }
}

// 根据uint8_t key获取子节点的地址
N *N48::getChild(const uint8_t k) {
    uint8_t index = childIndex[k].load();
    if (index == emptyMarker) {
        return nullptr;
    } else {
        N *child = children[index].load();
        return child;
    }
}

// 判断某个key在该节点内的范围（最大、最小、两者之间），若在2者之间，则返回小于该key的最大child
N *N48::checkKeyRange(uint8_t k,bool& hasSmaller,bool& hasBigger) const{
    hasSmaller = false;
    hasBigger = false;
    N* res = nullptr;
    for(int i=k-1;i>=0;i--){
        uint8_t index = childIndex[i].load();

        if (index != emptyMarker){
            res=children[index].load();
            if(res!=nullptr){
                hasSmaller = true;
                break;
            }
        }  
    }
    for(int i=k+1;i<255;i++){
        uint8_t index = childIndex[i].load();

        if (index != emptyMarker){
            N* tmp=children[index].load();
            if(tmp!=nullptr){
                hasBigger = true;
                break;
            }
        }  
    }
    return res;
}

// 获取最大的子节点
N *N48::getMaxChild() const {
    N *maxChild=nullptr;
    for(unsigned i=255;i>=0;i--){
        uint8_t index = childIndex[i].load();

        if (index != emptyMarker){
            maxChild=children[index].load();
            if(maxChild!=nullptr){
                return maxChild;
            }
        }
    }
    return nullptr;
}

// 获取最小的子节点
N *N48::getMinChild() const {
    N *minChild=nullptr;
    for(unsigned i=0;i<256;i++){
        uint8_t index = childIndex[i].load();

        if (index != emptyMarker){
            minChild=children[index].load();
            if(minChild!=nullptr){
                return minChild;
            }
        }
    }
    return nullptr;
}

// 获取小于k的 最大的子节点
N *N48::getMaxSmallerChild(uint8_t k) const{
    if(count==1){
        return getAnyChild();
    }
    
    for(uint8_t i=k-1;i!=0;i--){
        uint8_t index = childIndex[i].load();
        if(index != emptyMarker){
            N* tmp = children[index].load();
            if(tmp!=nullptr){
                return tmp;
            }
        }
    }
    return getAnyChild();
}

// 根据uint8_t key删除对应的子节点信息
bool N48::remove(uint8_t k, bool force, bool flush) {
    //这一步不太理解为何要做这样的判断
    if (count <= 12 && !force) {
        return false;
    }

    uint8_t index = childIndex[k].load();
    assert(index != emptyMarker);
    if(index!=emptyMarker){
        children[index].store(nullptr, std::memory_order_seq_cst);
        flush_data((void *)&children[index], sizeof(std::atomic<N *>));
    }

    childIndex[k].store(emptyMarker,std::memory_order_seq_cst);

    count--;
    compactCount--;
    assert(getChild(k) == nullptr);
    return true;
}

//获取任意子节点。
//优先返回Leaf节点，否则返回已存在数据中最大Key对应的子节点
N *N48::getAnyChild() const {
    N *anyChild = nullptr;
    for (unsigned i = 0; i < 48; i++) {

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

//删除所有子节点
void N48::deleteChildren() {
    for (unsigned i = 0; i < 256; i++) {
        uint8_t index = childIndex[i].load();

        if (index != emptyMarker && children[index].load() != nullptr) {
            N *child = N::clearDirty(children[index].load());
            N::deleteChildren(child);
            N::deleteNode(child);
        }

    }
    count=0;
    compactCount=0;
}

// 根据start与end作为起始位置，获取所有子节点,实际上是比较Key的值是否在start与end之间
void N48::getChildren(uint8_t start, uint8_t end,
                      std::tuple<uint8_t, N *> children[],
                      uint32_t &childrenCount) {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        uint8_t index = this->childIndex[i].load();

        if (index != emptyMarker && this->children[index] != nullptr) {
            N *child = this->children[index].load();

            if (child != nullptr) {
                children[childrenCount] = std::make_tuple(i, child);
                childrenCount++;
            }
        }

    }
}

// 返回子节点数目
uint32_t N48::getCount() const {
    uint32_t cnt = 0;
    for (uint32_t i = 0; i < 256 && cnt < 3; i++) {
        uint8_t index = childIndex[i].load();

        if (index != emptyMarker && children[index].load() != nullptr)
            cnt++;

    }
    return cnt;
}
//图形化Debug
void N48::graphviz_debug(std::ofstream &f) {
    char buf[10000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "N48 %d\n", level);
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

    for (auto &i : childIndex) {
        auto ci = i.load();
        if (ci != emptyMarker) {
            auto p = children[i].load();
            auto x = ci;
            if (p != nullptr) {
                auto addr = reinterpret_cast<uintptr_t>(p);
                if (isLeaf(p)) {
                    addr = reinterpret_cast<uintptr_t>(getLeaf(p));
                }
                sprintf(buf + strlen(buf),
                        "node%lx -- node%lx [label=\"%c\"]\n",
                        reinterpret_cast<uintptr_t>(this), addr, x);
            }
        }
    }
    f << buf;

    for (auto &i : childIndex) {
        auto ci = i.load();
        if (ci != emptyMarker) {
            auto p = children[ci].load();
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
}
} // namespace PART_ns