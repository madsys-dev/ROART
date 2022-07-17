//
// Created by 潘许飞 on 2022/5.
//

#include "N16.h"
#include "LeafArray.h"
#include "N.h"
#include <algorithm>
#include <assert.h>

namespace PART_ns {

// 删除全部子节点
void N16::deleteChildren() {
    for (uint32_t i = 0; i < count; ++i) {

        N *child = N::clearDirty(children[i].load());

        if (child != nullptr) {
            N::deleteChildren(child);
            N::deleteNode(child);
        }
    }
}

//插入数据
bool N16::insert(uint8_t key, N *n, bool flush) {
    if (count==16) {
        return false;
    }

    // 从小到大的排序插入
    uint8_t tmpKey;
    N* tmpChild;
    int pos;
    for(pos=count;pos>=1;pos--){
        tmpKey = keys[pos-1].load();
        tmpChild = children[pos-1].load();
        if(tmpKey > key){
            keys[pos].store(tmpKey, std::memory_order_seq_cst);
            children[pos].store(tmpChild,std::memory_order_seq_cst)
            if (flush){
                flush_data((void *)&keys[pos], sizeof(std::atomic<uint8_t>));
                flush_data((void *)&children[pos], sizeof(std::atomic<N *>));
            }
                
        }else{
            keys[pos].store(key, std::memory_order_seq_cst);
            children[pos].store(n,std::memory_order_seq_cst)
            if (flush){
                flush_data((void *)&keys[pos], sizeof(std::atomic<uint8_t>));
                flush_data((void *)&children[pos], sizeof(std::atomic<N *>));
            }
            count++;
            return true;
        }
    }

    //到此处说明，新插入的Key是最小的Key
    keys[0].store(key, std::memory_order_seq_cst);
    children[0].store(n,std::memory_order_seq_cst)
    if (flush){
        flush_data((void *)&keys[0], sizeof(std::atomic<uint8_t>));
        flush_data((void *)&children[0], sizeof(std::atomic<N *>));
    }
        
    count++;
    return true;

}

void N16::change(uint8_t key, N *val) {
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

N *N16::getChild(const uint8_t k) {
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

bool N16::remove(uint8_t k, bool force, bool flush) {
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
            return true;
        }
    }
    return false;
}

N *N16::getAnyChild() const {
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
std::tuple<N *, uint8_t> N16::getSecondChild(const uint8_t key) const {
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
void N16::getChildren(uint8_t start, uint8_t end,
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

uint32_t N16::getCount() const {
    // uint32_t cnt = 0;
    // for (uint32_t i = 0; i < compactCount && cnt < 3; i++) {

    //     N *child = children[i].load();

    //     if (child != nullptr)
    //         cnt++;
    // }
    // return cnt;
    return count;
}
void N16::graphviz_debug(std::ofstream &f) {
    char buf[1000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "N16 %d\n", level);
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




// #include "N16.h"
// #include "LeafArray.h"
// #include "N.h"
// #include <algorithm>
// #include <assert.h>
// #include <emmintrin.h> // x86 SSE intrinsics

// namespace PART_ns {

// bool N16::insert(uint8_t key, N *n, bool flush) {
//     // 若已满，则返回false
//     if (compactCount == 16) {
//         return false;
//     }
//     //存储key的flipsign
//     keys[compactCount].store(flipSign(key), std::memory_order_seq_cst);
//     if (flush)
//         flush_data((void *)&keys[compactCount], sizeof(std::atomic<uint8_t>));

//     children[compactCount].store(n, std::memory_order_seq_cst);
//     if (flush)
//         flush_data((void *)&children[compactCount], sizeof(std::atomic<N *>));

//     compactCount++;
//     count++;
//     return true;
// }

// void N16::change(uint8_t key, N *val) {
//     auto childPos = getChildPos(key);
//     assert(childPos != -1);

//     children[childPos].store(val, std::memory_order_seq_cst);
//     flush_data(&children[childPos], sizeof(std::atomic<N *>));

// }

// int N16::getChildPos(const uint8_t k) {
//     __m128i cmp = _mm_cmpeq_epi8(
//         _mm_set1_epi8(flipSign(k)),
//         _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
//     unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << compactCount) - 1);
//     while (bitfield) {
//         uint8_t pos = ctz(bitfield);
//         if (children[pos].load() != nullptr) {
//             return pos;
//         }
//         bitfield = bitfield ^ (1 << pos);
//     }
//     return -1;
// }

// N *N16::getChild(const uint8_t k) {
//     __m128i cmp = _mm_cmpeq_epi8(
//         _mm_set1_epi8(flipSign(k)),
//         _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys)));
//     unsigned bitfield = _mm_movemask_epi8(cmp) & ((1 << 16) - 1);
//     while (bitfield) {
//         uint8_t pos = ctz(bitfield);

//         N *child = children[pos].load();
//         if (child != nullptr && keys[pos].load() == flipSign(k)) {
//             return child;
//         }
//         bitfield = bitfield ^ (1 << pos);
//     }
//     return nullptr;
// }

// bool N16::remove(uint8_t k, bool force, bool flush) {
//     if (count <= 3 && !force) {
//         return false;
//     }
//     auto leafPlace = getChildPos(k);
//     assert(leafPlace != -1);

//     children[leafPlace].store(nullptr, std::memory_order_seq_cst);
//     flush_data((void *)&children[leafPlace], sizeof(std::atomic<N *>));

//     count--;
//     assert(getChild(k) == nullptr);
//     return true;
// }

// N *N16::getAnyChild() const {
//     N *anyChild = nullptr;
//     for (int i = 0; i < 16; ++i) {
//         N *child = children[i].load();
//         if (child != nullptr) {
//             if (N::isLeaf(child)) {
//                 return child;
//             }
//             anyChild = child;
//         }
//     }
//     return anyChild;
// }

// void N16::deleteChildren() {
//     for (std::size_t i = 0; i < compactCount; ++i) {
//         N *child = N::clearDirty(children[i].load());
//         if (child != nullptr) {
//             N::deleteChildren(child);
//             N::deleteNode(child);
//         }
//     }
// }

// void N16::getChildren(uint8_t start, uint8_t end,
//                       std::tuple<uint8_t, N *> children[],
//                       uint32_t &childrenCount) {
//     childrenCount = 0;
//     for (int i = 0; i < compactCount; ++i) {
//         uint8_t key = flipSign(this->keys[i].load());
//         if (key >= start && key <= end) {
//             N *child = this->children[i].load();
//             if (child != nullptr) {
//                 children[childrenCount] = std::make_tuple(key, child);
//                 childrenCount++;
//             }
//         }
//     }
//     std::sort(children, children + childrenCount,
//               [](auto &first, auto &second) {
//                   return std::get<0>(first) < std::get<0>(second);
//               });
// }

// uint32_t N16::getCount() const {
//     uint32_t cnt = 0;
//     for (uint32_t i = 0; i < compactCount && cnt < 3; i++) {
//         N *child = children[i].load();
//         if (child != nullptr)
//             ++cnt;
//     }
//     return cnt;
// }
// void N16::graphviz_debug(std::ofstream &f) {
//     char buf[10000] = {};
//     sprintf(buf + strlen(buf), "node%lx [label=\"",
//             reinterpret_cast<uintptr_t>(this));
//     sprintf(buf + strlen(buf), "N16 %d\n", level);
//     auto pre = this->getPrefi();
//     sprintf(buf + strlen(buf), "Prefix Len: %d\n", pre.prefixCount);
//     sprintf(buf + strlen(buf), "Prefix: ");
//     for (int i = 0; i < std::min(pre.prefixCount, maxStoredPrefixLength); i++) {
//         sprintf(buf + strlen(buf), "%c ", pre.prefix[i]);
//     }
//     sprintf(buf + strlen(buf), "\n");
//     sprintf(buf + strlen(buf), "count: %d\n", count);
//     sprintf(buf + strlen(buf), "compact: %d\n", compactCount);
//     sprintf(buf + strlen(buf), "\"]\n");

//     for (int i = 0; i < compactCount; i++) {
//         auto p = children[i].load();
//         auto x = keys[i].load();
//         if (p != nullptr) {
//             x = flipSign(x);
//             auto addr = reinterpret_cast<uintptr_t>(p);
//             if (isLeaf(p)) {
//                 addr = reinterpret_cast<uintptr_t>(getLeaf(p));
//             }
//             sprintf(buf + strlen(buf), "node%lx -- node%lx [label=\"%c\"]\n",
//                     reinterpret_cast<uintptr_t>(this), addr, x);
//         }
//     }
//     f << buf;

//     for (int i = 0; i < compactCount; i++) {
//         auto p = children[i].load();
//         if (p != nullptr) {
//             if (isLeaf(p)) {
// #ifdef LEAF_ARRAY
//                 auto la = getLeafArray(p);
//                 la->graphviz_debug(f);
// #else
//                 auto l = getLeaf(p);
//                 l->graphviz_debug(f);
// #endif
//             } else {
//                 N::graphviz_debug(f, p);
//             }
//         }
//     }
// }

// } // namespace PART_ns