//
// Created by 潘许飞 on 2022/5.
//

#include "LeafArray.h"
#include "EpochGuard.h"
#include "threadinfo.h"

using namespace NVMMgr_ns;
namespace PART_ns {
// 从Bitmap中获取首个SetBit的位置
size_t PART_ns::LeafArray::getRightmostSetBit() const {
    auto b = bitmap.load();
    auto pos = b._Find_first();
    assert(pos < LeafArrayLength);
    return pos;
}
// 将Bitmap的第bit_pos位置的值，设置为参数to
void PART_ns::LeafArray::setBit(size_t bit_pos, bool to) {
    auto b = bitmap.load();
    b[bit_pos] = to;
    bitmap.store(b);
}
// 从叶数组中根据pos获取叶节点的地址。叶节点地址的最高16位为指纹。
uint16_t PART_ns::LeafArray::getFingerPrint(size_t pos) const {
    auto x = reinterpret_cast<uint64_t>(leaf[pos].load());
    uint16_t re = x >> FingerPrintShift;
    return re;
}
// 点查询操作
Leaf *LeafArray::lookup(const Key *k) const {
    // 计算待查询的Key的指纹
    uint16_t finger_print = k->getFingerPrint();

    // 遍历节点内数据，判断指纹是否相等。若指纹相等，则再确认整个Key是否一致，若一致则返回叶子节点的地址
    auto b = bitmap.load();
    
#ifdef FIND_FIRST
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
#else
    for (int i = 0; i < LeafArrayLength; i++) {
        if (b[i] == false)
            continue;
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
    }
#endif

    return nullptr;
}
// 插入操作
// 需要注意的是，Insert操作是直接找的第一个空闲的位置。那么对于相同的Key，岂不是会存在2个可能的副本，而不是直接进行更新？
// problem mark
bool LeafArray::insert(Leaf *l, bool flush) {
    auto b = bitmap.load();
    // 先翻转bitmap，从而方便找到第一个还未set的位置
    b.flip();
    auto pos = b._Find_first();
    if (pos < LeafArrayLength) {
        b.flip();
        b[pos] = true;
        bitmap.store(b);
        // 计算指纹s
        // 目前的计算逻辑是，根据叶节点中存储的Key计算指纹。叶数组中存储的则是 16位指纹 与 叶节点地址 的复合值。
        auto s =
            (static_cast<uintptr_t>(l->getFingerPrint()) << FingerPrintShift) |
            (reinterpret_cast<uintptr_t>(l));
        leaf[pos].store(s);
        // 将leaf[pos]这个元素进行持久化
        if (flush)
            flush_data((void *)&leaf[pos], sizeof(std::atomic<uintptr_t>));

        return true;
    } else {
        return false;
    }
}
// 删除操作
// 我们的实现里，删除操作实际上就是插入一个标记为DelFlag=true的Key。
// 细分步骤，则为：（1)先查询这个Key是否在Radix Tree中存在，若存在则直接将其置为无效。（2）若不存在，则插入新的数据
// probelm mark
bool LeafArray::remove(const Key *k) {

}
// bool LeafArray::remove(const Key *k) {
//     uint16_t finger_print = k->getFingerPrint();
//     auto b = bitmap.load();
//     auto i = b[0] ? 0 : 1;
//     while (i < LeafArrayLength) {
//         auto fingerprint_ptr = this->leaf[i].load();
//         if (fingerprint_ptr != 0) {
//             uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
//             auto ptr = reinterpret_cast<Leaf *>(
//                 fingerprint_ptr ^
//                 (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
//             if (finger_print == thisfp && ptr->checkKey(k)) {
//                 leaf[i].store(0);
//                 flush_data(&leaf[i], sizeof(std::atomic<uintptr_t>));
//                 EpochGuard::DeleteNode(ptr);
//                 b[i] = false;
//                 bitmap.store(b);
//                 return true;
//             }
//         }
//         i = b._Find_next(i);
//     }
//     return false;
// }

// 根据叶数组数据，重新加载Bitmap
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
// 调试Debug
void LeafArray::graphviz_debug(std::ofstream &f) {
    char buf[1000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
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

            sprintf(buf + strlen(buf), "node%lx -- node%lx \n",
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

// 节点分裂操作
void LeafArray::splitAndUnlock(N *parentNode, uint8_t parentKey,
                               bool &need_restart) {

    parentNode->writeLockOrRestart(need_restart);

    if (need_restart) {
        this->writeUnlock();
        return;
    }

    auto b = bitmap.load();
    auto leaf_count = b.count();
    std::vector<char *> keys;
    //    char **keys = new char *[leaf_count];
    std::vector<int> lens;
    //    int *lens = new int[leaf_count];

    auto i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<Leaf *>(
                fingerprint_ptr ^
                (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            keys.push_back(ptr->GetKey());
            lens.push_back(ptr->key_len);
        }
        i = b._Find_next(i);
    }
    //    printf("spliting\n");

    std::vector<char> common_prefix;
    int level = 0;
    level = parentNode->getLevel() + 1;
    // assume keys are not substring of another key

    // todo: get common prefix can be optimized by binary search
    while (true) {
        bool out = false;
        for (i = 0; i < leaf_count; i++) {
            if (level < lens[i]) {
                if (i == 0) {
                    common_prefix.push_back(keys[i][level]);
                } else {
                    if (keys[i][level] != common_prefix.back()) {

                        common_prefix.pop_back();

                        out = true;
                        break;
                    }
                }
            } else {
                // assume keys are not substring of another key
                assert(0);
            }
        }
        if (out)
            break;
        level++;
    }
    std::map<char, LeafArray *> split_array;
    for (i = 0; i < leaf_count; i++) {
        if (split_array.count(keys[i][level]) == 0) {
            split_array[keys[i][level]] =
                new (alloc_new_node_from_type(NTypes::LeafArray))
                    LeafArray(level);
        }
        split_array.at(keys[i][level])->insert(getLeafAt(i), false);
    }

    N *n;
    uint8_t *prefix_start = reinterpret_cast<uint8_t *>(common_prefix.data());
    auto prefix_len = common_prefix.size();
    auto leaf_array_count = split_array.size();
    if (leaf_array_count <= 4) {
        n = new (alloc_new_node_from_type(NTypes::N4))
            N4(level, prefix_start, prefix_len);
    } else if (leaf_array_count > 4 && leaf_array_count <= 16) {
        n = new (alloc_new_node_from_type(NTypes::N16))
            N16(level, prefix_start, prefix_len);
    } else if (leaf_array_count > 16 && leaf_array_count <= 48) {
        n = new (alloc_new_node_from_type(NTypes::N48))
            N48(level, prefix_start, prefix_len);
    } else if (leaf_array_count > 48 && leaf_array_count <= 256) {
        n = new (alloc_new_node_from_type(NTypes::N256))
            N256(level, prefix_start, prefix_len);
    } else {
        assert(0);
    }
    for (const auto &p : split_array) {
        unchecked_insert(n, p.first, setLeafArray(p.second), true);
        flush_data(p.second, sizeof(LeafArray));
    }

    N::change(parentNode, parentKey, n);
    parentNode->writeUnlock();

    this->writeUnlockObsolete();
    EpochGuard::DeleteNode(this);
}



// 修改后的节点分裂函数，添加了对前驱与后继节点的连接
void splitAndUnlock(N *parentNode, uint8_t parentKey, bool &need_restart, LeafArray* prev,LeafArray* next)
{
    parentNode->writeLockOrRestart(need_restart);

    if (need_restart) {
        this->writeUnlock();
        return;
    }

    auto b = bitmap.load();
    auto leaf_count = b.count();
    std::vector<char *> keys;
    //    char **keys = new char *[leaf_count];
    std::vector<int> lens;
    //    int *lens = new int[leaf_count];

    auto i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<Leaf *>(
                fingerprint_ptr ^
                (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            keys.push_back(ptr->GetKey());
            lens.push_back(ptr->key_len);
        }
        i = b._Find_next(i);
    }
    //    printf("spliting\n");

    std::vector<char> common_prefix;
    int level = 0;
    level = parentNode->getLevel() + 1;
    // assume keys are not substring of another key

    // todo: get common prefix can be optimized by binary search
    while (true) {
        bool out = false;
        for (i = 0; i < leaf_count; i++) {
            if (level < lens[i]) {
                if (i == 0) {
                    common_prefix.push_back(keys[i][level]);
                } else {
                    if (keys[i][level] != common_prefix.back()) {

                        common_prefix.pop_back();

                        out = true;
                        break;
                    }
                }
            } else {
                // assume keys are not substring of another key
                assert(0);
            }
        }
        if (out)
            break;
        level++;
    }
    // std::map默认采用升序排列Key
    std::map<char, LeafArray *> split_array;
    for (i = 0; i < leaf_count; i++) {
        if (split_array.count(keys[i][level]) == 0) {
            split_array[keys[i][level]] =
                new (alloc_new_node_from_type(NTypes::LeafArray))
                    LeafArray(level);
        }
        split_array.at(keys[i][level])->insert(getLeafAt(i), false);
    }

    N *n;
    uint8_t *prefix_start = reinterpret_cast<uint8_t *>(common_prefix.data());
    auto prefix_len = common_prefix.size();
    auto leaf_array_count = split_array.size();
    if (leaf_array_count <= 4) {
        n = new (alloc_new_node_from_type(NTypes::N4))
            N4(level, prefix_start, prefix_len);
    } else if (leaf_array_count > 4 && leaf_array_count <= 16) {
        n = new (alloc_new_node_from_type(NTypes::N16))
            N16(level, prefix_start, prefix_len);
    } else if (leaf_array_count > 16 && leaf_array_count <= 48) {
        n = new (alloc_new_node_from_type(NTypes::N48))
            N48(level, prefix_start, prefix_len);
    } else if (leaf_array_count > 48 && leaf_array_count <= 256) {
        n = new (alloc_new_node_from_type(NTypes::N256))
            N256(level, prefix_start, prefix_len);
    } else {
        assert(0);
    }
    for (const auto &p : split_array) {
        unchecked_insert(n, p.first, setLeafArray(p.second), true);
        flush_data(p.second, sizeof(LeafArray));
    }

    N::change(parentNode, parentKey, n);
    parentNode->writeUnlock();

    this->writeUnlockObsolete();
    EpochGuard::DeleteNode(this);
}



// 获取相应位置的叶子节点
Leaf *LeafArray::getLeafAt(size_t pos) const {
    auto t = reinterpret_cast<uintptr_t>(this->leaf[pos].load());
    // 获取叶子节点的地址
    // 但是在计算的时候，是 指纹左移48位 与 叶节点地址 进行逻辑或操作，这样的解析不会产生问题吗？（除非地址空间的高16位全为0）
    // problem mark
    t = (t << 16) >> 16;
    return reinterpret_cast<Leaf *>(t);
}
// 根据Bitmap获取有效数据的数目
uint32_t LeafArray::getCount() const { return bitmap.load().count(); }
// 判断叶数组是否已满
bool LeafArray::isFull() const { return getCount() == LeafArrayLength; }
// 在当前叶数组中，根据起始与截止的Key，获取对应的叶子节点数组。若预定义SORT_LEAVEES则对这部分数据进行排序
std::vector<Leaf *> LeafArray::getSortedLeaf(const Key *start, const Key *end,
                                             int start_level,
                                             bool compare_start,
                                             bool compare_end) {
    std::vector<Leaf *> leaves;
    auto b = bitmap.load();
    auto i = b[0] ? 0 : 1;

    // 遍历叶数组，判断叶节点的Key是否在范围区间内，若是，则添加到返回的叶节点指针数组中
    while (i < LeafArrayLength) {
        auto ptr = getLeafAt(i);
        i = b._Find_next(i);
        // start <= ptr < end
        if (compare_start) {
            auto lt_start = leaf_key_lt(ptr, start, start_level);
            if (lt_start == true) {
                continue;
            }
        }
        if (compare_end) {
            auto lt_end = leaf_key_lt(ptr, end, start_level);
            if (lt_end == false) {
                continue;
            }
        }
        leaves.push_back(ptr);
    }
    // 若预定义SORT_LEAVES，则进行排序
#ifdef SORT_LEAVES
    std::sort(leaves.begin(), leaves.end(),
              [start_level](Leaf *a, Leaf *b) -> bool {
                  leaf_lt(a, b, start_level);
              });
#endif
    return leaves;
}
// 更新操作
// 可能需要修改
// problem mark
bool LeafArray::update(const Key *k, Leaf *l) {
    // 前面的步骤与lookup是一致的，仅仅在最终lookup成功后，修改了叶数组
    uint16_t finger_print = k->getFingerPrint();
    auto b = bitmap.load();

#ifdef FIND_FIRST
    auto i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<Leaf *>(
                fingerprint_ptr ^
                (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            if (finger_print == thisfp && ptr->checkKey(k)) {
                auto news = fingerPrintLeaf(finger_print, l);
                leaf[i].store(news);        // 存储新的复合指纹
                flush_data(&leaf[i], sizeof(std::atomic<uintptr_t>));   // Flush持久化
                return true;
            }
        }
        i = b._Find_next(i);
    }
#else
    for (int i = 0; i < LeafArrayLength; i++) {
        if (b[i] == false)
            continue;
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<Leaf *>(
                fingerprint_ptr ^
                (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            if (finger_print == thisfp && ptr->checkKey(k)) {
                auto news = fingerPrintLeaf(finger_print, l);
                leaf[i].store(news);        // 存储新的复合指纹
                flush_data(&leaf[i], sizeof(std::atomic<uintptr_t>));   // Flush持久化
                return true;
            }
        }
    }
#endif
    return false;
}
// 根据指纹与叶子节点地址，计算出叶数组中需要存储的复合型指纹
uintptr_t LeafArray::fingerPrintLeaf(uint16_t fingerPrint, Leaf *l) {
    uintptr_t mask = (1LL << FingerPrintShift) - 1;
    auto f = uintptr_t(fingerPrint);
    return (reinterpret_cast<uintptr_t>(l) & mask) | (f << FingerPrintShift);
}
// 根据Bitmap寻找首个有效的子节点，并将其类型转化为N*返回
N *LeafArray::getAnyChild() const {
    auto b = bitmap.load();
    auto i = b._Find_first();
    if (i == LeafArrayLength) {
        return nullptr;
    } else {
        return N::setLeaf(getLeafAt(i));
    }
}

} // namespace PART_ns