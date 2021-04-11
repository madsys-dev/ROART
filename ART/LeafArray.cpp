//
// Created by 谢威宇 on 2021/4/3.
//

#include "LeafArray.h"
#include "EpochGuard.h"
#include "threadinfo.h"

using namespace NVMMgr_ns;
namespace PART_ns {
size_t PART_ns::LeafArray::getRightmostSetBit() const {
    auto b = bitmap.load();
    auto pos = b._Find_first();
    assert(pos < LeafArrayLength);
    return pos;
}
void PART_ns::LeafArray::setBit(size_t bit_pos, bool to) {
    auto b = bitmap.load();
    b[bit_pos] = to;
    bitmap.store(b);
}
uint16_t PART_ns::LeafArray::getFingerPrint(size_t pos) const {
    auto x = reinterpret_cast<uint64_t>(leaf[pos].load());
    uint16_t re = x >> FingerPrintShift;
    return re;
}
Leaf *LeafArray::lookup(const Key *k) const {
    uint16_t finger_print = k->getFingerPrint();
    auto b = bitmap.load();
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

    return nullptr;
}
bool LeafArray::insert(Leaf *l, bool flush) {

    auto b = bitmap.load();
    b.flip();
    auto pos = b._Find_first();
    if (pos < LeafArrayLength) {
        b.flip();
        b[pos] = true;
        bitmap.store(b);
        auto s =
            (static_cast<uintptr_t>(l->getFingerPrint()) << FingerPrintShift) |
            (reinterpret_cast<uintptr_t>(l));
        leaf[pos].store(s);
        if (flush)
            flush_data((void *)&leaf[pos], sizeof(std::atomic<uintptr_t>));

        return true;
    } else {
        return false;
    }
}
bool LeafArray::remove(const Key *k) {
    uint16_t finger_print = k->getFingerPrint();
    auto b = bitmap.load();
    auto i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<Leaf *>(
                fingerprint_ptr ^
                (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            if (finger_print == thisfp && ptr->checkKey(k)) {
                leaf[i].store(0);
                flush_data(&leaf[i], sizeof(std::atomic<uintptr_t>));
                b[i] = false;
                bitmap.store(b);
                return true;
            }
        }
        i = b._Find_next(i);
    }
    return false;
}
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
    int cnt = 0;
    auto i = b[0] ? 0 : 1;
    while (i < LeafArrayLength) {
        auto fingerprint_ptr = this->leaf[i].load();
        if (fingerprint_ptr != 0) {
            uint16_t thisfp = fingerprint_ptr >> FingerPrintShift;
            auto ptr = reinterpret_cast<Leaf *>(
                fingerprint_ptr ^
                (static_cast<uintptr_t>(thisfp) << FingerPrintShift));
            keys.push_back(ptr->GetKey());
            //            keys[cnt] = ptr->GetKey();
            lens.push_back(ptr->key_len);
            //            lens[cnt] = ptr->key_len;
            cnt++;
        }
        i = b._Find_next(i);
    }
//    printf("spliting\n");

    std::vector<char> common_prefix;
    cnt = parentNode->getLevel() + 1;
    // assume keys are not substring of another key

    // todo: get common prefix can be optimized by binary search
    while (true) {
        bool out = false;
        for (i = 0; i < leaf_count; i++) {
            if (cnt < lens[i]) {
                if (i == 0) {
                    common_prefix.push_back(keys[i][cnt]);
                } else {
                    if (keys[i][cnt] != common_prefix.back()) {

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
        cnt++;
    }
    std::map<char, LeafArray *> split_array;
    for (i = 0; i < leaf_count; i++) {
        if (split_array.count(keys[i][cnt]) == 0) {
            split_array[keys[i][cnt]] =
                new (alloc_new_node_from_type(NTypes::LeafArray)) LeafArray();
        }
        split_array.at(keys[i][cnt])->insert(getLeafAt(i), false);
    }

    N *n;
    uint8_t *prefix_start = reinterpret_cast<uint8_t *>(common_prefix.data());
    auto prefix_len = common_prefix.size();
    auto leaf_array_count = split_array.size();
    if (leaf_array_count <= 4) {
        n = new (alloc_new_node_from_type(NTypes::N4))
            N4(cnt, prefix_start, prefix_len);
    } else if (leaf_array_count > 4 && leaf_array_count <= 16) {
        n = new (alloc_new_node_from_type(NTypes::N16))
            N16(cnt, prefix_start, prefix_len);
    } else if (leaf_array_count > 16 && leaf_array_count <= 48) {
        n = new (alloc_new_node_from_type(NTypes::N48))
            N48(cnt, prefix_start, prefix_len);
    } else if (leaf_array_count > 48 && leaf_array_count <= 256) {
        n = new (alloc_new_node_from_type(NTypes::N256))
            N256(cnt, prefix_start, prefix_len);
    } else {
        assert(0);
    }
    for (const auto &p : split_array) {
        unchecked_insert(n, p.first, setLeafArray(p.second), true);
        flush_data(p.second, sizeof(LeafArray));
    }

    change(parentNode, parentKey, n);
    parentNode->writeUnlock();

    this->writeUnlockObsolete();
    EpochGuard::DeleteNode(this);
}
Leaf *LeafArray::getLeafAt(size_t pos) {
    auto t = reinterpret_cast<uintptr_t>(this->leaf[pos].load());
    t = (t << 16) >> 16;
    return reinterpret_cast<Leaf *>(t);
}
uint32_t LeafArray::getCount() const { return bitmap.load().count(); }
bool LeafArray::isFull() const { return getCount() == LeafArrayLength; }

} // namespace PART_ns