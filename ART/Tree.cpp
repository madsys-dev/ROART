//
// Created by 潘许飞 on 2022/5.
//

#include "Tree.h"
#include "EpochGuard.h"
#include "N.h"
#include "nvm_mgr.h"
#include "threadinfo.h"
#include "timer.h"
#include <algorithm>
#include <assert.h>
#include <fstream>
#include <functional>
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace NVMMgr_ns;

namespace PART_ns {

#ifdef CHECK_COUNT
__thread int checkcount = 0;
#endif

#ifdef COUNT_ALLOC
__thread cpuCycleTimer *alloc_time = nullptr;
double getalloctime() { return alloc_time->duration(); }
#endif

#ifdef ARTPMDK
POBJ_LAYOUT_BEGIN(DLART);
POBJ_LAYOUT_TOID(DLART, char);
POBJ_LAYOUT_END(DLART);
PMEMobjpool *pmem_pool;

//从NVM中根据参数size分配固定大小空间
void *allocate_size(size_t size) {
#ifdef COUNT_ALLOC
    if (alloc_time == nullptr)
        alloc_time = new cpuCycleTimer();
    alloc_time->start();
#endif
    PMEMoid ptr;
    pmemobj_zalloc(pmem_pool, &ptr, size, TOID_TYPE_NUM(char));
    void *addr = (void *)pmemobj_direct(ptr);

#ifdef COUNT_ALLOC
    alloc_time->end();
#endif
    return addr;
}
#endif

// RadixTree的构造函数
Tree::Tree() {
    std::cout << "[P-ART]\tnew P-ART\n";

    // 初始化NVM_MGR的实例
    // problem mark
    init_nvm_mgr();
    // 注册线程信息
    // problem mark
    register_threadinfo();
    // 获取NVM内存全局管理器
    NVMMgr *mgr = get_nvm_mgr();
    //    Epoch_Mgr * epoch_mgr = new Epoch_Mgr();
    
#ifdef LEAF_ARRAY
    // 初始化LeafArray链表的头尾指针
    head = new (alloc_new_node_from_type(NTypes::LeafArray)) LeafArray();
    tail = new (alloc_new_node_from_type(NTypes::LeafArray)) LeafArray();
    head->setLinkedList(nullptr,tail);
    tail->setLinkedList(head,nullptr);
#endif

    //ARTPMDK相关暂时还未弄明白 Problem mark
#ifdef ARTPMDK
    const char *pool_name = "/mnt/pmem0/pxf/dlartpmdk.data";
    const char *layout_name = "DLART";
    size_t pool_size = 64LL * 1024 * 1024 * 1024; // 16GB

    if (access(pool_name, 0)) {
        pmem_pool = pmemobj_create(pool_name, layout_name, pool_size, 0666);
        if (pmem_pool == nullptr) {
            std::cout << "[DLART]\tcreate fail\n";
            assert(0);
        }
        std::cout << "[DLART]\tcreate\n";
    } else {
        pmem_pool = pmemobj_open(pool_name, layout_name);
        std::cout << "[DLART]\topen\n";
    }
    std::cout << "[DLART]\topen pmem pool successfully\n";

    root = new (allocate_size(sizeof(N256))) N256(0, {});
    flush_data((void *)root, sizeof(N256));

#else
    // 若首次创建NVM文件
    if (mgr->first_created) {
        // first open
        // 创建1个N256节点，层级Level=0，无前缀Prefix
        // Problem mark 暂未弄清楚 为什么前面要写(mgr->alloc_tree_root)
        root = new (mgr->alloc_tree_root()) N256(0, {});
        flush_data((void *)root, sizeof(N256));
        //        N::clflush((char *)root, sizeof(N256), true, true);
        std::cout << "[P-ART]\tfirst create a P-ART\n";
    } else {
        // recovery
        root = reinterpret_cast<N256 *>(mgr->alloc_tree_root());
#ifdef INSTANT_RESTART
        root->check_generation();
#endif
        std::cout << "[RECOVERY]\trecovery P-ART and reclaim the memory, root "
                     "addr is "
                  << (uint64_t)root << "\n";
        //        rebuild(mgr->recovery_set);
        //#ifdef RECLAIM_MEMORY
        //        mgr->recovery_free_memory();
        //#endif
    }

#endif
}

Tree::~Tree() {
    // TODO: reclaim the memory of PM
    //    N::deleteChildren(root);
    //    N::deleteNode(root);
    std::cout << "[P-ART]\tshut down, free the tree\n";
    unregister_threadinfo();
    close_nvm_mgr();
}

// allocate a leaf and persist it
Leaf *Tree::allocLeaf(const Key *k) const {
#ifdef KEY_INLINE

#ifdef ARTPMDK
    Leaf *newLeaf =
        new (allocate_size(sizeof(Leaf) + k->key_len + k->val_len)) Leaf(k);
    flush_data((void *)newLeaf, sizeof(Leaf) + k->key_len + k->val_len);
#else

    Leaf *newLeaf =
        new (alloc_new_node_from_size(sizeof(Leaf) + k->key_len + k->val_len))
            Leaf(k);
    flush_data((void *)newLeaf, sizeof(Leaf) + k->key_len + k->val_len);
#endif
    return newLeaf;
#else
    Leaf *newLeaf =
        new (alloc_new_node_from_type(NTypes::Leaf)) Leaf(k); // not persist
    flush_data((void *)newLeaf, sizeof(Leaf));
    return newLeaf;
#endif
}
#ifdef LEAF_ARRAY
Leaf *Tree::lookup(const Key *k) const {
    // enter a new epoch
    EpochGuard NewEpoch;
    bool need_restart;
    int restart_cnt = 0;
restart:
    need_restart = false;
    N *node = root;

    uint32_t level = 0;
    bool optimisticPrefixMatch = false;

    while (true) {
#ifdef INSTANT_RESTART
        node->check_generation();
#endif

#ifdef CHECK_COUNT
        int pre = level;
#endif
        switch (checkPrefix(node, k, level)) { // increases level
        case CheckPrefixResult::NoMatch:
            return nullptr;
        case CheckPrefixResult::OptimisticMatch:
            optimisticPrefixMatch = true;
            // fallthrough
        case CheckPrefixResult::Match: {
            if (k->getKeyLen() <= level) {
                return nullptr;
            }
            node = N::getChild(k->fkey[level], node);

#ifdef CHECK_COUNT
            checkcount += std::min(4, (int)level - pre);
#endif

            if (node == nullptr) {
                return nullptr;
            }

            if (N::isLeafArray(node)) {

                auto la = N::getLeafArray(node);
                //                auto v = la->getVersion();
                auto ret = la->lookup(k);
                //                if (la->isObsolete(v) ||
                //                !la->readVersionOrRestart(v)) {
                //                    printf("read restart\n");
                //                    goto restart;
                //                }
                // 如果寻找到的叶节点为空，或者节点删除标志位为true，则寻找失败
                if(ret==nullptr || ret->DelFlag==true){
                    return nullptr;
                }

                if (ret == nullptr && restart_cnt < 0) {
                    restart_cnt++;
                    goto restart;
                }
                return ret;
            }
        }
        }
        level++;
    }
}
#else
Leaf *Tree::lookup(const Key *k) const {
    // enter a new epoch
    EpochGuard NewEpoch;

    N *node = root;

    uint32_t level = 0;
    bool optimisticPrefixMatch = false;

    while (true) {
#ifdef INSTANT_RESTART
        node->check_generation();
#endif

#ifdef CHECK_COUNT
        int pre = level;
#endif
        switch (checkPrefix(node, k, level)) { // increases level
        case CheckPrefixResult::NoMatch:
            return nullptr;
        case CheckPrefixResult::OptimisticMatch:
            optimisticPrefixMatch = true;
            // fallthrough
        case CheckPrefixResult::Match: {
            if (k->getKeyLen() <= level) {
                return nullptr;
            }
            node = N::getChild(k->fkey[level], node);

#ifdef CHECK_COUNT
            checkcount += std::min(4, (int)level - pre);
#endif

            if (node == nullptr) {
                return nullptr;
            }
            if (N::isLeaf(node)) {
                Leaf *ret = N::getLeaf(node);
                if (level < k->getKeyLen() - 1 || optimisticPrefixMatch) {
#ifdef CHECK_COUNT
                    checkcount += k->getKeyLen();
#endif
                    if (ret->checkKey(k)) {
                        return ret;
                    } else {
                        return nullptr;
                    }
                } else {
                    return ret;
                }
            }
        }
        }
        level++;
    }
}
#endif
#ifdef CHECK_COUNT
int get_count() { return checkcount; }
#endif

typename Tree::OperationResults Tree::update(const Key *k) const {
    EpochGuard NewEpoch;
restart:
    bool needRestart = false;

    N *node = nullptr;
    N *nextNode = root;
    uint8_t nodeKey = 0;
    uint32_t level = 0;
    // bool optimisticPrefixMatch = false;

    while (true) {
        node = nextNode;
#ifdef INSTANT_RESTART
        node->check_generation();
#endif
        auto v = node->getVersion(); // check version

        switch (checkPrefix(node, k, level)) { // increases level
        case CheckPrefixResult::NoMatch:
            if (N::isObsolete(v) || !node->readVersionOrRestart(v)) {
                goto restart;
            }
            return OperationResults::NotFound;
        case CheckPrefixResult::OptimisticMatch:
            // fallthrough
        case CheckPrefixResult::Match: {
            // if (level >= k->getKeyLen()) {
            //     // key is too short
            //     // but it next fkey is 0
            //     return OperationResults::NotFound;
            // }
            nodeKey = k->fkey[level];

            nextNode = N::getChild(nodeKey, node);

            if (nextNode == nullptr) {
                if (N::isObsolete(v) || !node->readVersionOrRestart(v)) {
                    //                        std::cout<<"retry\n";
                    goto restart;
                }
                return OperationResults::NotFound;
            }
#ifdef LEAF_ARRAY
            if (N::isLeafArray(nextNode)) {
                node->lockVersionOrRestart(v, needRestart);
                if (needRestart) {
                    //                        std::cout<<"retry\n";
                    goto restart;
                }

                auto *leaf_array = N::getLeafArray(nextNode);
                auto leaf = allocLeaf(k);
                auto result = leaf_array->update(k, leaf);
                node->writeUnlock();
                if (!result) {
                    EpochGuard::DeleteNode(leaf);
                    return OperationResults::NotFound;
                } else {
                    return OperationResults::Success;
                }
            }
#else
            if (N::isLeaf(nextNode)) {
                node->lockVersionOrRestart(v, needRestart);
                if (needRestart) {
                    //                        std::cout<<"retry\n";
                    goto restart;
                }

                Leaf *leaf = N::getLeaf(nextNode);
                if (!leaf->checkKey(k)) {
                    node->writeUnlock();
                    return OperationResults::NotFound;
                }
                //
                Leaf *newleaf = allocLeaf(k);
                //
                N::change(node, nodeKey, N::setLeaf(newleaf));
                node->writeUnlock();
                return OperationResults::Success;
            }
#endif
            level++;
        }
        }
    }
}
#ifdef LEAF_ARRAY
bool Tree::lookupRange(const Key *start, const Key *end, const Key *continueKey,
                       Leaf *result[], std::size_t resultSize,
                       std::size_t &resultsFound) const {
    if (!N::key_key_lt(start, end)) {
        resultsFound = 0;
        return false;
    }
    //    for (uint32_t i = 0; i < std::min(start->getKeyLen(),
    //    end->getKeyLen());
    //         ++i) {
    //        if (start->fkey[i] > end->fkey[i]) {
    //            resultsFound = 0;
    //            return false;
    //        } else if (start->fkey[i] < end->fkey[i]) {
    //            break;
    //        }
    //    }
    char scan_value[100];
    EpochGuard NewEpoch;

    Leaf *toContinue = nullptr;
    bool restart;
    std::function<void(N *, int, bool, bool)> copy =
        [&result, &resultSize, &resultsFound, &toContinue, &copy, &scan_value,
         &start, &end](N *node, int compare_level, bool compare_start,
                       bool compare_end) {
            if (N::isLeafArray(node)) {

                auto la = N::getLeafArray(node);

                auto leaves = la->getSortedLeaf(start, end, compare_level,
                                                compare_start, compare_end);

                for (auto leaf : leaves) {
                    if (resultsFound == resultSize) {
                        toContinue = N::getLeaf(node);
                        return;
                    }

                    // 可能节点为最新的删除的数据，此时需要跳过。
                    // 为什么不直接删除呢？因为RadixLSM结构的SSD中，可能有旧数据，需要把删除指令通过Flush刷到SSD中，避免误读SSD中的旧数据
                    if(leaf->DelFlag==true){
                        continue;
                    }

                    result[resultsFound] = leaf;
                    resultsFound++;
                }
            } else {
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0u, 255u, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    N *n = std::get<1>(children[i]);
                    copy(n, node->getLevel() + 1, compare_start, compare_end);
                    if (toContinue != nullptr) {
                        break;
                    }
                }
            }
        };
    std::function<void(N *, uint32_t)> findStart =
        [&copy, &start, &findStart, &toContinue, &restart,
         this](N *node, uint32_t level) {
            if (N::isLeafArray(node)) {
                copy(node, level, true, false);
                return;
            }

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, start, level);
            switch (prefixResult) {
            case PCCompareResults::Bigger:
                copy(node, level, false, false);
                break;
            case PCCompareResults::Equal: {
                uint8_t startLevel = (start->getKeyLen() > level)
                                         ? start->fkey[level]
                                         : (uint8_t)0;
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, startLevel, 255, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const uint8_t k = std::get<0>(children[i]);
                    N *n = std::get<1>(children[i]);
                    if (k == startLevel) {
                        findStart(n, level + 1);
                    } else if (k > startLevel) {
                        copy(n, level + 1, false, false);
                    }
                    if (toContinue != nullptr || restart) {
                        break;
                    }
                }
                break;
            }
            case PCCompareResults::SkippedLevel:
                restart = true;
                break;
            case PCCompareResults::Smaller:
                break;
            }
        };
    std::function<void(N *, uint32_t)> findEnd =
        [&copy, &end, &toContinue, &restart, &findEnd, this](N *node,
                                                             uint32_t level) {
            if (N::isLeafArray(node)) {
                // there might be some leaves less than end
                copy(node, level, false, true);
                return;
            }

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, end, level);

            switch (prefixResult) {
            case PCCompareResults::Smaller:
                copy(node, level, false, false);
                break;
            case PCCompareResults::Equal: {
                uint8_t endLevel = (end->getKeyLen() > level) ? end->fkey[level]
                                                              : (uint8_t)255;
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0, endLevel, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const uint8_t k = std::get<0>(children[i]);
                    N *n = std::get<1>(children[i]);
                    if (k == endLevel) {
                        findEnd(n, level + 1);
                    } else if (k < endLevel) {
                        copy(n, level + 1, false, false);
                    }
                    if (toContinue != nullptr || restart) {
                        break;
                    }
                }
                break;
            }
            case PCCompareResults::Bigger:
                break;
            case PCCompareResults::SkippedLevel:
                restart = true;
                break;
            }
        };

restart:
    restart = false;
    resultsFound = 0;

    uint32_t level = 0;
    N *node = nullptr;
    N *nextNode = root;

    while (true) {
        if (!(node = nextNode) || toContinue)
            break;
        if (N::isLeafArray(node)) {
            copy(node, level, true, true);
            break;
        }

        PCEqualsResults prefixResult;
        prefixResult = checkPrefixEquals(node, level, start, end);
        switch (prefixResult) {
        case PCEqualsResults::SkippedLevel:
            goto restart;
        case PCEqualsResults::NoMatch: {
            return false;
        }
        case PCEqualsResults::Contained: {
            copy(node, level + 1, false, false);
            break;
        }
        case PCEqualsResults::BothMatch: {
            uint8_t startLevel =
                (start->getKeyLen() > level) ? start->fkey[level] : (uint8_t)0;
            uint8_t endLevel =
                (end->getKeyLen() > level) ? end->fkey[level] : (uint8_t)255;
            if (startLevel != endLevel) {
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, startLevel, endLevel, children,
                               childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const uint8_t k = std::get<0>(children[i]);
                    N *n = std::get<1>(children[i]);

                    if (k == startLevel) {
                        findStart(n, level + 1);
                    } else if (k > startLevel && k < endLevel) {
                        copy(n, level + 1, false, false);
                    } else if (k == endLevel) {
                        findEnd(n, level + 1);
                    }
                    if (restart) {
                        goto restart;
                    }
                    if (toContinue) {
                        break;
                    }
                }
            } else {

                nextNode = N::getChild(startLevel, node);

                level++;
                continue;
            }
            break;
        }
        }
        break;
    }

    if (toContinue != nullptr) {
        Key *newkey = new Key();
#ifdef KEY_INLINE
        newkey->Init((char *)toContinue->GetKey(), toContinue->key_len,
                     toContinue->GetValue(), toContinue->val_len);
#else
        newkey->Init((char *)toContinue->fkey, toContinue->key_len,
                     toContinue->value, toContinue->val_len);
#endif
        continueKey = newkey;
        return true;
    } else {
        return false;
    }
}

#else
bool Tree::lookupRange(const Key *start, const Key *end, const Key *continueKey,
                       Leaf *result[], std::size_t resultSize,
                       std::size_t &resultsFound) const {
    if (!N::key_key_lt(start, end)) {
        resultsFound = 0;
        return false;
    }
    //    for (uint32_t i = 0; i < std::min(start->getKeyLen(),
    //    end->getKeyLen());
    //         ++i) {
    //        if (start->fkey[i] > end->fkey[i]) {
    //            resultsFound = 0;
    //            return false;
    //        } else if (start->fkey[i] < end->fkey[i]) {
    //            break;
    //        }
    //    }
    char scan_value[100];
    // enter a new epoch
    EpochGuard NewEpoch;

    Leaf *toContinue = nullptr;
    bool restart;
    std::function<void(N *)> copy = [&result, &resultSize, &resultsFound,
                                     &toContinue, &copy, &scan_value,
                                     start](N *node) {
        if (N::isLeaf(node)) {
            if (resultsFound == resultSize) {
                toContinue = N::getLeaf(node);
                return;
            }
            Leaf *leaf = N::getLeaf(node);
            result[resultsFound] = N::getLeaf(node);
            resultsFound++;
        } else {
            std::tuple<uint8_t, N *> children[256];
            uint32_t childrenCount = 0;
            N::getChildren(node, 0u, 255u, children, childrenCount);
            for (uint32_t i = 0; i < childrenCount; ++i) {
                N *n = std::get<1>(children[i]);
                copy(n);
                if (toContinue != nullptr) {
                    break;
                }
            }
        }
    };
    std::function<void(N *, uint32_t)> findStart =
        [&copy, &start, &findStart, &toContinue, &restart,
         this](N *node, uint32_t level) {
            if (N::isLeaf(node)) {
                // correct the bug
                if (N::leaf_key_lt(N::getLeaf(node), start, level) == false) {
                    copy(node);
                }
                return;
            }

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, start, level);
            switch (prefixResult) {
            case PCCompareResults::Bigger:
                copy(node);
                break;
            case PCCompareResults::Equal: {
                uint8_t startLevel = (start->getKeyLen() > level)
                                         ? start->fkey[level]
                                         : (uint8_t)0;
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, startLevel, 255, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const uint8_t k = std::get<0>(children[i]);
                    N *n = std::get<1>(children[i]);
                    if (k == startLevel) {
                        findStart(n, level + 1);
                    } else if (k > startLevel) {
                        copy(n);
                    }
                    if (toContinue != nullptr || restart) {
                        break;
                    }
                }
                break;
            }
            case PCCompareResults::SkippedLevel:
                restart = true;
                break;
            case PCCompareResults::Smaller:
                break;
            }
        };
    std::function<void(N *, uint32_t)> findEnd =
        [&copy, &end, &toContinue, &restart, &findEnd, this](N *node,
                                                             uint32_t level) {
            if (N::isLeaf(node)) {
                if (N::leaf_key_lt(N::getLeaf(node), end, level)) {
                    copy(node);
                }
                return;
            }

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, end, level);

            switch (prefixResult) {
            case PCCompareResults::Smaller:
                copy(node);
                break;
            case PCCompareResults::Equal: {
                uint8_t endLevel = (end->getKeyLen() > level) ? end->fkey[level]
                                                              : (uint8_t)255;
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0, endLevel, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const uint8_t k = std::get<0>(children[i]);
                    N *n = std::get<1>(children[i]);
                    if (k == endLevel) {
                        findEnd(n, level + 1);
                    } else if (k < endLevel) {
                        copy(n);
                    }
                    if (toContinue != nullptr || restart) {
                        break;
                    }
                }
                break;
            }
            case PCCompareResults::Bigger:
                break;
            case PCCompareResults::SkippedLevel:
                restart = true;
                break;
            }
        };

restart:
    restart = false;
    resultsFound = 0;

    uint32_t level = 0;
    N *node = nullptr;
    N *nextNode = root;

    while (true) {
        if (!(node = nextNode) || toContinue)
            break;
        PCEqualsResults prefixResult;
        prefixResult = checkPrefixEquals(node, level, start, end);
        switch (prefixResult) {
        case PCEqualsResults::SkippedLevel:
            goto restart;
        case PCEqualsResults::NoMatch: {
            return false;
        }
        case PCEqualsResults::Contained: {
            copy(node);
            break;
        }
        case PCEqualsResults::BothMatch: {
            uint8_t startLevel =
                (start->getKeyLen() > level) ? start->fkey[level] : (uint8_t)0;
            uint8_t endLevel =
                (end->getKeyLen() > level) ? end->fkey[level] : (uint8_t)255;
            if (startLevel != endLevel) {
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, startLevel, endLevel, children,
                               childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const uint8_t k = std::get<0>(children[i]);
                    N *n = std::get<1>(children[i]);
                    if (k == startLevel) {
                        findStart(n, level + 1);
                    } else if (k > startLevel && k < endLevel) {
                        copy(n);
                    } else if (k == endLevel) {
                        findEnd(n, level + 1);
                    }
                    if (restart) {
                        goto restart;
                    }
                    if (toContinue) {
                        break;
                    }
                }
            } else {

                nextNode = N::getChild(startLevel, node);

                level++;
                continue;
            }
            break;
        }
        }
        break;
    }

    if (toContinue != nullptr) {
        Key *newkey = new Key();
#ifdef KEY_INLINE
        newkey->Init((char *)toContinue->GetKey(), toContinue->key_len,
                     toContinue->GetValue(), toContinue->val_len);
#else
        newkey->Init((char *)toContinue->fkey, toContinue->key_len,
                     toContinue->value, toContinue->val_len);
#endif
        continueKey = newkey;
        return true;
    } else {
        return false;
    }
}
#endif
bool Tree::checkKey(const Key *ret, const Key *k) const {
    return ret->getKeyLen() == k->getKeyLen() &&
           memcmp(ret->fkey, k->fkey, k->getKeyLen()) == 0;
}

// Radix Tree的插入操作
typename Tree::OperationResults Tree::insert(const Key *k) {
    EpochGuard NewEpoch;

restart:
    bool needRestart = false;
    N *node = nullptr;
    N *nextNode = root;
    N *parentNode = nullptr;
    uint8_t parentKey, nodeKey = 0;
    uint32_t level = 0;
    // 新添加的相邻node，用于添加节点时，构建双向链表(由于普通的Radix Tree的内部节点内的数据项是有序的，因此可以找到)
    N * prevSiblingNode = nullptr;
    N * nextSiblingNode = nullptr;
    LeafArray * prevLeafArray = nullptr;    // 为LeafArray进行节点分裂做准备。记录当前LeafArray的前驱节点。
    LeafArray * nextLeafArray = nullptr;    // 为LeafArray进行节点分裂做准备。记录当前LeafArray的后继节点。
    
    //按层级不断迭代向叶子节点方向查找
    while (true) {
        //迭代
        parentNode = node;  //初始化时，parentNode为nullptr
        parentKey = nodeKey;
        node = nextNode;    //初始化时，node即为root节点
        //problem mark
#ifdef INSTANT_RESTART
        node->check_generation();
#endif
        //problem mark
        auto v = node->getVersion();

        uint32_t nextLevel = level; //初始化时，level=0.nextLevel=0

        uint8_t nonMatchingKey;     //若前缀不匹配NoMatch，则会在checkPrefixPessimistic函数中，存储第一个不匹配的字符值
        Prefix remainingPrefix;
        // 先检查node节点的前缀信息，如果node没有prefix，则直接返回Match
        switch (
            checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey,
                                   remainingPrefix)) { // increases nextLevel
        case CheckPrefixPessimisticResult::SkippedLevel:
            goto restart;
        case CheckPrefixPessimisticResult::NoMatch: {
            // 若未匹配，则插入新节点
            // 避免重复的Key
            assert(nextLevel < k->getKeyLen()); // prevent duplicate key
            // Problem mark
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart)
                goto restart;

            // 1) Create new node which will be parent of node, Set common
            // prefix, level to this node
            Prefix prefi = node->getPrefi();
            prefi.prefixCount = nextLevel - level;

#ifdef ARTPMDK
            N4 *newNode = new (allocate_size(sizeof(N4))) N4(nextLevel, prefi);
#else
            auto newNode = new (alloc_new_node_from_type(NTypes::N4))
                N4(nextLevel, prefi); // not persist
#endif

            // 2)  add node and (tid, *k) as children

            auto *newLeaf = allocLeaf(k);

#ifdef LEAF_ARRAY
            auto newLeafArray =
                new (alloc_new_node_from_type(NTypes::LeafArray)) LeafArray();
            // 设置双向指针
            // PXF
            // 判断新插入的Key与现有Key的大小关系，来寻找nearest leaf arrays
            if(k->fkey[nextLevel] > nonMatchingKey){
                prevSiblingNode = node;
                while(! N::isLeafArray(prevSiblingNode)){
                    // 找到最相邻的prevLeafArray
                    if(prevSiblingNode==nullptr){
                        break;
                    }
                    prevSiblingNode = prevSiblingNode->getMaxChild();
                }
                if(N::isLeafArray(prevSiblingNode)){
                    // 连接next leafarray
                    prevSiblingNode->next->prev=newLeafArray;
                    newLeafArray->next =  prevSiblingNode->next;
                    // 连接prev leafarray
                    prevSiblingNode->next = newLeafArray;
                    newLeafArray->prev = prevSiblingNode;
                }
            }else{
                nextSiblingNode = node;
                while(! N::isLeafArray(nextSiblingNode)){
                    // 找到最相邻的nextLeafArray
                    if(nextSiblingNode==nullptr){
                        break;
                    }
                    nextSiblingNode = nextSiblingNode->getMinChild();
                }
                if(N::isLeafArray(nextSiblingNode)){
                    // 连接prev leafarray
                    nextSiblingNode->prev->next=newLeafArray;
                    newLeafArray->prev =  nextSiblingNode->prev;
                    // 连接next leafarray
                    nextSiblingNode->prev = newLeafArray;
                    newLeafArray->next = nextSiblingNode;
                }
            }
            newLeafArray->insert(newLeaf, true);    //新生成的LeafArray 作为新叶子节点的 父节点
            newNode->insert(k->fkey[nextLevel], N::setLeafArray(newLeafArray),  //新生成的N4 NewNode 作为 新生成的LeafArray的 父节点。nextLevel是第一个不匹配的字符的层级
                            false);
#else
            newNode->insert(k->fkey[nextLevel], N::setLeaf(newLeaf), false);
#endif
            // not persist
            newNode->insert(nonMatchingKey, node, false);   //将原有的single child sub tree也添加过来。
            // persist the new node
            flush_data((void *)newNode, sizeof(N4));

            // 3) lockVersionOrRestart, update parentNode to point to the
            // new node, unlock
            parentNode->writeLockOrRestart(needRestart);
            if (needRestart) {
                EpochGuard::DeleteNode((void *)newNode);
#ifdef LEAF_ARRAY
                EpochGuard::DeleteNode(newLeafArray);
#endif
                EpochGuard::DeleteNode((void *)newLeaf);

                node->writeUnlock();
                goto restart;
            }
            // 设置ParentNode的数据项，将parentKey对应的子节点设置为newNode
            N::change(parentNode, parentKey, newNode);
            parentNode->writeUnlock();

            // 4) update prefix of node, unlock
            node->setPrefix(
                remainingPrefix.prefix,
                node->getPrefi().prefixCount - ((nextLevel - level) + 1), true);
            //            std::cout<<"insert success\n";

            node->writeUnlock();
            return OperationResults::Success;

        } // end case  NoMatch
        case CheckPrefixPessimisticResult::Match:
            // 若Match，则不进行实际操作。跳出Switch，直接后续的迭代与判断过程
            break;
        }
        assert(nextLevel < k->getKeyLen()); // prevent duplicate key
        // TODO: maybe one string is substring of another, so it fkey[level]
        // will be 0 solve problem of substring

        // 在搜索路径中，向下方的节点迭代
        level = nextLevel;
        nodeKey = k->fkey[level];   //nodekey是uint8_t，即按照字节去判断
        //根据Key获取下一个节点
        nextNode = N::getChild(nodeKey, node);
        // 若寻找下一个节点为空（搜索到Radix Tree的最底层了），则：直接新建叶节点与叶数组
        if (nextNode == nullptr) {
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart)
                goto restart;
            // 生成新的叶子节点
            Leaf *newLeaf = allocLeaf(k);
#ifdef LEAF_ARRAY
            // 若预定义了LEAF_ARRAY，则还需要新建叶数组
            auto newLeafArray =
                new (alloc_new_node_from_type(NTypes::LeafArray)) LeafArray();
            // 叶数组中插入新的叶节点
            newLeafArray->insert(newLeaf, true);

            // 此时，可能node内无数据(例如初始状态下root节点无数据)，或者node有数据但无匹配的子节点
            // 若node内无数据，此时理论上，应该node仅为root节点
            // problem mark:需要保证在删除节点，导致父节点为空时，需要释放父节点空间
            uint32_t tmpDataCount=N::getCount(node);
            if(tmpDataCount==0){
                if(node==root){
                    head->next=newLeafArray;
                    tail->prev=newLeafArray;
                    newLeafArray->prev=head;
                    newLeafArray->next=tail;   
                }else{
                    // 如果没实现完善，导致存在空节点，且不为root节点时，那么需要寻找其相邻节点
                    if(N::getCount(parentNode)>1 && N::getMinChild(parentNode)!=node){
                        prevSiblingNode = N::getMaxSmallerChild(parentNode,parentKey);
                    }else{
                        //需要寻找比parentnode更小的node
                        printf("We cannot find sibing node,when insert\n");
                    }
                }
            }else{
                // 若node内部有数据但无匹配的子节点
                // 分为3种情况：小于node内所有数据、大于node内所有数据、在node内2数据之间
                N* tmpPrevSiblingNode = nullptr;
                bool hasSmaller=false;
                bool hasBigger=false;
                tmpPrevSiblingNode = N::checkKeyRange(node,nodeKey,hasSmaller,hasBigger);
                if(hasSmaller){
                    if(hasBigger){
                        // 此时，key范围在node节点内
                        prevSiblingNode = tmpPrevSiblingNode;
                        while(! N::isLeafArray(prevSiblingNode)){
                            // 找到最相邻的prevLeafArray
                            if(prevSiblingNode==nullptr){
                                break;
                            }
                            prevSiblingNode = prevSiblingNode->getMaxChild();
                        }
                        if(N::isLeafArray(prevSiblingNode)){
                            // 连接next leafarray
                            prevSiblingNode->next->prev=newLeafArray;
                            newLeafArray->next =  prevSiblingNode->next;
                            // 连接prev leafarray
                            prevSiblingNode->next = newLeafArray;
                            newLeafArray->prev = prevSiblingNode;
                        }
                    }else{
                        // 此时，key比node内所有数据都大
                        prevSiblingNode = node;
                        while(! N::isLeafArray(prevSiblingNode)){
                            // 找到最相邻的prevLeafArray
                            if(prevSiblingNode==nullptr){
                                break;
                            }
                            prevSiblingNode = prevSiblingNode->getMaxChild();
                        }
                        if(N::isLeafArray(prevSiblingNode)){
                            // 连接next leafarray
                            prevSiblingNode->next->prev=newLeafArray;
                            newLeafArray->next =  prevSiblingNode->next;
                            // 连接prev leafarray
                            prevSiblingNode->next = newLeafArray;
                            newLeafArray->prev = prevSiblingNode;
                        }
                    }
                }else{
                    // 此时，key比node内所有数据都小
                    // 2种实现方式
                    // (1)一种是 寻找 距离node最近的前一个SiblingNode，然后不断getMaxChild
                    // (2)另外一种是 直接根据当前node不断getMinChild，获取tmpLeafArray，然后新添加的LeafArray就在tmpLeafArray之前
                    
                    // 第一种实现，但可能getMaxSmallChild比较耗时？
                    // prevSibingNode = N::getMaxSmallerChild(parentNode,parentKey);
                    // while(! N::isLeafArray(prevSiblingNode)){
                    //     // 找到最相邻的prevLeafArray
                    //     if(prevSiblingNode==nullptr){
                    //         break;
                    //     }
                    //     prevSiblingNode = prevSiblingNode->getMaxChild();
                    // }
                    // if(N::isLeafArray(prevSiblingNode)){
                    //     // 连接next leafarray
                    //     prevSiblingNode->next->prev=newLeafArray;
                    //     newLeafArray->next =  prevSiblingNode->next;
                    //     // 连接prev leafarray
                    //     prevSiblingNode->next = newLeafArray;
                    //     newLeafArray->prev = prevSiblingNode;
                    // }
                    
                    // 第2种实现
                    nextSiblingNode = node;
                    while(! N::isLeafArray(nextSiblingNode)){
                        // 找到最相邻的nextLeafArray
                        if(nextSiblingNode==nullptr){
                            break;
                        }
                        nextSiblingNode = nextSiblingNode->getMinChild();
                    }
                    if(N::isLeafArray(nextSiblingNode)){
                        // 连接prev leafarray
                        nextSiblingNode->prev->next=newLeafArray;
                        newLeafArray->prev =  nextSiblingNode->prev;
                        // 连接next leafarray
                        nextSiblingNode->prev = newLeafArray;
                        newLeafArray->next = nextSiblingNode;
                    }
                }
            }
            // 将 叶数组 LeafArray 设置为 当前内部node 的子节点
            // 若 当前内部node已满，则进行扩容，并重新分配node节点，修改parentNode指向子节点node
            N::insertAndUnlock(node, parentNode, parentKey, nodeKey,
                               N::setLeafArray(newLeafArray), needRestart);
#else
            // 将 叶子节点 Leaf 设置为 当前内部node 的子节点
            N::insertAndUnlock(node, parentNode, parentKey, nodeKey,
                               N::setLeaf(newLeaf), needRestart);
#endif
            if (needRestart)
                goto restart;

            return OperationResults::Success;
        }
#ifdef LEAF_ARRAY
        // 若寻找的下一个节点nextnode是叶数组（搜索到Radix Tree的次底层了），则：直接插入到LeafArray中，并判断是否需要叶数组分裂
        if (N::isLeafArray(nextNode)) {
            auto leaf_array = N::getLeafArray(nextNode);
            // 若已经查询到这个Key，说明已经存在了。则直接返回Existed状态。
            if (leaf_array->lookup(k) != nullptr) {
                return OperationResults::Existed;
            } else {
                // 若未查询到这个Key，则直接在LeafArray中插入
                auto lav = leaf_array->getVersion();
                leaf_array->lockVersionOrRestart(lav, needRestart);
                if (needRestart) {
                    goto restart;
                }
                // 当叶数组已满，则进行节点分裂
                // 这种情况下，当前叶数组肯定已维护好LeafArray层的双向链表，因此分裂后的指针关系也比较好确定
                if (leaf_array->isFull()) {
                    //记录之前的LeafArray的前驱与后继节点
                    prevLeafArray = leaf_array->prev.load();
                    nextLeafArray = leaf_array->next.load();
                    leaf_array->splitAndUnlock(node, nodeKey, needRestart,prevLeafArray,nextLeafArray);
                    if (needRestart) {
                        goto restart;
                    }
                    nextNode = N::getChild(nodeKey, node);
                    // insert at the next iteration
                } else {
                    // 当叶数组未满，则直接分配叶节点，如何插入到叶数组中
                    auto leaf = allocLeaf(k);
                    leaf_array->insert(leaf, true);
                    leaf_array->writeUnlock();
                    return OperationResults::Success;
                }
            }
        }
#else
        // 若寻找到的下一个节点是叶子节点（若采用LeafArray叶数组压缩的策略，则不存在改情况）
        if (N::isLeaf(nextNode)) {
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart)
                goto restart;
            Leaf *leaf = N::getLeaf(nextNode);

            level++;
            // assert(level < leaf->getKeyLen());
            // prevent inserting when
            // prefix of leaf exists already
            // but if I want to insert a prefix of this leaf, i also need to
            // insert successfully
            uint32_t prefixLength = 0;
#ifdef KEY_INLINE
            while (level + prefixLength <
                       std::min(k->getKeyLen(), leaf->getKeyLen()) &&
                   leaf->kv[level + prefixLength] ==
                       k->fkey[level + prefixLength]) {
                prefixLength++;
            }
#else
            while (level + prefixLength <
                       std::min(k->getKeyLen(), leaf->getKeyLen()) &&
                   leaf->fkey[level + prefixLength] ==
                       k->fkey[level + prefixLength]) {
                prefixLength++;
            }
#endif
            // equal
            if (k->getKeyLen() == leaf->getKeyLen() &&
                level + prefixLength == k->getKeyLen()) {
                // duplicate leaf
                node->writeUnlock();
                //                std::cout<<"ohfinish\n";
                return OperationResults::Existed;
            }
            // substring

#ifdef ARTPMDK
            N4 *n4 = new (allocate_size(sizeof(N4)))
                N4(level + prefixLength, &k->fkey[level],
                   prefixLength); // not persist
#else
            auto n4 = new (alloc_new_node_from_type(NTypes::N4))
                N4(level + prefixLength, &k->fkey[level],
                   prefixLength); // not persist
#endif
            Leaf *newLeaf = allocLeaf(k);
            n4->insert(k->fkey[level + prefixLength], N::setLeaf(newLeaf),
                       false);
#ifdef KEY_INLINE
            n4->insert(leaf->kv[level + prefixLength], nextNode, false);
#else
            n4->insert(leaf->fkey[level + prefixLength], nextNode, false);
#endif
            flush_data((void *)n4, sizeof(N4));

            N::change(node, k->fkey[level - 1], n4);
            node->writeUnlock();
            return OperationResults::Success;
        }
#endif
        // 递增Level
        level++;
    }
    //    std::cout<<"ohfinish\n";
}

typename Tree::OperationResults Tree::remove(const Key *k) {
    EpochGuard NewEpoch;
restart:
    bool needRestart = false;

    N *node = nullptr;
    N *nextNode = root;
    N *parentNode = nullptr;
    uint8_t parentKey, nodeKey = 0;
    uint32_t level = 0;
    // bool optimisticPrefixMatch = false;

    while (true) {
        parentNode = node;
        parentKey = nodeKey;
        node = nextNode;
#ifdef INSTANT_RESTART
        node->check_generation();
#endif
        auto v = node->getVersion();

        switch (checkPrefix(node, k, level)) { // increases level
        case CheckPrefixResult::NoMatch:
            if (N::isObsolete(v) || !node->readVersionOrRestart(v)) {
                goto restart;
            }
            return OperationResults::NotFound;
        case CheckPrefixResult::OptimisticMatch:
            // fallthrough
        case CheckPrefixResult::Match: {
            // if (level >= k->getKeyLen()) {
            //     // key is too short
            //     // but it next fkey is 0
            //     return OperationResults::NotFound;
            // }
            nodeKey = k->fkey[level];

            nextNode = N::getChild(nodeKey, node);

            if (nextNode == nullptr) {
                if (N::isObsolete(v) ||
                    !node->readVersionOrRestart(v)) { // TODO
                    goto restart;
                }
                return OperationResults::NotFound;
            }
#ifdef LEAF_ARRAY
            if (N::isLeafArray(nextNode)) {
                auto *leaf_array = N::getLeafArray(nextNode);
                auto lav = leaf_array->getVersion();
                leaf_array->lockVersionOrRestart(lav, needRestart);
                if (needRestart) {
                    goto restart;
                }
                auto result = leaf_array->remove(k);
                leaf_array->writeUnlock();
                if (!result) {
                    return OperationResults::NotFound;
                } else {
                    return OperationResults::Success;
                }
            }
#else
            if (N::isLeaf(nextNode)) {
                node->lockVersionOrRestart(v, needRestart);
                if (needRestart)
                    goto restart;

                Leaf *leaf = N::getLeaf(nextNode);
                if (!leaf->checkKey(k)) {
                    node->writeUnlock();
                    return OperationResults::NotFound;
                }
                assert(parentNode == nullptr || N::getCount(node) != 1);
                if (N::getCount(node) == 2 && node != root) {
                    // 1. check remaining entries
                    N *secondNodeN;
                    uint8_t secondNodeK;
                    std::tie(secondNodeN, secondNodeK) =
                        N::getSecondChild(node, nodeKey);
                    if (N::isLeaf(secondNodeN)) {
                        parentNode->writeLockOrRestart(needRestart);
                        if (needRestart) {
                            node->writeUnlock();
                            goto restart;
                        }

                        // N::remove(node, k[level]); not necessary
                        N::change(parentNode, parentKey, secondNodeN);

                        parentNode->writeUnlock();
                        node->writeUnlockObsolete();

                        // remove the node
                        EpochGuard::DeleteNode((void *)node);
                    } else {
                        uint64_t vChild = secondNodeN->getVersion();
                        secondNodeN->lockVersionOrRestart(vChild, needRestart);
                        if (needRestart) {
                            node->writeUnlock();
                            goto restart;
                        }
                        parentNode->writeLockOrRestart(needRestart);
                        if (needRestart) {
                            node->writeUnlock();
                            secondNodeN->writeUnlock();
                            goto restart;
                        }

                        // N::remove(node, k[level]); not necessary
                        N::change(parentNode, parentKey, secondNodeN);

                        secondNodeN->addPrefixBefore(node, secondNodeK);

                        parentNode->writeUnlock();
                        node->writeUnlockObsolete();

                        // remove the node
                        EpochGuard::DeleteNode((void *)node);

                        secondNodeN->writeUnlock();
                    }
                } else {
                    N::removeAndUnlock(node, k->fkey[level], parentNode,
                                       parentKey, needRestart);
                    if (needRestart)
                        goto restart;
                }
                // remove the leaf
                EpochGuard::DeleteNode((void *)leaf);

                return OperationResults::Success;
            }
#endif
            level++;
        }
        }
    }
}

// 对于RadixLSM，将Key的标记位设置为删除。
typename Tree::OperationResults Tree::radixLSMRemove(const Key *k){
    k->setDelKey(); // 设置Key的删除标记位
    Leaf * existLeaf = nullptr;
    existLeaf = lookup(k);
    // 3种情况：（1）存在有效数据（2）存在无效数据（3）不存在任何数据
    if(existLeaf!=nullptr){
        if(existLeaf->DelFlag == true){
            // 存在无效数据
            return OperationResults::Success;
        }else{
            // 存在有效数据
            existLeaf->DelFlag = true;
            return OperationResults::Success;
        }
    }else{
        // 不存在任何数据，则插入一个DelFlag为true的Key的叶节点
        return insert(k);
    }
    return OperationResults::Success;
}

void Tree::rebuild(std::vector<std::pair<uint64_t, size_t>> &rs,
                   uint64_t start_addr, uint64_t end_addr, int thread_id) {
    // rebuild meta data count/compactcount
    // record all used memory (offset, size) into rs set
    N::rebuild_node(root, rs, start_addr, end_addr, thread_id);
}

// 检查前缀，返回检查结果CheckPrefixResult
typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key *k,
                                                   uint32_t &level) {
    //若待查询的Key的整体长度，小于节点n当前的层级Level，认为 不匹配
    if (k->getKeyLen() <= n->getLevel()) {
        return CheckPrefixResult::NoMatch;
    }
    // 若 节点n存储的前缀数目+上层Level 小于 节点n目前位于的Level ，则认为 乐观匹配
    Prefix p = n->getPrefi();
    if (p.prefixCount + level < n->getLevel()) {
        level = n->getLevel();
        return CheckPrefixResult::OptimisticMatch;
    }
    // 若节点n存储的前缀数目大于0，则依次进行判断：
    if (p.prefixCount > 0) {
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < std::min(p.prefixCount, maxStoredPrefixLength); ++i) {
            if (p.prefix[i] != k->fkey[level]) {
                return CheckPrefixResult::NoMatch;
            }
            ++level;
        }
        // 若前缀数目大于最大能存储的前缀长度，则认为 乐观匹配
        if (p.prefixCount > maxStoredPrefixLength) {
            // level += p.prefixCount - maxStoredPrefixLength;
            level = n->getLevel();      //乐观匹配则直接将当前正在匹配的层级Level设置为节点n的Level
            return CheckPrefixResult::OptimisticMatch;
        }
    }
    return CheckPrefixResult::Match;
}

typename Tree::CheckPrefixPessimisticResult
Tree::checkPrefixPessimistic(N *n, const Key *k, uint32_t &level,
                             uint8_t &nonMatchingKey,
                             Prefix &nonMatchingPrefix) {
    Prefix p = n->getPrefi();
    if (p.prefixCount + level != n->getLevel()) {
        // Intermediate or inconsistent state from path compression
        // "splitAndUnlock" or "merge" is detected Inconsistent path compressed
        // prefix should be recovered in here
        // 用于检测路径压缩的不一致状态，并进行恢复
        bool needRecover = false;
        auto v = n->getVersion();
        n->lockVersionOrRestart(v, needRecover);
        if (!needRecover) {
            // Inconsistent state due to prior system crash is suspected --> Do
            // recovery

            // 1) Picking up arbitrary two leaf nodes and then 2) rebuilding
            // correct compressed prefix
            uint32_t discrimination =
                (n->getLevel() > level ? n->getLevel() - level
                                       : level - n->getLevel());
            Leaf *kr = N::getAnyChildTid(n);
            p.prefixCount = discrimination;
            for (uint32_t i = 0;
                 i < std::min(discrimination, maxStoredPrefixLength); i++) {
#ifdef KEY_INLINE
                p.prefix[i] = kr->kv[level + i];
#else
                p.prefix[i] = kr->fkey[level + i];
#endif
            }
            n->setPrefix(p.prefix, p.prefixCount, true);
            n->writeUnlock();
        }

        // path compression merge is in progress --> restart from root
        // path compression splitAndUnlock is in progress --> skipping an
        // intermediate compressed prefix by using level (invariant)
        if (p.prefixCount + level < n->getLevel()) {
            return CheckPrefixPessimisticResult::SkippedLevel;
        }
    }

    // 逐个比较node的前缀prev是否匹配。值得注意的是：若无前缀，也会被判定为match
    if (p.prefixCount > 0) {
        uint32_t prevLevel = level;
        Leaf *kt = nullptr;
        bool load_flag = false;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i >= maxStoredPrefixLength && !load_flag) {
                //            if (i == maxStoredPrefixLength) {
                // Optimistic path compression
                kt = N::getAnyChildTid(n);
                load_flag = true;
            }
#ifdef KEY_INLINE
            uint8_t curKey = i >= maxStoredPrefixLength ? (uint8_t)kt->kv[level]
                                                        : p.prefix[i];
#else
            uint8_t curKey =
                i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
#endif
            if (curKey != k->fkey[level]) {
                nonMatchingKey = curKey;
                if (p.prefixCount > maxStoredPrefixLength) {
                    if (i < maxStoredPrefixLength) {
                        kt = N::getAnyChildTid(n);
                    }
                    for (uint32_t j = 0;
                         j < std::min((p.prefixCount - (level - prevLevel) - 1),
                                      maxStoredPrefixLength);
                         ++j) {
#ifdef KEY_INLINE
                        nonMatchingPrefix.prefix[j] =
                            (uint8_t)kt->kv[level + j + 1];
#else
                        nonMatchingPrefix.prefix[j] = kt->fkey[level + j + 1];
#endif
                    }
                } else {
                    for (uint32_t j = 0; j < p.prefixCount - i - 1; ++j) {
                        nonMatchingPrefix.prefix[j] = p.prefix[i + j + 1];
                    }
                }
                return CheckPrefixPessimisticResult::NoMatch;
            }
            ++level;
        }
    }
    return CheckPrefixPessimisticResult::Match;
}

typename Tree::PCCompareResults
Tree::checkPrefixCompare(const N *n, const Key *k, uint32_t &level) {
    Prefix p = n->getPrefi();
    if (p.prefixCount + level < n->getLevel()) {
        return PCCompareResults::SkippedLevel;
    }
    if (p.prefixCount > 0) {
        Leaf *kt = nullptr;
        bool load_flag = false;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i >= maxStoredPrefixLength && !load_flag) {
                // loadKey(N::getAnyChildTid(n), kt);
                kt = N::getAnyChildTid(n);
                load_flag = true;
            }
            uint8_t kLevel =
                (k->getKeyLen() > level) ? k->fkey[level] : (uint8_t)0;

#ifdef KEY_INLINE
            uint8_t curKey = i >= maxStoredPrefixLength ? (uint8_t)kt->kv[level]
                                                        : p.prefix[i];
#else
            uint8_t curKey =
                i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
#endif
            if (curKey < kLevel) {
                return PCCompareResults::Smaller;
            } else if (curKey > kLevel) {
                return PCCompareResults::Bigger;
            }
            ++level;
        }
    }
    return PCCompareResults::Equal;
}

typename Tree::PCEqualsResults Tree::checkPrefixEquals(const N *n,
                                                       uint32_t &level,
                                                       const Key *start,
                                                       const Key *end) {
    Prefix p = n->getPrefi();
    if (p.prefixCount + level < n->getLevel()) {
        return PCEqualsResults::SkippedLevel;
    }
    if (p.prefixCount > 0) {
        Leaf *kt = nullptr;
        bool load_flag = false;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i >= maxStoredPrefixLength && !load_flag) {
                // loadKey(N::getAnyChildTid(n), kt);
                kt = N::getAnyChildTid(n);
                load_flag = true;
            }
            uint8_t startLevel =
                (start->getKeyLen() > level) ? start->fkey[level] : (uint8_t)0;
            uint8_t endLevel =
                (end->getKeyLen() > level) ? end->fkey[level] : (uint8_t)0;

#ifdef KEY_INLINE
            uint8_t curKey = i >= maxStoredPrefixLength ? (uint8_t)kt->kv[level]
                                                        : p.prefix[i];
#else
            uint8_t curKey =
                i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
#endif
            if (curKey > startLevel && curKey < endLevel) {
                return PCEqualsResults::Contained;
            } else if (curKey < startLevel || curKey > endLevel) {
                return PCEqualsResults::NoMatch;
            }
            ++level;
        }
    }
    return PCEqualsResults::BothMatch;
}

// 用于Debug
void Tree::graphviz_debug() {
    std::ofstream f("../dot/tree-view.dot");

    f << "graph tree\n"
         "{\n"
         "    graph[dpi = 400];\n"
         "    label=\"Tree View\"\n"
         "    node []\n";
    N::graphviz_debug(f, root);
    f << "}";
    f.close();
    //    printf("ok2\n");
}

} // namespace PART_ns
