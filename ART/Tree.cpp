#include "Tree.h"
#include "EpochGuard.h"
#include "N.h"
#include "nvm_mgr.h"
#include "threadinfo.h"
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
#ifdef LOG_FREE

#ifdef CHECK_COUNT
__thread int checkcount = 0;
#endif

Tree::Tree() {
    std::cout << "[P-RP-ART]\tnew P-RP-ART\n";

    init_nvm_mgr();
    register_threadinfo();
    NVMMgr *mgr = get_nvm_mgr();
    //    Epoch_Mgr * epoch_mgr = new Epoch_Mgr();

    if (mgr->first_created) {
        // first open
        root = new (mgr->alloc_tree_root()) N256(0, {});
        flush_data((void *)root, sizeof(N256));
        //        N::clflush((char *)root, sizeof(N256), true, true);
        std::cout << "[P-RP-ART]\tfirst create a P-RP-ART\n";
    } else {
        // recovery
        root = reinterpret_cast<N256 *>(mgr->alloc_tree_root());
        std::cout << "[RECOVERY]\trecovery P-RP-ART and reclaim the memory\n";
        rebuild(mgr->recovery_set);
#ifdef RECLAIM_MEMORY
        mgr->recovery_free_memory();
#endif
    }
}

Tree::~Tree() {
    // TODO: reclaim the memory of PM
    //    N::deleteChildren(root);
    //    N::deleteNode(root);
    std::cout << "[P-RP-ART]\tshut down, free the tree\n";
    unregister_threadinfo();
    close_nvm_mgr();
}

Leaf *Tree::lookup(const Key *k) const {
    // enter a new epoch
    EpochGuard NewEpoch;

    N *node = root;
    std::atomic<N *> *parentref = nullptr;
    std::atomic<N *> *curref = nullptr;

    uint32_t level = 0;
    bool optimisticPrefixMatch = false;

    while (true) {

#ifdef CHECK_COUNT
        int pre = level;
#endif
        switch (checkPrefix(node, k, level)) { // increases level
        case CheckPrefixResult::NoMatch:
            return NULL;
        case CheckPrefixResult::OptimisticMatch:
            optimisticPrefixMatch = true;
            // fallthrough
        case CheckPrefixResult::Match: {
            if (k->getKeyLen() <= level) {
                return NULL;
            }
            parentref = curref;
            curref = N::getChild(k->fkey[level], node);

#ifdef CHECK_COUNT
            checkcount += std::min(4, (int)level - pre);
#endif

            if (curref == nullptr)
                node = nullptr;
            else
                node = N::clearDirty(curref->load());

            if (node == nullptr) {
                N::helpFlush(parentref);
                N::helpFlush(curref);
                return NULL;
            }
            if (N::isLeaf(node)) {
                Leaf *ret = N::getLeaf(node);
                N::helpFlush(parentref);
                N::helpFlush(curref);
                if (level < k->getKeyLen() - 1 || optimisticPrefixMatch) {
#ifdef CHECK_COUNT
                    checkcount += k->getKeyLen();
#endif
                    if (ret->checkKey(k)) {
                        return ret;
                    } else {
                        return NULL;
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

#ifdef CHECK_COUNT
int get_count() { return checkcount; }
#endif

typename Tree::OperationResults Tree::update(const Key *k) const {
    EpochGuard NewEpoch;
restart:
    bool needRestart = false;

    N *node = nullptr;
    N *nextNode = root;
    N *parentNode = nullptr;
    uint8_t parentKey, nodeKey = 0;
    uint32_t level = 0;
    std::atomic<N *> *parentref = nullptr;
    std::atomic<N *> *curref = nullptr;
    // bool optimisticPrefixMatch = false;

    while (true) {
        parentNode = node;
        parentKey = nodeKey;
        node = nextNode;

        auto v = node->getVersion(); // check version

        switch (checkPrefix(node, k, level)) { // increases level
        case CheckPrefixResult::NoMatch:
            if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {
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

            parentref = curref;
            curref = N::getChild(nodeKey, node);
            if (curref == nullptr)
                nextNode = nullptr;
            else
                nextNode = N::clearDirty(curref->load());

            if (nextNode == nullptr) {
                if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {
                    //                        std::cout<<"retry\n";
                    goto restart;
                }
                return OperationResults::NotFound;
            }
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
                Leaf *newleaf = new (alloc_new_node_from_type(NTypes::Leaf))
                    Leaf(leaf->fkey, k->key_len, (char *)k->value, k->val_len);
                //                    std::cout<<(int)(((BaseNode
                //                    *)newleaf)->type)<<"\n";
                flush_data((void *)newleaf, sizeof(Leaf));
                //
                N::change(node, nodeKey, N::setLeaf(newleaf));
                node->writeUnlock();
                return OperationResults::Success;
            }
            level++;
        }
        }
    }
}

bool Tree::lookupRange(const Key *start, const Key *end, const Key *continueKey,
                       Leaf *result[], std::size_t resultSize,
                       std::size_t &resultsFound) const {
    for (uint32_t i = 0; i < std::min(start->getKeyLen(), end->getKeyLen());
         ++i) {
        if (start->fkey[i] > end->fkey[i]) {
            resultsFound = 0;
            return false;
        } else if (start->fkey[i] < end->fkey[i]) {
            break;
        }
    }
    char scan_value[100];
    // enter a new epoch
    EpochGuard NewEpoch;

    Leaf *toContinue = NULL;
    bool restart;
    std::function<void(N *, std::atomic<N *> *, std::atomic<N *> *)> copy =
        [&result, &resultSize, &resultsFound, &toContinue, &copy, &scan_value](
            N *node, std::atomic<N *> *paref, std::atomic<N *> *curef) {
            if (N::isLeaf(node)) {
                if (resultsFound == resultSize) {
                    toContinue = N::getLeaf(node);
                    return;
                }
                // result[resultsFound] =
                // reinterpret_cast<TID>((N::getLeaf(node))->value);
                N::helpFlush(paref);
                N::helpFlush(curef);
                Leaf *leaf = N::getLeaf(node);
                result[resultsFound] = N::getLeaf(node);
                //                memcpy(scan_value, leaf->value, 50);
                resultsFound++;
            } else {
                std::tuple<uint8_t, std::atomic<N *> *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0u, 255u, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    std::atomic<N *> *n = std::get<1>(children[i]);
                    copy(n->load(), curef, n);
                    if (toContinue != NULL) {
                        break;
                    }
                }
            }
        };
    std::function<void(N *, uint32_t, std::atomic<N *> *, std::atomic<N *> *)>
        findStart = [&copy, &start, &findStart, &toContinue, &restart,
                     this](N *node, uint32_t level, std::atomic<N *> *paref,
                           std::atomic<N *> *curef) {
            if (N::isLeaf(node)) {
                copy(node, paref, curef);
                return;
            }

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, start, level);
            switch (prefixResult) {
            case PCCompareResults::Bigger:
                copy(node, paref, curef);
                break;
            case PCCompareResults::Equal: {
                uint8_t startLevel =
                    (start->getKeyLen() > level) ? start->fkey[level] : 0;
                std::tuple<uint8_t, std::atomic<N *> *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, startLevel, 255, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const uint8_t k = std::get<0>(children[i]);
                    std::atomic<N *> *n = std::get<1>(children[i]);
                    if (k == startLevel) {
                        findStart(n->load(), level + 1, curef, n);
                    } else if (k > startLevel) {
                        copy(n->load(), curef, n);
                    }
                    if (toContinue != NULL || restart) {
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
    std::function<void(N *, uint32_t, std::atomic<N *> *, std::atomic<N *> *)>
        findEnd = [&copy, &end, &toContinue, &restart, &findEnd,
                   this](N *node, uint32_t level, std::atomic<N *> *paref,
                         std::atomic<N *> *curef) {
            if (N::isLeaf(node)) {
                return;
            }

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, end, level);

            switch (prefixResult) {
            case PCCompareResults::Smaller:
                copy(node, paref, curef);
                break;
            case PCCompareResults::Equal: {
                uint8_t endLevel =
                    (end->getKeyLen() > level) ? end->fkey[level] : 255;
                std::tuple<uint8_t, std::atomic<N *> *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, 0, endLevel, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const uint8_t k = std::get<0>(children[i]);
                    std::atomic<N *> *n = std::get<1>(children[i]);
                    if (k == endLevel) {
                        findEnd(n->load(), level + 1, curef, n);
                    } else if (k < endLevel) {
                        copy(n->load(), curef, n);
                    }
                    if (toContinue != NULL || restart) {
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
    std::atomic<N *> *parentref = nullptr;
    std::atomic<N *> *curref = nullptr;

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
            copy(node, parentref, curref);
            break;
        }
        case PCEqualsResults::BothMatch: {
            uint8_t startLevel =
                (start->getKeyLen() > level) ? start->fkey[level] : 0;
            uint8_t endLevel =
                (end->getKeyLen() > level) ? end->fkey[level] : 255;
            if (startLevel != endLevel) {
                std::tuple<uint8_t, std::atomic<N *> *> children[256];
                uint32_t childrenCount = 0;
                N::getChildren(node, startLevel, endLevel, children,
                               childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const uint8_t k = std::get<0>(children[i]);
                    std::atomic<N *> *n = std::get<1>(children[i]);
                    if (k == startLevel) {
                        findStart(n->load(), level + 1, curref, n);
                    } else if (k > startLevel && k < endLevel) {
                        copy(n->load(), curref, n);
                    } else if (k == endLevel) {
                        findEnd(n->load(), level + 1, curref, n);
                    }
                    if (restart) {
                        goto restart;
                    }
                    if (toContinue) {
                        break;
                    }
                }
            } else {
                parentref = curref;
                curref = N::getChild(startLevel, node);
                if (curref == nullptr)
                    nextNode = nullptr;
                else
                    nextNode = N::clearDirty(curref->load());
                level++;
                continue;
            }
            break;
        }
        }
        break;
    }

    if (toContinue != NULL) {
        Key *newkey = new Key();
        newkey->Init((char *)toContinue->fkey, toContinue->key_len,
                     toContinue->value, toContinue->val_len);
        continueKey = newkey;
        return true;
    } else {
        return false;
    }
}

bool Tree::checkKey(const Key *ret, const Key *k) const {
    if (ret->getKeyLen() == k->getKeyLen() &&
        memcmp(ret->fkey, k->fkey, k->getKeyLen()) == 0) {
        return true;
    }
    return false;
}

typename Tree::OperationResults Tree::insert(const Key *k) {
    EpochGuard NewEpoch;

restart:
    bool needRestart = false;
    N *node = nullptr;
    N *nextNode = root;
    N *parentNode = nullptr;
    uint8_t parentKey, nodeKey = 0;
    uint32_t level = 0;
    std::atomic<N *> *parentref = nullptr;
    std::atomic<N *> *curref = nullptr;

    while (true) {
        parentNode = node;
        parentKey = nodeKey;
        node = nextNode;
        auto v = node->getVersion();

        uint32_t nextLevel = level;

        uint8_t nonMatchingKey;
        Prefix remainingPrefix;
        switch (checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey,
                                       remainingPrefix)) { // increases level
        case CheckPrefixPessimisticResult::SkippedLevel:
            goto restart;
        case CheckPrefixPessimisticResult::NoMatch: {
            assert(nextLevel < k->getKeyLen()); // prevent duplicate key
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart)
                goto restart;

            // 1) Create new node which will be parent of node, Set common
            // prefix, level to this node
            Prefix prefi = node->getPrefi();
            prefi.prefixCount = nextLevel - level;
            auto newNode = new (alloc_new_node_from_type(NTypes::N4))
                N4(nextLevel, prefi); // not persist

            // 2)  add node and (tid, *k) as children
            Leaf *newLeaf = new (alloc_new_node_from_type(NTypes::Leaf))
                Leaf(k); // not persist
            flush_data((void *)newLeaf, sizeof(Leaf));
            //            N::clflush((char *)newLeaf, sizeof(Leaf), true,
            //                       true); // persist leaf and key pointer

            // not persist
            newNode->insert(k->fkey[nextLevel], N::setLeaf(newLeaf), false);
            newNode->insert(nonMatchingKey, node, false);
            // persist the new node
            flush_data((void *)newNode, sizeof(N4));
            //            N::clflush((char *)newNode, sizeof(N4), true, true);

            // 3) lockVersionOrRestart, update parentNode to point to the
            // new node, unlock
            parentNode->writeLockOrRestart(needRestart);
            if (needRestart) {
                EpochGuard::DeleteNode((void *)newNode);
                EpochGuard::DeleteNode((void *)newLeaf);
                node->writeUnlock();
                goto restart;
            }

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
            break;
        }
        assert(nextLevel < k->getKeyLen()); // prevent duplicate key
        // TODO: maybe one string is substring of another, so it fkey[level]
        // will be 0 solve problem of substring
        level = nextLevel;
        nodeKey = k->fkey[level];

        parentref = curref;
        curref = N::getChild(nodeKey, node);
        if (curref == nullptr)
            nextNode = nullptr;
        else
            nextNode = N::clearDirty(curref->load());

        if (nextNode == nullptr) {
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart)
                goto restart;

            Leaf *newLeaf =
                new (alloc_new_node_from_type(NTypes::Leaf)) Leaf(k);
            flush_data((void *)newLeaf, sizeof(Leaf));
            //            N::clflush((char *)newLeaf, sizeof(Leaf), true, true);

            N::insertAndUnlock(node, parentNode, parentKey, nodeKey,
                               N::setLeaf(newLeaf), needRestart);
            if (needRestart)
                goto restart;
            //            std::cout<<"insert success\n";
            return OperationResults::Success;
        }
        if (N::isLeaf(nextNode)) {
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart)
                goto restart;
            Leaf *key = N::getLeaf(nextNode);

            level++;
            // assert(level < key->getKeyLen());
            // prevent inserting when
            // prefix of key exists already
            // but if I want to insert a prefix of this key, i also need to
            // insert successfully
            uint32_t prefixLength = 0;
            while (level + prefixLength <
                       std::min(k->getKeyLen(), key->getKeyLen()) &&
                   key->fkey[level + prefixLength] ==
                       k->fkey[level + prefixLength]) {
                prefixLength++;
            }
            // equal
            if (k->getKeyLen() == key->getKeyLen() &&
                level + prefixLength == k->getKeyLen()) {
                // duplicate key
                node->writeUnlock();
                //                std::cout<<"ohfinish\n";
                return OperationResults::Existed;
            }
            // substring

            auto n4 = new (alloc_new_node_from_type(NTypes::N4))
                N4(level + prefixLength, &k->fkey[level], prefixLength);
            Leaf *newLeaf =
                new (alloc_new_node_from_type(NTypes::Leaf)) Leaf(k);
            flush_data((void *)newLeaf, sizeof(Leaf));
            //            N::clflush((char *)newLeaf, sizeof(Leaf), true, true);

            n4->insert(k->fkey[level + prefixLength], N::setLeaf(newLeaf),
                       false);
            n4->insert(key->fkey[level + prefixLength], nextNode, false);
            flush_data((void *)n4, sizeof(N4));
            //            N::clflush((char *)n4, sizeof(N4), true, true);

            N::change(node, k->fkey[level - 1], n4);
            node->writeUnlock();
            //            std::cout<<"insert success\n";
            return OperationResults::Success;
        }
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
    std::atomic<N *> *parentref = nullptr;
    std::atomic<N *> *curref = nullptr;
    // bool optimisticPrefixMatch = false;

    while (true) {
        parentNode = node;
        parentKey = nodeKey;
        node = nextNode;
        auto v = node->getVersion();

        switch (checkPrefix(node, k, level)) { // increases level
        case CheckPrefixResult::NoMatch:
            if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {
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

            parentref = curref;
            curref = N::getChild(nodeKey, node);
            if (curref == nullptr)
                nextNode = nullptr;
            else
                nextNode = N::clearDirty(curref->load());

            if (nextNode == nullptr) {
                if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) { // TODO
                    goto restart;
                }
                return OperationResults::NotFound;
            }
            if (N::isLeaf(nextNode)) {
                node->lockVersionOrRestart(v, needRestart);
                if (needRestart)
                    goto restart;

                Leaf *leaf = N::getLeaf(nextNode);
                if (!leaf->checkKey(k)) {
                    node->writeUnlock();
                    return OperationResults::NotFound;
                }
                assert(parentNode == nullptr || node->getCount() != 1);
                if (node->getCount() == 2 && node != root) {
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
            level++;
        }
        }
    }
}

void Tree::rebuild(std::set<std::pair<uint64_t, size_t>> &rs) {
    // rebuild meta data count/compactcount
    // record all used memory (offset, size) into rs set
    N::rebuild_node(root, rs);
}

typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key *k,
                                                   uint32_t &level) {
    if (k->getKeyLen() <= n->getLevel()) {
        return CheckPrefixResult::NoMatch;
    }
    Prefix p = n->getPrefi();
    if (p.prefixCount + level < n->getLevel()) {
        level = n->getLevel();
        return CheckPrefixResult::OptimisticMatch;
    }
    if (p.prefixCount > 0) {
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < std::min(p.prefixCount, maxStoredPrefixLength); ++i) {
            if (p.prefix[i] != k->fkey[level]) {
                return CheckPrefixResult::NoMatch;
            }
            ++level;
        }
        if (p.prefixCount > maxStoredPrefixLength) {
            // level += p.prefixCount - maxStoredPrefixLength;
            level = n->getLevel();
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
        // Intermediate or inconsistent state from path compression "split" or
        // "merge" is detected Inconsistent path compressed prefix should be
        // recovered in here
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
                 i < std::min(discrimination, maxStoredPrefixLength); i++)
                p.prefix[i] = kr->fkey[level + i];
            n->setPrefix(p.prefix, p.prefixCount, true);
            n->writeUnlock();
        }

        // path compression merge is in progress --> restart from root
        // path compression split is in progress --> skipping an intermediate
        // compressed prefix by using level (invariant)
        if (p.prefixCount + level < n->getLevel()) {
            return CheckPrefixPessimisticResult::SkippedLevel;
        }
    }

    if (p.prefixCount > 0) {
        uint32_t prevLevel = level;
        Leaf *kt = NULL;
        bool load_flag = false;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i >= maxStoredPrefixLength && !load_flag) {
                //            if (i == maxStoredPrefixLength) {
                // Optimistic path compression
                kt = N::getAnyChildTid(n);
                load_flag = true;
            }
            uint8_t curKey =
                i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
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
                        nonMatchingPrefix.prefix[j] = kt->fkey[level + j + 1];
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
        Leaf *kt = NULL;
        bool load_flag = false;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i >= maxStoredPrefixLength && !load_flag) {
                // loadKey(N::getAnyChildTid(n), kt);
                kt = N::getAnyChildTid(n);
                load_flag = true;
            }
            uint8_t kLevel = (k->getKeyLen() > level) ? k->fkey[level] : 0;

            uint8_t curKey =
                i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
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
        Leaf *kt = NULL;
        bool load_flag = false;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i >= maxStoredPrefixLength && !load_flag) {
                // loadKey(N::getAnyChildTid(n), kt);
                kt = N::getAnyChildTid(n);
                load_flag = true;
            }
            uint8_t startLevel =
                (start->getKeyLen() > level) ? start->fkey[level] : 0;
            uint8_t endLevel =
                (end->getKeyLen() > level) ? end->fkey[level] : 0;

            uint8_t curKey =
                i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
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
#else

#ifdef CHECK_COUNT
__thread int checkcount = 0;
#endif

Tree::Tree() {
    std::cout << "[P-ART]\tnew P-ART\n";

    init_nvm_mgr();
    register_threadinfo();
    NVMMgr *mgr = get_nvm_mgr();
    //    Epoch_Mgr * epoch_mgr = new Epoch_Mgr();

    if (mgr->first_created) {
        // first open
        root = new (mgr->alloc_tree_root()) N256(0, {});
        flush_data((void *)root, sizeof(N256));
        //        N::clflush((char *)root, sizeof(N256), true, true);
        std::cout << "[P-ART]\tfirst create a P-ART\n";
    } else {
        // recovery
        root = reinterpret_cast<N256 *>(mgr->alloc_tree_root());
        std::cout << "[RECOVERY]\trecovery P-ART and reclaim the memory\n";
        rebuild(mgr->recovery_set);
#ifdef RECLAIM_MEMORY
        mgr->recovery_free_memory();
#endif
    }
}

Tree::~Tree() {
    // TODO: reclaim the memory of PM
    //    N::deleteChildren(root);
    //    N::deleteNode(root);
    std::cout << "[P-ART]\tshut down, free the tree\n";
    unregister_threadinfo();
    close_nvm_mgr();
}

Leaf *Tree::lookup(const Key *k) const {
    // enter a new epoch
    EpochGuard NewEpoch;

    N *node = root;

    uint32_t level = 0;
    bool optimisticPrefixMatch = false;

    while (true) {

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

        auto v = node->getVersion(); // check version

        switch (checkPrefix(node, k, level)) { // increases level
        case CheckPrefixResult::NoMatch:
            if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {
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
                if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {
                    //                        std::cout<<"retry\n";
                    goto restart;
                }
                return OperationResults::NotFound;
            }
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
                Leaf *newleaf = new (alloc_new_node_from_type(NTypes::Leaf))
                    Leaf(leaf->fkey, k->key_len, (char *)k->value, k->val_len);
                //                    std::cout<<(int)(((BaseNode
                //                    *)newleaf)->type)<<"\n";
                flush_data((void *)newleaf, sizeof(Leaf));
                //
                N::change(node, nodeKey, N::setLeaf(newleaf));
                node->writeUnlock();
                return OperationResults::Success;
            }
            level++;
        }
        }
    }
}

bool Tree::lookupRange(const Key *start, const Key *end, const Key *continueKey,
                       Leaf *result[], std::size_t resultSize,
                       std::size_t &resultsFound) const {
    for (uint32_t i = 0; i < std::min(start->getKeyLen(), end->getKeyLen());
         ++i) {
        if (start->fkey[i] > end->fkey[i]) {
            resultsFound = 0;
            return false;
        } else if (start->fkey[i] < end->fkey[i]) {
            break;
        }
    }
    char scan_value[100];
    // enter a new epoch
    EpochGuard NewEpoch;

    Leaf *toContinue = nullptr;
    bool restart;
    std::function<void(N *)> copy = [&result, &resultSize, &resultsFound,
                                     &toContinue, &copy, &scan_value](N *node) {
        if (N::isLeaf(node)) {
            if (resultsFound == resultSize) {
                toContinue = N::getLeaf(node);
                return;
            }
            // result[resultsFound] =
            // reinterpret_cast<TID>((N::getLeaf(node))->value);
            Leaf *leaf = N::getLeaf(node);
            result[resultsFound] = N::getLeaf(node);
            //                memcpy(scan_value, leaf->value, 50);
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
                copy(node);
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
        newkey->Init((char *)toContinue->fkey, toContinue->key_len,
                     toContinue->value, toContinue->val_len);
        continueKey = newkey;
        return true;
    } else {
        return false;
    }
}

bool Tree::checkKey(const Key *ret, const Key *k) const {
    return ret->getKeyLen() == k->getKeyLen() &&
           memcmp(ret->fkey, k->fkey, k->getKeyLen()) == 0;
}

typename Tree::OperationResults Tree::insert(const Key *k) {
    EpochGuard NewEpoch;

restart:
    bool needRestart = false;
    N *node = nullptr;
    N *nextNode = root;
    N *parentNode = nullptr;
    uint8_t parentKey, nodeKey = 0;
    uint32_t level = 0;

    while (true) {
        parentNode = node;
        parentKey = nodeKey;
        node = nextNode;
        auto v = node->getVersion();

        uint32_t nextLevel = level;

        uint8_t nonMatchingKey;
        Prefix remainingPrefix;
        switch (checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey,
                                       remainingPrefix)) { // increases level
        case CheckPrefixPessimisticResult::SkippedLevel:
            goto restart;
        case CheckPrefixPessimisticResult::NoMatch: {
            assert(nextLevel < k->getKeyLen()); // prevent duplicate key
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart)
                goto restart;

            // 1) Create new node which will be parent of node, Set common
            // prefix, level to this node
            Prefix prefi = node->getPrefi();
            prefi.prefixCount = nextLevel - level;
            auto newNode = new (alloc_new_node_from_type(NTypes::N4))
                N4(nextLevel, prefi); // not persist

            // 2)  add node and (tid, *k) as children
            Leaf *newLeaf = new (alloc_new_node_from_type(NTypes::Leaf))
                Leaf(k); // not persist
            flush_data((void *)newLeaf, sizeof(Leaf));
            //            N::clflush((char *)newLeaf, sizeof(Leaf), true,
            //                       true); // persist leaf and key pointer

            // not persist
            newNode->insert(k->fkey[nextLevel], N::setLeaf(newLeaf), false);
            newNode->insert(nonMatchingKey, node, false);
            // persist the new node
            flush_data((void *)newNode, sizeof(N4));
            //            N::clflush((char *)newNode, sizeof(N4), true, true);

            // 3) lockVersionOrRestart, update parentNode to point to the
            // new node, unlock
            parentNode->writeLockOrRestart(needRestart);
            if (needRestart) {
                EpochGuard::DeleteNode((void *)newNode);
                EpochGuard::DeleteNode((void *)newLeaf);
                node->writeUnlock();
                goto restart;
            }

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
            break;
        }
        assert(nextLevel < k->getKeyLen()); // prevent duplicate key
        // TODO: maybe one string is substring of another, so it fkey[level]
        // will be 0 solve problem of substring
        level = nextLevel;
        nodeKey = k->fkey[level];

        nextNode = N::getChild(nodeKey, node);

        if (nextNode == nullptr) {
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart)
                goto restart;

            Leaf *newLeaf =
                new (alloc_new_node_from_type(NTypes::Leaf)) Leaf(k);
            flush_data((void *)newLeaf, sizeof(Leaf));
            //            N::clflush((char *)newLeaf, sizeof(Leaf), true, true);

            N::insertAndUnlock(node, parentNode, parentKey, nodeKey,
                               N::setLeaf(newLeaf), needRestart);
            if (needRestart)
                goto restart;
            //            std::cout<<"insert success\n";
            return OperationResults::Success;
        }
        if (N::isLeaf(nextNode)) {
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart)
                goto restart;
            Leaf *key = N::getLeaf(nextNode);

            level++;
            // assert(level < key->getKeyLen());
            // prevent inserting when
            // prefix of key exists already
            // but if I want to insert a prefix of this key, i also need to
            // insert successfully
            uint32_t prefixLength = 0;
            while (level + prefixLength <
                       std::min(k->getKeyLen(), key->getKeyLen()) &&
                   key->fkey[level + prefixLength] ==
                       k->fkey[level + prefixLength]) {
                prefixLength++;
            }
            // equal
            if (k->getKeyLen() == key->getKeyLen() &&
                level + prefixLength == k->getKeyLen()) {
                // duplicate key
                node->writeUnlock();
                //                std::cout<<"ohfinish\n";
                return OperationResults::Existed;
            }
            // substring

            auto n4 = new (alloc_new_node_from_type(NTypes::N4))
                N4(level + prefixLength, &k->fkey[level], prefixLength);
            Leaf *newLeaf =
                new (alloc_new_node_from_type(NTypes::Leaf)) Leaf(k);
            flush_data((void *)newLeaf, sizeof(Leaf));
            //            N::clflush((char *)newLeaf, sizeof(Leaf), true, true);

            n4->insert(k->fkey[level + prefixLength], N::setLeaf(newLeaf),
                       false);
            n4->insert(key->fkey[level + prefixLength], nextNode, false);
            flush_data((void *)n4, sizeof(N4));
            //            N::clflush((char *)n4, sizeof(N4), true, true);

            N::change(node, k->fkey[level - 1], n4);
            node->writeUnlock();
            //            std::cout<<"insert success\n";
            return OperationResults::Success;
        }
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
        auto v = node->getVersion();

        switch (checkPrefix(node, k, level)) { // increases level
        case CheckPrefixResult::NoMatch:
            if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {
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
                if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) { // TODO
                    goto restart;
                }
                return OperationResults::NotFound;
            }
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
            level++;
        }
        }
    }
}

void Tree::rebuild(std::set<std::pair<uint64_t, size_t>> &rs) {
    // rebuild meta data count/compactcount
    // record all used memory (offset, size) into rs set
    N::rebuild_node(root, rs);
}

typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key *k,
                                                   uint32_t &level) {
    if (k->getKeyLen() <= n->getLevel()) {
        return CheckPrefixResult::NoMatch;
    }
    Prefix p = n->getPrefi();
    if (p.prefixCount + level < n->getLevel()) {
        level = n->getLevel();
        return CheckPrefixResult::OptimisticMatch;
    }
    if (p.prefixCount > 0) {
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < std::min(p.prefixCount, maxStoredPrefixLength); ++i) {
            if (p.prefix[i] != k->fkey[level]) {
                return CheckPrefixResult::NoMatch;
            }
            ++level;
        }
        if (p.prefixCount > maxStoredPrefixLength) {
            // level += p.prefixCount - maxStoredPrefixLength;
            level = n->getLevel();
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
        // Intermediate or inconsistent state from path compression "split" or
        // "merge" is detected Inconsistent path compressed prefix should be
        // recovered in here
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
                 i < std::min(discrimination, maxStoredPrefixLength); i++)
                p.prefix[i] = kr->fkey[level + i];
            n->setPrefix(p.prefix, p.prefixCount, true);
            n->writeUnlock();
        }

        // path compression merge is in progress --> restart from root
        // path compression split is in progress --> skipping an intermediate
        // compressed prefix by using level (invariant)
        if (p.prefixCount + level < n->getLevel()) {
            return CheckPrefixPessimisticResult::SkippedLevel;
        }
    }

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
            uint8_t curKey =
                i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
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
                        nonMatchingPrefix.prefix[j] = kt->fkey[level + j + 1];
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

            uint8_t curKey =
                i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
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

            uint8_t curKey =
                i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
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
#endif

} // namespace PART_ns
