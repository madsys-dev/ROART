#include "N.h"
#include "EpochGuard.h"
#include "N16.h"
#include "N256.h"
#include "N4.h"
#include "N48.h"
#include "Tree.h"
#include "threadinfo.h"
#include <algorithm>
#include <assert.h>
#include <iostream>

using namespace NVMMgr_ns;

namespace PART_ns {
__thread int helpcount = 0;

int gethelpcount() { return helpcount; }

Leaf::Leaf(const Key *k) : BaseNode(NTypes::Leaf) {
    key_len = k->key_len;
    val_len = k->val_len;
#ifdef KEY_INLINE
    // have allocate the memory for kv
    memcpy(kv, k->fkey, key_len);
    memcpy(kv + key_len, (void *)k->value, val_len);
#else
    // allocate from NVM for variable key
    fkey = new (alloc_new_node_from_size(key_len)) uint8_t[key_len];
    value = new (alloc_new_node_from_size(val_len)) char[val_len];
    memcpy(fkey, k->fkey, key_len);
    memcpy(value, (void *)k->value, val_len);
    flush_data((void *)fkey, key_len);
    flush_data((void *)value, val_len);

    // persist the key, without persist the link to leaf
    // no one can see the key
    // if crash without link the leaf, key can be reclaimed safely
#endif
}

// update value, so no need to alloc key
Leaf::Leaf(uint8_t *key_, size_t key_len_, char *value_, size_t val_len_)
    : BaseNode(NTypes::Leaf) {
    key_len = key_len_;
    val_len = val_len_;
#ifdef KEY_INLINE
    memcpy(kv, key_, key_len);
    memcpy(kv + key_len, value_, val_len);
#else
    fkey = key_; // no need to alloc a new key, key_ is persistent
    value = new (alloc_new_node_from_size(val_len)) char[val_len];
    memcpy(value, (void *)value_, val_len);
    flush_data((void *)value, val_len);
#endif
}

void N::helpFlush(std::atomic<N *> *n) {
    if (n == nullptr)
        return;
    N *now_node = n->load();
    // printf("help\n");
    if (N::isDirty(now_node)) {
        //        printf("help, point to type is %d\n",
        //               ((BaseNode *)N::clearDirty(now_node))->type);
        flush_data((void *)n, sizeof(N *));
        //        clflush((char *)n, sizeof(N *), true, true);
        n->compare_exchange_strong(now_node, N::clearDirty(now_node));
    }
}

void N::set_generation() {
    NVMMgr *mgr = get_nvm_mgr();
    generation_version = mgr->get_generation_version();
}

uint64_t N::get_generation() { return generation_version; }

#ifdef INSTANT_RESTART
void N::check_generation() {
    //    if(generation_version != 1100000){
    //        return;
    //    }else{
    //        generation_version++;
    //        return;
    //    }
    uint64_t mgr_generation = get_threadlocal_generation();

    uint64_t zero = 0;
    uint64_t one = 1;
    if (generation_version != mgr_generation) {
        //        printf("start to recovery of this node %lld\n",
        //        (uint64_t)this);
        if (recovery_latch.compare_exchange_strong(zero, one)) {
            //            printf("start to recovery of this node %lld\n",
            //            (uint64_t)this);
            type_version_lock_obsolete = new std::atomic<uint64_t>;
            type_version_lock_obsolete->store(convertTypeToVersion(type));
            type_version_lock_obsolete->fetch_add(0b100);

            count = 0;
            compactCount = 0;

            NTypes t = this->type;
            switch (t) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(this);
                for (int i = 0; i < 4; i++) {
                    N *child = n->children[i].load();
                    if (child != nullptr) {
                        count++;
                        compactCount = i;
                    }
                }
                break;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(this);
                for (int i = 0; i < 16; i++) {
                    N *child = n->children[i].load();
                    if (child != nullptr) {
                        count++;
                        compactCount = i;
                    }
                }
                break;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(this);
                for (int i = 0; i < 48; i++) {
                    N *child = n->children[i].load();
                    if (child != nullptr) {
                        count++;
                        compactCount = i;
                    }
                }
                break;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(this);
                for (int i = 0; i < 256; i++) {
                    N *child = n->children[i].load();
                    if (child != nullptr) {
                        count++;
                        compactCount = i;
                    }
                }
                break;
            }
            default: {
                std::cout << "[Recovery]\twrong type " << (int)type << "\n";
                assert(0);
            }
            }

            generation_version = mgr_generation;
            flush_data(&generation_version, sizeof(uint64_t));
            recovery_latch.store(zero);

        } else {
            while (generation_version != mgr_generation) {
            }
        }
    }
}
#endif

void N::setType(NTypes type) {
    type_version_lock_obsolete->fetch_add(convertTypeToVersion(type));
}

uint64_t N::convertTypeToVersion(NTypes type) {
    return (static_cast<uint64_t>(type) << 60);
}

NTypes N::getType() const {
    return static_cast<NTypes>(
        type_version_lock_obsolete->load(std::memory_order_relaxed) >> 60);
}

uint32_t N::getLevel() const { return level; }

void N::writeLockOrRestart(bool &needRestart) {
    uint64_t version;
    do {
        version = type_version_lock_obsolete->load();
        while (isLocked(version)) {
            _mm_pause();
            version = type_version_lock_obsolete->load();
        }
        if (isObsolete(version)) {
            needRestart = true;
            return;
        }
    } while (!type_version_lock_obsolete->compare_exchange_weak(version,
                                                             version + 0b10));
}

// true:  locked or obsolete or a different version
void N::lockVersionOrRestart(uint64_t &version, bool &needRestart) {
    if (isLocked(version) || isObsolete(version)) {
        needRestart = true;
        return;
    }
    if (type_version_lock_obsolete->compare_exchange_strong(version,version + 0b10)) {
        version = version + 0b10;
    } else {
        needRestart = true;
    }
}

void N::writeUnlock() { type_version_lock_obsolete->fetch_add(0b10); }

N *N::getAnyChild(N *node) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
    case NTypes::N4: {
        auto n = static_cast<const N4 *>(node);
        return n->getAnyChild();
    }
    case NTypes::N16: {
        auto n = static_cast<const N16 *>(node);
        return n->getAnyChild();
    }
    case NTypes::N48: {
        auto n = static_cast<const N48 *>(node);
        return n->getAnyChild();
    }
    case NTypes::N256: {
        auto n = static_cast<const N256 *>(node);
        return n->getAnyChild();
    }
    default: {
        assert(false);
    }
    }
    return nullptr;
}

void N::change(N *node, uint8_t key, N *val) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
    case NTypes::N4: {
        auto n = static_cast<N4 *>(node);
        n->change(key, val);
        return;
    }
    case NTypes::N16: {
        auto n = static_cast<N16 *>(node);
        n->change(key, val);
        return;
    }
    case NTypes::N48: {
        auto n = static_cast<N48 *>(node);
        n->change(key, val);
        return;
    }
    case NTypes::N256: {
        auto n = static_cast<N256 *>(node);
        n->change(key, val);
        return;
    }
    default: {
        assert(false);
    }
    }
}

template <typename curN, typename biggerN>
void N::insertGrow(curN *n, N *parentNode, uint8_t keyParent, uint8_t key,
                   N *val, NTypes type, bool &needRestart) {
    if (n->insert(key, val, true)) {
        n->writeUnlock();
        return;
    }

    // grow and lock parent
    parentNode->writeLockOrRestart(needRestart);
    if (needRestart) {
        // free_node(type, nBig);
        n->writeUnlock();
        return;
    }

    // allocate a bigger node from NVMMgr
#ifdef ARTPMDK
    biggerN *nBig = new (allocate_size(sizeof(biggerN)))
        biggerN(n->getLevel(), n->getPrefi()); // not persist
#else
    auto nBig = new (NVMMgr_ns::alloc_new_node_from_type(type))
        biggerN(n->getLevel(), n->getPrefi()); // not persist
#endif
    n->copyTo(nBig);               // not persist
    nBig->insert(key, val, false); // not persist
    // persist the node
    flush_data((void *)nBig, sizeof(biggerN));
    //    clflush((char *)nBig, sizeof(biggerN), true, true);

    N::change(parentNode, keyParent, nBig);
    parentNode->writeUnlock();

    n->writeUnlockObsolete();
    EpochGuard::DeleteNode((void *)n);
}

template <typename curN>
void N::insertCompact(curN *n, N *parentNode, uint8_t keyParent, uint8_t key,
                      N *val, NTypes type, bool &needRestart) {
    // compact and lock parent
    parentNode->writeLockOrRestart(needRestart);
    if (needRestart) {
        // free_node(type, nNew);
        n->writeUnlock();
        return;
    }

    // allocate a new node from NVMMgr
#ifdef ARTPMDK
    curN *nNew = new (allocate_size(sizeof(curN)))
        curN(n->getLevel(), n->getPrefi()); // not persist
#else
    auto nNew = new (NVMMgr_ns::alloc_new_node_from_type(type))
        curN(n->getLevel(), n->getPrefi()); // not persist
#endif
    n->copyTo(nNew);               // not persist
    nNew->insert(key, val, false); // not persist
    // persist the node
    flush_data((void *)nNew, sizeof(curN));
    //    clflush((char *)nNew, sizeof(curN), true, true);

    N::change(parentNode, keyParent, nNew);
    parentNode->writeUnlock();

    n->writeUnlockObsolete();
    EpochGuard::DeleteNode((void *)n);
}

void N::insertAndUnlock(N *node, N *parentNode, uint8_t keyParent, uint8_t key,
                        N *val, bool &needRestart) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
    case NTypes::N4: {
        auto n = static_cast<N4 *>(node);
        if (n->compactCount == 4 && n->count <= 3) {
            insertCompact<N4>(n, parentNode, keyParent, key, val, NTypes::N4,
                              needRestart);
            break;
        }
        insertGrow<N4, N16>(n, parentNode, keyParent, key, val, NTypes::N16,
                            needRestart);
        break;
    }
    case NTypes::N16: {
        auto n = static_cast<N16 *>(node);
        if (n->compactCount == 16 && n->count <= 14) {
            insertCompact<N16>(n, parentNode, keyParent, key, val, NTypes::N16,
                               needRestart);
            break;
        }
        insertGrow<N16, N48>(n, parentNode, keyParent, key, val, NTypes::N48,
                             needRestart);
        break;
    }
    case NTypes::N48: {
        auto n = static_cast<N48 *>(node);
        if (n->compactCount == 48 && n->count != 48) {
            insertCompact<N48>(n, parentNode, keyParent, key, val, NTypes::N48,
                               needRestart);
            break;
        }
        insertGrow<N48, N256>(n, parentNode, keyParent, key, val, NTypes::N256,
                              needRestart);
        break;
    }
    case NTypes::N256: {
        auto n = static_cast<N256 *>(node);
        n->insert(key, val, true);
        node->writeUnlock();
        break;
    }
    default: {
        assert(false);
    }
    }
}

N *N::getChild(const uint8_t k, N *node) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
    case NTypes::N4: {
        auto n = static_cast<N4 *>(node);
        return n->getChild(k);
    }
    case NTypes::N16: {
        auto n = static_cast<N16 *>(node);
        return n->getChild(k);
    }
    case NTypes::N48: {
        auto n = static_cast<N48 *>(node);
        return n->getChild(k);
    }
    case NTypes::N256: {
        auto n = static_cast<N256 *>(node);
        return n->getChild(k);
    }
    default: {
        assert(false);
    }
    }
    return nullptr;
}

// only use in normally shutdown
void N::deleteChildren(N *node) {
    if (N::isLeaf(node)) {
        return;
    }
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
    case NTypes::N4: {
        auto n = static_cast<N4 *>(node);
        n->deleteChildren();
        return;
    }
    case NTypes::N16: {
        auto n = static_cast<N16 *>(node);
        n->deleteChildren();
        return;
    }
    case NTypes::N48: {
        auto n = static_cast<N48 *>(node);
        n->deleteChildren();
        return;
    }
    case NTypes::N256: {
        auto n = static_cast<N256 *>(node);
        n->deleteChildren();
        return;
    }
    default: {
        assert(false);
    }
    }
}

template <typename curN, typename smallerN>
void N::removeAndShrink(curN *n, N *parentNode, uint8_t keyParent, uint8_t key,
                        NTypes type, bool &needRestart) {
    if (n->remove(key, parentNode == nullptr, true)) {
        n->writeUnlock();
        return;
    }

    // shrink and lock parent
    parentNode->writeLockOrRestart(needRestart);
    if (needRestart) {
        // free_node(type, nSmall);
        n->writeUnlock();
        return;
    }

    // allocate a smaller node from NVMMgr
#ifdef ARTPMDK
    smallerN *nSmall = new (allocate_size(sizeof(smallerN)))
        smallerN(n->getLevel(), n->getPrefi()); // not persist
#else
    auto nSmall = new (NVMMgr_ns::alloc_new_node_from_type(type))
        smallerN(n->getLevel(), n->getPrefi()); // not persist
#endif
    n->remove(key, true, true);
    n->copyTo(nSmall); // not persist

    // persist the node
    flush_data((void *)nSmall, sizeof(smallerN));
    //    clflush((char *)nSmall, sizeof(smallerN), true, true);
    N::change(parentNode, keyParent, nSmall);

    parentNode->writeUnlock();
    n->writeUnlockObsolete();
    EpochGuard::DeleteNode((void *)n);
}

void N::removeAndUnlock(N *node, uint8_t key, N *parentNode, uint8_t keyParent,
                        bool &needRestart) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
    case NTypes::N4: {
        auto n = static_cast<N4 *>(node);
        n->remove(key, false, true);
        n->writeUnlock();
        break;
    }
    case NTypes::N16: {
        auto n = static_cast<N16 *>(node);
        removeAndShrink<N16, N4>(n, parentNode, keyParent, key, NTypes::N4,
                                 needRestart);
        break;
    }
    case NTypes::N48: {
        auto n = static_cast<N48 *>(node);
        removeAndShrink<N48, N16>(n, parentNode, keyParent, key, NTypes::N16,
                                  needRestart);
        break;
    }
    case NTypes::N256: {
        auto n = static_cast<N256 *>(node);
        removeAndShrink<N256, N48>(n, parentNode, keyParent, key, NTypes::N48,
                                   needRestart);
        break;
    }
    default: {
        assert(false);
    }
    }
}

bool N::isLocked(uint64_t version) const { return ((version & 0b10) == 0b10); }

uint64_t N::getVersion() const { return type_version_lock_obsolete->load(); }

bool N::isObsolete(uint64_t version) { return (version & 1) == 1; }

bool N::checkOrRestart(uint64_t startRead) const {
    return readUnlockOrRestart(startRead);
}

bool N::readUnlockOrRestart(uint64_t startRead) const {
    return startRead == type_version_lock_obsolete->load();
}

void N::setCount(uint16_t count_, uint16_t compactCount_) {
    count = count_;
    compactCount = compactCount_;
}

// only invoked in the critical section
uint32_t N::getCount(N *node) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
    case NTypes::N4: {
        auto n = static_cast<const N4 *>(node);
        return n->getCount();
    }
    case NTypes::N16: {
        auto n = static_cast<const N16 *>(node);
        return n->getCount();
    }
    case NTypes::N48: {
        auto n = static_cast<const N48 *>(node);
        return n->getCount();
    }
    case NTypes::N256: {
        auto n = static_cast<const N256 *>(node);
        return n->getCount();
    }
    default: {
        return 0;
    }
    }
}

Prefix N::getPrefi() const { return prefix.load(); }

void N::setPrefix(const uint8_t *prefix, uint32_t length, bool flush) {
    if (length > 0) {
        Prefix p;
        memcpy(p.prefix, prefix, std::min(length, maxStoredPrefixLength));
        p.prefixCount = length;
        this->prefix.store(p, std::memory_order_release);
    } else {
        Prefix p;
        p.prefixCount = 0;
        this->prefix.store(p, std::memory_order_release);
    }
    if (flush)
        flush_data((void *)&(this->prefix), sizeof(Prefix));
}

void N::addPrefixBefore(N *node, uint8_t key) {
    Prefix p = this->getPrefi();
    Prefix nodeP = node->getPrefi();
    uint32_t prefixCopyCount =
        std::min(maxStoredPrefixLength, nodeP.prefixCount + 1);
    memmove(p.prefix + prefixCopyCount, p.prefix,
            std::min(p.prefixCount, maxStoredPrefixLength - prefixCopyCount));
    memcpy(p.prefix, nodeP.prefix,
           std::min(prefixCopyCount, nodeP.prefixCount));
    if (nodeP.prefixCount < maxStoredPrefixLength) {
        p.prefix[prefixCopyCount - 1] = key;
    }
    p.prefixCount += nodeP.prefixCount + 1;
    this->prefix.store(p, std::memory_order_release);
    flush_data((void *)&(this->prefix), sizeof(Prefix));
}

bool N::isLeaf(const N *n) {
    return (reinterpret_cast<uintptr_t>(n) & (1ULL << 0));
}

N *N::setLeaf(const Leaf *k) {
    return reinterpret_cast<N *>(reinterpret_cast<void *>(
        (reinterpret_cast<uintptr_t>(k) | (1ULL << 0))));
}

Leaf *N::getLeaf(const N *n) {
    return reinterpret_cast<Leaf *>(reinterpret_cast<void *>(
        (reinterpret_cast<uintptr_t>(n) & ~(1ULL << 0))
        ));
}

// only invoke this in remove and N4
std::tuple<N *, uint8_t> N::getSecondChild(N *node, const uint8_t key) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
    case NTypes::N4: {
        auto n = static_cast<N4 *>(node);
        return n->getSecondChild(key);
    }
    default: {
        assert(false);
    }
    }
}

// only invoke in the shutdown normally
void N::deleteNode(N *node) {
    if (N::isLeaf(node)) {
        return;
    }
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
    case NTypes::N4: {
        auto n = static_cast<N4 *>(node);
        delete n;
        return;
    }
    case NTypes::N16: {
        auto n = static_cast<N16 *>(node);
        delete n;
        return;
    }
    case NTypes::N48: {
        auto n = static_cast<N48 *>(node);
        delete n;
        return;
    }
    case NTypes::N256: {
        auto n = static_cast<N256 *>(node);
        delete n;
        return;
    }
    default: {
        assert(false);
    }
    }
    delete node;
}

// invoke in the insert
// not all nodes are in the critical secton
Leaf *N::getAnyChildTid(const N *n) {
    const N *nextNode = n;

    while (true) {
        N *node = const_cast<N *>(nextNode);
        nextNode = getAnyChild(node);

        assert(nextNode != nullptr);
        if (isLeaf(nextNode)) {
            return getLeaf(nextNode);
        }
    }
}

// for range query
void N::getChildren(N *node, uint8_t start, uint8_t end,
                    std::tuple<uint8_t, N *> children[],
                    uint32_t &childrenCount) {
#ifdef INSTANT_RESTART
    node->check_generation();
#endif
    switch (node->getType()) {
    case NTypes::N4: {
        auto n = static_cast<N4 *>(node);
        n->getChildren(start, end, children, childrenCount);
        return;
    }
    case NTypes::N16: {
        auto n = static_cast<N16 *>(node);
        n->getChildren(start, end, children, childrenCount);
        return;
    }
    case NTypes::N48: {
        auto n = static_cast<N48 *>(node);
        n->getChildren(start, end, children, childrenCount);
        return;
    }
    case NTypes::N256: {
        auto n = static_cast<N256 *>(node);
        n->getChildren(start, end, children, childrenCount);
        return;
    }
    default: {
        assert(false);
    }
    }
}

void N::rebuild_node(N *node, std::vector<std::pair<uint64_t, size_t>> &rs,
                     uint64_t start_addr, uint64_t end_addr, int thread_id) {
    if (N::isLeaf(node)) {
        // leaf node
#ifdef RECLAIM_MEMORY
        Leaf *leaf = N::getLeaf(node);
        if ((uint64_t)leaf < start_addr || (uint64_t)leaf >= end_addr)
            return;
#ifdef KEY_INLINE
        size_t size =
            size_align(sizeof(Leaf) + leaf->key_len + leaf->val_len, 64);
        //        size = convert_power_two(size);
        rs.push_back(std::make_pair((uint64_t)leaf, size));
#else
        NTypes type = leaf->type;
        size_t size = size_align(get_node_size(type), 64);
        //        size = convert_power_two(size);
        rs.insert(std::make_pair((uint64_t)leaf, size));

        // leaf key also need to insert into rs set
        size = leaf->key_len;
        //        size = convert_power_two(size);
        rs.insert(std::make_pair((uint64_t)(leaf->fkey), size));

        // value
        size = leaf->val_len;
        //        size = convert_power_two(size);
        rs.insert(std::make_pair((uint64_t)(leaf->value), size));
#endif // KEY_INLINE

#endif
        return;
    }
    // insert internal node into set
    NTypes type = node->type;
#ifdef RECLAIM_MEMORY
    if ((uint64_t)node >= start_addr && (uint64_t)node < end_addr) {
        size_t size = size_align(get_node_size(type), 64);
        //    size = convert_power_two(size);
        rs.push_back(std::make_pair((uint64_t)node, size));
    }
#endif

    int xcount = 0;
    int xcompactCount = 0;
    // type is persistent when first create this node
    // TODO: using SIMD to accelerate recovery
    switch (type) {
    case NTypes::N4: {
        auto n = static_cast<N4 *>(node);
        for (int i = 0; i < 4; i++) {
            N *child = n->children[i].load();
            if (child != nullptr) {
                xcount++;
                xcompactCount = i;
                rebuild_node(child, rs, start_addr, end_addr, thread_id);
            }
        }
        break;
    }
    case NTypes::N16: {
        auto n = static_cast<N16 *>(node);
        for (int i = 0; i < 16; i++) {
            N *child = n->children[i].load();
            if (child != nullptr) {
                xcount++;
                xcompactCount = i;
                rebuild_node(child, rs, start_addr, end_addr, thread_id);
            }
        }
        break;
    }
    case NTypes::N48: {
        auto n = static_cast<N48 *>(node);
        for (int i = 0; i < 48; i++) {
            N *child = n->children[i].load();
            if (child != nullptr) {
                xcount++;
                xcompactCount = i;
                rebuild_node(child, rs, start_addr, end_addr, thread_id);
            }
        }
        break;
    }
    case NTypes::N256: {
        auto n = static_cast<N256 *>(node);
        for (int i = 0; i < 256; i++) {
            N *child = n->children[i].load();
            if (child != nullptr) {
                xcount++;
                xcompactCount = i;
                rebuild_node(child, rs, start_addr, end_addr, thread_id);
            }
        }
        break;
    }
    default: {
        std::cout << "[Rebuild]\twrong type is " << (int)type << "\n";
        assert(0);
    }
    }
    // reset count and version and lock
#ifdef INSTANT_RESTART
#else
    if ((uint64_t)node >= start_addr && (uint64_t)node < end_addr) {
        node->setCount(xcount, xcompactCount);
        node->type_version_lock_obsolete = new std::atomic<uint64_t>;
        node->type_version_lock_obsolete->store(convertTypeToVersion(type));
        node->type_version_lock_obsolete->fetch_add(0b100);
        node->old_pointer.store(0);
        //        rs.push_back(std::make_pair(0,0));
    }
#endif
}
} // namespace PART_ns
