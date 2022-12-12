//
// Created by 潘许飞 on 2022/5.
//

#pragma once
#include "N.h"
#include "N16.h"
#include "N256.h"
#include "N4.h"
#include "N48.h"
#include "LeafArray.h"
#include <libpmemobj.h>
#include <set>

namespace PART_ns {

// 类Tree定义整个Radix Tree的操作
class Tree {
  public:
  private:
    N *root;    //根节点root
#ifdef LEAF_ARRAY
    // 添加LeafArray链表的头尾指针
    LeafArray * head;
    LeafArray * tail;
    //long long leafArrayCount;
#endif

    bool checkKey(const Key *ret, const Key *k) const;

  public:
    // 用于判断前缀是否匹配的枚举类型：匹配、不匹配、乐观匹配
    enum class CheckPrefixResult : uint8_t { Match, NoMatch, OptimisticMatch };
    // 用于判断前缀是否匹配的枚举类型：匹配、不匹配、跳过层级
    enum class CheckPrefixPessimisticResult : uint8_t {
        Match,
        NoMatch,
        SkippedLevel
    };

    enum class PCCompareResults : uint8_t {
        Smaller,
        Equal,
        Bigger,
        SkippedLevel
    };
    enum class PCEqualsResults : uint8_t {
        BothMatch,
        Contained,
        NoMatch,
        SkippedLevel
    };
    enum class OperationResults : uint8_t {
        Success,
        NotFound, // remove
        Existed,  // insert
        UnSuccess
    };
    static CheckPrefixResult checkPrefix(N *n, const Key *k, uint32_t &level);

    static CheckPrefixPessimisticResult
    checkPrefixPessimistic(N *n, const Key *k, uint32_t &level,
                           uint8_t &nonMatchingKey, Prefix &nonMatchingPrefix);

    static PCCompareResults checkPrefixCompare(const N *n, const Key *k,
                                               uint32_t &level);

    static PCEqualsResults checkPrefixEquals(const N *n, uint32_t &level,
                                             const Key *start, const Key *end);

  public:
    Tree();

    Tree(const Tree &) = delete;

    ~Tree();

    void rebuild(std::vector<std::pair<uint64_t, size_t>> &rs,
                 uint64_t start_addr, uint64_t end_addr, int thread_id);

    Leaf *lookup(const Key *k) const;

    OperationResults update(const Key *k) const;

    bool lookupRange(const Key *start, const Key *end, const Key *continueKey,
                     Leaf *result[], std::size_t resultLen,
                     std::size_t &resultCount) const;

    OperationResults insert(const Key *k);

    OperationResults remove(const Key *k);

    OperationResults radixLSMRemove(const Key *k);

    Leaf *allocLeaf(const Key *k) const;

    void graphviz_debug();
} __attribute__((aligned(64)));

#ifdef ARTPMDK
void *allocate_size(size_t size);

#endif

#ifdef COUNT_ALLOC
double getalloctime();
#endif

#ifdef CHECK_COUNT
int get_count();
#endif
} // namespace PART_ns
