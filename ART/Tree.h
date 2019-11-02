//
// Created by florian on 18.11.15.
//

#ifndef ART_ROWEX_TREE_H
#define ART_ROWEX_TREE_H
#include "N.h"

namespace PART_ns {

class Tree {
   public:
    using LoadKeyFunction = void (*)(TID tid, Key &key);

   private:
    N *root;

    bool checkKey(const Key *ret, const Key *k) const;

    LoadKeyFunction loadKey;

    Epoche epoche{256};

   public:
    enum class CheckPrefixResult : uint8_t { Match, NoMatch, OptimisticMatch };

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
        NotFound,  // remove
        Existed,   // insert
        UnSuccess
    };
    static CheckPrefixResult checkPrefix(N *n, const Key *k, uint32_t &level);

    static CheckPrefixPessimisticResult checkPrefixPessimistic(
        N *n, const Key *k, uint32_t &level, uint8_t &nonMatchingKey,
        Prefix &nonMatchingPrefix, LoadKeyFunction loadKey);

    static PCCompareResults checkPrefixCompare(const N *n, const Key *k,
                                               uint32_t &level,
                                               LoadKeyFunction loadKey);

    static PCEqualsResults checkPrefixEquals(const N *n, uint32_t &level,
                                             const Key *start, const Key *end,
                                             LoadKeyFunction loadKey);

   public:
    Tree();

    Tree(const Tree &) = delete;

    Tree(Tree &&t) : root(t.root), loadKey(t.loadKey) {}

    ~Tree();

    void rebuild();

    ThreadInfo getThreadInfo();

    void *lookup(const Key *k, ThreadInfo &threadEpocheInfo) const;

    bool lookupRange(const Key *start, const Key *end, const Key *continueKey,
                     Leaf *result[], std::size_t resultLen,
                     std::size_t &resultCount,
                     ThreadInfo &threadEpocheInfo) const;

    OperationResults insert(const Key *k, ThreadInfo &epocheInfo);

    OperationResults remove(const Key *k, ThreadInfo &epocheInfo);
};
}  // namespace ART_ROWEX
#endif  // ART_ROWEX_TREE_H
