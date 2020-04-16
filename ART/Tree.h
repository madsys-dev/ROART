#pragma once
#include "N.h"
#include "N16.h"
#include "N256.h"
#include "N4.h"
#include "N48.h"
#include <set>

namespace PART_ns {
#ifdef LOG_FREE
class Tree {
  public:
  private:
    N *root;

    bool checkKey(const Key *ret, const Key *k) const;

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

    void rebuild(std::set<std::pair<uint64_t, size_t>> &rs);

    Leaf *lookup(const Key *k) const;

    OperationResults update(const Key *k) const;

    bool lookupRange(const Key *start, const Key *end, const Key *continueKey,
                     Leaf *result[], std::size_t resultLen,
                     std::size_t &resultCount) const;

    OperationResults insert(const Key *k);

    OperationResults remove(const Key *k);
} __attribute__((aligned(64)));

#ifdef CHECK_COUNT
int get_count();
#endif

#else
class Tree {
  public:
  private:
    N *root;

    bool checkKey(const Key *ret, const Key *k) const;

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

    void rebuild(std::set<std::pair<uint64_t, size_t>> &rs);

    Leaf *lookup(const Key *k) const;

    OperationResults update(const Key *k) const;

    bool lookupRange(const Key *start, const Key *end, const Key *continueKey,
                     Leaf *result[], std::size_t resultLen,
                     std::size_t &resultCount) const;

    OperationResults insert(const Key *k);

    OperationResults remove(const Key *k);
} __attribute__((aligned(64)));

#ifdef CHECK_COUNT
int get_count();
#endif
#endif
} // namespace PART_ns
