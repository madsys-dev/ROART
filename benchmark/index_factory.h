#ifndef index_factory
#define index_factory

#include "config.h"
#include "index.h"

namespace nvindex {

template <typename K, typename V, int size>
static Index<K, V, size>* getIndex(IndexType type) {
    Index<K, V, size>* btree = nullptr;
    // switch (type) {
    //     case NV_TREE:
    //         btree = new NV_tree::Btree<K, V, size>();
    //         break;
    //     case FP_TREE:
    //         btree = new FP_tree::Btree<K, V, size>();
    //         break;
    //     case RN_TREE:
    //         btree = new RN_tree::Btree<K, V, size>();
    //         break;
    //     case RN_TREE_R:
    //         btree = new RN_treeR::Btree<K, V, size>();
    //         break;
    //     case WB_TREE:
    //         btree = new WBTREE::Btree<K, V, size>();
    //         break;
    //     case WB_TREE_2:
    //         btree = new WBTREE_2::Btree<K, V, size>();
    //         break;
    //     default:
    //         printf("unknown index type\n");
    //         exit(-1);
    // }
    return btree;
}

}  // namespace nvindex

#endif
