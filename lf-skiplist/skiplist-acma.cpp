#include "skiplist-acma.h"
#include <string.h>

 using namespace NVMMgr_ns;

 namespace skiplist {

    static inline UINT_PTR unmarked_ptr(UINT_PTR p) {
        return (p & ~(UINT_PTR)0x01);
    }

#define UNMARKED_PTR(p) (node_t *)unmarked_ptr((UINT_PTR)p)

    static inline UINT_PTR unmarked_ptr_all(UINT_PTR p) {
        return (p & ~(UINT_PTR)0x07);
    }

#define UNMARKED_PTR_ALL(p) (node_t *)unmarked_ptr_all((UINT_PTR)p)

    static inline UINT_PTR marked_ptr(UINT_PTR p) { return (p |
    (UINT_PTR)0x01); }

#define MARKED_PTR(p) (node_t *)marked_ptr((UINT_PTR)p)

    static inline int ptr_is_marked(UINT_PTR p) {
        return (int)(p & (UINT_PTR)0x01);
    }

#define PTR_IS_MARKED(p) ptr_is_marked((UINT_PTR)p)

    int comparekey(node_t *n, skey_t k2) {
#ifdef VARIABLE_LENGTH
        int len2 = strlen(k2);
        if (n->max_min_flag == MIN_KEY)
            return -1;
        if (n->max_min_flag == MAX_KEY)
            return 1;
        skey_t k1 = n->key;
        int len1 = n->key_len;
        if (len1 == len2) {
            return memcmp(k1, k2, len1);
        } else {
            if (len1 < len2) {
                int ret = memcmp(k1, k2, len1);
                if (ret != 0)
                    return ret;
                else
                    return -1;
            } else {
                int ret = memcmp(k1, k2, len2);
                if (ret != 0)
                    return ret;
                else
                    return 1;
            }
        }
#else // fix length
        if (n->max_min_flag == MIN_KEY)
        return -1;
    if (n->max_min_flag == MAX_KEY)
        return 1;
    skey_t k1 = n->key;
    if (k1 == k2)
        return 0;
    else if (k1 < k2)
        return -1;
    else
        return 1;
#endif
    }

    int get_random_level() {
        int i;
        int level = 1;

        for (i = 0; i < max_level - 1; i++) {
            if (rand() % 100 < 50) { // TODO replace the mod with something else?
                level++;
            } else {
                break;
            }
        }
        return level;
    }

    void init_new_node_and_set_next(node_t *the_node, skey_t key, svalue_t
    value,
                                    int height) {
#ifdef VARIABLE_LENGTH
        the_node->key = key;
        the_node->key_len = (key == nullptr) ? 0 : strlen(key);
        the_node->value = value;
        the_node->val_len = (value == nullptr) ? 0 : strlen(value);
        the_node->max_min_flag = 0;
        the_node->toplevel = height;
#else
        the_node->key = key;
    the_node->key_len = sizeof(uint64_t);
    the_node->value = value;
    the_node->val_len = sizeof(uint64_t);
    the_node->max_min_flag = 0;
    the_node->toplevel = height;
#endif
    }

    skiplist_t *new_skiplist() {
        std::cout << "skiplist with ACMA test\n";

        init_nvm_mgr();
        register_threadinfo();
        NVMMgr *mgr = get_nvm_mgr();

        node_t *min = nullptr;
        node_t *max = nullptr;

#ifdef USE_PMDK
        min = (node_t *)alloc_new_node_from_size(sizeof(node_t));
        max = (node_t *)alloc_new_node_from_size(sizeof(node_t));
#else
        min = new node_t;
    max = new node_t;
#endif

        init_new_node_and_set_next(max, (skey_t) nullptr, (svalue_t) nullptr,
                                   max_level);
        init_new_node_and_set_next(min, (skey_t) nullptr, (svalue_t) nullptr,
                                   max_level);
        for (int i = 0; i < max_level; i++) {
            min->next[i] = max;
        }

        min->max_min_flag = MIN_KEY;
        max->max_min_flag = MAX_KEY;
        flush_data(min, sizeof(node_t));
        flush_data(max, sizeof(node_t));

        skiplist_t *sl = (skiplist_t *)malloc(sizeof(skiplist_t));
        (*sl) = min;

        std::cout << "test different component\n";
#ifdef USE_PMDK
        std::cout << "with DCMM allocator\n";

#else
        std::cout << "using DRAM allocator\n";
#endif

#ifdef VARIABLE_LENGTH
        std::cout << "variable length key\n";
#else
        std::cout << "fixed length key\n";
#endif

        return sl;
    }

    void sl_finalize_node(void *node) {
//        ti->AddGarbageNode(node);
    }


    int sl_search(skiplist_t *sl, skey_t key, node_t **left_nodes,
                  node_t **right_nodes) {
        int i;
        node_t *left;
        node_t *right = nullptr;
        node_t *left_next;
        node_t *right_next;

        retry:
        left = (*sl);
        for (i = max_level - 1; i >= 0; i--) {
            left_next = (node_t *)unmark_ptr_cache((UINT_PTR)left->next[i]);
            if (PTR_IS_MARKED(left_next)) {
                goto retry;
            }

            for (right = left_next;; right = right_next) {
                // right_next = right->next[i];
                right_next = (node_t
                *)unmark_ptr_cache((UINT_PTR)right->next[i]); while
                ((PTR_IS_MARKED(right_next))) {
                    right = UNMARKED_PTR_ALL(right_next);
                    right_next =
                            (node_t
                            *)unmark_ptr_cache((UINT_PTR)right->next[i]);
                }
                if (comparekey((node_t *)right, key) >= 0) {
                    break;
                }
                left = right;
                left_next = right_next;
            }

            if ((left_next != right) &&
                (CAS_PTR((PVOID *)&(left->next[i]), (PVOID)left_next,
                         (PVOID)right) != left_next)) {
                goto retry;
            }
            if (left_next != right) {
                flush_data((void *)&(left->next[i]), sizeof(uint64_t));
            }
            if ((left_next != right) && ((i == 0))) {
                flush_data((void *)&(left->next[i]), sizeof(uint64_t));
            }

            if (i != 0) {
                flush_data(&(left_next), sizeof(uint64_t));

                if ((left_next != right) &&
                    (CAS_PTR((PVOID *)&(left->next[i]), (PVOID)left_next,
                             (PVOID)right) != left_next)) {
                    goto retry;
                }
                if (left_next != right) {
                    flush_data((void *)&(left->next[i]), sizeof(uint64_t));
                }
            } else {
                if ((left_next != right) &&
                    ((node_t *)link_and_persist((PVOID *)&(left->next[i]),
                                                (PVOID)left_next,
                                                (PVOID)right) != left_next)) {
                    goto retry;
                }
            }

            // recovery can handle out-of order links

            if (left_nodes != nullptr) {
                left_nodes[i] = left;
            }

            if (right_nodes != nullptr) {
                right_nodes[i] = right;
            }
        }

        flush_and_try_unflag((PVOID *)&(left->next[0]));
        return (comparekey((node_t *)right, key) == 0);
    }


    int sl_search_no_cleanup(skiplist_t *sl, skey_t key, node_t **left_nodes,
                             node_t **right_nodes) {
        int i;
        node_t *left;
        node_t *right = nullptr;
        node_t *left_next;
        node_t *lpred;

        left = (*sl);
        lpred = (*sl);

        for (i = max_level - 1; i >= 0; i--) {
            left_next = UNMARKED_PTR_ALL(left->next[i]);
            right = left_next;
            while (1) {
                if (!PTR_IS_MARKED(right->next[i])) {
                    if (comparekey((node_t *)right, key) >= 0) {
                        break;
                    }
                    lpred = left;
                    left = right;
                }
                right = UNMARKED_PTR_ALL(right->next[i]);
            }
            left_nodes[i] = left;
            right_nodes[i] = right;
        }

        flush_and_try_unflag((PVOID *)&(left->next[0]));
        flush_and_try_unflag((PVOID *)&(lpred->next[0]));

        return (comparekey((node_t *)right, key) == 0);
    }

// simple search, does not do any cleanup, only sets the successors of the
// retrieved node
    int sl_search_no_cleanup_succs(skiplist_t *sl, skey_t key,
                                   node_t **right_nodes) {
        int i;
        node_t *left;
        node_t *right = nullptr;
        node_t *left_next;
        node_t *lpred;

        left = (*sl);
        lpred = (*sl);
        for (i = max_level - 1; i >= 0; i--) {
            left_next = UNMARKED_PTR_ALL(left->next[i]);
            right = left_next;
            while (1) {
                if (!PTR_IS_MARKED(right->next[i])) {
                    if (comparekey((node_t *)right, key) >= 0) {
                        break;
                    }
                    lpred = left;
                    left = right;
                }
                right = UNMARKED_PTR_ALL(right->next[i]);
            }
            right_nodes[i] = right;
        }

        // if (right->key != key) {
        flush_and_try_unflag((PVOID *)&(left->next[0]));
        flush_and_try_unflag((PVOID *)&(lpred->next[0]));
        //}

        return (comparekey((node_t *)right, key) == 0);
    }

    inline int mark_node_pointers(node_t *node) {
        int i;
        int success = 0;
        // fprintf(stderr, "in mark node ptrs\n");

        node_t *next_node;

        for (i = node->toplevel - 1; i >= 1; i--) {
            do {
                next_node = node->next[i];
                if (PTR_IS_MARKED(next_node)) {
                    success = 0;
                    break;
                }
                success =
                        (CAS_PTR((PVOID *)&node->next[i],
                        UNMARKED_PTR(next_node),
                                 MARKED_PTR(next_node)) ==
                                 UNMARKED_PTR(next_node))
                        ? 1
                        : 0;
#ifdef SIMULATE_NAIVE_IMPLEMENTATION
                write_data_wait((void *)(&node->next[i]), 1);
#endif
            } while (success == 0);
        }

        do {
            next_node = node->next[0];
            if (PTR_IS_MARKED(next_node)) {
                success = 0;
                break;
            }
            success = ((node_t *)link_and_persist(
                    (PVOID *)&node->next[0], UNMARKED_PTR(next_node),
                    MARKED_PTR(next_node)) == UNMARKED_PTR(next_node))
                      ? 1
                      : 0;
        } while (success == 0);
        return success;
    }

    svalue_t skiplist_remove(skiplist_t *sl, skey_t key) {
        node_t *successors[max_level];
        svalue_t result = 0;

        // fprintf(stderr, "in rmove\n");
        EpochGuard NewEpoch;
        int found = sl_search_no_cleanup_succs(sl, key, successors);

        // fprintf(stderr, "in rmove 2\n");
        if (!found) {

            return 0;
        }

        node_t *node_to_delete = successors[0];
        int delete_successful = mark_node_pointers(node_to_delete);

        if (delete_successful) {
            result = node_to_delete->value;
            sl_search(sl, key, nullptr, nullptr);

#ifdef USE_PMDK
#ifdef FF_GC
            sl_finalize_node((void *)node_to_delete);
            sl_finalize_node((void *)node_to_delete->key);
            sl_finalize_node((void *)node_to_delete->value);

#endif
#endif
        }
        return result;
    }

    int skiplist_insert(skiplist_t *sl, skey_t key, svalue_t val) {
        node_t *to_insert;
        node_t *pred;
        node_t *succ;

        node_t *succs[max_level];
        node_t *preds[max_level];

        UINT32 i;
        UINT32 j;
        int found;

        EpochGuard NewEpoch;
        retry:
        found = sl_search_no_cleanup(sl, key, preds, succs);

        if (found) {
            flush_and_try_unflag((PVOID *)&(preds[0]->next));
            return 0;
        }

#ifdef USE_PMDK

#ifdef VARIABLE_LENGTH
        char *kk, *vv;
        kk = (char *)alloc_new_node_from_size(strlen(key) + 1);
        vv = (char *)alloc_new_node_from_size(strlen(val) + 1);
        to_insert = (node_t *)alloc_new_node_from_size(sizeof(node_t));

                        memcpy(kk, key, strlen(key) + 1);
                        memcpy(vv, val, strlen(val) + 1);
                        flush_data(kk, strlen(key) + 1);
                        flush_data(vv, strlen(val) + 1);
                        init_new_node_and_set_next(to_insert, (skey_t)kk,
                        (svalue_t)vv,
                                                   get_random_level());
#else // fix length
        to_insert = (node_t *)alloc_new_node_from_size(sizeof(node_t));
        init_new_node_and_set_next(to_insert, key, val, get_random_level());

#endif

#else // USE_PMDK

        #ifdef VARIABLE_LENGTH
    char *kk, *vv;
    to_insert = new node_t;
    kk = new char[strlen(key) + 1];
    vv = new char[strlen(val) + 1];
    memcpy(kk, key, strlen(key) + 1);
    memcpy(vv, val, strlen(val) + 1);
    flush_data(kk, strlen(key) + 1);
    flush_data(vv, strlen(val) + 1);
    init_new_node_and_set_next(to_insert, (skey_t)kk, (svalue_t)vv,
                               get_random_level());

#else // fix length
    to_insert = new node_t;
    init_new_node_and_set_next(to_insert, (skey_t)key, (svalue_t)val,
                               get_random_level());

#endif

#endif
        for (i = 0; i < to_insert->toplevel; i++) {
            to_insert->next[i] = succs[i];
        }
        flush_data(
                (void *)to_insert,
                sizeof(node_t));

        if ((node_t *)link_and_persist(
                (PVOID *)&(preds[0]->next[0]),
                (PVOID)UNMARKED_PTR_ALL(succs[0]), (PVOID)to_insert) !=
                UNMARKED_PTR_ALL(succs[0])) {
            // if (CAS_PTR((PVOID*)&preds[0]->next[0], UNMARKED_PTR(succs[0]),
            // (PVOID)to_insert) != UNMARKED_PTR(succs[0])) { failed to insert the
            // node, so I can actually free it if I want there's no chance anyone
            // has a reference to it, right? so I can just call the finalize on it
            // directly EpochReclaimObject(epoch, (void*) to_insert, NULL, NULL,
            // finalize_node);
#ifdef USE_PMDK
#ifdef FF_GC
            sl_finalize_node((void *)to_insert);
            sl_finalize_node((void *)to_insert->key);
            sl_finalize_node((void *)to_insert->value);
#endif
#endif
            goto retry;
        }
        // write_data_wait((void*)&(preds[0]->next[0]), 1);

        node_t *new_next;

        for (i = 1; i < to_insert->toplevel; i++) {
            while (1) {
                pred = preds[i];
                succ = succs[i];

                new_next = to_insert->next[i];
                if (PTR_IS_MARKED(new_next)) {
                    // fprintf(stderr, "marked 2\n");
                    // sl_search(sl, key+1, NULL, NULL, buffer, epoch);
                    // sl_search(sl, key, NULL, NULL, buffer, epoch);
                    return 1;
                }

                // tentative fix for problematic case in the original fraser
                // algorithm
                if ((succ != new_next) &&
                    (CAS_PTR(&(to_insert->next[i]), new_next, succ) !=
                    new_next)) {
                    // fprintf(stderr, "case\n");
                    for (j = 1; j < to_insert->toplevel; j++) {
                        flush_data(&preds[j], sizeof(uint64_t));
                    }
                    return 1;
                }

                if (CAS_PTR((PVOID *)&pred->next[i], (PVOID)succ,
                            (PVOID)to_insert) == succ) {
                    flush_data((void *)&(pred->next[i]), sizeof(uint64_t));
                    if (PTR_IS_MARKED(to_insert->next[to_insert->toplevel -
                    1])) {
                        // fprintf(stderr, "marked\n");
                        sl_search(sl, key + 1, NULL, NULL);
                        // sl_search(sl, key, NULL, NULL, buffer, epoch);
                        for (j = 1; j < to_insert->toplevel; j++) {
                            flush_data(&preds[j], sizeof(uint64_t));
                        }
                        return 1;
                    }
                    break;
                }
                sl_search(sl, key, preds, succs);
            }
        }

        for (j = 1; j < to_insert->toplevel; j++) {
            flush_data(&preds[j], sizeof(uint64_t));
        }
        return 1;
    }

    void skiplist_update(skiplist_t *sl, skey_t key, svalue_t val) {
        node_t *successors[max_level];
        svalue_t result = 0;

        // fprintf(stderr, "in rmove\n");
        EpochGuard NewEpoch;
        retry:
        int found = sl_search_no_cleanup_succs(sl, key, successors);

        // fprintf(stderr, "in rmove 2\n");
        if (!found) {
            return;
        }

        node_t *node_to_update = successors[0];

        if (!PTR_IS_MARKED(node_to_update)) {
            svalue_t new_val;
            svalue_t old_val = node_to_update->value;

#ifdef USE_PMDK

#ifdef VARIABLE_LENGTH
            new_val = (char *)alloc_new_node_from_size(strlen(val) + 1);
            memcpy(new_val, val, strlen(val) + 1);
            flush_data(new_val, strlen(val) + 1);
#else // fix length
            // do nothing
        new_val = val;
#endif

#else // USE_PMDK

            #ifdef VARIABLE_LENGTH
        new_val = new char[strlen(val) + 1];
        memcpy(new_val, val, strlen(val) + 1);
        flush_data(new_val, strlen(val) + 1);
#else // fix length
        new_val = val;
#endif

#endif

            PVOID res;
            res = link_and_persist((PVOID *)&(node_to_update->value),
                                   (PVOID)old_val, (PVOID)new_val);

            if (res != (PVOID)old_val) {
                goto retry; // update fail, retry
            }

#ifdef VARIABLE_LENGTH
            node_to_update->val_len = strlen(val);
#else // fix length
            node_to_update->val_len = sizeof(uint8_t);
#endif
        }
        return;
    }

// simple search, no cleanup;
    static node_t *sl_left_search(skiplist_t *sl, skey_t key) {
        node_t *left = nullptr;
        node_t *left_prev;

        left_prev = UNMARKED_PTR_ALL(*sl);

        int level;
        for (level = max_level - 1; level >= 0; level--) {
            left = UNMARKED_PTR_ALL(left_prev->next[level]);
            while (comparekey((node_t *)left, key) < 0 ||
                   PTR_IS_MARKED(left->next[level])) {
                if (!PTR_IS_MARKED(left->next[level])) {
                    left_prev = left;
                }
                left = UNMARKED_PTR_ALL(left->next[level]);
            }
            if (comparekey((node_t *)left, key) == 0) {
                break;
            }
        }
        flush_and_try_unflag((PVOID *)&(left->next[0]));
        return left;
    }

    svalue_t skiplist_find(skiplist_t *sl, skey_t key) {

        svalue_t result = (svalue_t) nullptr;

        EpochGuard NewEpoch;

        node_t *left = sl_left_search(sl, key);

        if (comparekey((node_t *)left, key) == 0) {
            flush_and_try_unflag((PVOID *)&(left->value));
            result = left->value;
        }


        return result;
    }

    void skiplist_scan(skiplist_t *sl, skey_t min, svalue_t *buf, int num, int
    &off,
                       char *scan_value) {
        EpochGuard NewEpoch;
        node_t *left = sl_left_search(sl, min);
        if (comparekey((node_t *)left, min) >= 0) {
            while (left->max_min_flag != MAX_KEY) {
                flush_and_try_unflag((PVOID *)&(left->value));
                if (off == num) {
                    return;
                }
                buf[off++] = left->value;
                //                memcpy(scan_value, left->value, 0);

                flush_and_try_unflag((PVOID *)&(left->next[0]));
                left = left->next[0];
            }
        }
    }
} // namespace skiplist
