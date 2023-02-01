#ifndef BTREE_H_
#define BTREE_H_

#include "pmdk_gc.h"
#include "util.h"
#include <boost/thread/shared_mutex.hpp>
#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#include <libpmemobj.h>
#include <math.h>
#include <mutex>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <vector>

namespace fastfair {
#define PAGESIZE 512

static uint64_t CPU_FREQ_MHZ = 2100;
static uint64_t CACHE_LINE_SIZE = 64;
#define QUERY_NUM 25

#define IS_FORWARD(c) (c % 2 == 0)

pthread_mutex_t print_mtx;

static inline void cpu_pause() { __asm__ volatile("pause" ::: "memory"); }
static inline unsigned long read_tsc(void) {
    unsigned long var;
    unsigned int hi, lo;

    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    var = ((unsigned long long int)hi << 32) | lo;

    return var;
}

static unsigned long write_latency_in_ns = 0;
unsigned long long search_time_in_insert = 0;
unsigned int gettime_cnt = 0;
unsigned long long clflush_time_in_insert = 0;
unsigned long long update_time_in_insert = 0;
int node_cnt = 0;

using namespace std;

static inline void mfence() { asm volatile("mfence" ::: "memory"); }

static inline void clflush(char *data, int len) {
    volatile char *ptr = (char *)((unsigned long)data & ~(CACHE_LINE_SIZE - 1));
    mfence();
    for (; ptr < data + len; ptr += CACHE_LINE_SIZE) {
        unsigned long etsc = read_tsc() + (unsigned long)(write_latency_in_ns *
                                                          CPU_FREQ_MHZ / 1000);
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m"(*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m"(*(volatile char *)(ptr)));
#elif CLWB
        asm volatile(".byte 0x66; xsaveopt %0" : "+m"(*(volatile char *)(ptr)));
#endif
        while (read_tsc() < etsc)
            cpu_pause();
    }
    mfence();
}

// using entry_key_t = uint64_t;

/**
 *  epoch-based GC
 */
std::mutex ti_mtx;
__thread threadinfo *ti = nullptr;
threadinfo *ti_list = nullptr;
Epoch_Mgr *e_mgr = nullptr;
int tid = 0;

PMEMobjpool *pmem_pool;

union Key {
    uint64_t ikey;
    key_item *skey;
};

class page;

class btree {
  public:
    int height;
    char *root;

  public:
    btree();
    ~btree() {}
    void setNewRoot(char *);
    char *getRoot() { return root; }
    void getNumberOfNodes();
    void btree_insert(uint64_t, char *, bool);
    void btree_insert(char *, char *, bool);
    void btree_insert_internal(char *, uint64_t, char *, uint32_t);
    void btree_insert_internal(char *, key_item *, char *, uint32_t);
    void btree_delete(uint64_t);
    void btree_delete(char *);
    // void btree_delete_internal
    //    (entry_key_t, char *, uint32_t, entry_key_t *, bool *, page **);
    char *btree_search(uint64_t);
    char *btree_search(char *);
    void btree_update(uint64_t, char *);
    void btree_update(char *, char *);
    void btree_search_range(uint64_t, uint64_t, unsigned long *, int, int &);
    void btree_search_range(char *, char *, unsigned long *, int, int &,
                            char *);
    key_item *make_key_item(char *, size_t, bool);

    friend class page;
};

class header {
  private:
    page *leftmost_ptr;      // 8 bytes
    page *sibling_ptr;       // 8 bytes
    uint32_t level;          // 4 bytes
    uint32_t switch_counter; // 4 bytes
                             //    std::mutex *mtx;         // 8 bytes
    boost::shared_mutex *mtx;
    union Key highest;  // 8 bytes
    uint8_t is_deleted; // 1 bytes
    int16_t last_index; // 2 bytes
    uint8_t dummy[5];   // 5 bytes

    friend class page;
    friend class btree;

  public:
    header() {
        //        mtx = new std::mutex();
        mtx = new boost::shared_mutex();

        leftmost_ptr = nullptr;
        sibling_ptr = nullptr;
        switch_counter = 0;
        last_index = -1;
        is_deleted = false;
#ifdef LOCK_INIT
        lock_initializer.push_back(mtx);
#endif
    }

    ~header() { delete mtx; }
};

class entry {
  private:
    union Key key; // 8 bytes
    char *ptr;     // 8 bytes

  public:
    entry() {
        key.ikey = UINT64_MAX;
        ptr = nullptr;
    }

    friend class page;
    friend class btree;
};

const int cardinality = (PAGESIZE - sizeof(header)) / sizeof(entry);
const int count_in_line = CACHE_LINE_SIZE / sizeof(entry);

class page {
  private:
    header hdr;                 // header in persistent memory, 16 bytes
    entry records[cardinality]; // slots in persistent memory, 16 bytes * n

  public:
    friend class btree;

    page(uint32_t level = 0) {
        hdr.level = level;
        records[0].ptr = nullptr;
    }

    void init(uint32_t level = 0) {
        hdr.level = level;
        records[0].ptr = nullptr;
    }

    // this is called when tree grows
    page(page *left, uint64_t key, page *right, uint32_t level = 0) {
        hdr.leftmost_ptr = left;
        hdr.level = level;
        records[0].key.ikey = key;
        records[0].ptr = (char *)right;
        records[1].ptr = nullptr;

        hdr.last_index = 0;

        flush_data((void *)this, sizeof(page));
    }

    // this is called when tree grows
    page(page *left, key_item *key, page *right, uint32_t level = 0) {
        hdr.leftmost_ptr = left;
        hdr.level = level;
        records[0].key.skey = key;
        records[0].ptr = (char *)right;
        records[1].ptr = nullptr;

        hdr.last_index = 0;

        flush_data((void *)this, sizeof(page));
    }

#ifndef USE_PMDK
    void *operator new(size_t size) {
        void *ret;
        posix_memalign(&ret, 64, size);
        return ret;
    }
#endif

    inline int count() {
        uint32_t previous_switch_counter;
        int count = 0;
        do {
            previous_switch_counter = hdr.switch_counter;
            count = hdr.last_index + 1;

            while (count >= 0 && records[count].ptr != nullptr) {
                if (IS_FORWARD(previous_switch_counter))
                    ++count;
                else
                    --count;
            }

            if (count < 0) {
                count = 0;
                while (records[count].ptr != nullptr) {
                    ++count;
                }
            }

        } while (IS_FORWARD(previous_switch_counter) !=
                 IS_FORWARD(hdr.switch_counter));

        return count;
    }

    inline bool remove_key(uint64_t key) {
        // Set the switch_counter
        if (IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;
        else
            hdr.switch_counter += 2;

        bool shift = false;
        int i;
        for (i = 0; records[i].ptr != nullptr; ++i) {
            if (!shift && records[i].key.ikey == key) {
                records[i].ptr =
                    (i == 0) ? (char *)hdr.leftmost_ptr : records[i - 1].ptr;
                shift = true;
            }

            if (shift) {
                records[i].key.ikey = records[i + 1].key.ikey;
                records[i].ptr = records[i + 1].ptr;

                // flush
                uint64_t records_ptr = (uint64_t)(&records[i]);
                int remainder = records_ptr % CACHE_LINE_SIZE;
                bool do_flush =
                    (remainder == 0) ||
                    ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) ==
                      1) &&
                     ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
                if (do_flush) {
                    flush_data((void *)records_ptr, CACHE_LINE_SIZE);
                }
            }
        }

        if (shift) {
            --hdr.last_index;
        }
        return shift;
    }

    inline bool remove_key(key_item *key) {
        // Set the switch_counter
        if (IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;
        else
            hdr.switch_counter += 2;

        bool shift = false;
        int i;
        for (i = 0; records[i].ptr != nullptr; ++i) {
            if (!shift && memcmp(key->key, records[i].key.skey->key,
                                 std::min(key->key_len,
                                          records[i].key.skey->key_len)) == 0) {
                records[i].ptr =
                    (i == 0) ? (char *)hdr.leftmost_ptr : records[i - 1].ptr;
#ifdef USE_PMDK
#ifdef FF_GC // ff_gc
                ti->AddGarbageNode((void *)records[i].key.skey);
//                printf("add garbage node, key is %s, len is %d\n",
//                records[i].key.skey->key, records[i].key.skey->key_len);
#endif
#endif
                shift = true;
            }

            if (shift) {
                records[i].key.ikey = records[i + 1].key.ikey;
                records[i].ptr = records[i + 1].ptr;

                // flush
                uint64_t records_ptr = (uint64_t)(&records[i]);
                int remainder = records_ptr % CACHE_LINE_SIZE;
                bool do_flush =
                    (remainder == 0) ||
                    ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) ==
                      1) &&
                     ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
                if (do_flush) {
                    flush_data((void *)records_ptr, CACHE_LINE_SIZE);
                }
            }
        }

        if (shift) {
            --hdr.last_index;
        }
        return shift;
    }

    bool remove(btree *bt, uint64_t key, bool only_rebalance = false,
                bool with_lock = true) {
        hdr.mtx->lock();

        bool ret = remove_key(key);

        hdr.mtx->unlock();

        return ret;
    }

    bool remove(btree *bt, key_item *key, bool only_rebalance = false,
                bool with_lock = true) {
        hdr.mtx->lock();

        bool ret = remove_key(key);

        hdr.mtx->unlock();

        return ret;
    }

    // integer
    inline void insert_key(uint64_t key, char *ptr, int *num_entries,
                           bool flush = true, bool update_last_index = true) {
        // update switch_counter
        if (!IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;
        else
            hdr.switch_counter += 2;

        // FAST
        if (*num_entries == 0) { // this page is empty
            entry *new_entry = (entry *)&records[0];
            entry *array_end = (entry *)&records[1];
            new_entry->key.ikey = (uint64_t)key;
            new_entry->ptr = (char *)ptr;

            array_end->ptr = (char *)nullptr;

            flush_data((void *)this, CACHE_LINE_SIZE);
        } else {
            int i = *num_entries - 1, inserted = 0, to_flush_cnt = 0;
            records[*num_entries + 1].ptr = records[*num_entries].ptr;
            if ((uint64_t) &
                (records[*num_entries + 1].ptr) % CACHE_LINE_SIZE == 0)
                flush_data((void *)&(records[*num_entries + 1].ptr),
                           sizeof(char *));

            // FAST
            for (i = *num_entries - 1; i >= 0; i--) {
                if (key < records[i].key.ikey) {
                    records[i + 1].ptr = records[i].ptr;
                    records[i + 1].key.ikey = records[i].key.ikey;

                    uint64_t records_ptr = (uint64_t)(&records[i + 1]);

                    int remainder = records_ptr % CACHE_LINE_SIZE;
                    bool do_flush =
                        (remainder == 0) ||
                        ((((int)(remainder + sizeof(entry)) /
                           CACHE_LINE_SIZE) == 1) &&
                         ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
                    if (do_flush) {
                        flush_data((void *)records_ptr, CACHE_LINE_SIZE);
                        to_flush_cnt = 0;
                    } else
                        ++to_flush_cnt;

                } else {
                    records[i + 1].ptr = records[i].ptr;
                    records[i + 1].key.ikey = key;

                    records[i + 1].ptr = ptr;
                    flush_data((void *)&records[i + 1], sizeof(entry));

                    inserted = 1;
                    break;
                }
            }
            if (inserted == 0) {
                records[0].ptr = (char *)hdr.leftmost_ptr;
                records[0].key.ikey = key;
                records[0].ptr = ptr;
                flush_data((void *)&records[0], sizeof(entry));
            }
        }

        if (update_last_index) {
            hdr.last_index = *num_entries;
        }
        ++(*num_entries);
    }

    key_item *make_new_key_item(key_item *key) {
        void *aligned_alloc;
        posix_memalign(&aligned_alloc, 64, sizeof(key_item) + key->key_len);
        key_item *new_key = (key_item *)aligned_alloc;
        new_key->key_len = key->key_len;
        //    new_key->key = key;
        memcpy(new_key->key, key,
               key->key_len); // copy including nullptr character

        flush_data((void *)new_key, sizeof(key_item) + key->key_len);
        return new_key;
    }

    // variable string
    inline void insert_key(key_item *key, char *ptr, int *num_entries,
                           bool flush = true, bool update_last_index = true) {
        // update switch_counter
        if (!IS_FORWARD(hdr.switch_counter))
            ++hdr.switch_counter;
        else
            hdr.switch_counter += 2;

        // FAST
        if (*num_entries == 0) { // this page is empty
            entry *new_entry = (entry *)&records[0];
            entry *array_end = (entry *)&records[1];
#ifdef USE_PMDK
            TX_BEGIN(pmem_pool) {
                pmemobj_tx_add_range_direct(new_entry, sizeof(entry));
                PMEMoid p1 = pmemobj_tx_zalloc(sizeof(key_item) + key->key_len,
                                               TOID_TYPE_NUM(struct key_item));
                key_item *k = (key_item *)pmemobj_direct(p1);

                k->key_len = key->key_len;
                memcpy(k->key, key->key, key->key_len);
                flush_data(k, sizeof(key_item) + key->key_len);
                new_entry->key.skey = k;
            }
            TX_END

#else
            //            new_entry->key.skey = make_new_key_item(key);
            new_entry->key.skey = key;
#endif
            new_entry->ptr = (char *)ptr;
            array_end->ptr = (char *)nullptr;

            flush_data((void *)this, CACHE_LINE_SIZE);

        } else {
            int i = *num_entries - 1, inserted = 0, to_flush_cnt = 0;
            records[*num_entries + 1].ptr = records[*num_entries].ptr;

            if ((uint64_t) &
                (records[*num_entries + 1].ptr) % CACHE_LINE_SIZE == 0)
                flush_data((void *)&(records[*num_entries + 1].ptr),
                           sizeof(char *));

            // FAST
            for (i = *num_entries - 1; i >= 0; i--) {
                if (memcmp(key->key, records[i].key.skey->key,
                           std::min(key->key_len,
                                    records[i].key.skey->key_len)) < 0) {
                    records[i + 1].ptr = records[i].ptr;
                    records[i + 1].key.skey = records[i].key.skey;

                    uint64_t records_ptr = (uint64_t)(&records[i + 1]);

                    int remainder = records_ptr % CACHE_LINE_SIZE;
                    bool do_flush =
                        (remainder == 0) ||
                        ((((int)(remainder + sizeof(entry)) /
                           CACHE_LINE_SIZE) == 1) &&
                         ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
                    if (do_flush) {
                        flush_data((void *)records_ptr, CACHE_LINE_SIZE);
                        to_flush_cnt = 0;
                    } else
                        ++to_flush_cnt;

                } else {
                    records[i + 1].ptr = records[i].ptr;
#ifdef USE_PMDK
                    TX_BEGIN(pmem_pool) {
                        // undo log
                        pmemobj_tx_add_range_direct(&(records[i + 1]),
                                                    sizeof(entry));

                        // allocate
                        PMEMoid p1 =
                            pmemobj_tx_zalloc(sizeof(key_item) + key->key_len,
                                              TOID_TYPE_NUM(struct key_item));

                        // copy key and persist
                        key_item *k = (key_item *)pmemobj_direct(p1);
                        if ((uint64_t)k == static_cast<uint64_t>(-1)) {
                            std::cout << "alloc failed\n";
                        }
                        k->key_len = key->key_len;
                        memcpy(k->key, key->key, key->key_len);
                        flush_data((void *)k, sizeof(key_item) + k->key_len);
                        records[i + 1].key.skey = k;
                    }
                    TX_END

#else

                    //                    records[i + 1].key.skey =
                    //                    make_new_key_item(key);
                    records[i + 1].key.skey = key;
#endif
                    records[i + 1].ptr = ptr;
                    flush_data((void *)&records[i + 1], sizeof(entry));
                    inserted = 1;
                    break;
                }
            }
            if (inserted == 0) {
                records[0].ptr = (char *)hdr.leftmost_ptr;

#ifdef USE_PMDK
                TX_BEGIN(pmem_pool) {
                    // undo log
                    pmemobj_tx_add_range_direct(&records[0], sizeof(entry));
                    PMEMoid p1 =
                        pmemobj_tx_zalloc(sizeof(key_item) + key->key_len,
                                          TOID_TYPE_NUM(struct key_item));

                    // copy key
                    key_item *k = (key_item *)pmemobj_direct(p1);
                    if ((uint64_t)k == static_cast<uint64_t>(-1)) {
                        std::cout << "alloc failed\n";
                    }
                    k->key_len = key->key_len;
                    memcpy(k->key, key->key, key->key_len);
                    flush_data((void *)k, sizeof(key_item) + k->key_len);
                    records[0].key.skey = k;
                }
                TX_END
#else
                //                records[0].key.skey = make_new_key_item(key);
                records[0].key.skey = key;
#endif
                records[0].ptr = ptr;
                flush_data((void *)&records[0], sizeof(entry));
            }
        }

        if (update_last_index) {
            hdr.last_index = *num_entries;
        }
        ++(*num_entries);
    }

    // Insert a new integer key - FAST and FAIR
    page *store(btree *bt, char *left, uint64_t key, char *right, bool flush,
                bool with_lock, page *invalid_sibling = nullptr) {
        if (with_lock) {
            hdr.mtx->lock(); // Lock the write lock
        }
        if (hdr.is_deleted) {
            if (with_lock) {
                hdr.mtx->unlock();
            }

            return nullptr;
        }

        // If this node has a sibling node,
        if (hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
            // Compare this key with the first key of the sibling
            if (hdr.level > 0) {
                if (key >= hdr.sibling_ptr->hdr.highest.ikey) {
                    if (with_lock) {
                        hdr.mtx->unlock(); // Unlock the write lock
                    }
                    return hdr.sibling_ptr->store(bt, left, key, right, true,
                                                  with_lock, invalid_sibling);
                }
            } else {
                if (key >= hdr.sibling_ptr->hdr.highest.ikey) {
                    if (with_lock) {
                        hdr.sibling_ptr->hdr.mtx->lock();
                        bt->btree_insert_internal(
                            (char *)this, hdr.sibling_ptr->hdr.highest.ikey,
                            (char *)hdr.sibling_ptr, hdr.level + 1);
                        hdr.sibling_ptr->hdr.mtx->unlock();
                        hdr.mtx->unlock(); // Unlock the write lock
                    }
                    return hdr.sibling_ptr->store(bt, left, key, right, true,
                                                  with_lock, invalid_sibling);
                }
            }
        }

        if (left != nullptr) {
            char *_ret = this->linear_search(key);
            if (_ret == left || _ret == right) {
                hdr.mtx->unlock();
                return this;
            } else {
                printf("Need to recover\n");
                hdr.mtx->unlock();
                return this;
            }
        }

        register int num_entries = count();

        // FAST
        if (num_entries < cardinality - 1) {
            insert_key(key, right, &num_entries, flush);

            if (with_lock) {
                hdr.mtx->unlock(); // Unlock the write lock
            }

            return this;
        } else { // FAIR
                 // overflow
                 // create a new node
#ifdef USE_PMDK
            //            printf("num %d %d, start pmdk allocate\n",
            //            num_entries, cardinality);
            register int m;
            uint64_t split_key;
            page *sibling;
            int sibling_cnt;
            TX_BEGIN(pmem_pool) {
                pmemobj_tx_add_range_direct(&hdr.sibling_ptr,
                                            sizeof(uint64_t)); // undo log
                PMEMoid ptr = pmemobj_tx_zalloc(sizeof(char) * PAGESIZE,
                                                TOID_TYPE_NUM(char));
                sibling = new (pmemobj_direct(ptr)) page(hdr.level);
                // copy half to sibling and init sibling
                m = (int)ceil(num_entries / 2);
                split_key = records[m].key.ikey;

                // migrate half of keys into the sibling
                sibling_cnt = 0;
                if (hdr.leftmost_ptr == nullptr) { // leaf node
                    for (int i = m; i < num_entries; ++i) {
                        sibling->insert_key(records[i].key.ikey, records[i].ptr,
                                            &sibling_cnt, false);
                    }
                } else { // internal node
                    for (int i = m + 1; i < num_entries; ++i) {
                        sibling->insert_key(records[i].key.ikey, records[i].ptr,
                                            &sibling_cnt, false);
                    }
                    sibling->hdr.leftmost_ptr = (page *)records[m].ptr;
                }

                sibling->hdr.highest.ikey = records[m].key.ikey;
                sibling->hdr.sibling_ptr = hdr.sibling_ptr;
                flush_data((void *)sibling, sizeof(page));

                if (hdr.leftmost_ptr == nullptr)
                    sibling->hdr.mtx->lock();

                if (IS_FORWARD(hdr.switch_counter))
                    hdr.switch_counter++;
                else
                    hdr.switch_counter += 2;
                mfence();
                hdr.sibling_ptr = sibling;
            }
            TX_END
#else
            page *sibling = new page(hdr.level); // 重载了new运算符
            register int m = (int)ceil(num_entries / 2);
            uint64_t split_key = records[m].key.ikey;

            // migrate half of keys into the sibling
            int sibling_cnt = 0;
            if (hdr.leftmost_ptr == nullptr) { // leaf node
                for (int i = m; i < num_entries; ++i) {
                    sibling->insert_key(records[i].key.ikey, records[i].ptr,
                                        &sibling_cnt, false);
                }
            } else { // internal node
                for (int i = m + 1; i < num_entries; ++i) {
                    sibling->insert_key(records[i].key.ikey, records[i].ptr,
                                        &sibling_cnt, false);
                }
                sibling->hdr.leftmost_ptr = (page *)records[m].ptr;
            }

            sibling->hdr.highest.ikey = records[m].key.ikey;
            sibling->hdr.sibling_ptr = hdr.sibling_ptr;
            flush_data((void *)sibling, sizeof(page));

            if (hdr.leftmost_ptr == nullptr)
                sibling->hdr.mtx->lock();

            if (IS_FORWARD(hdr.switch_counter))
                hdr.switch_counter++;
            else
                hdr.switch_counter += 2;
            mfence();
            hdr.sibling_ptr = sibling;
            flush_data((void *)&hdr, sizeof(hdr));
#endif
            // set to nullptr
            records[m].ptr = nullptr;
            flush_data((void *)&records[m], sizeof(entry));

            hdr.last_index = m - 1;
            flush_data((void *)&(hdr.last_index), sizeof(int16_t));

            num_entries = hdr.last_index + 1;

            page *ret;

            // insert the key for internal node
            if (hdr.leftmost_ptr != nullptr) {
                if (key < split_key) {
                    insert_key(key, right, &num_entries);
                    ret = this;
                } else {
                    sibling->insert_key(key, right, &sibling_cnt);
                    ret = sibling;
                }
            }

            // Set a new root or insert the split key to the parent
            if (bt->root ==
                (char *)this) { // only one node can update the root ptr
#ifdef USE_PMDK
                //                printf("split root\n");
                page *new_root;
                TX_BEGIN(pmem_pool) {
                    pmemobj_tx_add_range_direct(
                        &(bt->root), sizeof(uint64_t)); // root undo log
                    pmemobj_tx_add_range_direct(&(bt->height),
                                                sizeof(uint64_t));
                    PMEMoid ptr = pmemobj_tx_zalloc(sizeof(char) * PAGESIZE,
                                                    TOID_TYPE_NUM(char));
                    new_root = new (pmemobj_direct(ptr))
                        page((page *)this, split_key, sibling, hdr.level + 1);
                    bt->root = (char *)new_root;
                    bt->height++;
                }
                TX_END
#else

                page *new_root =
                    new page((page *)this, split_key, sibling, hdr.level + 1);
                bt->setNewRoot((char *)new_root);
#endif
                if (with_lock && hdr.leftmost_ptr != nullptr) {
                    hdr.mtx->unlock(); // Unlock the write lock
                }
            } else {
                if (with_lock && hdr.leftmost_ptr != nullptr) {
                    hdr.mtx->unlock(); // Unlock the write lock
                }

                bt->btree_insert_internal(nullptr, split_key, (char *)sibling,
                                          hdr.level + 1);
            }

            // insert the key for leaf node
            if (hdr.leftmost_ptr == nullptr) {
                if (key < split_key) {
                    insert_key(key, right, &num_entries);
                    ret = this;
                } else {
                    sibling->insert_key(key, right, &sibling_cnt);
                    ret = sibling;
                }

                if (with_lock) {
                    hdr.mtx->unlock();
                    sibling->hdr.mtx->unlock();
                }
            }

            return ret;
        }
    }

    // Insert a new string key - FAST and FAIR
    page *store(btree *bt, char *left, key_item *key, char *right, bool flush,
                bool with_lock, page *invalid_sibling = nullptr) {
        if (with_lock) {
            hdr.mtx->lock(); // Lock the write lock
        }
        if (hdr.is_deleted) {
            if (with_lock) {
                hdr.mtx->unlock();
            }

            return nullptr;
        }

        // If this node has a sibling node,
        if (hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
            // Compare this key with the first key of the sibling
            if (hdr.level > 0) {
                if (memcmp(key->key, hdr.sibling_ptr->hdr.highest.skey->key,
                           std::min(key->key_len, hdr.sibling_ptr->hdr.highest
                                                      .skey->key_len)) >= 0) {
                    if (with_lock) {
                        hdr.mtx->unlock(); // Unlock the write lock
                    }
                    return hdr.sibling_ptr->store(bt, left, key, right, true,
                                                  with_lock, invalid_sibling);
                }
            } else {
                if (memcmp(key->key, hdr.sibling_ptr->hdr.highest.skey->key,
                           std::min(key->key_len, hdr.sibling_ptr->hdr.highest
                                                      .skey->key_len)) >= 0) {
                    if (with_lock) {
                        hdr.sibling_ptr->hdr.mtx->lock();
                        bt->btree_insert_internal(
                            (char *)this, hdr.sibling_ptr->hdr.highest.skey,
                            (char *)hdr.sibling_ptr, hdr.level + 1);
                        hdr.sibling_ptr->hdr.mtx->unlock();
                        hdr.mtx->unlock(); // Unlock the write lock
                    }
                    return hdr.sibling_ptr->store(bt, left, key, right, true,
                                                  with_lock, invalid_sibling);
                }
            }
        }

        if (left != nullptr) {
            char *_ret = this->linear_search(key);
            if (_ret == left || _ret == right) {
                hdr.mtx->unlock();
                return this;
            } else {
                printf("Need to recover\n");
                hdr.mtx->unlock();
                return this;
            }
        }

        register int num_entries = count();

        // FAST
        if (num_entries < cardinality - 1) {
            insert_key(key, right, &num_entries, flush);

            if (with_lock) {
                hdr.mtx->unlock(); // Unlock the write lock
            }

            return this;
        } else { // FAIR
                 // overflow
                 // create a new node
#ifdef USE_PMDK
            page *sibling;
            register int m;
            key_item *split_key;
            int sibling_cnt;
            TX_BEGIN(pmem_pool) {
                pmemobj_tx_add_range_direct(&hdr.sibling_ptr, sizeof(uint64_t));
                PMEMoid ptr = pmemobj_tx_zalloc(sizeof(char) * PAGESIZE,
                                                TOID_TYPE_NUM(char));
                sibling = new (pmemobj_direct(ptr)) page(hdr.level);

                m = (int)ceil(num_entries / 2);
                split_key = records[m].key.skey;
                sibling_cnt = 0;

                if (hdr.leftmost_ptr == nullptr) { // leaf node
                    for (int i = m; i < num_entries; ++i) {
                        sibling->insert_key(records[i].key.skey, records[i].ptr,
                                            &sibling_cnt, false);
                    }
                } else { // internal node
                    for (int i = m + 1; i < num_entries; ++i) {
                        sibling->insert_key(records[i].key.skey, records[i].ptr,
                                            &sibling_cnt, false);
                    }
                    sibling->hdr.leftmost_ptr = (page *)records[m].ptr;
                }

                sibling->hdr.highest.skey = records[m].key.skey;
                sibling->hdr.sibling_ptr = hdr.sibling_ptr;
                flush_data((void *)sibling, sizeof(page));

                if (hdr.leftmost_ptr == nullptr)
                    sibling->hdr.mtx->lock();

                // set to nullptr
                if (IS_FORWARD(hdr.switch_counter))
                    hdr.switch_counter++;
                else
                    hdr.switch_counter += 2;
                mfence();
                hdr.sibling_ptr = sibling;
            }
            TX_END
#else

            page *sibling = new page(hdr.level);
            register int m = (int)ceil(num_entries / 2);
            key_item *split_key = records[m].key.skey;

            // migrate half of keys into the sibling
            int sibling_cnt = 0;
            if (hdr.leftmost_ptr == nullptr) { // leaf node
                for (int i = m; i < num_entries; ++i) {
                    sibling->insert_key(records[i].key.skey, records[i].ptr,
                                        &sibling_cnt, false);
                }
            } else { // internal node
                for (int i = m + 1; i < num_entries; ++i) {
                    sibling->insert_key(records[i].key.skey, records[i].ptr,
                                        &sibling_cnt, false);
                }
                sibling->hdr.leftmost_ptr = (page *)records[m].ptr;
            }

            sibling->hdr.highest.skey = records[m].key.skey;
            sibling->hdr.sibling_ptr = hdr.sibling_ptr;
            flush_data((void *)sibling, sizeof(page));

            if (hdr.leftmost_ptr == nullptr)
                sibling->hdr.mtx->lock();

            // set to nullptr
            if (IS_FORWARD(hdr.switch_counter))
                hdr.switch_counter++;
            else
                hdr.switch_counter += 2;
            mfence();
            hdr.sibling_ptr = sibling;
            flush_data((void *)&hdr, sizeof(hdr));
#endif

            records[m].ptr = nullptr;
            flush_data((void *)&records[m], sizeof(entry));

            hdr.last_index = m - 1;
            flush_data((void *)&(hdr.last_index), sizeof(int16_t));

            num_entries = hdr.last_index + 1;

            page *ret;

            if (hdr.leftmost_ptr != nullptr) {
                // insert the key for internal node
                if (memcmp(key->key, split_key->key,
                           std::min(key->key_len, split_key->key_len)) < 0) {
                    insert_key(key, right, &num_entries);
                    ret = this;
                } else {
                    sibling->insert_key(key, right, &sibling_cnt);
                    ret = sibling;
                }
            }

            // Set a new root or insert the split key to the parent
            if (bt->root ==
                (char *)this) { // only one node can update the root ptr

#ifdef USE_PMDK
                page *new_root;
                TX_BEGIN(pmem_pool) {
                    pmemobj_tx_add_range_direct(
                        &(bt->root), sizeof(uint64_t)); // root undo log
                    pmemobj_tx_add_range_direct(&(bt->height),
                                                sizeof(uint64_t));
                    PMEMoid ptr = pmemobj_tx_zalloc(sizeof(char) * PAGESIZE,
                                                    TOID_TYPE_NUM(char));
                    new_root = new (pmemobj_direct(ptr))
                        page((page *)this, split_key, sibling, hdr.level + 1);
                    bt->root = (char *)new_root;
                    bt->height++;
                }
                TX_END
#else
                page *new_root =
                    new page((page *)this, split_key, sibling, hdr.level + 1);
                bt->setNewRoot((char *)new_root);
#endif

                if (with_lock && hdr.leftmost_ptr != nullptr) {
                    hdr.mtx->unlock(); // Unlock the write lock
                }
            } else {
                if (with_lock && hdr.leftmost_ptr != nullptr) {
                    hdr.mtx->unlock(); // Unlock the write lock
                }

                bt->btree_insert_internal(nullptr, split_key, (char *)sibling,
                                          hdr.level + 1);
            }

            if (hdr.leftmost_ptr == nullptr) {
                // insert the key for leafnode node
                if (memcmp(key->key, split_key->key,
                           std::min(key->key_len, split_key->key_len)) < 0) {
                    insert_key(key, right, &num_entries);
                    ret = this;
                } else {
                    sibling->insert_key(key, right, &sibling_cnt);
                    ret = sibling;
                }

                if (with_lock) {
                    hdr.mtx->unlock();
                    sibling->hdr.mtx->unlock();
                }
            }

            return ret;
        }
    }

    // Search integer keys with linear search
    void linear_search_range(uint64_t min, uint64_t max, unsigned long *buf,
                             int num, int &off) {
        int i;
        uint32_t previous_switch_counter;
        page *current = this;
        void *snapshot_n;
        off = 0;

        while (current) {
            int old_off = off;
            snapshot_n = current->hdr.sibling_ptr;
            mfence();
            do {
                previous_switch_counter = current->hdr.switch_counter;
                off = old_off;

                uint64_t tmp_key;
                char *tmp_ptr;

                if (IS_FORWARD(previous_switch_counter)) {
                    if ((tmp_key = current->records[0].key.ikey) > min) {
                        if (tmp_key < max && off < num) {
                            if ((tmp_ptr = current->records[0].ptr) !=
                                nullptr) {
                                if (tmp_key == current->records[0].key.ikey) {
                                    if (tmp_ptr) {
                                        buf[off++] = (unsigned long)tmp_ptr;
                                    }
                                }
                            }
                        } else
                            return;
                    }

                    for (i = 1; current->records[i].ptr != nullptr; ++i) {
                        if ((tmp_key = current->records[i].key.ikey) > min) {
                            if (tmp_key < max && off < num) {
                                if ((tmp_ptr = current->records[i].ptr) !=
                                    current->records[i - 1].ptr) {
                                    if (tmp_key ==
                                        current->records[i].key.ikey) {
                                        if (tmp_ptr) {
                                            buf[off++] = (unsigned long)tmp_ptr;
                                        }
                                    }
                                }
                            } else
                                return;
                        }
                    }
                } else {
                    for (i = count() - 1; i > 0; --i) {
                        if ((tmp_key = current->records[i].key.ikey) > min) {
                            if (tmp_key < max && off < num) {
                                if ((tmp_ptr = current->records[i].ptr) !=
                                    current->records[i - 1].ptr) {
                                    if (tmp_key ==
                                        current->records[i].key.ikey) {
                                        if (tmp_ptr) {
                                            buf[off++] = (unsigned long)tmp_ptr;
                                        }
                                    }
                                }
                            } else
                                return;
                        }
                    }

                    if ((tmp_key = current->records[0].key.ikey) > min) {
                        if (tmp_key < max && off < num) {
                            if ((tmp_ptr = current->records[0].ptr) !=
                                nullptr) {
                                if (tmp_key == current->records[0].key.ikey) {
                                    if (tmp_ptr) {
                                        buf[off++] = (unsigned long)tmp_ptr;
                                    }
                                }
                            }
                        } else
                            return;
                    }
                }
            } while (previous_switch_counter != current->hdr.switch_counter);

            if (snapshot_n == current->hdr.sibling_ptr)
                current = current->hdr.sibling_ptr;
            else
                off = old_off;
        }
    }

    // Search string keys with linear search
    void linear_search_range(key_item *min, key_item *max, unsigned long *buf,
                             int num, int &off, char *scan_value) {
        int i;
        uint32_t previous_switch_counter;
        page *current = this;
        void *snapshot_n;
        off = 0;

        bool compare_flag = false;
        int copy_len = 0;

        while (current) {
            boost::shared_lock<boost::shared_mutex> sharedlock(
                *(current->hdr.mtx));
            int old_off = off;
            snapshot_n = current->hdr.sibling_ptr;
            mfence();
            do {
                previous_switch_counter = current->hdr.switch_counter;
                off = old_off;

                key_item *tmp_key;
                char *tmp_ptr;

                if (IS_FORWARD(previous_switch_counter)) {
                    tmp_key = current->records[0].key.skey;
                    tmp_ptr = current->records[0].ptr;

                    if (compare_flag) {
                        if (off == num)
                            return;
                        buf[off++] = (unsigned long)tmp_ptr;
                        if ((uint64_t)tmp_ptr == static_cast<uint64_t>(-1) ||
                            tmp_ptr == nullptr) {
                            std::cout << "boom!!!\n";
                        } else
                            memcpy(scan_value, tmp_ptr, copy_len);
                    } else {
                        if (memcmp(tmp_key->key, min->key,
                                   std::min(tmp_key->key_len, min->key_len)) >=
                            0) {
                            if (off == num)
                                return;
                            buf[off++] = (unsigned long)tmp_ptr;
                            if ((uint64_t)tmp_ptr ==
                                    static_cast<uint64_t>(-1) ||
                                tmp_ptr == nullptr) {
                                std::cout << "boom!!!\n";
                            } else
                                memcpy(scan_value, tmp_ptr, copy_len);
                            compare_flag = true;
                        }
                    }

                    for (i = 1; current->records[i].ptr != nullptr; ++i) {
                        tmp_key = current->records[i].key.skey;
                        tmp_ptr = current->records[i].ptr;
                        if (compare_flag) {
                            if (off == num)
                                return;
                            buf[off++] = (unsigned long)tmp_ptr;
                            if ((uint64_t)tmp_ptr ==
                                    static_cast<uint64_t>(-1) ||
                                tmp_ptr == nullptr) {
                                std::cout << "boom!!!\n";
                            } else
                                memcpy(scan_value, tmp_ptr, copy_len);
                        } else {
                            if (memcmp(tmp_key->key, min->key,
                                       std::min(tmp_key->key_len,
                                                min->key_len)) >= 0) {
                                if (off == num)
                                    return;
                                buf[off++] = (unsigned long)tmp_ptr;
                                if ((uint64_t)tmp_ptr ==
                                        static_cast<uint64_t>(-1) ||
                                    tmp_ptr == nullptr) {
                                    std::cout << "boom!!!\n";
                                } else
                                    memcpy(scan_value, tmp_ptr, copy_len);
                                compare_flag = true;
                            }
                        }
                    }
                } else {
                    for (i = count() - 1; i > 0; --i) {
                        tmp_key = current->records[i].key.skey;
                        tmp_ptr = current->records[i].ptr;
                        if (compare_flag) {
                            if (off == num)
                                return;
                            buf[off++] = (unsigned long)tmp_ptr;
                            if ((uint64_t)tmp_ptr ==
                                    static_cast<uint64_t>(-1) ||
                                tmp_ptr == nullptr) {
                                std::cout << "boom!!!\n";
                            } else
                                memcpy(scan_value, tmp_ptr, copy_len);
                        } else {
                            if (memcmp(tmp_key->key, min->key,
                                       std::min(tmp_key->key_len,
                                                min->key_len)) >= 0) {
                                if (off == num)
                                    return;
                                buf[off++] = (unsigned long)tmp_ptr;
                                if ((uint64_t)tmp_ptr ==
                                        static_cast<uint64_t>(-1) ||
                                    tmp_ptr == nullptr) {
                                    std::cout << "boom!!!\n";
                                } else
                                    memcpy(scan_value, tmp_ptr, copy_len);
                                compare_flag = true;
                            }
                        }
                    }

                    tmp_key = current->records[0].key.skey;
                    tmp_ptr = current->records[0].ptr;
                    if (compare_flag) {
                        if (off == num)
                            return;
                        buf[off++] = (unsigned long)tmp_ptr;
                        if ((uint64_t)tmp_ptr == static_cast<uint64_t>(-1) ||
                            tmp_ptr == nullptr) {
                            std::cout << "boom!!!\n";
                        } else
                            memcpy(scan_value, tmp_ptr, copy_len);
                    } else {
                        if (memcmp(tmp_key->key, min->key,
                                   std::min(tmp_key->key_len, min->key_len)) >=
                            0) {
                            if (off == num)
                                return;
                            buf[off++] = (unsigned long)tmp_ptr;
                            if ((uint64_t)tmp_ptr ==
                                    static_cast<uint64_t>(-1) ||
                                tmp_ptr == nullptr) {
                                std::cout << "boom!!!\n";
                            } else
                                memcpy(scan_value, tmp_ptr, copy_len);
                            compare_flag = true;
                        }
                    }
                }
            } while (previous_switch_counter != current->hdr.switch_counter);

            if (snapshot_n == current->hdr.sibling_ptr)
                current = current->hdr.sibling_ptr;
            else
                off = old_off;
        }
    }

    char *linear_search(btree *bt, uint64_t key) {
        int i = 1;
        uint32_t previous_switch_counter;
        char *ret = nullptr;
        char *t;
        uint64_t k;

        if (hdr.leftmost_ptr == nullptr) { // Search a leaf node
            do {
                previous_switch_counter = hdr.switch_counter;
                ret = nullptr;

                // search from left ro right
                if (IS_FORWARD(previous_switch_counter)) {
                    if ((k = records[0].key.ikey) == key) {
                        if ((t = records[0].ptr) != nullptr) {
                            if (k == records[0].key.ikey) {
                                ret = t;
                                continue;
                            }
                        }
                    }

                    for (i = 1; records[i].ptr != nullptr; ++i) {
                        if ((k = records[i].key.ikey) == key) {
                            if (records[i - 1].ptr != (t = records[i].ptr)) {
                                if (k == records[i].key.ikey) {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }
                } else { // search from right to left
                    for (i = count() - 1; i > 0; --i) {
                        if ((k = records[i].key.ikey) == key) {
                            if (records[i - 1].ptr != (t = records[i].ptr) &&
                                t) {
                                if (k == records[i].key.ikey) {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }

                    if (!ret) {
                        if ((k = records[0].key.ikey) == key) {
                            if (nullptr != (t = records[0].ptr) && t) {
                                if (k == records[0].key.ikey) {
                                    ret = t;
                                    continue;
                                }
                            }
                        }
                    }
                }
            } while (IS_FORWARD(hdr.switch_counter) !=
                     IS_FORWARD(previous_switch_counter));

            if (ret) {
                return ret;
            }

            if ((t = (char *)hdr.sibling_ptr) &&
                key >= ((page *)t)->hdr.highest.ikey) {
                hdr.mtx->lock();
                hdr.sibling_ptr->hdr.mtx->lock();
                bt->btree_insert_internal(
                    (char *)this, hdr.sibling_ptr->hdr.highest.ikey,
                    (char *)hdr.sibling_ptr, hdr.level + 1);
                hdr.sibling_ptr->hdr.mtx->unlock();
                hdr.mtx->unlock();
                return t;
            }

            return nullptr;
        } else { // internal node
            do {
                previous_switch_counter = hdr.switch_counter;
                ret = nullptr;

                if (IS_FORWARD(previous_switch_counter)) {
                    if (key < (k = records[0].key.ikey)) {
                        if ((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
                            ret = t;
                            continue;
                        }
                    }

                    for (i = 1; records[i].ptr != nullptr; ++i) {
                        if (key < (k = records[i].key.ikey)) {
                            if ((t = records[i - 1].ptr) != records[i].ptr) {
                                ret = t;
                                break;
                            }
                        }
                    }

                    if (!ret) {
                        ret = records[i - 1].ptr;
                        continue;
                    }
                } else { // search from right to left
                    for (i = count() - 1; i >= 0; --i) {
                        if (key >= (k = records[i].key.ikey)) {
                            if (i == 0) {
                                if ((char *)hdr.leftmost_ptr !=
                                    (t = records[i].ptr)) {
                                    ret = t;
                                    break;
                                }
                            } else {
                                if (records[i - 1].ptr !=
                                    (t = records[i].ptr)) {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }
                }
            } while (IS_FORWARD(hdr.switch_counter) !=
                     IS_FORWARD(previous_switch_counter));

            if ((t = (char *)hdr.sibling_ptr) != nullptr) {
                if (key >= ((page *)t)->hdr.highest.ikey) {
                    hdr.mtx->lock();
                    hdr.sibling_ptr->hdr.mtx->lock();
                    bt->btree_insert_internal(
                        (char *)this, hdr.sibling_ptr->hdr.highest.ikey,
                        (char *)hdr.sibling_ptr, hdr.level + 1);
                    hdr.sibling_ptr->hdr.mtx->unlock();
                    hdr.mtx->unlock();
                    return t;
                }
            }

            if (ret) {
                return ret;
            } else
                return (char *)hdr.leftmost_ptr;
        }

        return nullptr;
    }

    char *linear_search(uint64_t key, char *value = nullptr) {
        int i = 1;
        uint32_t previous_switch_counter;
        char *ret = nullptr;
        char *t;
        uint64_t k;

        if (hdr.leftmost_ptr == nullptr) { // Search a leaf node
            do {
                if (value == nullptr) {
                    // read
                    //                    boost::shared_lock<boost::shared_mutex>
                    //                    sharedlock(
                    //                        *hdr.mtx);
                } else {
                    // update
                    boost::unique_lock<boost::shared_mutex> uniquelock(
                        *hdr.mtx);
                }
                previous_switch_counter = hdr.switch_counter;
                ret = nullptr;

                // search from left ro right
                if (IS_FORWARD(previous_switch_counter)) {
                    if ((k = records[0].key.ikey) == key) {
                        if ((t = records[0].ptr) != nullptr) {
                            if (k == records[0].key.ikey) {
                                ret = t;
                                continue;
                            }
                        }
                    }

                    for (i = 1; records[i].ptr != nullptr; ++i) {
                        if ((k = records[i].key.ikey) == key) {
                            if (records[i - 1].ptr != (t = records[i].ptr)) {
                                if (k == records[i].key.ikey) {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }
                } else { // search from right to left
                    for (i = count() - 1; i > 0; --i) {
                        if ((k = records[i].key.ikey) == key) {
                            if (records[i - 1].ptr != (t = records[i].ptr) &&
                                t) {
                                if (k == records[i].key.ikey) {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }

                    if (!ret) {
                        if ((k = records[0].key.ikey) == key) {
                            if (nullptr != (t = records[0].ptr) && t) {
                                if (k == records[0].key.ikey) {
                                    ret = t;
                                    continue;
                                }
                            }
                        }
                    }
                }
            } while (IS_FORWARD(hdr.switch_counter) !=
                     IS_FORWARD(previous_switch_counter));

            if (ret) {
                return ret;
            }

            if ((t = (char *)hdr.sibling_ptr) &&
                key >= ((page *)t)->records[0].key.ikey) {
                return t;
            }

            return nullptr;
        } else { // internal node
            do {
                previous_switch_counter = hdr.switch_counter;
                ret = nullptr;

                if (IS_FORWARD(previous_switch_counter)) {
                    if (key < (k = records[0].key.ikey)) {
                        if ((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
                            ret = t;
                            continue;
                        }
                    }

                    for (i = 1; records[i].ptr != nullptr; ++i) {
                        if (key < (k = records[i].key.ikey)) {
                            if ((t = records[i - 1].ptr) != records[i].ptr) {
                                ret = t;
                                break;
                            }
                        }
                    }

                    if (!ret) {
                        ret = records[i - 1].ptr;
                        continue;
                    }
                } else { // search from right to left
                    for (i = count() - 1; i >= 0; --i) {
                        if (key >= (k = records[i].key.ikey)) {
                            if (i == 0) {
                                if ((char *)hdr.leftmost_ptr !=
                                    (t = records[i].ptr)) {
                                    ret = t;
                                    break;
                                }
                            } else {
                                if (records[i - 1].ptr !=
                                    (t = records[i].ptr)) {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }
                }
            } while (IS_FORWARD(hdr.switch_counter) !=
                     IS_FORWARD(previous_switch_counter));

            if ((t = (char *)hdr.sibling_ptr) != nullptr) {
                if (key >= ((page *)t)->records[0].key.ikey) {
                    return t;
                }
            }

            if (ret) {
                return ret;
            } else
                return (char *)hdr.leftmost_ptr;
        }

        return nullptr;
    }

    char *linear_search(key_item *key, char *value = nullptr) {
        int i = 1;
        uint32_t previous_switch_counter;
        char *ret = nullptr;
        char *t;
        key_item *k;

        if (hdr.leftmost_ptr == nullptr) { // Search a leaf node
            do {
                if (value == nullptr) {
                    // read
                    //                    boost::shared_lock<boost::shared_mutex>
                    //                    sharedlock(
                    //                        *hdr.mtx);
                } else {
                    // update
                    boost::unique_lock<boost::shared_mutex> uniquelock(
                        *hdr.mtx);
                }

                previous_switch_counter = hdr.switch_counter;
                ret = nullptr;

                // search from left ro right
                if (IS_FORWARD(previous_switch_counter)) {
                    k = records[0].key.skey;
                    if ((uint64_t)k == static_cast<uint64_t>(-1)) {
                        std::cout << "boom!!!\n";
                        return nullptr;
                    }
                    if (memcmp(k->key, key->key,
                               std::min(k->key_len, key->key_len)) == 0) {
                        if ((t = records[0].ptr) != nullptr) {
                            if (memcmp(
                                    k->key, records[0].key.skey->key,
                                    std::min(k->key_len,
                                             records[0].key.skey->key_len)) ==
                                0) {
                                ret = t;
                                if (value) {
                                    char *garbage = records[0].ptr;
                                    records[0].ptr = value;
                                    flush_data(&records[0].ptr,
                                               sizeof(uint64_t));
#ifdef USE_PMDK
#ifdef FF_GC
                                    ti->AddGarbageNode((void *)garbage);
#endif
#endif
                                }
                                continue;
                            }
                        }
                    }

                    for (i = 1; records[i].ptr != nullptr; ++i) {
                        k = records[i].key.skey;
                        if ((uint64_t)k == static_cast<uint64_t>(-1)) {
                            std::cout << "boom!!!\n";
                            return nullptr;
                        }

                        if (memcmp(k->key, key->key,
                                   std::min(k->key_len, key->key_len)) == 0) {
                            if (records[i - 1].ptr != (t = records[i].ptr)) {
                                if (memcmp(k->key, records[i].key.skey->key,
                                           std::min(
                                               k->key_len,
                                               records[i].key.skey->key_len)) ==
                                    0) {
                                    ret = t;
                                    if (value) {
                                        char *garbage = records[i].ptr;
                                        records[i].ptr = value;
                                        flush_data(&records[i].ptr,
                                                   sizeof(uint64_t));
#ifdef USE_PMDK
#ifdef FF_GC
                                        ti->AddGarbageNode((void *)garbage);
#endif
#endif
                                    }
                                    break;
                                }
                            }
                        }
                    }
                } else { // search from right to left
                    for (i = count() - 1; i > 0; --i) {
                        k = records[i].key.skey;

                        if ((uint64_t)k == static_cast<uint64_t>(-1)) {
                            std::cout << "boom!!!\n";
                            return nullptr;
                        }

                        if (memcmp(k->key, key->key,
                                   std::min(k->key_len, key->key_len)) == 0) {
                            if (records[i - 1].ptr != (t = records[i].ptr) &&
                                t) {
                                if (memcmp(k->key, records[i].key.skey->key,
                                           std::min(
                                               k->key_len,
                                               records[i].key.skey->key_len)) ==
                                    0) {
                                    ret = t;
                                    if (value) {
                                        char *garbage = records[i].ptr;
                                        records[i].ptr = value;
                                        flush_data(&records[i].ptr,
                                                   sizeof(uint64_t));
#ifdef USE_PMDK
#ifdef FF_GC
                                        ti->AddGarbageNode((void *)garbage);
#endif
#endif
                                    }
                                    break;
                                }
                            }
                        }
                    }

                    if (!ret) {
                        k = records[0].key.skey;
                        if ((uint64_t)k == static_cast<uint64_t>(-1)) {
                            std::cout << "boom!!!\n";
                            return nullptr;
                        }

                        if (memcmp(k->key, key->key,
                                   std::min(k->key_len, key->key_len)) == 0) {
                            if (nullptr != (t = records[0].ptr) && t) {
                                if (memcmp(k->key, records[0].key.skey->key,
                                           std::min(
                                               k->key_len,
                                               records[0].key.skey->key_len)) ==
                                    0) {
                                    ret = t;
                                    if (value) {
                                        char *garbage = records[0].ptr;
                                        records[0].ptr = value;
                                        flush_data(&records[0].ptr,
                                                   sizeof(uint64_t));
#ifdef USE_PMDK
#ifdef FF_GC
                                        ti->AddGarbageNode((void *)garbage);
#endif
#endif
                                    }
                                    continue;
                                }
                            }
                        }
                    }
                }
            } while (IS_FORWARD(hdr.switch_counter) !=
                     IS_FORWARD(previous_switch_counter));

            if (ret) {
                return ret;
            }

            if ((t = (char *)hdr.sibling_ptr) &&
                memcmp(key->key, ((page *)t)->hdr.highest.skey->key,
                       std::min(key->key_len,
                                ((page *)t)->hdr.highest.skey->key_len)) >= 0) {
                return t;
            }

            return nullptr;
        } else { // internal node
            do {
                previous_switch_counter = hdr.switch_counter;
                ret = nullptr;

                if (IS_FORWARD(previous_switch_counter)) {
                    k = records[0].key.skey;
                    if ((uint64_t)k == static_cast<uint64_t>(-1)) {
                        std::cout << "boom!!!\n";
                        return nullptr;
                    }
                    if (memcmp(key->key, k->key,
                               std::min(key->key_len, k->key_len)) < 0) {
                        if ((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
                            ret = t;
                            continue;
                        }
                    }

                    for (i = 1; records[i].ptr != nullptr; ++i) {
                        k = records[i].key.skey;

                        // a patch to bypass a bug of origin fast_fair
                        if ((uint64_t)k == static_cast<uint64_t>(-1)) {
                            std::cout << "boom!!!\n";
                            return nullptr;
                        }
                        if (memcmp(key->key, k->key,
                                   std::min(key->key_len, k->key_len)) < 0) {
                            if ((t = records[i - 1].ptr) != records[i].ptr) {
                                ret = t;
                                break;
                            }
                        }
                    }

                    if (!ret) {
                        ret = records[i - 1].ptr;
                        continue;
                    }
                } else { // search from right to left
                    for (i = count() - 1; i >= 0; --i) {
                        k = records[i].key.skey;
                        if ((uint64_t)k == static_cast<uint64_t>(-1)) {
                            std::cout << "boom!!!\n";
                            return nullptr;
                        }
                        if (memcmp(key->key, k->key,
                                   std::min(key->key_len, k->key_len)) >= 0) {
                            if (i == 0) {
                                if ((char *)hdr.leftmost_ptr !=
                                    (t = records[i].ptr)) {
                                    ret = t;
                                    break;
                                }
                            } else {
                                if (records[i - 1].ptr !=
                                    (t = records[i].ptr)) {
                                    ret = t;
                                    break;
                                }
                            }
                        }
                    }
                }
            } while (IS_FORWARD(hdr.switch_counter) !=
                     IS_FORWARD(previous_switch_counter));

            if ((t = (char *)hdr.sibling_ptr) != nullptr) {
                if (memcmp(key->key, ((page *)t)->hdr.highest.skey->key,
                           std::min(key->key_len,
                                    ((page *)t)->hdr.highest.skey->key_len)) >=
                    0) {
                    return t;
                }
            }

            if (ret) {
                return ret;
            } else
                return (char *)hdr.leftmost_ptr;
        }

        return nullptr;
    }
};

void init_pmem() {
    // create pool
    const char *pool_name = "/mnt/pmem_pxf/fast_fair.data";
    const char *layout_name = "fast_fair";
    size_t pool_size = 64LL * 1024 * 1024 * 1024; // 16GB

    if (access(pool_name, 0)) {
        pmem_pool = pmemobj_create(pool_name, layout_name, pool_size, 0666);
        if (pmem_pool == nullptr) {
            std::cout << "[FAST FAIR]\tcreate fail\n";
            assert(0);
        }
        std::cout << "[FAST FAIR]\tcreate\n";
    } else {
        pmem_pool = pmemobj_open(pool_name, layout_name);
        std::cout << "[FAST FAIR]\topen\n";
    }
    std::cout << "[FAST FAIR]\topen pmem pool successfully\n";
}

void *allocate(size_t size) {
    void *addr;
    PMEMoid ptr;
    int ret = pmemobj_zalloc(pmem_pool, &ptr, sizeof(char) * size,
                             TOID_TYPE_NUM(char));
    if (ret) {
        std::cout << "[FAST FAIR]\tallocate btree successfully\n";
        assert(0);
    }
    addr = (char *)pmemobj_direct(ptr);
    return addr;
}

void register_thread() {
    std::lock_guard<std::mutex> lock_guard(ti_mtx);
    if (e_mgr == nullptr) {
        e_mgr = new Epoch_Mgr();
        e_mgr->StartThread();
    }
    if (ti_list == nullptr) {
#ifdef USE_PMDK
        PMEMoid ptr;
        int ret = pmemobj_zalloc(pmem_pool, &ptr, sizeof(threadinfo),
                                 TOID_TYPE_NUM(char));
        if (ret) {
            std::cout << "[FAST FAIR]\tti head allocate fail\n";
            assert(0);
        }
        ti_list = new (pmemobj_direct(ptr)) threadinfo(e_mgr);
#else
        ti_list = new threadinfo(e_mgr);
#endif
        ti_list->next = nullptr;
    }

#ifdef USE_PMDK
    PMEMoid ptr;
    int ret = pmemobj_zalloc(pmem_pool, &ptr, sizeof(threadinfo),
                             TOID_TYPE_NUM(char));
    if (ret) {
        std::cout << "[FAST FAIR]\tti allocate fail\n";
        assert(0);
    }
    ti = new (pmemobj_direct(ptr)) threadinfo(e_mgr);
#else
    ti = new threadinfo(e_mgr);
#endif
    ti->id = tid++;
    ti->next = ti_list->next;
    ti->head = ti_list;
    ti_list->next = ti;
#ifdef USE_PMDK
    ti->pool = pmem_pool;

    ret = pmemobj_zalloc(pmem_pool, &ptr, sizeof(GCMetaData),
                         TOID_TYPE_NUM(char));
    if (ret) {
        std::cout << "[FAST FAIR]\tgc meta data allocate fail\n";
        assert(0);
    }
    ti->md = new (pmemobj_direct(ptr)) GCMetaData();
#else
    ti->md = new GCMetaData();
#endif
    // persist garbage list
    flush_data((void *)ti->md, sizeof(GCMetaData));
    flush_data((void *)ti, sizeof(threadinfo));
    flush_data((void *)ti_list, sizeof(threadinfo));
}

/*
 * class btree
 */
btree::btree() {
    register_thread();

    // allocate root
#ifdef USE_PMDK
    TX_BEGIN(pmem_pool) {
        pmemobj_tx_add_range_direct(&root, sizeof(uint64_t));
        std::cout << "[FAST FAIR]\topen pmem pool successfully\n";
        PMEMoid ptr =
            pmemobj_tx_zalloc(sizeof(char) * PAGESIZE, TOID_TYPE_NUM(char));
        root = (char *)(new (pmemobj_direct(ptr)) page());
        flush_data((void *)root, sizeof(page));
    }
    TX_END
    std::cout << "[FAST FAIR]\talloc root successfully\n";
#else
    root = (char *)new page();
    flush_data((void *)root, sizeof(page));
#endif
    height = 1;

    std::cout << "test different component\n";

#ifdef USE_PMDK

#ifdef PMALLOC
    std::cout << "atomic PMALLOC\n";
#elif TXPMALLOC
    std::cout << "tx+atomic pmalloc\n";
#elif TRANSACTIONAL
    std::cout << "transactional\n";
#endif

#else
    std::cout << "using DRAM allocator\n";
#endif

#ifdef VARIABLE_LENGTH
    std::cout << "variable length key\n";
#else
    std::cout << "fixed length key\n";
#endif
}

void btree::setNewRoot(char *new_root) {
    this->root = (char *)new_root;
    flush_data((void *)&this->root, sizeof(char *));
    ++height;
}

key_item *btree::make_key_item(char *key, size_t key_len, bool flush) {
    void *aligned_alloc;
    posix_memalign(&aligned_alloc, 64, sizeof(key_item) + key_len);
    key_item *new_key = (key_item *)aligned_alloc;
    new_key->key_len = key_len;
    //    new_key->key = key;
    memcpy(new_key->key, key, key_len); // copy including nullptr character

    //    flush_data((void *)new_key, sizeof(key_item) + key_len);

    return new_key;
}

char *btree::btree_search(uint64_t key) {
    ti->JoinEpoch();
    page *p = (page *)root;

    while (p->hdr.leftmost_ptr != nullptr) {
        p = (page *)p->linear_search(key, nullptr);
    }

    page *t;
    while ((t = (page *)p->linear_search(key, nullptr)) == p->hdr.sibling_ptr) {
        p = t;
        if (!p) {
            break;
        }
    }

    ti->LeaveEpoch();
    return (char *)t;
}

char *btree::btree_search(char *key) {
    ti->JoinEpoch();
    page *p = (page *)root;

    key_item *new_item = make_key_item(key, strlen(key) + 1, false);

    while (p->hdr.leftmost_ptr != nullptr) {
        p = (page *)p->linear_search(new_item, nullptr);
    }

    page *t;
    while ((t = (page *)p->linear_search(new_item, nullptr)) ==
           p->hdr.sibling_ptr) {
        p = t;
        if (!p) {
            break;
        }
    }

    ti->LeaveEpoch();
    return (char *)t;
}

void btree::btree_update(uint64_t key, char *right) {
    ti->JoinEpoch();
    page *p = (page *)root;

    char *value = right;
#ifdef USE_PMDK
    TX_BEGIN(pmem_pool) {
        pmemobj_tx_add_range_direct(&value, sizeof(uint64_t));
        PMEMoid p = pmemobj_tx_zalloc(strlen(right) + 1, TOID_TYPE_NUM(char));
        value = (char *)pmemobj_direct(p);
        memcpy(value, right, strlen(right) + 1);
        flush_data(value, strlen(right) + 1);
    }
    TX_END
#else
    value = new char[strlen(right) + 1];
    memcpy(value, right, strlen(right) + 1);
    flush_data(value, strlen(right) + 1);
#endif

    while (p->hdr.leftmost_ptr != nullptr) {
        p = (page *)p->linear_search(key, value);
    }

    page *t;
    while ((t = (page *)p->linear_search(key, value)) == p->hdr.sibling_ptr) {
        p = t;
        if (!p) {
            break;
        }
    }

    if (t != nullptr) {
    }
    ti->LeaveEpoch();
}

void btree::btree_update(char *key, char *right) {
    ti->JoinEpoch();
    page *p = (page *)root;

    key_item *new_item = make_key_item(key, strlen(key) + 1, false);

    char *value = right;
#ifdef USE_PMDK
    TX_BEGIN(pmem_pool) {
        pmemobj_tx_add_range_direct(&value, sizeof(uint64_t));
        PMEMoid p = pmemobj_tx_zalloc(strlen(right) + 1, TOID_TYPE_NUM(char));
        value = (char *)pmemobj_direct(p);
        memcpy(value, right, strlen(right) + 1);
        flush_data(value, strlen(right) + 1);
    }
    TX_END
#else
    value = new char[strlen(right) + 1];
    memcpy(value, right, strlen(right) + 1);
    flush_data(value, strlen(right) + 1);
#endif

    while (p->hdr.leftmost_ptr != nullptr) {
        p = (page *)p->linear_search(new_item, value);
    }

    page *t;
    while ((t = (page *)p->linear_search(new_item, value)) ==
           p->hdr.sibling_ptr) {
        p = t;
        if (!p) {
            break;
        }
    }

    ti->LeaveEpoch();
}

// insert the key in the leaf node
void btree::btree_insert(uint64_t key, char *right,
                         bool persist = false) { // need to be string
    ti->JoinEpoch();
    page *p = (page *)root;
    char *value = right;
    if (persist == false) {
#ifdef USE_PMDK
        TX_BEGIN(pmem_pool) {
            pmemobj_tx_add_range_direct(&value, sizeof(uint64_t));
            PMEMoid p =
                pmemobj_tx_zalloc(strlen(right) + 1, TOID_TYPE_NUM(char));
            value = (char *)pmemobj_direct(p);
            memcpy(value, right, strlen(right) + 1);
            flush_data(value, strlen(right) + 1);
        }
        TX_END
#else
        char *value = new char[strlen(right) + 1];
        memcpy(value, right, strlen(right) + 1);
        flush_data(value, strlen(right) + 1);
#endif
    }

    while (p && p->hdr.leftmost_ptr != nullptr) {
        p = (page *)p->linear_search(this, key);
    }

    if (p && !p->store(this, nullptr, key, value, true, true)) { // store
        btree_insert(key, value, true);
    }
    ti->LeaveEpoch();
}

// insert the key in the leaf node
void btree::btree_insert(char *key, char *right,
                         bool persist = false) { // need to be string
                                                 //    std::cout<<strlen(right);
    ti->JoinEpoch();
    page *p = (page *)root;

    key_item *new_item = make_key_item(key, strlen(key) + 1, true);
    char *value = right;
    if (persist == false) {
#ifdef USE_PMDK
        TX_BEGIN(pmem_pool) {
            pmemobj_tx_add_range_direct(&value, sizeof(uint64_t));
            PMEMoid p =
                pmemobj_tx_zalloc(strlen(right) + 1, TOID_TYPE_NUM(char));
            value = (char *)pmemobj_direct(p);
            memcpy(value, right, strlen(right) + 1);
            flush_data(value, strlen(right) + 1);
        }
        TX_END
#else
        char *value = new char[strlen(right) + 1];
        memcpy(value, right, strlen(right) + 1);
        flush_data(value, strlen(right) + 1);
#endif
    }

    while (p && p->hdr.leftmost_ptr != nullptr) {
        p = (page *)p->linear_search(new_item);
    }

    if (p && !p->store(this, nullptr, new_item, value, true, true)) { // store
        btree_insert(key, value, true);
    }
    ti->LeaveEpoch();
}

// store the integer key into the node at the given level
void btree::btree_insert_internal(char *left, uint64_t key, char *right,
                                  uint32_t level) {
    if (level > ((page *)root)->hdr.level)
        return;

    page *p = (page *)this->root;

    while (p->hdr.level > level)
        p = (page *)p->linear_search(key);

    if (!p->store(this, left, key, right, true, true)) {
        btree_insert_internal(left, key, right, level);
    }
}

// store the string key into the node at the given level
void btree::btree_insert_internal(char *left, key_item *key, char *right,
                                  uint32_t level) {
    if (level > ((page *)root)->hdr.level)
        return;

    page *p = (page *)this->root;

    while (p->hdr.level > level)
        p = (page *)p->linear_search(key);

    if (!p->store(this, nullptr, key, right, true, true)) {
        btree_insert_internal(left, key, right, level);
    }
}

void btree::btree_delete(uint64_t key) {
    ti->JoinEpoch();
    page *p = (page *)root;

    while (p->hdr.leftmost_ptr != nullptr) {
        p = (page *)p->linear_search(key);
    }

    page *t;
    while ((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
        p = t;
        if (!p)
            break;
    }

    if (p && t) {
        if (!p->remove(this, key)) {
            //            btree_delete(key);
        }
    } else {
        printf("not found the key to delete %lu\n", key);
    }
    ti->LeaveEpoch();
}

void btree::btree_delete(char *key) {
    //    std::cout<<key<<" "<<strlen(key)<<"\n";
    ti->JoinEpoch();
    page *p = (page *)root;

    key_item *new_item = make_key_item(key, strlen(key) + 1, true);

    while (p->hdr.leftmost_ptr != nullptr) {
        p = (page *)p->linear_search(new_item);
    }

    page *t;
    while ((t = (page *)p->linear_search(new_item)) == p->hdr.sibling_ptr) {
        p = t;
        if (!p)
            break;
    }
    //    std::cout<<"t is "<<(uint64_t)t<<"\n";

    if (p && t) {
        if (!p->remove(this, new_item)) {
            //            btree_delete(key);
        }
    } else {
        printf("not found the key to delete %lu\n", key);
    }
    ti->LeaveEpoch();
}

// Function to search integer keys from "min" to "max"
void btree::btree_search_range(uint64_t min, uint64_t max, unsigned long *buf,
                               int num, int &off) {
    page *p = (page *)root;

    while (p) {
        if (p->hdr.leftmost_ptr != nullptr) {
            // The current page is internal
            p = (page *)p->linear_search(min);
        } else {
            // Found a leaf
            p->linear_search_range(min, max, buf, num, off);

            break;
        }
    }
}

// Function to search string keys from "min" to "max"
void btree::btree_search_range(char *min, char *max, unsigned long *buf,
                               int num, int &off, char *scan_value) {
    page *p = (page *)root;
    key_item *min_item = make_key_item(min, strlen(min) + 1, false);
    key_item *max_item = make_key_item(max, strlen(max) + 1, false);

    while (p) {
        if (p->hdr.leftmost_ptr != nullptr) {
            // The current page is internal
            p = (page *)p->linear_search(min_item);
        } else {
            // Found a leaf
            p->linear_search_range(min_item, max_item, buf, num, off,
                                   scan_value);

            break;
        }
    }
}
} // namespace fastfair
#endif
