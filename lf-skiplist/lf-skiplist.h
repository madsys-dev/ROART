#ifndef _LF_SKIPLIST_H_
#define _LF_SKIPLIST_H_

/*
	lock-free skip-list algorithm
*/
#include <cstdint>
#include <stdlib.h>
#include "util.h"
#include "atomic_ops_if.h"
#include <stdio.h>
#include <libpmemobj.h>
#include "sl_gc.h"

namespace skiplist {

#define CACHE_LINES_PER_NV_NODE 3 //TODO does nv-jemalloc need to be aware of this?

const int max_level = 20; //one cache-line node; use 13 for two cache-line nodes

	typedef uintptr_t UINT_PTR;
	typedef uint32_t UINT32;
	typedef void *PVOID;

#define MIN_KEY 1
#define MAX_KEY 2

#define NODE_PADDING 1

#define CACHE_SIZE 64
#define PMEM_CACHE_ALIGNED ALIGNED(CACHE_SIZE)

	static inline UINT_PTR mark_ptr_cache(UINT_PTR p) {
		return (p | (UINT_PTR) 0x04);
	}

	static inline UINT_PTR unmark_ptr_cache(UINT_PTR p) {
		return (p & ~(UINT_PTR) 0x04);
	}

	static inline int is_marked_ptr_cache(UINT_PTR p) {
		return (int) (p & (UINT_PTR) 0x04);
	}


	inline void flush_and_try_unflag(PVOID *target) {
		//return;
		PVOID value = *target;
		if (is_marked_ptr_cache((UINT_PTR) value)) {
			flush_data(target, sizeof(PVOID));
			CAS_PTR((volatile PVOID *) target, value, (PVOID) unmark_ptr_cache((UINT_PTR) value));
		}
	}


//links a node and persists it
//marks the link while it is doing the persist
	inline PVOID link_and_persist(PVOID *target, PVOID oldvalue, PVOID value) {
		//return CAS_PTR(target,oldvalue, value);
		PVOID res;
		res = CAS_PTR(target, (PVOID) oldvalue, (PVOID) mark_ptr_cache((UINT_PTR) value));

		//if cas successful, we updated the link, but it still needs flushing
		if (res != oldvalue) {
			return res; //nothing gets fluhed
		}
		flush_data(target, sizeof(PVOID));
		CAS_PTR((volatile PVOID *) target, (PVOID) mark_ptr_cache((UINT_PTR) value), (PVOID) value);
		return res;
	}

	typedef char * skey_t;
	typedef char * svalue_t;

	struct node_t {
		skey_t key;
		svalue_t value;
		int key_len;
		int val_len;

		uint8_t max_min_flag;

		node_t *next[max_level + 2]; //in our allocator, we will be working with chunks of the same size, so every node should have the same size
		uint32_t toplevel;
		uint8_t node_flags;
	};


	typedef node_t *skiplist_t;

	const int skiplist_node_size = sizeof(node_t);

	svalue_t skiplist_find(skiplist_t *sl, skey_t key);

	int skiplist_insert(skiplist_t *sl, skey_t key, svalue_t val);

	svalue_t skiplist_remove(skiplist_t *sl, skey_t key);

	void skiplit_update(skiplist_t *sl, skey_t key, svalue_t val);

	skiplist_t *new_skiplist();

    void init_pmem();
    void *allocate(size_t size);
    void register_thread();

}

#endif
