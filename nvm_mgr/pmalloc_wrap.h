#ifndef pmalloc_wrap_h
#define pmalloc_wrap_h

#include "nvm_mgr.h"
#include <assert.h>
#include <iostream>
#include <list>
#include <mutex>
#include <numa.h>
#include <stdio.h>
#include <stdlib.h>

#define USE_NVM_MALLOC

namespace NVMMgr_ns {

class PMBlockAllocator {
    int alignment = 64;
    NVMMgr *mgr;

  public:
    PMBlockAllocator(NVMMgr *mgr_ = NULL) { mgr = mgr_; }
    ~PMBlockAllocator() {}

    void *alloc_block(int tid) {
        if (mgr == NULL) {
            mgr = get_nvm_mgr();
            if (mgr == NULL) {
                std::cout << "[PMBLOCK]\tneed to call init_nvm_mgr() first\n";
                assert(0);
            }
        }
#ifdef USE_NVM_MALLOC
        // mgr is thread safe
        return mgr->alloc_block(tid);
#else
        return aligned_alloc(alignment, NVMMgr::PGSIZE);
#endif // USE_NVM_MALLOC
    }

    void free_block(void *block) {
        // TODO: free block
    }
} __attribute__((aligned(64)));

} // namespace NVMMgr_ns
#endif
