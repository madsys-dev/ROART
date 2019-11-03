#ifndef nvm_mgr_h
#define nvm_mgr_h

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string>
#include <list>
#include "util.h"

namespace NVMMgr_ns {

    class NVMMgr {
        /*
         *
         * A simple structure to manage the NVM file
         *
         *   / 256K  /  128 * 256K  /     ...     /
         *  / head  / thread local / data blocks /
         *
         *  head:
         *       Avaiable to the NVM manager, including root and bitmap to indicate
         *       how many blocks have been allocated.
         *
         *  thread local:
         *       Each thread can own a thread local persistent memory area. After
         * system crashes, NVM manager gives these thread local persistent memory to
         * the application. The application may use thread local variables to help
         * recovery. For example, in RNTree, a leaf node is logged in the thread
         * local area before it is splitted. The recovery procedure can use the
         * thread local log to guarantee the crash consistency of the leaf.
         *
         *       The function "recover_done" should be invoked after the recovery,
         * otherwise these thread local persistent memories are leaked. The maximum
         * number of thread local blocks can be allocated is hard coded in this
         * file.
         *
         *  data:
         *       True persistent memory allocated for applications. For simplicity,
         * we do not recyle memory.
         *
         */
        static const int magic_number = 12345;
        static const int max_threads = 128;

        static const size_t start_addr = 0x50000000;
        static const int PGSIZE = 256 * 1024; // 256K
        static const long long filesize = 16LL * 1024 * PGSIZE;  // 4GB

        static const size_t tree_meta_start = start_addr + PGSIZE;
        static const size_t thread_local_start = tree_meta_start + PGSIZE;
        static const size_t data_block_start =
                thread_local_start + PGSIZE * max_threads;

        static const char *get_filename() {
            static const std::string filename = "/mnt/dax/matianmao/part.data";
            return filename.c_str();
        }

        struct Head {
            // TODO: some other meta data for recovery
            char root[4096]; // for root

            int status; // if equal to magic_number, it is reopen
            int threads; // threads number
            int bitmap[0];   // show every page type
            // 0: free, 1: N4, 2: N16, 3: N48, 4: N256, 5: Leaf
        };

    public:
        NVMMgr();

        ~NVMMgr();

        void reload_free_blocks();

        // TODO: raise exception if this function is not invoked before any alloc
        // function.
//        void recover_done();

        void *alloc_tree_root() { return (void *)meta_data; }

        void *alloc_thread_info();

        void *alloc_block(int pages);

        uint64_t free_bit_offset;
        std::list<uint64_t> free_page_list;
        int fd;
        Head *meta_data;
    };

    NVMMgr *get_nvm_mgr();

    // true: first initialize
    // false: have been initialized
    bool init_nvm_mgr();

    void close_nvm_mgr();
}

#endif  // nvm_mgr_h
