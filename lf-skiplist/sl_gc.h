#pragma once

#include <libpmemobj.h>

namespace skiplist {


    static const int GC_NODE_COUNT_THREADHOLD = 1024;

    static const int GC_INTERVAL = 50;

    POBJ_LAYOUT_BEGIN(skiplist);
    POBJ_LAYOUT_TOID(skiplist, struct key_item);
    POBJ_LAYOUT_TOID(skiplist, char);
    POBJ_LAYOUT_END(skiplist);

    typedef struct key_item {
        size_t key_len;
        char key[];
    } key_item;

    class GarbageNode {
    public:
        uint64_t delete_epoch;
        void *node_p;
        GarbageNode *next_p;

        GarbageNode(uint64_t p_delete_epoch, void *p_node_p)
                : delete_epoch{p_delete_epoch}, node_p{p_node_p}, next_p{nullptr} {}

        GarbageNode() : delete_epoch{0UL}, node_p{nullptr}, next_p{nullptr} {}
    } __attribute__((aligned(64)));

    class GCMetaData {
    public:
        uint64_t last_active_epoch;

        GarbageNode header;
        GarbageNode *last_p;

        int node_count;

        GCMetaData() {
            last_active_epoch = static_cast<uint64_t>(-1);
            last_p = &header;
            node_count = 0;
        }

        ~GCMetaData() {}
    } __attribute__((aligned(64)));

    class Epoch_Mgr {
    public:
        std::thread *thread_p;
        bool exit_flag;
        uint64_t epoch;

    public:
        Epoch_Mgr() : exit_flag(false), epoch(0) {}

        ~Epoch_Mgr() {
            exit_flag = true;
            std::cout << "[EPOCH]\tepoch mgr exit\n";
        }

        inline void IncreaseEpoch() { epoch++; }

        uint64_t GetGlobalEpoch() { return epoch; }

        void ThreadFunc() {
            std::cout << "[EPOCH]\tglobal epoch thread start\n";
            while (exit_flag == false) {
                IncreaseEpoch();
                std::chrono::milliseconds duration(GC_INTERVAL);
                std::this_thread::sleep_for(duration);
            }
            std::cout << "[EPOCH]\tglobal epoch thread exit\n";
            return;
        }

        void StartThread() {
            thread_p = new std::thread{[this]() { this->ThreadFunc(); }};
            return;
        }

    } __attribute__((aligned(64)));

    class threadinfo {
    public:
        GCMetaData *md;
        Epoch_Mgr *mgr;

        threadinfo *next;
        threadinfo *head;

        PMEMobjpool *pool;

        int id;

        threadinfo(Epoch_Mgr *mgr_) : mgr(mgr_) {}

        ~threadinfo() {}

        void JoinEpoch() { md->last_active_epoch = mgr->GetGlobalEpoch(); }

        void LeaveEpoch() { md->last_active_epoch = static_cast<uint64_t>(-1); }

        void AddGarbageNode(void *node_p) {
            TX_BEGIN(pool) {
                            pmemobj_tx_add_range_direct(&md->last_p->next_p, sizeof(uint64_t));
                            pmemobj_tx_add_range_direct(&md->last_p, sizeof(uint64_t));
                            pmemobj_tx_add_range_direct(&md->node_count, sizeof(int));

                            PMEMoid p =
                                    pmemobj_tx_alloc(sizeof(GarbageNode), TOID_TYPE_NUM(char));
                            GarbageNode *gn = new(pmemobj_direct(p))
                                    GarbageNode(mgr->GetGlobalEpoch(), node_p);
                            md->last_p->next_p = gn;
                            md->last_p = gn;
                            md->node_count++;
                        }
            TX_END

            if (md->node_count > GC_NODE_COUNT_THREADHOLD) {
                // Use current thread's gc id to perform GC
                PerformGC();
            }

            return;
        }

        uint64_t SummarizeGCEpoch(threadinfo *head_) {
            threadinfo *tti = head->next;
            uint64_t min_epoch = static_cast<uint64_t>(-1);
            while (tti != nullptr) {
                min_epoch = std::min(min_epoch, tti->md->last_active_epoch);
                tti = tti->next;
            }
            return min_epoch;
        }

        void PerformGC() {
            // First of all get the minimum epoch of all active threads
            // This is the upper bound for deleted epoch in garbage node
            uint64_t min_epoch = SummarizeGCEpoch(head);

            // This is the pointer we use to perform GC
            // Note that we only fetch the metadata using the current thread-local
            // id
            GarbageNode *header_p = &(md->header);
            GarbageNode *first_p = header_p->next_p;

            // Then traverse the linked list
            // Only reclaim memory when the deleted epoch < min epoch
            while (first_p != nullptr && first_p->delete_epoch < min_epoch) {
                // First unlink the current node from the linked list
                // This could set it to nullptr
                TX_BEGIN(pool) {
                                pmemobj_tx_add_range_direct(&header_p->next_p,
                                                            sizeof(uint64_t));
                                pmemobj_tx_add_range_direct(&md->node_count, sizeof(int));
                                header_p->next_p = first_p->next_p;

                                PMEMoid ptr = pmemobj_oid(first_p->node_p);
                                pmemobj_tx_free(ptr);

                                md->node_count--;
                            }
                TX_END

                // Then free memory
                //            FreeEpochNode(first_p->node_p);

                //            delete first_p;
                first_p = header_p->next_p;
            }

            // If we have freed all nodes in the linked list we should
            // reset last_p to the header
            if (first_p == nullptr) {
                md->last_p = header_p;
            }
            return;
        }
    };
}
