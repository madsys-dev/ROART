#ifndef coordinator_h
#define coordinator_h

#include "Key.h"
#include "N.h"
#include "Tree.h"
#include "benchmarks.h"
#include "config.h"
#include <time.h>

#ifdef ACMA
#include "fast_fair_acma.h"
#include "skiplist-acma.h"
#else
#include "fast_fair.h"
#include "skiplist.h"
#endif

#include "nvm_mgr.h"
#include "threadinfo.h"
#include "timer.h"
#include "util.h"
#include <boost/thread/barrier.hpp>
#include <string>
#include <thread>
#include <unistd.h>

using namespace NVMMgr_ns;

const int thread_to_core[36] = {
    0,  4,  8,  12, 16, 20, 24, 28, 32,
    36, 40, 44, 48, 52, 56, 60, 64, 68, // numa node 0
    1,  5,  9,  13, 17, 21, 25, 29, 33,
    37, 41, 45, 49, 53, 57, 61, 65, 69 // numa node 1
};

template <typename K, typename V, int size> class Coordinator {
    class Result {
      public:
        double throughput;
        double update_latency_breaks[3];
        double find_latency_breaks[3];
        double total;
        long long count;
        long long helpcount;
        long long writecount;

        Result() {
            throughput = 0;
            total = 0;
            count = 0;
            helpcount = 0;
            writecount = 0;
            for (int i = 0; i < 3; i++) {
                update_latency_breaks[i] = find_latency_breaks[i] = 0;
            }
        }

        void operator+=(Result &r) {
            this->throughput += r.throughput;
            this->total += r.total;
            this->count += r.count;
            this->helpcount += r.helpcount;
            this->writecount += r.writecount;
            for (int i = 0; i < 3; i++) {
                this->update_latency_breaks[i] += r.update_latency_breaks[i];
                this->find_latency_breaks[i] += r.find_latency_breaks[i];
            }
        }

        void operator/=(double r) {
            this->throughput /= r;
            for (int i = 0; i < 3; i++) {
                this->update_latency_breaks[i] /= r;
                this->find_latency_breaks[i] /= r;
            }
        }
    };

  public:
    Coordinator(Config _conf) : conf(_conf) {}

    int stick_this_thread_to_core(int core_id) {
        int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
        if (core_id < 0 || core_id >= num_cores)
            return EINVAL;

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(thread_to_core[core_id], &cpuset);

        pthread_t current_thread = pthread_self();
        return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t),
                                      &cpuset);
    }

    void art_worker(PART_ns::Tree *art, int workerid, Result *result,
                    Benchmark *b) {
        //            Benchmark *benchmark = getBenchmark(conf);
        Benchmark *benchmark = getBenchmark(conf);
        printf("[WORKER]\thello, I am worker %d\n", workerid);
        NVMMgr_ns::register_threadinfo();
        stick_this_thread_to_core(workerid);
        bar->wait();

        unsigned long tx = 0;

        double total_update_latency_breaks[3] = {0.0, 0.0, 0.0};
        double update_latency_breaks[3] = {0.0, 0.0, 0.0};
        int update_count = 0;

        double total_find_latency_breaks[3] = {0.0, 0.0, 0.0};
        double find_latency_breaks[3] = {0.0, 0.0, 0.0};
        int find_count = 0;

        memset(total_update_latency_breaks, 0, sizeof(double) * 3);
        memset(total_find_latency_breaks, 0, sizeof(double) * 3);

#define sync_latency(a, b, count)                                              \
    do {                                                                       \
        if (b[0] > 0.0 || (count > 100) && (b[0] > 10 * a[0] / count)) {       \
            for (int i = 0; i < 3; i++) {                                      \
                a[i] += b[i];                                                  \
            }                                                                  \
            count++;                                                           \
        }                                                                      \
    } while (0)

        static int scan_values = 0;
        static int scan_length;
        scan_length = conf.scan_length;

        auto scan_func = [](K key, V value) -> bool {
            scan_values++;

            if (scan_values > scan_length) {
                return true;
            }
            return false;
        };

        int frequency = conf.throughput / conf.num_threads;
        int submit_time = 1000000000.0 / frequency;
        int count = 0;
        PART_ns::Key *k = new PART_ns::Key();
        PART_ns::Key *maxkey = new PART_ns::Key();
        PART_ns::Key *continueKey;
        PART_ns::Leaf *scan_result[505];

        // variable value
        const int val_len = conf.val_length;
        char value[val_len + 5];
        memset(value, 'a', val_len);
        value[val_len] = 0;
        maxkey->Init("z", 1, value, val_len);

        char scan_value[val_len + 5];

        int xcount = 0;
        cpuCycleTimer t;
        int write = 0;

#ifdef COUNT_ALLOC
        cpuCycleTimer writeop;
#endif

        while (done == 0) {

            V result = 1;

            OperationType op;
            long long d;
            std::string s;

            if (conf.key_type == Integer) {
                auto next_operation = benchmark->nextIntOperation();
                op = next_operation.first;
                d = next_operation.second;
                //                std::string s = std::to_string(d);
                //                k->Init((char *)s.c_str(), s.size(), value,
                //                val_len);
                k->Init((char *)&d, sizeof(uint64_t), value, val_len);
            } else if (conf.key_type == String) {
                auto next_operation = benchmark->nextStrOperation();
                op = next_operation.first;
                s = next_operation.second;
                k->Init((char *)s.c_str(), s.size(), value, val_len);
            }

            PART_ns::Tree::OperationResults res;
            switch (op) {
            case UPDATE: {
                //                std::cout<<"update key "<<s<<"\n";
#ifdef COUNT_ALLOC
                writeop.start();
#endif

                art->update(k);

#ifdef COUNT_ALLOC
                writeop.end();
#endif
                write++;
                break;
            }
            case INSERT: {
                // printf("[%d] start insert %lld\n", workerid, d);
#ifdef COUNT_ALLOC
                writeop.start();
#endif

                res = art->insert(k);

#ifdef COUNT_ALLOC
                writeop.end();
#endif
                if (res == PART_ns::Tree::OperationResults::Success)
                    xcount++;
                break;
            }
            case REMOVE: {
                //                art->insert(k);
                art->remove(k);
                break;
            }
            case GET: {
                if (conf.latency_test) {
                    t.start();
                }
                //                std::cout<<"lookup key "<<s<<"\n";
                art->lookup(k);
                if (conf.latency_test) {
                    t.end();
                }
                break;
            }
            case SCAN: {
                continueKey = nullptr;
                size_t resultFound = 0;
                //                std::cout<<"scan "<<(char *)(k->fkey)<<"\n";
                art->lookupRange(k, maxkey, continueKey, scan_result,
                                 conf.scan_length, resultFound);
                //                std::cout<<"found"<<resultFound<<"\n";
                //                for(int i = 0; i < resultFound; i++){
                //                    memcpy(scan_value, scan_result[i]->value,
                //                    100);
                //                }
                break;
            }
            default: {
                printf("not support such operation: %d\n", op);
                exit(-1);
            }
            }

            tx++;
        }
        result->throughput = tx;
        if (conf.latency_test) {
            result->total = t.duration();
            result->count = t.Countnum();
        }
        result->helpcount = PART_ns::gethelpcount();
        result->writecount = write;

        printf("[%d] finish %d insert\n", workerid, xcount);

#ifdef COUNT_ALLOC
#ifdef ARTPMDK
        printf("[%d]\twrite latency %.3f, alloc time %.3f\n", workerid,
               writeop.average(), PART_ns::getalloctime() / writeop.Countnum());
#else
        printf("[%d]\twrite latency %.3f, alloc time %.3f\n", workerid,
               writeop.average(),
               NVMMgr_ns::getdcmmalloctime() / writeop.Countnum());
#endif
#endif

#ifdef PERF_LATENCY
        for (int i = 0; i < 3; i++) {
            result->update_latency_breaks[i] =
                total_update_latency_breaks[i] / update_count;
            result->find_latency_breaks[i] =
                total_find_latency_breaks[i] / find_count;
            printf("%d result %lf %lf\n", workerid,
                   total_update_latency_breaks[i],
                   total_find_latency_breaks[i]);
        }
#endif // PERF_LATENCY

        unregister_threadinfo();

#ifdef CHECK_COUNT
        printf("[COUNT]\tworker %d check keys %.2lf\n", workerid,
               1.0 * PART_ns::get_count() / tx);
#endif

        printf("[WORKER]\tworker %d finished\n", workerid);
    }

    void ff_worker(fastfair::btree *bt, int workerid, Result *result,
                   Benchmark *b) {
        //            Benchmark *benchmark = getBenchmark(conf);
        Benchmark *benchmark = getBenchmark(conf);
        printf("[WORKER]\thello, I am worker %d\n", workerid);
        stick_this_thread_to_core(workerid);
#ifdef ACMA
        NVMMgr_ns::register_threadinfo();
#else
        fastfair::register_thread();
#endif
        bar->wait();

        unsigned long tx = 0;

        double total_update_latency_breaks[3] = {0.0, 0.0, 0.0};
        double update_latency_breaks[3] = {0.0, 0.0, 0.0};
        int update_count = 0;

        double total_find_latency_breaks[3] = {0.0, 0.0, 0.0};
        double find_latency_breaks[3] = {0.0, 0.0, 0.0};
        int find_count = 0;

        memset(total_update_latency_breaks, 0, sizeof(double) * 3);
        memset(total_find_latency_breaks, 0, sizeof(double) * 3);

#define sync_latency(a, b, count)                                              \
    do {                                                                       \
        if (b[0] > 0.0 || (count > 100) && (b[0] > 10 * a[0] / count)) {       \
            for (int i = 0; i < 3; i++) {                                      \
                a[i] += b[i];                                                  \
            }                                                                  \
            count++;                                                           \
        }                                                                      \
    } while (0)

        static int scan_values = 0;
        static int scan_length;
        scan_length = conf.scan_length;

        auto scan_func = [](K key, V value) -> bool {
            scan_values++;

            if (scan_values > scan_length) {
                return true;
            }
            return false;
        };

        int frequency = conf.throughput / conf.num_threads;
        int submit_time = 1000000000.0 / frequency;
        int count = 0;

        const int val_len = conf.val_length;
        char value[val_len + 5];
        memset(value, 'a', val_len);
        value[val_len] = 0;

        // for scan
        std::string maxkey = "z";
        uint64_t buf[505];
        char scan_value[val_len + 5];

        while (done == 0) {

            V result = 1;
            OperationType op;
            long long d;
            std::string s;

            if (conf.key_type == Integer) {
                auto next_operation = benchmark->nextIntOperation();

                op = next_operation.first;
                d = next_operation.second;
            } else if (conf.key_type == String) {
                auto next_operation = benchmark->nextStrOperation();

                op = next_operation.first;
                s = next_operation.second;
            }

            cpuCycleTimer t;
            if (conf.latency_test) {
                t.start();
            }

            switch (op) {
            case UPDATE: {
                if (conf.key_type == Integer) {
                    //                    std::cout<<"insert key "<<d<<"\n";
                    bt->btree_update(d, value);
                } else if (conf.key_type == String) {
                    bt->btree_update((char *)s.c_str(), value);
                }

                break;
            }
            case INSERT: {
                // printf("[%d] start insert %lld\n", workerid, d);

                if (conf.key_type == Integer) {
                    //                    std::cout<<"insert key "<<d<<"\n";
                    bt->btree_insert(d, value);
                } else if (conf.key_type == String) {
                    bt->btree_insert((char *)s.c_str(), value);
                }

                //                if (conf.key_type == Integer) {
                //                    //                    std::cout<<"delete
                //                    key "<<d<<"\n"; bt->btree_delete(d);
                //                } else if (conf.key_type == String) {
                //                    //                    std::cout<<"delete
                //                    key "<<s<<"\n"; bt->btree_delete((char
                //                    *)s.c_str());
                //                }

                break;
            }
            case REMOVE: {
                // first insert then remove
                //                                if (conf.key_type == Integer)
                //                                {
                //                                    // std::cout<<"insert key
                //                                    "<<d<<"\n";
                //                                    bt->btree_insert(d,
                //                                    value);
                //                                } else if (conf.key_type ==
                //                                String) {
                //                                    bt->btree_insert((char
                //                                    *)s.c_str(), value);
                //                                }

                if (conf.key_type == Integer) {
                    //                    std::cout<<"delete key "<<d<<"\n";
                    bt->btree_delete(d);
                } else if (conf.key_type == String) {
                    //                    std::cout<<"delete key "<<s<<"\n";
                    bt->btree_delete((char *)s.c_str());
                }

                break;
            }
            case GET: {
                if (conf.key_type == Integer) {
                    bt->btree_search(d);
                } else if (conf.key_type == String) {
                    bt->btree_search((char *)s.c_str());
                }

                // if (tx % 100 == 0) {
                //     sync_latency(total_find_latency_breaks,
                //                  find_latency_breaks, find_count);
                // }
                break;
            }
            case SCAN: {
                int resultFound = 0;
                //                std::cout<<"ff scan "<<s<<"\n";
                // TODO
                bt->btree_search_range((char *)s.c_str(),
                                       (char *)maxkey.c_str(),
                                       (unsigned long *)buf, conf.scan_length,
                                       resultFound, scan_value);

                //                std::cout<<"find "<<resultFound<<"\n";
                break;
            }
            default: {
                printf("not support such operation: %d\n", op);
                exit(-1);
            }
            }
            if (conf.latency_test) {
                t.end();
                while (t.duration() < submit_time) {
                    t.start();
                    t.end();
                }
            }

            tx++;
        }
        result->throughput = tx;
        // printf("[%d] finish %d insert\n", workerid, count);

#ifdef PERF_LATENCY
        for (int i = 0; i < 3; i++) {
            result->update_latency_breaks[i] =
                total_update_latency_breaks[i] / update_count;
            result->find_latency_breaks[i] =
                total_find_latency_breaks[i] / find_count;
            printf("%d result %lf %lf\n", workerid,
                   total_update_latency_breaks[i],
                   total_find_latency_breaks[i]);
        }
#endif // PERF_LATENCY

        printf("[WORKER]\tworker %d finished\n", workerid);
    }

    void sl_worker(skiplist::skiplist_t *sl, int workerid, Result *result,
                   Benchmark *b) {
        //            Benchmark *benchmark = getBenchmark(conf);
        Benchmark *benchmark = getBenchmark(conf);
        printf("[WORKER]\thello, I am worker %d\n", workerid);
        stick_this_thread_to_core(workerid);
#ifdef ACMA
        NVMMgr_ns::register_threadinfo();
#else
        skiplist::register_thread();
#endif
        bar->wait();

        unsigned long tx = 0;

        double total_update_latency_breaks[3] = {0.0, 0.0, 0.0};
        double update_latency_breaks[3] = {0.0, 0.0, 0.0};
        int update_count = 0;

        double total_find_latency_breaks[3] = {0.0, 0.0, 0.0};
        double find_latency_breaks[3] = {0.0, 0.0, 0.0};
        int find_count = 0;

        memset(total_update_latency_breaks, 0, sizeof(double) * 3);
        memset(total_find_latency_breaks, 0, sizeof(double) * 3);

#define sync_latency(a, b, count)                                              \
    do {                                                                       \
        if (b[0] > 0.0 || (count > 100) && (b[0] > 10 * a[0] / count)) {       \
            for (int i = 0; i < 3; i++) {                                      \
                a[i] += b[i];                                                  \
            }                                                                  \
            count++;                                                           \
        }                                                                      \
    } while (0)

        static int scan_values = 0;
        static int scan_length;
        scan_length = conf.scan_length;

        auto scan_func = [](K key, V value) -> bool {
            scan_values++;

            if (scan_values > scan_length) {
                return true;
            }
            return false;
        };

        int frequency = conf.throughput / conf.num_threads;
        int submit_time = 1000000000.0 / frequency;
        int count = 0;

        const int val_len = conf.val_length;
        char value[val_len + 5];
        memset(value, 'a', val_len);
        value[val_len] = 0;
        char *buf[505];
        char scan_value[val_len + 5];

        while (done == 0) {

            V result = 1;
            long long d;
            OperationType op;
            std::string s;

            if (conf.key_type == Integer) {
                auto next_operation = benchmark->nextIntOperation();

                op = next_operation.first;
                d = next_operation.second;
            } else if (conf.key_type == String) {
                auto next_operation = benchmark->nextStrOperation();

                op = next_operation.first;
                s = next_operation.second;
            }

            cpuCycleTimer t;
            if (conf.latency_test) {
                t.start();
            }

            switch (op) {
            case UPDATE: {
#ifdef VARIABLE_LENGTH
                skiplist::skiplist_update(sl, (char *)s.c_str(), value);
#else
                skiplist::skiplist_update(sl, d, d);
#endif
                break;
            }
            case INSERT: {
#ifdef VARIABLE_LENGTH
                skiplist::skiplist_insert(sl, (char *)s.c_str(), value);
//                skiplist::skiplist_remove(sl, (char *)s.c_str());
#else
                skiplist::skiplist_insert(sl, d, d);
//                skiplist::skiplist_remove(sl, d);
#endif
                break;
            }
            case REMOVE: {
#ifdef VARIABLE_LENGTH
                skiplist::skiplist_remove(sl, (char *)s.c_str());
#else
                skiplist::skiplist_remove(sl, d);
#endif
                break;
            }
            case GET: {
#ifdef VARIABLE_LENGTH
                skiplist::skiplist_find(sl, (char *)s.c_str());
#else
                skiplist::skiplist_find(sl, d);
#endif
                break;
            }
            case SCAN: {
                int resultFound = 0;
#ifdef VARIABLE_LENGTH
                skiplist::skiplist_scan(sl, (char *)s.c_str(), buf,
                                        conf.scan_length, resultFound,
                                        scan_value);
#else
                skiplist::skiplist_scan(sl, d, (uint64_t *)buf,
                                        conf.scan_length, resultFound,
                                        scan_value);
#endif
                //                std::cout<<resultFound<<"\n";
                break;
            }
            default: {
                printf("not support such operation: %d\n", op);
                exit(-1);
            }
            }
            if (conf.latency_test) {
                t.end();
                while (t.duration() < submit_time) {
                    t.start();
                    t.end();
                }
            }

            tx++;
        }
        result->throughput = tx;
        // printf("[%d] finish %d insert\n", workerid, count);

#ifdef PERF_LATENCY
        for (int i = 0; i < 3; i++) {
            result->update_latency_breaks[i] =
                total_update_latency_breaks[i] / update_count;
            result->find_latency_breaks[i] =
                total_find_latency_breaks[i] / find_count;
            printf("%d result %lf %lf\n", workerid,
                   total_update_latency_breaks[i],
                   total_find_latency_breaks[i]);
        }
#endif // PERF_LATENCY

        printf("[WORKER]\tworker %d finished\n", workerid);
    }

#ifdef INSTANT_RESTART
    void art_restart(PART_ns::Tree *art, int workerid, Benchmark *b) {
        // Benchmark *benchmark = getBenchmark(conf);
        Benchmark *benchmark = getBenchmark(conf);
        printf("[WORKER]\thello, I am worker %d\n", workerid);
        NVMMgr_ns::register_threadinfo();
        stick_this_thread_to_core(workerid);
        bar->wait();

        unsigned long tx = 0;

        int count = 0;
        PART_ns::Key *k = new PART_ns::Key();

        // variable value
        const int val_len = conf.val_length;
        char value[val_len + 5];
        memset(value, 'a', val_len);
        value[val_len] = 0;

        int xcount = 0;
        cpuCycleTimer t;
        int write = 0;

        while (done == 0) {

            V result = 1;

            OperationType op;
            long long d;
            std::string s;

            if (conf.key_type == Integer) {
                auto next_operation = benchmark->nextIntOperation();
                op = next_operation.first;
                d = next_operation.second;
                //                std::string s = std::to_string(d);
                //                k->Init((char *)s.c_str(), s.size(), value,
                //                val_len);
                k->Init((char *)&d, sizeof(uint64_t), value, val_len);
            } else if (conf.key_type == String) {
                auto next_operation = benchmark->nextStrOperation();
                op = next_operation.first;
                s = next_operation.second;
                k->Init((char *)s.c_str(), s.size(), value, val_len);
            }

            PART_ns::Tree::OperationResults res;
            switch (op) {
            case UPDATE: {

                art->update(k);

                write++;
                break;
            }
            case INSERT: {

                res = art->insert(k);

                if (res == PART_ns::Tree::OperationResults::Success)
                    xcount++;
                break;
            }
            case REMOVE: {
                art->remove(k);
                break;
            }
            case GET: {
                art->lookup(k);
                break;
            }
            case SCAN: {
                break;
            }
            default: {
                printf("not support such operation: %d\n", op);
                exit(-1);
            }
            }
            increase(workerid);
        }

        unregister_threadinfo();

        printf("[WORKER]\tworker %d finished\n", workerid);
    }

    void monitor_work(int thread_num) {
        bar->wait();

        uint64_t preans = 0;
        int second = 0;
        timespec start, end;
        while (done == 0) {
            //            clock_gettime(CLOCK_REALTIME, &start);
            usleep(conf.duration * 1000000);
            //            clock_gettime(CLOCK_REALTIME, &end);

            //            double duration = end.tv_sec - start.tv_sec +
            //            (end.tv_nsec - start.tv_nsec) / 1000000000.0;

            uint64_t nowans = total(thread_num);
            printf("second %d the throughput is %.2f Mop/s\n", second,
                   (nowans - preans) / 1000000.0);
            preans = nowans;
            second++;
        }
    }

    void instant_restart() {
        PART_ns::Tree *art = new PART_ns::Tree();
        Benchmark *benchmark = getBenchmark(conf);

        std::thread **pid = new std::thread *[conf.num_threads];
        bar = new boost::barrier(conf.num_threads + 2);

        for (int i = 0; i < conf.num_threads; i++) {
            pid[i] = new std::thread(&Coordinator::art_restart, this, art, i,
                                     benchmark);
        }

        std::thread *monitor =
            new std::thread(&Coordinator::monitor_work, this, conf.num_threads);
        bar->wait();
        NVMMgr *mgr = get_nvm_mgr();
        mgr->recovery_free_memory(art, conf.num_threads);

        sleep(100);
        for (int i = 0; i < conf.num_threads; i++) {
            pid[i]->join();
        }
        monitor->join();

        delete art;
        delete[] pid;
    }
#endif

    void run() {
        printf("[COORDINATOR]\tStart benchmark..\n");
#ifdef INSTANT_RESTART
        if (conf.instant_restart == true) {
            printf("test instant restart\n");
            instant_restart();
            return;
        }
#endif

        if (conf.type == PART) {
            // ART
            printf("test ART---------------------\n");
            PART_ns::Tree *art = new PART_ns::Tree();
            Benchmark *benchmark = getBenchmark(conf);

            Result *results = new Result[conf.num_threads];
            memset(results, 0, sizeof(Result) * conf.num_threads);

            std::thread **pid = new std::thread *[conf.num_threads];
            bar = new boost::barrier(conf.num_threads + 1);

            std::cout << "start\n";
            PART_ns::Key *k = new PART_ns::Key();
            printf("init keys: %d\n", (int)conf.init_keys);
            // variable value
            const int val_len = conf.val_length;
            char value[val_len + 5];
            memset(value, 'a', val_len);
            value[val_len] = 0;

            for (unsigned long i = 0; i < conf.init_keys; i++) {
                if (conf.key_type == Integer) {
                    long long kk = benchmark->nextInitIntKey();
                    //                    std::string s = std::to_string(kk);
                    //                    k->Init((char *)s.c_str(), s.size(),
                    //                    value, val_len);
                    k->Init((char *)&kk, sizeof(long long), value, val_len);
                    art->insert(k);
                } else if (conf.key_type == String) {
                    std::string s = benchmark->nextInitStrKey();
                    k->Init((char *)s.c_str(), s.size(), value, val_len);
                    art->insert(k);
                }
            }
            printf("init insert finished\n");
            if (conf.benchmark == RECOVERY_BENCH) {
                NVMMgr *mgr = get_nvm_mgr();

                timer t;
                t.start();
                mgr->recovery_free_memory(art, 1);
                t.end();
                printf("rebuild takes time: %.2lf ms\n",
                       t.duration() / 1000000.0);
                return;
            }

            for (int i = 0; i < conf.num_threads; i++) {
                pid[i] = new std::thread(&Coordinator::art_worker, this, art, i,
                                         &results[i], benchmark);
            }

            bar->wait();
            usleep(conf.duration * 1000000);
            done = 1;

            Result final_result;
            for (int i = 0; i < conf.num_threads; i++) {
                pid[i]->join();
                final_result += results[i];
                printf("[WORKER]\tworker %d result %lf\n", i,
                       results[i].throughput);
            }

            printf("[COORDINATOR]\tFinish benchmark..\n");
            printf("[RESULT]\ttotal throughput: %.3lf Mtps, %d threads, %s, "
                   "%s, benchmark %d, zipfian %.2lf, rr is %d, read latency is "
                   "%.3lf ns， read op count is %d, help count is %d, "
                   "writecount is %d\n",
                   (double)final_result.throughput / 1000000.0 / conf.duration,
                   conf.num_threads, (conf.type == PART) ? "ART" : "FF",
                   (conf.key_type == Integer) ? "Int" : "Str", conf.benchmark,
                   (conf.workload == RANDOM) ? 0 : conf.skewness,
                   conf.read_ratio, final_result.total / final_result.count,
                   final_result.count, final_result.helpcount,
                   final_result.writecount);

            delete art;
            delete[] pid;
            delete[] results;
        } else if (conf.type == FAST_FAIR) {
            // FAST_FAIR
            printf("test FAST_FAIR---------------------\n");
#ifdef USE_PMDK

#ifdef ACMA
            fastfair::btree *bt = new fastfair::btree();
            std::cout << "[FF]\tcreate fastfair with DCMM\n";
#else
            fastfair::init_pmem();
            fastfair::btree *bt =
                new (fastfair::allocate(sizeof(fastfair::btree)))
                    fastfair::btree();
            std::cout << "[FF]\tPM create tree\n";
#endif

#else
            fastfair::btree *bt = new fastfair::btree();
            std::cout << "[FF]\tmemory create tree\n";
#endif
            Benchmark *benchmark = getBenchmark(conf);

            Result *results = new Result[conf.num_threads];
            memset(results, 0, sizeof(Result) * conf.num_threads);

            std::thread **pid = new std::thread *[conf.num_threads];
            bar = new boost::barrier(conf.num_threads + 1);
            printf("init keys: %d\n", (int)conf.init_keys);

            const int val_len = conf.val_length;
            char value[val_len + 5];
            memset(value, 'a', val_len);
            value[val_len] = 0;
            for (unsigned long i = 0; i < conf.init_keys; i++) {
                if (conf.key_type == Integer) {
                    long kk = benchmark->nextInitIntKey();
                    bt->btree_insert(kk, value);
                    //                    std::cout << "insert key " << kk <<
                    //                    "id: " << i << "\n";
                } else if (conf.key_type == String) {
                    std::string s = benchmark->nextInitStrKey();
                    bt->btree_insert((char *)s.c_str(), value);
                }
            }
            printf("init insert finished\n");

            for (int i = 0; i < conf.num_threads; i++) {
                pid[i] = new std::thread(&Coordinator::ff_worker, this, bt, i,
                                         &results[i], benchmark);
            }

            bar->wait();
            usleep(conf.duration * 1000000);
            done = 1;

            Result final_result;
            for (int i = 0; i < conf.num_threads; i++) {
                pid[i]->join();
                final_result += results[i];
                printf("[WORKER]\tworker %d result %lf\n", i,
                       results[i].throughput);
            }

            printf("[COORDINATOR]\tFinish benchmark..\n");
            printf("[RESULT]\ttotal throughput: %.3lf Mtps, %d threads, %s, "
                   "%s, benchmark %d, zipfian %.2lf, rr is %d\n",
                   (double)final_result.throughput / 1000000.0 / conf.duration,
                   conf.num_threads, (conf.type == PART) ? "ART" : "FF",
                   (conf.key_type == Integer) ? "Int" : "Str", conf.benchmark,
                   (conf.workload == RANDOM) ? 0 : conf.skewness,
                   conf.read_ratio);

            delete[] pid;
            delete[] results;
        } else if (conf.type == SKIPLIST) {
            printf("test skiplist\n");
#ifdef ACMA
#else

            skiplist::init_pmem();
#endif
            skiplist::skiplist_t *sl = skiplist::new_skiplist();
            printf("skiplist create\n");

            Benchmark *benchmark = getBenchmark(conf);

            Result *results = new Result[conf.num_threads];
            memset(results, 0, sizeof(Result) * conf.num_threads);

            std::thread **pid = new std::thread *[conf.num_threads];
            bar = new boost::barrier(conf.num_threads + 1);
            printf("init keys: %d\n", (int)conf.init_keys);

            const int val_len = conf.val_length;
            char value[val_len + 5];
            memset(value, 'a', val_len);
            value[val_len] = 0;

            for (unsigned long i = 0; i < conf.init_keys; i++) {
                long long kk;
                std::string s;
                if (conf.key_type == Integer) {
                    kk = benchmark->nextInitIntKey();
                    //                    std::cout << "insert key " << kk <<
                    //                    "id: " << i << "\n";
                } else if (conf.key_type == String) {
                    s = benchmark->nextInitStrKey();
                }
#ifdef VARIABLE_LENGTH
                skiplist::skiplist_insert(sl, (char *)s.c_str(), value);
#else
                skiplist::skiplist_insert(sl, kk, kk);
#endif
            }
            printf("init insert finished\n");

            for (int i = 0; i < conf.num_threads; i++) {
                pid[i] = new std::thread(&Coordinator::sl_worker, this, sl, i,
                                         &results[i], benchmark);
            }

            bar->wait();
            usleep(conf.duration * 1000000);
            done = 1;

            Result final_result;
            for (int i = 0; i < conf.num_threads; i++) {
                pid[i]->join();
                final_result += results[i];
                printf("[WORKER]\tworker %d result %lf\n", i,
                       results[i].throughput);
            }

            printf("[COORDINATOR]\tFinish benchmark..\n");
            printf("[RESULT]\ttotal throughput: %.3lf Mtps, %d threads, %s, "
                   "%s, benchmark %d, zipfian %.2lf, rr is %d\n",
                   (double)final_result.throughput / 1000000.0 / conf.duration,
                   conf.num_threads, "SL",
                   (conf.key_type == Integer) ? "Int" : "Str", conf.benchmark,
                   (conf.workload == RANDOM) ? 0 : conf.skewness,
                   conf.read_ratio);

            delete[] pid;
            delete[] results;
        }

#ifdef PERF_LATENCY
        printf("update latency: \t");
        final_result /= conf.num_threads;
        for (int i = 0; i < 3; i++) {
            printf("%lf, ", final_result.update_latency_breaks[i]);
        }
        printf("%lf\n", final_result.update_latency_breaks[0] -
                            final_result.update_latency_breaks[1] -
                            final_result.update_latency_breaks[2]);
        printf("find latency: ");
        printf("%lf, %lf, 0, %lf\t",
               final_result.find_latency_breaks[0] +
                   final_result.find_latency_breaks[1],
               final_result.find_latency_breaks[0],
               final_result.find_latency_breaks[1]);
        printf("\n");
#endif // PERF_LATENCY
    }

  private:
    Config conf __attribute__((aligned(64)));
    volatile int done __attribute__((aligned(64))) = 0;
    boost::barrier *bar __attribute__((aligned(64))) = 0;
};

#endif
