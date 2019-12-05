#ifndef coordinator_h
#define coordinator_h

#include "Key.h"
#include "N.h"
#include "RNTree.h"
#include "Tree.h"
#include "benchmarks.h"
#include "config.h"
#include "fast_fair.h"
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

        Result() {
            throughput = 0;
            for (int i = 0; i < 3; i++) {
                update_latency_breaks[i] = find_latency_breaks[i] = 0;
            }
        }

        void operator+=(Result &r) {
            this->throughput += r.throughput;
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

        // variable value
        const int val_len = 100;
        char value[val_len + 5];
        memset(value, 'a', val_len);

        while (done == 0) {

            V result = 1;

            OperationType op;
            long long d;
            std::string s;

            if (conf.key_type == Integer) {
                auto next_operation = benchmark->nextIntOperation();
                op = next_operation.first;
                d = next_operation.second;
                k->Init(d, sizeof(uint64_t), d);
            } else if (conf.key_type == String) {
                auto next_operation = benchmark->nextStrOperation();
                op = next_operation.first;
                s = next_operation.second;
                k->Init((char *)s.c_str(), sizeof(uint64_t), value, val_len);
            }

            cpuCycleTimer t;
            if (conf.latency_test) {
                t.start();
            }

            PART_ns::Tree::OperationResults res;
            switch (op) {
            case UPDATE:
                //                std::cout<<"update key "<<s<<"\n";
                art->update(k);
                break;
            case INSERT:
                // printf("[%d] start insert %lld\n", workerid, d);
                res = art->insert(k);
                break;
            case REMOVE:
//                art->insert(k);
                art->remove(k);
                break;
            case GET:
                //                std::cout<<"lookup key "<<s<<"\n";
                art->lookup(k);

                // if (tx % 100 == 0) {
                //     sync_latency(total_find_latency_breaks,
                //                  find_latency_breaks, find_count);
                // }
                break;
            case SCAN:
                // bt->scan(d, scan_func);
                // scan_values = 0;
                break;
            default:
                printf("not support such operation: %d\n", op);
                exit(-1);
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

        unregister_threadinfo();

        printf("[WORKER]\tworker %d finished\n", workerid);
    }

    void ff_worker(fastfair::btree *bt, int workerid, Result *result,
                   Benchmark *b) {
        //            Benchmark *benchmark = getBenchmark(conf);
        Benchmark *benchmark = getBenchmark(conf);
        printf("[WORKER]\thello, I am worker %d\n", workerid);
        stick_this_thread_to_core(workerid);
        fastfair::register_thread();
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

        const int val_len = 100;
        char value[val_len + 5];
        memset(value, 'a', val_len);
        value[val_len] = 0;
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
            case UPDATE:
                if (conf.key_type == Integer) {
                    //                    std::cout<<"insert key "<<d<<"\n";
                    bt->btree_update(d, value);
                } else if (conf.key_type == String) {
                    bt->btree_update((char *)s.c_str(), value);
                }

                break;
            case INSERT:
                // printf("[%d] start insert %lld\n", workerid, d);

                if (conf.key_type == Integer) {
                    //                    std::cout<<"insert key "<<d<<"\n";
                    bt->btree_insert(d, value);
                } else if (conf.key_type == String) {
                    bt->btree_insert((char *)s.c_str(), value);
                }

                break;
            case REMOVE:
                // first insert then remove
//                if (conf.key_type == Integer) {
//                    //                    std::cout<<"insert key "<<d<<"\n";
//                    bt->btree_insert(d, value);
//                } else if (conf.key_type == String) {
//                    bt->btree_insert((char *)s.c_str(), value);
//                }

                if (conf.key_type == Integer) {
                    //                    std::cout<<"delete key "<<d<<"\n";
                    bt->btree_delete(d);
                } else if (conf.key_type == String) {
                    //                    std::cout<<"delete key "<<s<<"\n";
                    bt->btree_delete((char *)s.c_str());
                }

                break;
            case GET:
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
            case SCAN:
                // bt->scan(d, scan_func);
                // scan_values = 0;
                break;
            default:
                printf("not support such operation: %d\n", op);
                exit(-1);
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

    void run() {
        printf("[COORDINATOR]\tStart benchmark..\n");

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
            const int val_len = 100;
            char value[val_len + 5];
            memset(value, 'a', val_len);

            for (unsigned long i = 0; i < conf.init_keys; i++) {
                if (conf.key_type == Integer) {
                    long kk = benchmark->nextInitIntKey();
                    k->Init(kk, sizeof(uint64_t), kk);
                    art->insert(k);
                } else if (conf.key_type == String) {
                    std::string s = benchmark->nextInitStrKey();
                    k->Init((char *)s.c_str(), sizeof(uint64_t), value,
                            val_len);
                    art->insert(k);
                }
            }
            printf("init insert finished\n");

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
                   "%s, benchmark %d, zipfian %.2lf\n",
                   (double)final_result.throughput / 1000000.0 / conf.duration,
                   conf.num_threads, (conf.type == PART) ? "ART" : "FF",
                   (conf.key_type == Integer) ? "Int" : "Str", conf.benchmark,(conf.workload == RANDOM) ? 0:conf.skewness);

            delete art;
            delete[] pid;
            delete[] results;
        } else if (conf.type == FAST_FAIR) {
            // FAST_FAIR
            printf("test FAST_FAIR---------------------\n");
#ifdef USE_PMDK
            fastfair::init_pmem();
            fastfair::btree *bt =
                new (fastfair::allocate(sizeof(fastfair::btree)))
                    fastfair::btree();
            std::cout << "[FF]\tPM create tree\n";
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

            const int val_len = 100;
            char value[val_len + 5];
            memset(value, 'a', val_len);
            value[val_len] = 0;
            for (unsigned long i = 0; i < conf.init_keys; i++) {
                if (conf.key_type == Integer) {
                    long kk = benchmark->nextInitIntKey();
                    bt->btree_insert(kk, value);
                                        std::cout << "insert key " << kk << "id: " << i << "\n";
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
                   "%s, benchmark %d, zipfian %.2lf\n",
                   (double)final_result.throughput / 1000000.0 / conf.duration,
                   conf.num_threads, (conf.type == PART) ? "ART" : "FF",
                   (conf.key_type == Integer) ? "Int" : "Str", conf.benchmark, (conf.workload == RANDOM) ? 0:conf.skewness);

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
