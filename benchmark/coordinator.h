#ifndef coordinator_h
#define coordinator_h

#include "Key.h"
#include "N.h"
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

template <typename K, typename V, int size> class Coordinator {
    class Result {
      public:
        double throughput;
        double update_latency_breaks[3];
        double find_latency_breaks[3];

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
        CPU_SET(core_id, &cpuset);

        pthread_t current_thread = pthread_self();
        return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t),
                                      &cpuset);
    }

    void art_worker(PART_ns::Tree *art, int workerid, Result *result,
                    Benchmark *b) {
        //            Benchmark *benchmark = getBenchmark(conf);
        Benchmark *benchmark = b;
        printf("[WORKER]\thello, I am worker %d\n", workerid);
        NVMMgr_ns::register_threadinfo();
        stick_this_thread_to_core(workerid);
        bar->wait();

        auto tinfo = art->getThreadInfo();

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
        while (done == 0) {

            V result = 1;

            OperationType op;
            long long d;
            std::string s;

            if (conf.key_type == Integer) {
                auto next_operation = benchmark->nextIntOperation(workerid);
                op = next_operation.first;
                d = next_operation.second;
                k->Init(d, sizeof(uint64_t), d);
            } else if (conf.key_type == String) {
                auto next_operation = benchmark->nextStrOperation(workerid);
                op = next_operation.first;
                s = next_operation.second;
                k->Init((char *)s.c_str(), sizeof(uint64_t), (char *)s.c_str());
            }

            cpuCycleTimer t;
            if (conf.latency_test) {
                t.start();
            }

            PART_ns::Tree::OperationResults res;
            switch (op) {
            case UPDATE:
                // result = bt->update(d, d, update_latency_breaks);
                // if (tx % 100 == 0) {
                //     sync_latency(total_update_latency_breaks,
                //                  update_latency_breaks, update_count);
                // }
                // break;
            case INSERT:
                // printf("[%d] start insert %lld\n", workerid, d);
                res = art->insert(k, tinfo);
                if (res == PART_ns::Tree::OperationResults::Success) {
                    count++;
                }
                break;
            case REMOVE:

                art->remove(k, tinfo);
                break;
            case GET:
                art->lookup(k, tinfo);

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
        Benchmark *benchmark = b;
        printf("[WORKER]\thello, I am worker %d\n", workerid);
        register_threadinfo();
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
        while (done == 0) {

            V result = 1;
            OperationType op;
            long long d;
            std::string s;

            if (conf.key_type == Integer) {
                auto next_operation = benchmark->nextIntOperation(workerid);

                op = next_operation.first;
                d = next_operation.second;
            } else if (conf.key_type == String) {
                auto next_operation = benchmark->nextStrOperation(workerid);

                op = next_operation.first;
                s = next_operation.second;
            }

            cpuCycleTimer t;
            if (conf.latency_test) {
                t.start();
            }

            switch (op) {
            case UPDATE:
                // result = bt->update(d, d, update_latency_breaks);
                // if (tx % 100 == 0) {
                //     sync_latency(total_update_latency_breaks,
                //                  update_latency_breaks, update_count);
                // }
                // break;
            case REMOVE:
            case INSERT:
                // printf("[%d] start insert %lld\n", workerid, d);

                if (conf.key_type == Integer) {
                    bt->btree_insert(d, (char *)d);
                } else if (conf.key_type == String) {
                    bt->btree_insert((char *)s.c_str(), (char *)s.c_str());
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

        unregister_threadinfo();

        printf("[WORKER]\tworker %d finished\n", workerid);
    }

    void run() {
        printf("[COORDINATOR]\tStart benchmark..\n");

        init_nvm_mgr();

        if (conf.type == PART) {
            // ART
            printf("test ART---------------------\n");
            PART_ns::Tree *art = new PART_ns::Tree();
            register_threadinfo();
            Benchmark *benchmark = getBenchmark(conf);

            Result *results = new Result[conf.num_threads];
            memset(results, 0, sizeof(Result) * conf.num_threads);

            std::thread **pid = new std::thread *[conf.num_threads];
            bar = new boost::barrier(conf.num_threads + 1);

            auto tinfo = art->getThreadInfo();

            PART_ns::Key *k = new PART_ns::Key();
            for (unsigned long i = 0; i < conf.init_keys; i++) {
                if (conf.key_type == Integer) {
                    long kk = benchmark->nextInitIntKey();
                    k->Init(kk, sizeof(uint64_t), kk);
                    art->insert(k, tinfo);
                } else if (conf.key_type == String) {
                    std::string s = benchmark->nextInitStrKey();
                    k->Init((char *)s.c_str(), sizeof(uint64_t),
                            (char *)s.c_str());
                    art->insert(k, tinfo);
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
            printf("[COORDINATOR]\ttotal throughput: %.3lf Mtps\n",
                   (double)final_result.throughput / 1000000.0 / conf.duration);

            delete art;
            delete[] pid;
            delete[] results;
        } else if (conf.type == FAST_FAIR) {
            // FAST_FAIR

            printf("test FAST_FAIR---------------------\n");
            fastfair::btree *bt = new fastfair::btree();
            register_threadinfo();
            Benchmark *benchmark = getBenchmark(conf);

            Result *results = new Result[conf.num_threads];
            memset(results, 0, sizeof(Result) * conf.num_threads);

            std::thread **pid = new std::thread *[conf.num_threads];
            bar = new boost::barrier(conf.num_threads + 1);

            for (unsigned long i = 0; i < conf.init_keys; i++) {
                if (conf.key_type == Integer) {
                    long kk = benchmark->nextInitIntKey();
                    bt->btree_insert(kk, (char *)kk);
                } else if (conf.key_type == String) {
                    std::string s = benchmark->nextInitStrKey();
                    bt->btree_insert((char *)s.c_str(), (char *)s.c_str());
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
            printf("[COORDINATOR]\ttotal throughput: %.3lf Mtps\n",
                   (double)final_result.throughput / 1000000.0 / conf.duration);

            delete bt;
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

        unregister_threadinfo();
        close_nvm_mgr();
    }

  private:
    Config conf __attribute__((aligned(64)));
    volatile int done __attribute__((aligned(64))) = 0;
    boost::barrier *bar __attribute__((aligned(64))) = 0;
    static const size_t val_len = sizeof(uint64_t);
    static const size_t key_len = sizeof(uint64_t);
};

#endif
