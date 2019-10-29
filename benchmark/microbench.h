#ifndef _MICRO_BEHCN_H
#define _MICRO_BEHCN_H

#include <assert.h>
#include <utility>
#include "config.h"
#include "util.h"

enum OperationType {
    INSERT,
    REMOVE,
    UPDATE,
    GET,
    SCAN,
    MIXED,
    _OpreationTypeNumber
};

template<typename T>
inline void swap(T &a, T &b) {
    T tmp = a;
    a = b;
    b = tmp;
}

long long *random_shuffle(int s) {
    long long *x = new long long[s];
    for (int i = 0; i < s; i++) {
        x[i] = i;
    }
    for (int i = 0; i < s; i++) {
        swap(x[i], x[random() % s]);
    }
    return x;
}

class Benchmark {
public:
    WorkloadGenerator *workload;
    long long key_range;
    long long init_key;
    long long *x;
    Config _conf;

    Benchmark(Config &conf) : init_key(0), _conf(conf) {
        if (conf.workload == RANDOM) {
            workload = new RandomGenerator();
        } else {
            workload = new ZipfGenerator(conf.skewness, conf.init_keys);
        }

        x = NULL;
    }

    virtual ~Benchmark() {
        if (x != NULL) delete[] x;
    }

    virtual std::pair<OperationType, long long> nextIntOperation(int tid) {
        return std::make_pair(INSERT, workload->NextInt(tid));
    }

    virtual std::pair<OperationType, std::string> nextStrOperation(int tid) {
        return std::make_pair(INSERT, workload->NextStr(tid));
    }

    virtual long long nextInitIntKey() {
        if (x == NULL) x = random_shuffle(_conf.init_keys);
        return x[init_key++ % _conf.init_keys];
        // return init_key++ % _conf.init_keys;
    }

    virtual std::string nextInitStrKey() {
        return workload->NextStr(0);
    }
};

class ReadOnlyBench : public Benchmark {
public:
    ReadOnlyBench(Config &conf) : Benchmark(conf) {}

    std::pair<OperationType, long long> nextIntOperation(int tid) {
        long long d = workload->NextInt(tid);
        return std::make_pair(GET, d % _conf.init_keys);
    }

    std::pair<OperationType, std::string> nextStrOperation(int tid) {
        std::string s = workload->NextStr(tid);
        return std::make_pair(GET, s);
    }
};

class InsertOnlyBench : public Benchmark {
    RandomFunc rdm[max_thread_num];

public:
    InsertOnlyBench(Config &conf) : Benchmark(conf) {

    }

    std::pair<OperationType, long long> nextIntOperation(int tid) {
        long long d = workload->NextInt(tid) % _conf.init_keys;
#ifdef INSERT_DUP
        long long x = 1;
#else
        long long x = rdm[tid].randomInt() % 128;
#endif

        return std::make_pair(INSERT, d * x);
    }

    std::pair<OperationType, std::string> nextStrOperation(int tid) {
        std::string s = workload->NextStr(tid);
        return std::make_pair(INSERT, s);
    }

#ifndef INSERT_DUP

    long long nextInitIntKey() { return 128 * Benchmark::nextInitIntKey(); }

#endif

};

class UpdateOnlyBench : public Benchmark {
public:
    UpdateOnlyBench(Config &conf) : Benchmark(conf) {}

    OperationType nextOp() { return UPDATE; }

    std::pair<OperationType, long long> nextIntOperation(int tid) {
        long long d = workload->NextInt(tid) % _conf.init_keys;
        return std::make_pair(UPDATE, d);
    }

    std::pair<OperationType, std::string> nextStrOperation(int tid) {
        std::string s = workload->NextStr(tid);
        return std::make_pair(UPDATE, s);
    }
};

class DeleteOnlyBench : public Benchmark {
public:
    DeleteOnlyBench(Config &conf) : Benchmark(conf) {}

    std::pair<OperationType, long long> nextIntOperation(int tid) {
        long long d = workload->NextInt(tid) % _conf.init_keys;
        return std::make_pair(REMOVE, d);
    }

    std::pair<OperationType, std::string> nextStrOperation(int tid) {
        std::string s = workload->NextStr(tid);
        return std::make_pair(REMOVE, s);
    }
};

class MixedBench : public Benchmark {
    int round;
    long long key;

public:
    MixedBench(Config &conf) : Benchmark(conf) {}

    std::pair<OperationType, long long> nextIntOperation(int tid) {
        std::pair<OperationType, long long> result;
        long long _key = workload->NextInt(tid) % _conf.init_keys;
        switch (round) {
            case 0:
                key = workload->NextInt(tid) % _conf.init_keys;
                result = std::make_pair(REMOVE, key);
                break;
            case 1:
                result = std::make_pair(INSERT, key);
                break;
            case 2:
                result = std::make_pair(UPDATE, _key);
                break;
            case 3:
                result = std::make_pair(GET, _key);
                break;
            default:
                assert(0);
        }
        round++;
        round %= 4;
        return result;
    }

    std::pair<OperationType, std::string> nextStrOperation(int tid) {
        std::pair<OperationType, std::string> result;
        std::string _key = workload->NextStr(tid);
        switch (round) {
            case 0:
                result = std::make_pair(REMOVE, _key);
                break;
            case 1:
                result = std::make_pair(INSERT, _key);
                break;
            case 2:
                result = std::make_pair(UPDATE, _key);
                break;
            case 3:
                result = std::make_pair(GET, _key);
                break;
            default:
                assert(0);
        }
        round++;
        round %= 4;
        return result;
    }
};

class ScanBench : public Benchmark {
public:
    ScanBench(Config &conf) : Benchmark(conf) {}

    std::pair<OperationType, long long> nextIntOperation(int tid) {
        long long d = workload->NextInt(tid) % _conf.init_keys;
        return std::make_pair(SCAN, d);
    }

    std::pair<OperationType, std::string> nextStrOperation(int tid) {
        std::string s = workload->NextStr(tid);
        return std::make_pair(SCAN, s);
    }
};

class YSCBA : public Benchmark {
public:
    //	readRate = 0.5;
    //	writeRate = 0.5;
    int read_ratio = 50;

    RandomFunc rdm[max_thread_num];

    YSCBA(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation(int tid) {
        int k = rdm[tid].randomInt() % 100;
        if (k > read_ratio) {
            return std::make_pair(UPDATE, workload->NextInt(tid) % _conf.init_keys);
        } else {
            return std::make_pair(GET, workload->NextInt(tid) % _conf.init_keys);
        }
    }

    virtual std::pair<OperationType, std::string> nextStrOperation(int tid) {
        int k = rdm[tid].randomInt() % 100;
        if (k > read_ratio) {
            return std::make_pair(UPDATE, workload->NextStr(tid));
        } else {
            return std::make_pair(GET, workload->NextStr(tid));
        }
    }
};

class YSCBB : public Benchmark {
public:
    //	readRate = 0.95;
    //	writeRate = 0.05;
    int read_ratio = 95;
    RandomFunc rdm[max_thread_num];

    YSCBB(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation(int tid) {
        int k = rdm[tid].randomInt() % 100;
        if (k < read_ratio) {
            return std::make_pair(GET, workload->NextInt(tid) % _conf.init_keys);
        } else {
            return std::make_pair(UPDATE, workload->NextInt(tid) % _conf.init_keys);
        }
    }

    virtual std::pair<OperationType, std::string> nextStrOperation(int tid) {
        int k = rdm[tid].randomInt() % 100;
        if (k > read_ratio) {
            return std::make_pair(UPDATE, workload->NextStr(tid));
        } else {
            return std::make_pair(GET, workload->NextStr(tid));
        }
    }
};

class YSCBC : public Benchmark {
public:
    YSCBC(Config &conf) : Benchmark(conf) {}

    virtual std::pair<OperationType, long long> nextIntOperation(int tid) {
        return std::make_pair(GET, workload->NextInt(tid) % _conf.init_keys);
    }

    virtual std::pair<OperationType, std::string> nextStrOperation(int tid) {
        return std::make_pair(GET, workload->NextStr(tid));
    }
};

class YSCBD : public Benchmark {
public:
    YSCBD(Config &conf) : Benchmark(conf) {}

    OperationType nextOp() { return GET; }
};

class YSCBE : public Benchmark {
public:
    YSCBE(Config &conf) : Benchmark(conf) {}

    OperationType nextOp() { return GET; }
};

#endif
