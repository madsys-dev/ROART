#ifndef GENERATOR_H
#define GENERATOR_H

#include <atomic>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <unistd.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "config.h"
#include <bits/stdc++.h>

class WorkloadGenerator {
  public:
    virtual long long Next() = 0;
};

/*
 * Fast random number generator, using eran48/nrand48; All the functions work by
 * generating a sequence of 48-bit integers, X[i], accourding to the liner
 * congruential formula: Xn+1 = (aXn + c) mod m;  where n >= 0 If you want to
 * generate the same sequence again, you can call "reset" function.
 */
class RandomGenerator : public WorkloadGenerator {
    unsigned short seed[3];
    unsigned short seed2[3];
    unsigned short inital[3];
    unsigned short inital2[3];
    int key_len;

  public:
    RandomGenerator(int key_len_ = 8) : key_len(key_len_) {
        for (int i = 0; i < 3; i++) {
            inital[i] = seed[i] = rand();
            inital2[i] = seed2[i] = rand();
        }
    }
    int randomInt() { return nrand48(seed) ^ nrand48(seed2); }

    std::string RandomStr() {
        int len = randomInt() % 10  + 5; // len 8-16 // 5-15
//        int len = randomInt() % 125  + 4;
        std::string res = "";
        for (int i = 0; i < len; i++) {
            //            char c = randomInt() % 10 + '0'; // 0-9
            char c = randomInt() % 94 + 33;
//            char c = randomInt() % 127 + 1;
            res += c;
        }
        return res;
    }

    double randomDouble() { return erand48(seed) * erand48(seed2); }
    void setSeed(unsigned short newseed[3]) {
        memcpy(seed, newseed, sizeof(unsigned short) * 3);
    }
    void reset() {
        memcpy(seed, inital, sizeof(unsigned short) * 3);
        memcpy(seed2, inital2, sizeof(unsigned short) * 3);
    }
    long long Next() { return randomInt(); }
} __attribute__((aligned(64)));

class WorkloadFile {
    int bufsize;
    int *buffer;

  public:
    WorkloadFile(std::string filename);
    int get(uint32_t c) { return buffer[c % bufsize]; }
};

class ZipfGenerator {
    double *zipfs;
    RandomGenerator rdm;
    int size;

    void init(double s, int inital);

  public:
    ZipfGenerator(double s, int inital = (1 << 20));
    ~ZipfGenerator() { delete zipfs; }
    int randomInt();
} __attribute__((aligned(64)));

class ZipfWrapper : public WorkloadGenerator {
    static std::mutex gen_mtx;
    static std::map<std::string, WorkloadFile *> wf_map;

    uint32_t cursor;
    WorkloadFile *wf;

    static std::string get_file_name(double s) {
        std::stringstream ss;
        ss << (int)((s + 0.001) * 100);
        return "/tmp/" + ss.str() + "zipfian_data";
    }

  public:
    ZipfWrapper(double s, int inital = (1 << 20));
    long long Next() { return wf->get(cursor++); }
};

class DataSet {
  public:
    std::string *wl_str;
    int data_size;
    int key_len;
    int emailkey;

    static std::string get_file_name_str(int len) {
        std::stringstream ss;
        ss << len;
        return "/tmp/random_str_data" + ss.str();
    }

    DataSet(int size, int key_length, int email);
};

#endif // GENERATOR_H