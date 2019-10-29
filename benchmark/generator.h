#ifndef GENERATOR_H
#define GENERATOR_H

#include <iostream>
#include <fstream>
#include <map>
#include <mutex>
#include <cstdio>
#include <unistd.h>
#include <atomic>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <bits/stdc++.h>
#include "config.h"

/*
 * Fast random number, using eran48/nrand48; All the functions work by generating
 * a sequence of 48-bit integers, X[i], accourding to the liner congruential formula:
 * 		Xn+1 = (aXn + c) mod m;  where n >= 0
 * If you want to generate the same sequence again, you can call "reset" function.
 */
class RandomFunc {
    unsigned short seed[3];
    unsigned short seed2[3];
    unsigned short inital[3];
    unsigned short inital2[3];
public:
    RandomFunc() {
        for (int i = 0; i < 3; i++) {
            inital[i] = seed[i] = rand();
            inital2[i] = seed2[i] = rand();
        }
    }

    int randomInt() {
        return nrand48(seed) ^ nrand48(seed2);
    }

    double randomDouble() {
        return erand48(seed) * erand48(seed2);
    }

    void setSeed(unsigned short newseed[3]) {
        memcpy(seed, newseed, sizeof(unsigned short) * 3);
    }

    void reset() {
        memcpy(seed, inital, sizeof(unsigned short) * 3);
        memcpy(seed2, inital2, sizeof(unsigned short) * 3);
    }

    long long Next() {
        return randomInt();
    }

    std::string NextStr() {
#ifdef VARIABLE_LENGTH
        int len = randomInt() % 10 + 5;
#else
        int len = sizeof(long long);
#endif
        std::string res = "";
        for (int i = 0; i < len; i++) {
            char c = randomInt() % 94 + 33;
            res += c;
        }
        return res;
    }
}__attribute__((aligned(64)));

class ZipfianFunc {
    double *zipfs;
    RandomFunc rdm;
    int size;

    void init(double s, int inital);

public:
    ZipfianFunc(double s, int inital = (1 << 20));

    ~ZipfianFunc() {
        delete zipfs;
    }

    int randomInt();
} __attribute__((aligned(64)));


class WorkloadGenerator {
public:
    static const int data_size = (1 << 25);
    int wl_int[data_size];
    std::string wl_str[data_size];

    uint64_t next_int[max_thread_num];
    uint64_t next_str[max_thread_num];

    static std::string get_file_name_int() {
        return "/tmp/random_int_data";
    }

    static std::string get_file_name_str() {
        return "/tmp/random_str_data";
    }

public:
    WorkloadGenerator() {
        for (int i = 0; i < max_thread_num; i++) {
            next_int[i] = next_str[i] = 0;
        }
        std::string fn_int = get_file_name_int();
        std::string fn_str = get_file_name_str();
        if (access(fn_int.c_str(), 0)) {
            std::cout << fn_int << " not exist, generate it now\n";
            RandomFunc rdm;
            std::ofstream myfile;
            myfile.open(fn_int, std::ios::out);
            for (unsigned long long i = 0; i < data_size; i++) {

                long long d = rdm.Next();
                myfile << d << "\n";
            }
            myfile.close();
        }
        if (access(fn_str.c_str(), 0)) {
            std::cout << fn_str << " not exist, generate it now\n";
            RandomFunc rdm;
            std::ofstream myfile;
            myfile.open(fn_str, std::ios::out);
            for (unsigned long long i = 0; i < data_size; i++) {

                std::string s = rdm.NextStr();
                myfile << s << "\n";
            }
            myfile.close();
        }

        std::cout << "start to load data\n";
        std::ifstream fint, fstr;
        fint.open(fn_int, std::ios::in);
        fstr.open(fn_str, std::ios::in);
        for (int i = 0; i < data_size; i++) {
            fint >> wl_int[i];
            fstr >> wl_str[i];
        }
        fint.close();
        fstr.close();
        std::cout << "load data successfully\n";
    }

    virtual ~WorkloadGenerator() {}

    virtual long long NextInt(int tid) {
        int index = next_int[tid] % data_size;
        next_int[tid]++;
        return wl_int[index];
    }

    virtual std::string NextStr(int tid) {
        int index = next_str[tid] % data_size;
        next_str[tid]++;
        return wl_str[index];
    }
};


class ZipfGenerator : public WorkloadGenerator {
    int zipfindex[data_size];

public:
    ZipfGenerator(double s, int initial = (1 << 20));

    long long NextInt(int tid) {
        int index = next_int[tid] % data_size;
        next_int[tid]++;
        return wl_int[zipfindex[index]];
    }

    std::string NextStr(int tid) {
        int index = next_str[tid] % data_size;
        next_str[tid]++;
        return wl_str[zipfindex[index]];
    }
};

class RandomGenerator : public WorkloadGenerator {
    RandomFunc rdm[max_thread_num];
public:
    RandomGenerator() {}

    long long NextInt(int tid) {
        int index = rdm[tid].Next() % data_size;
        return wl_int[index];
    }

    std::string NextStr(int tid) {
        int index = rdm[tid].Next() % data_size;
        return wl_str[index];
    }
};

class SequenceGenerator : public WorkloadGenerator {
    int size;
    std::atomic<int> next;
public:
    SequenceGenerator(int size) : size(size) {
        next.store(0);
    }

    long long NextInt() {
        return next.fetch_add(1);
    }
} __attribute__((aligned(64)));

#endif // GENERATOR_H