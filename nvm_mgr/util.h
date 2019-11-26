#ifndef UTIL_H
#define UTIL_H

#include "generator.h"
#include <stdio.h>
#include <stdlib.h>

#define MOR __ATOMIC_SEQ_CST
#define ATM_GET(var) (var)
#define ATM_LOAD(var, val) __atomic_load(&(var), &(val), MOR)
#define ATM_STORE(var, val) __atomic_store(&(var), &(val), MOR)
#define ATM_CAS(var, exp, val)                                                 \
    __atomic_compare_exchange(&(var), &(exp), &(val), false, MOR, MOR)
#define ATM_CAS_PTR(var, exp, val)                                             \
    __atomic_compare_exchange_n((var), &(exp), (val), false, MOR, MOR)
#define ATM_FETCH_ADD(var, val) __atomic_fetch_add(&(var), (val), MOR)
#define ATM_FETCH_SUB(var, val) __atomic_fetch_sub(&(var), (val), MOR)

/**
 * CPU cycles
 */

static __always_inline uint64_t rdtsc() {
    unsigned int lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
}

#define CPU_FREQUENCY 2.2 // 2.2 GHZ

#define PM_FENCE()                                                             \
    do {                                                                       \
        if (getenv("NVM_LATENCY")) {                                           \
            long long t1 = rdtsc();                                            \
            long long duration = CPU_FREQUENCY * atoi(getenv("NVM_LATENCY"));  \
            while (rdtsc() - t1 < duration) {                                  \
                asm("pause");                                                  \
            }                                                                  \
        }                                                                      \
    } while (0)

#define asm_clwb(addr)                                                         \
    asm volatile(".byte 0x66; xsaveopt %0" : "+m"(*(volatile char *)addr));

#define asm_clflush(addr)                                                      \
    ({ __asm__ __volatile__("clflush %0" : : "m"(*addr)); })

// static inline void asm_mfence(void)
#define asm_mfence()                                                           \
    ({                                                                         \
        PM_FENCE();                                                            \
        __asm__ __volatile__("mfence");                                        \
    })

// static inline void asm_sfence(void)
#define asm_sfence()                                                           \
    ({                                                                         \
        PM_FENCE();                                                            \
        __asm__ __volatile__("sfence");                                        \
    })

#define CACHE_ALIGN 64

static void flush_data(void *addr, size_t len) {
    char *end = (char *)(addr) + len;
    char *ptr = (char *)((unsigned long)addr & ~(CACHE_ALIGN - 1));
    for (; ptr < end; ptr += CACHE_ALIGN)
        asm_clwb(ptr);
    asm_mfence();
}

// prefetch instruction
//
inline void prefetch(const void *ptr) {
#ifdef NOPREFETCH
    (void)ptr;
#else
    typedef struct {
        char x[CACHE_ALIGN];
    } cacheline_t;
    asm volatile("prefetcht0 %0" : : "m"(*(const cacheline_t *)ptr));
#endif
}

/**
 * @brief find first zero bit in a word
 * @details
 *
 * @param long the word to find
 * @return the location of the first zero bit
 */

static __always_inline unsigned long ffz(unsigned long word) {
    asm("rep; bsf %1,%0" : "=r"(word) : "r"(~word));
    return word;
}
#endif