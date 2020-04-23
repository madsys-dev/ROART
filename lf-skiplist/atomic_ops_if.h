/* this file was taken from ascylib (https://github.com/LPD-EPFL/ASCYLIB) */

#ifndef _ATOMIC_OPS_IF_H_INCLUDED_
#define _ATOMIC_OPS_IF_H_INCLUDED_

#include <inttypes.h>

#ifdef __sparc__
/*
 *  sparc code
 */

#include <atomic.h>

// test-and-set uint8_t
static inline uint8_t tas_uint8(volatile uint8_t *addr) {
    uint8_t oldval;
    __asm__ __volatile__("ldstub %1,%0"
                         : "=r"(oldval), "=m"(*addr)
                         : "m"(*addr)
                         : "memory");
    return oldval;
}

// Compare-and-swap
#define CAS_PTR(a, b, c) atomic_cas_ptr(a, b, c)
#define CAS_U8(a, b, c) atomic_cas_8(a, b, c)
#define CAS_U16(a, b, c) atomic_cas_16(a, b, c)
#define CAS_U32(a, b, c) atomic_cas_32(a, b, c)
#define CAS_U64(a, b, c) atomic_cas_64(a, b, c)
// Swap
#define SWAP_PTR(a, b) atomic_swap_ptr(a, b)
#define SWAP_U8(a, b) atomic_swap_8(a, b)
#define SWAP_U16(a, b) atomic_swap_16(a, b)
#define SWAP_U32(a, b) atomic_swap_32(a, b)
#define SWAP_U64(a, b) atomic_swap_64(a, b)
// Fetch-and-increment
#define FAI_U8(a) (atomic_inc_8_nv(a) - 1)
#define FAI_U16(a) (atomic_inc_16_nv(a) - 1)
#define FAI_U32(a) (atomic_inc_32_nv(a) - 1)
#define FAI_U64(a) (atomic_inc_64_nv(a) - 1)
// Fetch-and-decrement
#define FAD_U8(a) (atomic_dec_8_nv(a, ) + 1)
#define FAD_U16(a) (atomic_dec_16_nv(a) + 1)
#define FAD_U32(a) (atomic_dec_32_nv(a) + 1)
#define FAD_U64(a) (atomic_dec_64_nv(a) + 1)
// Increment-and-fetch
#define IAF_U8(a) atomic_inc_8_nv(a)
#define IAF_U16(a) atomic_inc_16_nv(a)
#define IAF_U32(a) atomic_inc_32_nv(a)
#define IAF_U64(a) atomic_inc_64_nv(a)
// Decrement-and-fetch
#define DAF_U8(a) atomic_dec_8_nv(a)
#define DAF_U16(a) atomic_dec_16_nv(a)
#define DAF_U32(a) atomic_dec_32_nv(a)
#define DAF_U64(a) atomic_dec_64_nv(a)
// Test-and-set
#define TAS_U8(a) tas_uint8(a)
// Memory barrier
#define MEM_BARRIER                                                            \
    asm volatile("membar #LoadLoad | #LoadStore | #StoreLoad | #StoreStore");
// end of sparc code

#elif defined(__tile__)
/*
 *  Tilera code
 */
#include <arch/atomic.h>
#include <arch/cycle.h>
// atomic operations interface
// Compare-and-swap
#define CAS_PTR(a, b, c) arch_atomic_val_compare_and_exchange(a, b, c)
#define CAS_U8(a, b, c) arch_atomic_val_compare_and_exchange(a, b, c)
#define CAS_U16(a, b, c) arch_atomic_val_compare_and_exchange(a, b, c)
#define CAS_U32(a, b, c) arch_atomic_val_compare_and_exchange(a, b, c)
#define CAS_U64(a, b, c) arch_atomic_val_compare_and_exchange(a, b, c)
// Swap
#define SWAP_PTR(a, b) arch_atomic_exchange(a, b)
#define SWAP_U8(a, b) arch_atomic_exchange(a, b)
#define SWAP_U16(a, b) arch_atomic_exchange(a, b)
#define SWAP_U32(a, b) arch_atomic_exchange(a, b)
#define SWAP_U64(a, b) arch_atomic_exchange(a, b)
// Fetch-and-increment
#define FAI_U8(a) arch_atomic_increment(a)
#define FAI_U16(a) arch_atomic_increment(a)
#define FAI_U32(a) arch_atomic_increment(a)
#define FAI_U64(a) arch_atomic_increment(a)
// Fetch-and-decrement
#define FAD_U8(a) arch_atomic_decrement(a)
#define FAD_U16(a) arch_atomic_decrement(a)
#define FAD_U32(a) arch_atomic_decrement(a)
#define FAD_U64(a) arch_atomic_decrement(a)
// Increment-and-fetch
#define IAF_U8(a) (arch_atomic_increment(a) + 1)
#define IAF_U16(a) (arch_atomic_increment(a) + 1)
#define IAF_U32(a) (arch_atomic_increment(a) + 1)
#define IAF_U64(a) (arch_atomic_increment(a) + 1)
// Decrement-and-fetch
#define DAF_U8(a) (arch_atomic_decrement(a) - 1)
#define DAF_U16(a) (arch_atomic_decrement(a) - 1)
#define DAF_U32(a) (arch_atomic_decrement(a) - 1)
#define DAF_U64(a) (arch_atomic_decrement(a) - 1)
// Test-and-set
#define TAS_U8(a) arch_atomic_val_compare_and_exchange(a, 0, 0xff)
// Memory barrier
#define MEM_BARRIER arch_atomic_full_barrier()
#define LOAD_BARRIER arch_atomic_read_barrier()
#define STORE_BARRIER arch_atomic_write_barrier()

static inline void AO_nop_full(void) { MEM_BARRIER; }

#define AO_store_full(addr, val) arch_atomic_write(addr, val)
#define AO_load_full(addr) arch_atomic_access_once((*addr))
// Relax CPU
// define PAUSE cycle_relax()

// end of tilera code
#else
/*
 *  x86 code
 */

#if defined(__SSE__)
#include <xmmintrin.h>
#endif

// test-and-set uint8_t
static inline uint8_t tas_uint8(volatile uint8_t *addr) {
    uint8_t oldval;
    __asm__ __volatile__("xchgb %0,%1"
                         : "=q"(oldval), "=m"(*addr)
                         : "0"((unsigned char)0xff), "m"(*addr)
                         : "memory");
    return (uint8_t)oldval;
}

// atomic operations interface
// Compare-and-swap
#define CAS_PTR(a, b, c) __sync_val_compare_and_swap(a, b, c)
#define CAS_U8(a, b, c) __sync_val_compare_and_swap(a, b, c)
#define CAS_U16(a, b, c) __sync_val_compare_and_swap(a, b, c)
#define CAS_U32(a, b, c) __sync_val_compare_and_swap(a, b, c)
#define CAS_U64(a, b, c) __sync_val_compare_and_swap(a, b, c)
// Swap
#define SWAP_PTR(a, b) swap_pointer(a, b)
#define SWAP_U8(a, b) swap_uint8(a, b)
#define SWAP_U16(a, b) swap_uint16(a, b)
#define SWAP_U32(a, b) swap_uint32(a, b)
#define SWAP_U64(a, b) swap_uint64(a, b)
// Fetch-and-increment
#define FAI_U8(a) __sync_fetch_and_add(a, 1)
#define FAI_U16(a) __sync_fetch_and_add(a, 1)
#define FAI_U32(a) __sync_fetch_and_add(a, 1)
#define FAI_U64(a) __sync_fetch_and_add(a, 1)
// Fetch-and-decrement
#define FAD_U8(a) __sync_fetch_and_sub(a, 1)
#define FAD_U16(a) __sync_fetch_and_sub(a, 1)
#define FAD_U32(a) __sync_fetch_and_sub(a, 1)
#define FAD_U64(a) __sync_fetch_and_sub(a, 1)
// Increment-and-fetch
#define IAF_U8(a) __sync_add_and_fetch(a, 1)
#define IAF_U16(a) __sync_add_and_fetch(a, 1)
#define IAF_U32(a) __sync_add_and_fetch(a, 1)
#define IAF_U64(a) __sync_add_and_fetch(a, 1)
// Decrement-and-fetch
#define DAF_U8(a) __sync_sub_and_fetch(a, 1)
#define DAF_U16(a) __sync_sub_and_fetch(a, 1)
#define DAF_U32(a) __sync_sub_and_fetch(a, 1)
#define DAF_U64(a) __sync_sub_and_fetch(a, 1)
// Test-and-set
#define TAS_U8(a) tas_uint8(a)
// Memory barrier
/* #define MEM_BARRIER __sync_synchronize() */
#define MEM_BARRIER // nop on the opteron for these benchmarks
// Relax CPU
//#define PAUSE _mm_pause()

/*End of x86 code*/
#endif

/* start --generic code */

#define CAS_U64_bool(addr, old, new) (old == CAS_U64(addr, old, new))

/* static inline uint8_t */
/* CAS_U64_bool(volatile AO_t* addr, AO_t old, AO_t new) */
/* { */
/*   return (old == CAS_U64(addr, old, new)); */
/* } */

/* #define ATOMIC_CAS_MB(a, e, v)          (AO_compare_and_swap_full((volatile
 * AO_t *)(a), (AO_t)(e), (AO_t)(v))) */
/* #define ATOMIC_FETCH_AND_INC_FULL(a)    (AO_fetch_and_add1_full((volatile
 * AO_t *)(a))) */

#define ATOMIC_CAS_MB(a, e, v)                                                 \
    CAS_U64_bool((volatile AO_t *)(a), (AO_t)(e), (AO_t)(v))
#define ATOMIC_FETCH_AND_INC_FULL(a) FAI_U32(a)

/* end -- generic code */

#endif
