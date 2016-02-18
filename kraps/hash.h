#ifndef __HASH_H__
#define __HASH_H__

#include <inttypes.h>
#include <stdlib.h>

#ifndef USE_CRC_HASH_FUNCTION
#define USE_CRC_HASH_FUNCTION 1
#endif

#ifndef ROUND_HASH_TABLE_SIZE
#define ROUND_HASH_TABLE_SIZE 0
#endif

#if USE_CRC_HASH_FUNCTION

static inline uint32_t
crc32c_sse42_u32(uint32_t data, uint32_t init_val)
{
    __asm__ volatile("crc32l %[data], %[init_val];"
                     : [init_val] "+r" (init_val)
                     : [data] "rm" (data));
    return init_val;
}

static inline uint32_t
crc32c_sse42_u64(uint64_t data, uint64_t init_val)
{
    __asm__ volatile("crc32q %[data], %[init_val];"
                     : [init_val] "+r" (init_val)
                     : [data] "rm" (data));
    return init_val;
}


inline size_t hashCode(int32_t key) { 
    return crc32c_sse42_u32((uint32_t)key, -1);
}

inline size_t hashCode(uint32_t key) { 
    return crc32c_sse42_u32(key, -1);
}

inline size_t hashCode(float const& key) { 
    return crc32c_sse42_u32(*(uint32_t*)&key, -1);
}

inline size_t hashCode(int64_t key) { 
    return crc32c_sse42_u64((uint64_t)key, -1);
}

inline size_t hashCode(uint64_t key) { 
    return crc32c_sse42_u64(key, -1);
}

inline size_t hashCode(double const& key) { 
    return crc32c_sse42_u64(*(uint64_t*)&key, -1);
}

#endif

//
// Hash function for scalar fields
//
template<class T>
inline size_t hashCode(T const& key) 
{ 
//    return key;
//    return (key >> 8) ^ (key << (sizeof(key)*8 - 8));
    return murmur_hash3_32(&key, sizeof key);
}

//
// Hash function for string fields
//
inline size_t hashCode(char const* key) 
{ 
    size_t h = 0;
    while (*key != '\0') { 
        h = h*31 + (*key++ & 0xFF);
    }
    return h;
}

inline size_t hashTableSize(size_t estimation) 
{ 
#if ROUND_HASH_TABLE_SIZE
    size_t hashSize = 1024;
    while (hashSize < estimation) { 
        hashSize <<= 1;
    }
    return hashSize;
#else
    const unsigned prime_numbers[] = {
        17,             /* 0 */
        37,             /* 1 */
        79,             /* 2 */
        163,            /* 3 */
        331,            /* 4 */
        673,            /* 5 */
        1361,           /* 6 */
        2729,           /* 7 */
        5471,           /* 8 */
        10949,          /* 9 */
        21911,          /* 10 */
        43853,          /* 11 */
        87719,          /* 12 */
        175447,         /* 13 */
        350899,         /* 14 */
        701819,         /* 15 */
        1403641,        /* 16 */
        2807303,        /* 17 */
        5614657,        /* 18 */
        11229331,       /* 19 */
        22458671,       /* 20 */
        44917381,       /* 21 */
        89834777,       /* 22 */
        179669557,      /* 23 */
        359339171,      /* 24 */
        718678369,      /* 25 */
        1437356741,     /* 26 */
        2147483647,     /* 27 */
        4294967291U     /* 28 */
    };
    int i;
    for (i = 0; prime_numbers[i] < estimation; i++);
    return prime_numbers[i];
#endif
}

#if ROUND_HASH_TABLE_SIZE
inline size_t mod_power2(size_t x, size_t y) {
    return (x ^ (x >> 21) ^ (x >> 42)) & (y - 1);
}
#define MOD(x,y)  mod_power2(x, y)
#else
#define MOD(x,y) ((x) % (y))
#endif

extern uint32_t murmur_hash3_32(const void* key, const int len);

#endif
