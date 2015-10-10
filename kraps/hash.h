#ifndef __HASH_H__
#define __HASH_H__

#include <inttypes.h>
#include <stdlib.h>

#ifndef USE_CRC_HASH_FUNCTION
#define USE_CRC_HASH_FUNCTION 1
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
inline size_t hashCode(T const& key) { 
//    return key;
//    return (key >> 8) ^ (key << (sizeof(key)*8 - 8));
    return murmur_hash3_32(&key, sizeof key);
}

//
// Hash function for string fields
//
inline size_t hashCode(char const* key) { 
    size_t h = 0;
    while (*key != '\0') { 
        h = h*31 + (*key++ & 0xFF);
    }
    return h;
}

#endif
