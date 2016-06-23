#pragma once

#include <inttypes.h>
#include <stdlib.h>

#ifndef USE_CRC_HASH_FUNCTION
#define USE_CRC_HASH_FUNCTION 1
#endif

extern uint32_t murmur_hash3_32(const void* key, const int len);

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


template<class T>
class BlockAllocator
{
    enum { BlockSize=1024 };
    struct Block {
        Block* next;
        T data[BlockSize];

        Block(Block* chain) : next(chain) {}
    };
    size_t size;
    Block* used;
    Block* free;
    Block* lastUsed;
    Block* lastFree;
    
  public:
    void reset() {
        if (used != NULL) {
            if (free != NULL) {
                lastFree->next = used;
            } else {
                free = used;
            }
            lastFree = lastUsed;
            used = lastUsed = NULL;
        }
        size = BlockSize;
    }
    T* alloc() {
        if (size == BlockSize) {
            if (free != NULL) {
                Block* block = free;
                free = block->next;
                block->next = used;
                used = block;
            } else { 
                used = new Block(used);
            }
            if (lastUsed == NULL) {
                lastUsed = used;
            }
            size = 0;
        }
        return &used->data[size++];
    }
    BlockAllocator() : size(BlockSize), used(NULL), free(NULL), lastUsed(NULL), lastFree(NULL) {}
    ~BlockAllocator() {
        Block *curr, *next;
        for (curr = used; curr != NULL; curr = next) {
            next = curr->next;
            delete curr;
        }
        for (curr = free; curr != NULL; curr = next) {
            next = curr->next;
            delete curr;
        }
    }
};
