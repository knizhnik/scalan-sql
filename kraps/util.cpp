#include "util.h"


#define ROTL32(x, r) ((x) << (r)) | ((x) >> (32 - (r)))
#define HASH_BITS 25
#define N_HASHES (1 << (32 - HASH_BITS))
#define MURMUR_SEED 0x5C1DB

uint32_t murmur_hash3_32(const void* key, const int len)
{
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len / 4;
    
    uint32_t h1 = MURMUR_SEED;
    
    uint32_t c1 = 0xcc9e2d51;
    uint32_t c2 = 0x1b873593;
    int i;
    uint32_t k1;
    const uint8_t* tail;
    const uint32_t* blocks = (const uint32_t *)(data + nblocks*4);
    
    for(i = -nblocks; i; i++)
    {
        k1 = blocks[i];
        
        k1 *= c1;
        k1 = ROTL32(k1,15);
        k1 *= c2;
        
        h1 ^= k1;
        h1 = ROTL32(h1,13); 
        h1 = h1*5+0xe6546b64;
    }
    
    tail = (const uint8_t*)(data + nblocks*4);
    
    k1 = 0;

    switch(len & 3)
    {
      case 3: 
        k1 ^= tail[2] << 16;
        /* no break */
      case 2: 
        k1 ^= tail[1] << 8;
        /* no break */
      case 1: 
        k1 ^= tail[0];
        k1 *= c1;
        k1 = ROTL32(k1,15); 
        k1 *= c2; 
        h1 ^= k1;
    }
    
    h1 ^= len;   
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    
    return h1;
} 
