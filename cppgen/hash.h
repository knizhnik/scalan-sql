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

template<class K, class V, void (*reduce)(V& dst, V const& src)>
class KeyValueMap
{
  public:
	template<class R> 
	Job* iterate(R* reactor, size_t from = 0, size_t step = 1) { 
		return new IterateJob<R>(*this, reactor, from, step);
	}
	
	KeyValueMap(size_t estimation) { 
		used = 0;
		size = hashTableSize(estimation);
		table = new Entry*[size];
		memset(table, 0, size*sizeof(Entry*));
	}
	
	~KeyValueMap() { 
		delete[] table;
		table = NULL;
	}
	
	size_t count() const { 
		return size;
	}

	void add(Pair<K,V> const& pair) __attribute__((always_inline)) {
		Entry* entry;
		size_t hash = hashCode(pair.key);
		size_t h = MOD(hash, size);            
		for (entry = table[h]; !(entry == NULL || pair.key == entry->pair.key); entry = entry->collision);
		if (entry == NULL) { 
			entry = allocator.alloc();
			entry->collision = table[h];
			table[h] = entry;
			entry->pair = pair;
			if(++used > size) { 
				extendHash();
			}
		} else { 
			reduce(entry->pair.value, pair.value);
		}
	}
	
	void merge(KeyValueMap const& other) { 
		for (size_t i = 0; i < other.size; i++) { 
			for (Entry* entry = other.table[i]; entry != NULL; entry = entry->collision) { 
				add(entry->pair);
			}
		}
	}

  private:
    struct Entry {
        Pair<K,V> pair;
        Entry* collision;
    };

	void extendHash() 
	{
		Entry *entry, *next;
		size_t newSize = hashTableSize(size+1);
		Entry** newTable = new Entry*[newSize];
		memset(newTable, 0, newSize*sizeof(Entry*));
		for (size_t i = 0; i < size; i++) { 
			for (entry = table[i]; entry != NULL; entry = next) { 
				size_t h = MOD(hashCode(entry->pair.key), newSize);
				next = entry->collision;
				entry->collision = newTable[h];
				newTable[h] = entry;
			}
		}
		delete[] table;
		table = newTable;
		size = newSize;
	}
	
	template<class R>
	class IterateJob : public Job
	{
	  public:
		IterateJob(KeyValueMap& m, R* r, size_t origin, size_t inc) : map(m), reactor(r), from(origin), step(inc) { }
		
		void run() { 
			for (size_t i = from; i < map.size; i += step) { 
				for (Entry* entry = map.table[i]; entry != NULL; entry = entry->collision) { 
					reactor->react(entry->pair);
				}
			}
			delete reactor;
		}
	  private:
		KeyValueMap& map;
		R* reactor;
		size_t const from;
		size_t const step;
	};

private:
	BlockAllocator<Entry> allocator;
	Entry** table;
	size_t used;
	size_t size;
};
