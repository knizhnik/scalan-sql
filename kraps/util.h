#ifndef __UTIL_H__
#define __UTIL_H__

#include <sys/time.h>
#include <stdio.h>
#include "sync.h"
#include "hash.h"

/**
 * Print functions for scalars
 */
inline void print(int val, FILE* out)
{
    fprintf(out, "%d", val);
}
inline void print(long val, FILE* out)
{
    fprintf(out, "%ld", val);
}
inline void print(double val, FILE* out)
{
    fprintf(out, "%f", val);
}
inline void print(char const* val, FILE* out)
{
    fprintf(out, "%s", val);
}

/**
 * Pair is used for map-reduce
 */
template<class K, class V>
struct Pair
{
    K key;
    V value;
    friend size_t pack(Pair const& src, char* dst) {
        size_t size = pack(src.key, dst);
        return size + pack(src.value, dst + size);
    }

    friend size_t unpack(Pair& dst, char const* src) {
        size_t size = unpack(dst.key, src);
        return size + unpack(dst.value, src + size);
    }
    friend void print(Pair const& pair, FILE* out)
    {
        print(pair.key, out);
        fputs(", ", out);
        print(pair.value, out);
    }
};



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


inline time_t getCurrentTime()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec*1000 + tv.tv_usec/1000;
}

template<class T, class R>
class IterateArrayJob : public Job { 
public:
	IterateArrayJob(R* r, vector<T>& arr, size_t origin = 0, size_t inc = 1) : reactor(r), array(arr), offs(origin), step(inc) {}

	void run() { 
		for (size_t i = offs; i < array.size(); i += step) { 
			reactor->react(array[i]);
		}
		delete reactor;
	}
	
private:
	R* const reactor;
	vector<T>& array;
	size_t const offs;
	size_t const step;
};

#define HASH_PARTITIONS 64

template<class T, class K, void (*getKey)(K& key, T const& record)>  
class HashTable
{
  public:
    struct Entry {
        T record;
        Entry* collision;

        bool equalsKey(K const& other) {
            K key;
            getKey(key, record);
            return key == other;
        }
    };
        
    HashTable(size_t estimation) {
		used = 0;
        size = hashTableSize(estimation);
        table = new Entry*[size];
        memset(table, 0, size*sizeof(Entry*));
    }

	~HashTable() { 
		delete[] table;
		table = NULL;
	}

    Entry* get(K const& key)
    {
        size_t h = MOD(hashCode(key), size);
        return table[h];
    }
    
    void add(T const& record) {
        K key;
        getKey(key, record);
        size_t h = MOD(hashCode(key), size);
        size_t partition = h % HASH_PARTITIONS;
        {
            CriticalSection cs(mutex[partition]);
            Entry* entry = allocator[partition].alloc();
            entry->record = record;
            entry->collision = table[h];
            table[h] = entry;
        }
		if (__sync_add_and_fetch(&used, 1) == size) { 
            extendHash();
        }
    }
    
    template<class R>
    Job* iterate(R* reactor, size_t from = 0, size_t step = 1) {
        return new IterateJob<R>(*this, reactor, from, step);
    }

  private:
    template<class R>
    class IterateJob : public Job {
      public:
        IterateJob(HashTable& h, R* r, size_t origin = 0, size_t inc = 1) : hash(h), reactor(r), from(origin), step(inc) {}

        void run() {
            for (size_t i = from; i < size; i += step) {
                for (Entry* entry = table[i]; entry != NULL; entry = entry->collision) {
                    reactor->react(entry->record);
                }
            }
            delete reactor;
        }
        
      private:
        HashTable& hash;
        R* reactor;
        size_t const from;
        size_t const step;
    };
  private:
    void lock() { 
        for (size_t i = 0; i < HASH_PARTITIONS; i++) {
            mutex[i].lock();
        }
    }

    void unlock() { 
        for (size_t i = 0; i < HASH_PARTITIONS; i++) {
            mutex[i].unlock();
        }
    }

    
    void extendHash() 
    {
        Entry *entry, *next;
		size_t oldSize = size;
        size_t newSize = hashTableSize(oldSize+1);
        Entry** newTable = new Entry*[newSize];
        memset(newTable, 0, newSize*sizeof(Entry*));
        lock();
		if (size == oldSize) { 
			for (size_t i = 0; i < size; i++) { 
				for (entry = table[i]; entry != NULL; entry = next) { 
					K key;
					getKey(key, entry->record);
					size_t h = MOD(hashCode(key), newSize);
					next = entry->collision;
					entry->collision = newTable[h];
					newTable[h] = entry;
				}
			}
			delete[] table;
			table = newTable;
			size = newSize;
		} else { 
			delete[] newTable;
		}
        unlock();
    }

  private:
    Entry** table;
    size_t size;
    size_t used;
    BlockAllocator<Entry> allocator[HASH_PARTITIONS];
    Mutex mutex[HASH_PARTITIONS];
};


/**
 * Fixed size string 
 */
template<size_t size>
struct Char
{
    char body[size];

    operator char const*() const { 
        return body;
    }

    char const* cstr() const { 
        return body;
    }

    int compare(Char const& other) const
    {
        return strncmp(body, other.body, size);
    }

    int compare(char const* other) const
    {
        return strncmp(body, other, size);
    }

    bool operator<=(Char const& other) const
    {
        return compare(other)<= 0;
    }
    bool operator<(Char const& other) const
    {
        return compare(other)< 0;
    }
    bool operator>=(Char const& other) const
    {
        return compare(other)>= 0;
    }
    bool operator>(Char const& other) const
    {
        return compare(other)> 0;
    }
    bool operator==(Char const& other) const
    {
        return compare(other)== 0;
    }
    bool operator!=(Char const& other) const
    {
        return compare(other) != 0;
    }
    
    bool operator<=(char const* other) const
    {
        return compare(other)<= 0;
    }
    bool operator<(char const* other) const
    {
        return compare(other)< 0;
    }
    bool operator>=(char const* other) const
    {
        return compare(other)>= 0;
    }
    bool operator>(char const* other) const
    {
        return compare(other)> 0;
    }
    bool operator==(char const* other) const
    {
        return compare(other)== 0;
    }
    bool operator!=(char const* other) const
    {
        return compare(other)!= 0;
    }

    friend size_t hashCode(Char const& key)
    {
        return ::hashCode(key.body);
    }
    
    friend void print(Char const& key, FILE* out) 
    {
        fprintf(out, "%.*s", (int)size, key.body);
    }
    friend size_t unpack(Char& dst, char const* src)
    {
        return strcopy(dst.body, src, size);
    }

    friend size_t pack(Char const& src, char* dst)
    {
        return strcopy(dst, src.body, size);
    }
};

#endif
