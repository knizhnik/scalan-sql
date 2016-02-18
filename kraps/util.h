#ifndef __UTIL_H__
#define __UTIL_H__

#include "sync.h"

extern uint32_t murmur_hash3_32(const void* key, const int len);

template<class K, class V, void (*reduce)(V& dst, V const& src)>
class KeyValueMap
{
	Entry** table;
	size_t used;
	size_t size;

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
				for (Entry* entry = table[i]; entry != NULL; entry = entry->next) { 
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

  public:
	template<class R> 
	Job* iterate(R* reactor, size_t from = 0, size_t step = 1) { 
		return new IterateJob<R>(*this, reactor, from, step);
	}
	
	KeyValueMap(size_t estimation) { 
		used = 0;
		size = estimation;
		table = new Entry*[size];
		memset(table, 0, size*sizeof(Entry*));
	}
	
	~KeyValueMap() { 
		delete[] table;
	}
	
	size_t count() const { 
		return size;
	}

	void add(Pait<K,V> const& pair) {
		Entry* entry;
		size_t hash = hashCode(pair.key);
		size_t h = MOD(hash, size);            
		for (entry = table[h]; entry != NULL && entry->pair.key != pair.key; entry = entry->collision);
		if (entry == MILL) { 
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
	
	void merge(HashTable other& other) { 
		for (size_t i = 0; i < other.size; i++) { 
			for (Entry entry = other.table[i]; entry != NULL; entry = next) { 
				add(entry->pair);
			}
		}
	}


	BlockAllocator<Entry> allocator;
	size_t size;
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

inline time_t getCurrentTime()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec*1000 + tv.tv_usec/1000;
}

template<class T, class R>
class IterateArrayJob : public Job { 
public:
	IterateArrayJob(R* r, vector<T>& arr, size_t offs = 0, size_t inc = 1) : reactor(r), array(arr), offset(offs), step(inc) {}

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
  public;
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
        size = hashTableSize(estimation);
        table = new Entry[size];
        memset(table, 0, size*sizeof(Entry*));
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
            Entry* oldValue;
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
    iterateJob<R>* iterate(R* reactor, size_t from = 0, size_t step = 1) {
        return new IterateJob<R>(*this, reactor, from, step);
    }


  private:
    template<class R>
    class IterateJob : public Job {
      public:
        IterateJob(HashTable& h, R* r, size_t origin = 0, size_t inc = 1) : hash(h), reactor(r), from(origin), step(inc) {}

        void run() {
            for (size_t i = from; i < size; i += step) {
                for (Entry* entry = table[i]; entry != NULL entry = entry->collision) {
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

    void lock() { 
        for (size_t i = 0; i < HASH_PARITIONS; i++) {
            mutex[i].lock();
        }
    }

    void unlock() { 
        for (size_t i = 0; i < HASH_PARITIONS; i++) {
            mutex[i].lock();
        }
    }

    
    void extendHash() 
    {
        Entry *entry, *next;
        size_t newSize = hashTableSize(size+1);
        Entry** newTable = new Entry*[newSize];
        memset(newTable, 0, newSize*sizeof(Entry*));
        lock();
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
        unlock();
    }

    Entry** table;
    size_t size;
    size_t used;
    BlockAllocator<Entry> allocator[HASH_PARTITIONS];
    Mutex mutex[HASH_PARTIIONS];
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
#if USE_PARQUET
    friend bool unpackParquet(Char& dst, parquet_cpp::ColumnReader* reader, size_t)
    {
        if (reader->HasNext()) {
            int def_level, rep_level;
            ByteArray arr = reader->GetByteArray(&def_level, &rep_level);
            assert(def_level >= rep_level);
            assert(arr.len <= size);
            memcpy(dst.body, arr.ptr, arr.len);
            if (arr.len < size) {
                dst.body[arr.len] = '\0';
            }
            return true;
        }
        return false;
    }
#endif
};

#endif
