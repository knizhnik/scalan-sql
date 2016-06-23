#pragma once

/**
 * Pair is used for map-reduce
 */
template<class K, class V>
struct Pair
{
    K key;
    V value;

    friend void print(Pair const& pair, FILE* out)
    {
        print(pair.key, out);
        fputs(", ", out);
        print(pair.value, out);
    }
};

/**
 *  Result of join
 */
template<class Outer, class Inner>
struct Join : Outer, Inner 
{
    friend void print(Join const& r, FILE* out)
    {
        print((Outer&)r, out);
        fputs(", ", out);
        print((Inner&)r, out);
    }
};


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

const size_t INIT_BUF_SIZE = 64*1024; // should be power of two

/**
 * Join kind
 */
enum JoinKind 
{
    InnerJoin,
    OuterJoin
};

/**
 * Abstract plan Node
 */
template<class T>
class Node
{
  public:
    /**
     * Main Node method for iterating thoough records
     * @param record [out] placeholder for the next record
     * @return true if there is next record, false otherwise
     */
    virtual bool next(T& record) = 0;

    /**
     * Filter input Node
     * @return Node with records matching predicate 
     */
    template<bool (*predicate)(T const&)>
    Node<T>* filter();

    /**
     * Perfrom map-reduce
     * @param estimation esimation for number of pairs
     * @return Node with &lt;key,value&gt; pairs
     */
    template<class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src)>
    Node< Pair<K,V> >* mapReduce(size_t estimation = UNKNOWN_ESTIMATION);

    /**
     * Perform aggregation of input Node 
     * @param initState initial aggregate value
     * @return Node with aggregated value
     */
    template<class S,void (*accumulate)(S& state,  T const& in),void (*combine)(S& state, S const& in)>
    Node<S>* reduce(S const& initState);

    /**
     * Map records of input Node
     * @return projection of the input Node. 
     */
    template<class P, void (*projection)(P& out, T const& in)>
    Node<P>* project();

    /**
     * Sort input Node
     * @param estimation estimation for number of records in Node
     * @return Node with sorted records
     */
    template<int (*compare)(T const* a, T const* b)> 
    Node<T>* sort(size_t estimation = UNKNOWN_ESTIMATION);

    /**
     * Find top N records according to provided comparison function
     * @param n number of returned top records
     * @return Node with up to N top records
     */
    template<int (*compare)(T const* a, T const* b)> 
    Node<T>* top(size_t n);

    /**
     * Left join two Nodes. Inner join returns pairs of matches records in outer and inner table.
     * Outer join also returns records from outer table for which there are matching in inner table.
     * @param with inner join table
     * @param estimation estimation for number of joined records
     * @param kind join kind (inner/outer join)
     */
    template<class I, class K, void (*outerKey)(K& key, T const& outer), void (*innerKey)(K& key, I const& inner)>
    Node< Join<T,I> >* join(Node<I>* with, size_t estimation = UNKNOWN_ESTIMATION, JoinKind kind = InnerJoin);

    /**
     * Get single record from input Node or substitute it with default value of Node is empty.
     * This method is usful for obtaining aggregation result
     * @return Single record from input Node or substitute it with default value of Node is empty.
     */
    T result(T const& defaultValue) {
        T record;
        return next(record) ? record : defaultValue;
    }

    /**
     * Print Node records to the stream
     * @param out output stream
     */
    void output(FILE* out);

    virtual~Node() {}

};

/**
 * Filter resutls using provided condition
 */
template<class T, bool (*predicate)(T const&)>
class FilterNode : public Node<T>
{
  public:
    FilterNode(Node<T>* input) : in(input) {}

    bool next(T& record) {
        while (in->next(record)) { 
            if (predicate(record)) {
                return true;
            }
        }
        return false;
    }

    ~FilterNode() { delete in; }

  private:
    Node<T>* const in;
};
    
/**
 * Perform aggregation of input data (a-la Scala fold)
 */
template<class T, class S,void (*accumulate)(S& state, T const& in)>
class ReduceNode : public Node<S> 
{    
  public:
    ReduceNode(Node<T>* input, S const& initState) : state(initState), first(true) {
        aggregate(input);
    }

    bool next(S& record) {
        if (first) { 
            record = state;
            first = false;
            return true;
        }
        return false;
    }

  private:
    void aggregate(Node<T>* input) { 
        T record;
        while (input->next(record)) { 
            accumulate(state, record);
        }
        delete input;
    }
    
    S state;
    bool first;
};
        
/**
 * Classical Map-Reduce
 */
template<class T,class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src)>
class MapReduceNode : public Node< Pair<K,V> > 
{    
  public:
    MapReduceNode(Node<T>* input) : curr(NULL), i(0) {
		size = INIT_BUF_SIZE;
		table = new Entry*[size];
		memset(table, 0, size*sizeof(Entry*));

        loadHash(input);
    }

    bool next(Pair<K,V>& record) {
        while (curr == NULL) { 
            if (i == size) { 
                return false;
            }
            curr = table[i++];
        }
        record = curr->pair;
        curr = curr->collision;
        return true;
    }

    ~MapReduceNode() { 
        deleteHash();        
    }

  private:
    struct Entry {
        Pair<K,V> pair;
        Entry* collision;
    };
    
    size_t  size;
    Entry** table;
    size_t  i;
    Entry*  curr;
    BlockAllocator<Entry> allocator;

    void extendHash() 
    {
        Entry *entry, *next;
        size_t newSize = size*2;
        Entry** newTable = new Entry*[newSize];
        memset(newTable, 0, newSize*sizeof(Entry*));
        for (size_t i = 0; i < size; i++) { 
            for (entry = table[i]; entry != NULL; entry = next) { 
                size_t h = hashCode(entry->pair.key) & (newSize-1);
                next = entry->collision;
                entry->collision = newTable[h];
                newTable[h] = entry;
            }
        }
        delete[] table;
        table = newTable;
        size = newSize;
    }
             

    void loadHash(Node<T>* input) 
    {
        Entry* entry;
        T record;
        Pair<K,V> pair;
		size_t used = 0;
        
        while (input->next(record)) {
            map(pair, record);
            size_t hash = hashCode(pair.key);
            size_t h = hash & (size-1);            
            for (entry = table[h]; !(entry == NULL || pair.key == entry->pair.key); entry = entry->collision);
            if (entry == NULL) { 
                entry = allocator.alloc();
                entry->collision = table[h];
                table[h] = entry;
                entry->pair = pair;
                if (++used > size) { 
                    extendHash();
                }
            } else { 
                reduce(entry->pair.value, pair.value);
            }
        }
        delete input;
    }

    void deleteHash() {
        delete[] table;
    }
};

/**
 * Project (map) Node records
 */
template<class T, class P, void project(P& out, T const& in)>
class ProjectNode : public Node<P>
{
  public:
    ProjectNode(Node<T>* input) : in(input) {}

    bool next(P& projection) { 
        T record;
        if (in->next(record)) { 
            project(projection, record);
            return true;
        }
        return false;
    }

    ~ProjectNode() { delete in; }

  private:
    Node<T>* const in;
};

/**
 *  Sort using given comparison function
 */
template<class T, int compare(T const* a, T const* b)>
class SortNode : public Node<T>
{
  public:
    SortNode(Node<T>* input) : i(0) {
        loadArray(input, estimation);
    }

    bool next(T& record) { 
        if (i < used) { 
            record = buf[i++];
            return true;
        }
        return false;
    }
    
    ~SortNode() { 
        delete[] buf;
    }

  private:
    T* buf;
    size_t used;
    size_t i;

    typedef int(*comparator_t)(void const* p, void const* q);

    void loadArray(Node<T>* input) { 
		size_t size = INIT_BUF_SIZE;
		size_t n = 0;
		buf = new T[size];
		while (input->next(buf[n])) { 
			if (++n == size) { 
				T* newBuf = new T[size *= 2];
				memcpy(newBuf, buf, size*sizeof(T));
				delete[] buf;
				buf = newBuf;
			}
		}
        delete input;
		qsort(buf, n, sizeof(T), (comparator_t)compare);
		used = n;
        i = 0;
    }
};

/**
 * Get top N records using given comparison function
 */
template<class T, int compare(T const* a, T const* b)>
class TopNode : public Node<T>
{
  public:
    TopNode(Node<T>* input, size_t top) : i(0) {
        buf = new T[top];
        loadTop(input, top);
    }

    bool next(T& record) { 
        if (i < size) { 
            record = buf[i++];
            return true;
        }
        return false;
    }
    
    ~TopNode() { 
        delete[] buf;
    }

  private:
    T* buf;
    size_t size;
    size_t i;
    
    void loadTop(Node<T>* input, size_t top) { 
        T record;
		size_t n = 0;
        while (input->next(record)) {
            size_t l = 0, r = n;
            while (l < r) {
                size_t m = (l + r) >> 1;
                if (compare(&buf[m], &record) <= 0) {
                    l = m + 1;
                } else {
                    r = m;
                }
            }
            if (r < top) {
                if (n < top) {
                    n += 1;
                }
                memmove(&buf[r+1], &buf[r], (n-r-1)*sizeof(T));
                buf[r] = record;
            }
        }
        size = n;
		i = 0;
    }
};

/**
 * Join two Nodes using hash table.
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner)>
class HashJoinNode : public Node< Join<O,I> >
{
public:
    HashJoinNode(Node<O>* outer, Node<I>* inner, JoinKind joinKind) : kind(joinKind), entry(NULL)
    {
		size = INIT_BUF_SIZE;
		table = new Entry*[size];
		memset(table, 0, size*sizeof(Entry*));

		loadInner(inner);
    }

    bool next(Join<O,I>& record)
    {
        if (entry == NULL) { 
            do { 
                if (!outer->next(outerRec)) { 
                    entry = NULL;
                    return false;
                }
                outerKey(key, outerRec);
                size_t h = hashCode(key) & (size-1);
                for (entry = table[h]; !(entry == NULL || entry->equalsKey(key)); entry = entry->collision);
            } while (entry == NULL && kind == InnerJoin);
            
            if (entry == NULL) { 
                (O&)record = outerRec;
                (I&)record = innerRec;
                return true;
            }
        }
        (O&)record = outerRec;
        (I&)record = entry->record;
        do {
            entry = entry->collision;
        } while (!(entry == NULL || entry->equalsKey(key)));

        return true;
    }

    ~HashJoinNode() { 
		delete[] table;
    }

protected:
    struct Entry {
        I      record;
        Entry* collision;

        bool equalsKey(K const& other) {
            K key;
            innerKey(key, record);
            return key == other;
        }
    };
    
    JoinKind const kind;
    Entry** table;
    size_t  size;
    O       outerRec;
    I       innerRec;
    K       key;
    Entry*  entry;
    BlockAllocator<Entry> allocator;

    void loadInnerTable(Node<I>* outer)
    {
		I record;
		size_t used = 0;
		Entry* entry = allocator.alloc();
		while (outer->next(entry->record)) { 
			K key;
			innerKey(key, entry->record);
			size_t h = hashCode(key) & (size-1);
			entry->next = table[h];
  			table[h] = entry;

			if (++used > size) { 
				extendHash();
			}
		}
		delete inner;
		entry = NULL;
    }

    void extendHash() 
    {
        Entry *entry, *next;
        size_t newSize = size*2;
        Entry** newTable = new Entry*[newSize];
        memset(newTable, 0, newSize*sizeof(Entry*));
        for (size_t i = 0; i < size; i++) { 
            for (entry = table[i]; entry != NULL; entry = next) {
                K key;
                innerKey(key, entry->record);
                size_t h = hashCode(key) & (newSize-1);
                next = entry->collision;
                entry->collision = newTable[h];
                newTable[h] = entry;
            }
        }
        delete[] table;
        table = newTable;
        size = newSize;
    }
};
    
template<class T>
void Node<T>::output(FILE* out) 
{
	T record;
	while (next(record)) { 
		print(record, out);
		fputc('\n', out);
	}
}

template<class T>
template<bool (*predicate)(T const&)>
inline Node<T>* Node<T>::filter() { 
    return new FilterNode<T,predicate>(this);
}

template<class T>
template<class S,void (*accumulate)(S& state, T const& in), void(*combine)(S& state, S const& partial)>
inline Node<S>* Node<T>::reduce(S const& initState) {
    return new ReduceNode<T,S,accumulate,combine>(this, initState);
}

template<class T>
template<class K,class V,void (*map_f)(Pair<K,V>& out, T const& in), void (*reduce_f)(V& dst, V const& src)>
inline Node< Pair<K,V> >* Node<T>::mapReduce(size_t estimation) {
    return new MapReduceNode<T,K,V,map_f,reduce_f>(this, estimation);
}

template<class T>
template<class P, void (*projection)(P& out, T const& in)>
inline Node<P>* Node<T>::project() {
    return new ProjectNode<T,P,projection>(this);
}

template<class T>
template<int (*compare)(T const* a, T const* b)> 
inline Node<T>* Node<T>::sort(size_t estimation) {
    return new SortNode<T,compare>(this, estimation);
}

template<class T>
template<int (*compare)(T const* a, T const* b)> 
inline Node<T>* Node<T>::top(size_t n) {
    return new TopNode<T,compare>(this, n);
}

template<class T>
template<class I, class K, void (*outerKey)(K& key, T const& outer), void (*innerKey)(K& key, I const& inner)>
Node< Join<T,I> >* Node<T>::join(Node<I>* with, JoinKind kind) {
	return new HashJoinNode<T,I,K,outerKey,innerKey>(this, with, kind);
}
