#include <stdio.h>
#include <string.h>
#include <stdlib.h>

template<class K, class V>
struct Pair
{
    K head;
    V tail;
};

class Collection
{
public:
    virtual size_t count() = 0;
    virtual void print(FILE* out) = 0;
    virtual ~Collection() = 0;
};
    
template<class T>
class RDD : public Collection
{
  public:
    virtual bool next(T& record) = 0;

    template<bool (*predicate)(T const&)>
    RDD<T>* filter();

    template<class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src)>
    RDD< Pair<K,V> >* mapReduce(size_t estimation);

    template<class P, void (*projection)(P& out, T const& in)>
    RDD<P>* project();

    template<int (*compare)(T const* a, T const* b)> 
    RDD<T>* sort(size_t estimation);

    template<class I, class K, void (*outerKey)(K& key, T const& outer), void (*innerKey)(K& key, I const& inner)>
    RDD< Pair<T,I> >* join(RDD<I>* with, size_t estimation, bool outerJoin = false);

    void print(FILE* out) {
        T record;
        while (next(record)) { 
            record.print(out);
        }
    }

    size_t count() {
        T record;
        size_t n;
        for (n = 0; next(record); n++);
        return n;
    }
};

template<class T>
class FileRDD : public RDD<T>
{
  public:
    FileRDD(char const* path) : f(fopen(path, "rb")) {}

    bool next(T& record) {
        return fread(&record, sizeof(T), 1, f) == 1;
    }
    
    ~FileRDD() { fclose(f); }

  private:
    FILE* const f;    
};

class FileManager
{
public:
    template<class T>
    static FileRDD<T>* load(char const* fileName) { 
        return new FileRDD<T>(fileName);
    }
}


template<class T, bool (*predicate)(T const&)>
class FilterRDD : public RDD<T>
{
  public:
    FilterRDD(RDD<T>* input) : in(input) {}

    bool next(T& record) {
        while (in->next(record)) { 
            if (predicate(record)) {
                return true;
            }
        }
        return false;
    }

    ~FilterRDD() { delete in; }

  private:
    RDD<T>* const in;
};
    
    
template<class T,class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src)>
class MapReduceRDD : public RDD< Pair<K,V> > 
{    
  public:
    MapReduceRDD(RDD<T>* input, size_t estimation) : in(input), table(new Entry*[estimation]), size(estimation) {
        loadHash();
        curr = NULL;
        i = 0;
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

    ~MapReduce() { 
        deleteHash();        
        delete in;
    }
  private:
    struct Entry {
        Pair<K,V> pair;
        Entry* collision;
        size_t hash;
    };
    
    RDD<T>* const in;
    Entry** const table;
    size_t  const size;
    size_t  i;
    Entry*  curr;

    void loadHash() {
        Entry* entry;
        T record;
        Pair<K,V> pair;

        memset(table, 0, size*sizeof(Entry*));

        while (in->next(record)) {
            map(pair, record);
            size_t hash = pair.head.hashCode();
            size_t h = hash % size;            
            for (entry = table[h]; entry != NULL && !(entry->hash == hash && pair.head == entry->pair.head); entry = entry->collision);
            if (entry == NULL) { 
                entry = new Entry();
                entry->collision = table[h];
                entry->hash = hash;
                table[h] = entry;
                entry->pair = pair;
            } else { 
                reduce(entry->pair.tail, pair.tail);
            }
        }
    }

    void deleteHash() {
        for (size_t i = 0; i < size; i++) { 
            Entry *curr, *next;
            for (curr = table[i]; curr != NULL; curr = next) { 
                next = curr->collision;
                delete curr;
            }
        }
        delete[] table;
    }
};

template<class T, class P, void project(P& out, T const& in)>
class ProjectRDD : public RDD<P>
{
  public:
    ProjectRDD(RDD<T>* input) : in(input) {}

    bool next(P& projection) { 
        T record;
        if (in->next(record)) { 
            project(projection, record);
            return true;
        }
        return false;
    }

    ~ProjectRDD() { delete in; }

  private:
    RDD<T>* const in;
};

template<class T, int compare(T const* a, T const* b)>
class SortRDD : public RDD<T>
{
  public:
    SortRDD(RDD<T>* input, size_t estimation) : in(input) {
        loadArray(estimation);
        i = 0;
    }

    bool next(T& record) { 
        if (i < size) { 
            record = buf[i++];
            return true;
        }
        return false;
    }
    
    ~SortRDD() { 
        delete in; 
        delete[] buf;
    }

  private:
    RDD<T>* const in;
    T* buf;
    size_t size;
    size_t i;

    typedef int(*comparator_t)(void const* p, void const* q);

    void loadArray(size_t estimation) { 
        buf = new T[estimation];
        for (size = 0; in->next(buf[size]); size++) { 
            if (i == estimation) { 
                T* newBuf = new T[estimation *= 2];
                memcpy(newBuf, buf, size*sizeof(T));
                delete[] buf;
            }
        }
        qsort(buf, size, sizeof(T), (comparator_t)compare);
    }
};

template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner)>
class HashJoinRDD : public RDD< Pair<L,R> >
{
public:
    HashJoinRDD(RDD<O>* outerRDD, RDD<I>* innerRDD, size_t estimation, bool outerJoin) 
    : outer(outerRDD), isOuterJoin(outerJoin), table(new Entry*[estimation]), size(estimation), inner(NULL) {
        loadHash(innerRDD);
    }

    public bool next(Pair<O,I>& record)
    {
        if (inner == NULL) { 
            do { 
                if (!outer->next(outerRec)) { 
                    return false;
                }
                outerKey(key, outerRec);
                size_t hash = key.hashCode();
                size_t h = hash % size;
                for (inner = tables[h]; inner != NULL && !(inner->hash == hash && key == inner.pair.head); inner = inner->collision);
            } while (inner == NULL && !isOuterJoin);
            
            if (inner == NULL) { 
                record.head = outerRec;
                record.tail = innerRec;
                return true;
            }
        }
        record.head = outerRec;
        record.tail = inner->pair.tail;
        do {
            inner = inner->next;
        } while (inner != NULL && !(inner->hash == hash && key == inner.pair.head));

        return true;
    }

    ~HashJoinRDD() { 
        deleteHash();
        delete outer;
    }
private:
    struct Entry {
        Pair<K,I> pair;
        Entry* collision;
        size_t hash;
    };
    
    RDD<O>* const outer;
    bool       const isOuterJoin;
    Entry**    const table;
    size_t     const size;
    O          outerRec;
    I          innerRec;
    K          key;
    Entry*     inner;

    void loadHash(RDD<R>* inner) {
        Entry* entry;
        Entry* entry = new Entry();

        memset(table, 0, size*sizeof(Entry*));

        while (inner->next(entry->pair.tail)) {
            innerKey(entry->pair.head, inner);
            entry->hash = pair.head.hashCode();
            size_t h = entry->hash % size;  
            entry->collision = table[h]; 
            table[h] = entry;
            entry = new Entry();
        }
        delete entry;
        delete inner;
    }
    void deleteHash() {
        for (size_t i = 0; i < size; i++) { 
            Entry *curr, *next;
            for (curr = table[i]; curr != NULL; curr = next) { 
                next = curr->collision;
                delete curr;
            }
        }
        delete[] table;
    }
};
    

template<class T>
template<bool (*predicate)(T const&)>
inline RDD<T>* RDD<T>::filter() { 
    return new FilterRDD<T,predicate>(this);
}

template<class T>
template<class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src)>
inline RDD< Pair<K,V> >* RDD<T>::mapReduce(size_t estimation) {
    return new MapReduceRDD<T,K,V,map,reduce>(this, estimation);
}

template<class T>
template<class P, void (*projection)(P& out, T const& in)>
inline RDD<P>* RDD<T>::project() {
    return new ProjectRDD<T,P,projection>(this);
}

template<class T>
template<int (*compare)(T const* a, T const* b)> 
inline RDD<T>* RDD<T>::sort(size_t estimation) {
    return new SortRDD<T,compare>(this, estimation);
}

template<class T>
template<class I, class K, void (*outerKey)(K& key, T const& outer), void (*innerKey)(K& key, I const& inner)>
RDD< Pair<T,I> >* RDD<T>::join(RDD<I>* with, size_t estimation, bool outerJoin) {
    return new HashJoinRDD<T,I,outerKey,innerKey>(this, with, estimation, outerJoin);
}
