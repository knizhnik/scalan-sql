#include <stdio.h>
#include <string.h>
#include <stdlib.h>

template<class K, class V>
struct Pair
{
    K key;
    V value;
};

template<class T>
class RDD
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

    void print(FILE* out) {
        T record;
        while (next(record)) { 
            record.print(out);
        }
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
  private:
    FILE* const f;    
};


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
  private:
    RDD<T>* const in;
};
    
    
template<class T,class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src)>
class MapReduceRDD : public RDD< Pair<K,V> > 
{    
  public:
    MapReduceRDD(RDD<T>* input, size_t estimatedSize) : in(input), table(new Entry*[estimatedSize]), size(estimatedSize) {
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
        curr = curr->next;
        return true;
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
            size_t hash = pair.key.hashCode();
            size_t h = hash % size;            
            for (entry = table[h]; entry != NULL && !(entry->hash == hash && pair.key == entry->pair.key); entry = entry->collision);
            if (entry == NULL) { 
                entry = new Entry();
                entry->collision = table[h];
                table[h] = entry;
                entry->pair = pair;
            } else { 
                reduce(entry->pair.value, pair.value);
            }
        }
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
