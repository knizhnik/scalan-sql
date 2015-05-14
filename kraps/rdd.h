#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "cluster.h"

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

    friend class print(Pair const& pair, FILE* out)
    {
        print(pair.key, out);
        print(pair.value, out);
    }
};

template<class Outer, class Inner>
struct Join : Outer, Inner 
{
    friend size_t pack(Join const& src, char* dst) {
        size_t size = pack((Outer const&)src, dst);
        return size + pack((Inner const&)src, dst + size);
    }

    friend size_t unpack(Join& dst, char const* src) {
        size_t size = unpack((Outer&)dst, src);
        return size + unpack((Inner&)dst, src + size);
    }    

    friend class print(Join const& r, FILE* out)
    {
        print((Outer&)r, out);
        print((Inner&)r, out);
    }
};

//
// Print functions for scalars
//
inline void print(int val, FILE* out)
{
    fprintf(out, "%d\n", val);
}
inline void print(long val, FILE* out)
{
    fprintf(out, "%ld\n", val);
}
inline void print(double val, FILE* out)
{
    fprintf(out, "%f\n", val);
}
inline void print(char const* val, FILE* out)
{
    fprintf(out, "%s\n", val);
}

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

//
// Default pack/unpack functions
//
template<class T>
size_t pack(T const& src, char* dst) { 
    memcpy(dst, &src, sizeof(T));
    return sizeof(T);
}
template<class T>
size_t unpack(T& dst, char const* src) { 
    memcpy(&dst, src, sizeof(T));
    return sizeof(T);
}



template<class T>
class RDD
{
  public:
    virtual bool next(T& record) = 0;

    template<bool (*predicate)(T const&)>
    RDD<T>* filter();

    template<class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src)>
    RDD< Pair<K,V> >* mapReduce(size_t estimation);

    template<class S,void (*accumulate)(S& state,  T const& in)>
    RDD<S>* reduce(S const& initState);

    template<class P, void (*projection)(P& out, T const& in)>
    RDD<P>* project();

    template<int (*compare)(T const* a, T const* b)> 
    RDD<T>* sort(size_t estimation);

    template<class I, class K, void (*outerKey)(K& key, T const& outer), void (*innerKey)(K& key, I const& inner)>
    RDD< Join<T,I> >* join(RDD<I>* with, size_t estimation, bool outerJoin = false);

    void print(FILE* out);

    virtual~RDD() {}
};

//
// Tranfer data from RDD to queue
//
template<class T>
inline void enqueue(RDD<T>* input, Queue* queue, qid_t qid) 
{
    size_t bufferSize = Cluster::instance->bufferSize;
    Buffer* buf = Buffer::create(qid, bufferSize);
    size_t size, used = 0;
    T record;
    while (input->next(record)) { 
        if (used + sizeof(T) > bufferSize) { 
            buf->size = used;
            queue->put(buf);
            buf = Buffer::create(qid, bufferSize);
            used = 0;
        }
        size = pack(record, buf->data + used);
        assert(size <= sizeof(T));
        used += size;
    }
    buf->size = used;
    if (used != 0) { 
        queue->put(buf);
        queue->put(Buffer::eof(qid));
    } else { 
        buf->kind = MSG_EOF;
        queue->put(buf);
    }
}

template<class T>
inline void sendToCoordinator(RDD<T>* input, Queue* queue) 
{
    enqueue(input, Cluster::instance->sendQueues[COORDINATOR], queue->qid);
}

template<class T>
class FetchJob : public Job
{
public:
    FetchJob(RDD<T>* in, Queue* q) : input(in), queue(q) {}

    void run()
    {        
        enqueue(input, queue, queue->qid);
    }
private:
    RDD<T>* const input;
    Queue* const queue;
};

template<class T, class K, void (*dist_key)(K& key, T const& record)>
class ScatterJob : public Job
{
public:
    ScatterJob(RDD<T>* in, Queue* q) : input(in), queue(q) {}
    ~ScatterJob() { delete input; }
    
    void run()
    {
        K key;
        T record;
        Cluster* cluster = Cluster::instance;
        size_t nNodes = cluster->nNodes;
        size_t nodeId = cluster->nodeId;
        size_t bufferSize = cluster->bufferSize;
        Buffer** buffers = new Buffer*[nNodes];
        size_t sent = 0;
        for (size_t i = 0; i < nNodes; i++) { 
            buffers[i] = Buffer::create(queue->qid, bufferSize);
            buffers[i]->size = 0;
        }

        while (input->next(record)) { 
            dist_key(key, record);
            size_t hash = hashCode(key);
            size_t node = hash % nNodes;
            if (buffers[node]->size + sizeof(T) > bufferSize) {
                Queue* dst = (node == nodeId) ? queue : cluster->sendQueues[node];
                dst->put(buffers[node]);
                buffers[node] = Buffer::create(queue->qid, bufferSize);
                sent += buffers[node]->size;
                buffers[node]->size = 0;
                
                if (sent > cluster->syncInterval) { 
                    for (size_t node = 0; node < nNodes; node++) {
                        if (node != nodeId) { 
                            cluster->sendQueues[node]->put(Buffer::ping(queue->qid));
                        }
                    }
                    queue->wait(nNodes-1);
                    sent = 0;
                }                
            }
            size_t size = pack(record, buffers[node]->data + buffers[node]->size);
            assert(size <= sizeof(T));
            buffers[node]->size += size;
        }
            
        for (size_t node = 0; node < nNodes; node++) {
            Queue* dst = (node == nodeId) ? queue : cluster->sendQueues[node];
            if (buffers[node]->size != 0) { 
                sent += buffers[node]->size;
                dst->put(buffers[node]);
                dst->put(Buffer::eof(queue->qid));
            } else { 
                buffers[node]->kind = MSG_EOF;
                dst->put(buffers[node]);
            }                
        }
        delete[] buffers;
    }
private:
    RDD<T>* const input;
    Queue* const queue;
};

template<class T>
class GatherRDD : public RDD<T>
{
public:
    bool next(T& record) {
        Cluster* cluster = Cluster::instance;
        while (used == size) { 
            delete buf;
            buf = queue->get();
            switch (buf->kind) { 
            case MSG_EOF:
                if (--nWorkers == 0) { 
                    return false;
                }                
                continue;
            case MSG_PING:
                buf->kind = MSG_PONG;
                assert(buf->node < cluster->nNodes && buf->node != cluster->nodeId);
                Cluster::instance->sendQueues[buf->node]->putFirst(buf);
                buf = NULL; // will be deleted by sender
                continue;
            default:
                used = 0;
                size = buf->size;
            }
        }
        used += unpack(record, buf->data + used);
        return true;
    }

    GatherRDD(Queue* q) : buf(NULL), used(0), size(0), queue(q), nWorkers(Cluster::instance->nNodes) {}
    ~GatherRDD() { delete buf; }
private:
    Buffer* buf;
    size_t used;
    size_t size;
    Queue* queue;
    size_t nWorkers;
};


template<class T>
class FileRDD : public RDD<T>
{
  public:
    FileRDD(char const* path) : f(fopen(path, "rb")) {
        assert(f != NULL);
    }

    bool next(T& record) {
        return fread(&record, sizeof(T), 1, f) == 1;
    }
    
    ~FileRDD() { fclose(f); }

  private:
    FILE* const f;    
};

template<class T>
class DirRDD : public RDD<T>
{
  public:
    DirRDD(char const* path) : dir(path), segno(Cluster::instance->nodeId), step(Cluster::instance->nNodes), f(NULL) {}

    bool next(T& record) {
        while (true) {
            if (f == NULL) { 
                char path[1024];
                sprintf(path, "%s/%ld.rdd", dir, segno);
                f = fopen(path, "rb");
                if (f == NULL) { 
                    return false;
                }
            }
            if (fread(&record, sizeof(T), 1, f) == 1) { 
                return true;
            } else { 
                fclose(f);
                segno += step;
                f = NULL;
            }
        }
    }

  private:
    char const* dir;
    size_t segno;
    size_t step;
    FILE* f;    
};

class FileManager
{
public:
    template<class T>
    static RDD<T>* load(char const* fileName) { 
        size_t len = strlen(fileName);
        
        return (strcmp(fileName + len - 4, ".rdd") == 0) 
            ? (RDD<T>*)new FileRDD<T>(fileName)
            : (RDD<T>*)new DirRDD<T>(fileName);
    }
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

    ~FilterRDD() { delete in; }

  private:
    RDD<T>* const in;
};
    
    
template<class S,void (*accumulate)(S& state, T const& in)>
class ReduceRDD : public RDD<S> 
{    
  public:
    ReduceRDD(RDD<T>* input, S const& initState) : state(initState), first(true) {
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
    void aggregate(RDD<T>* input) { 
        R record;
        while (input->next(record)) { 
            accumulate(state, record);
        }
    }
    
    S state;
    bool first;
};
        
template<class T,class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src)>
class MapReduceRDD : public RDD< Pair<K,V> > 
{    
  public:
    MapReduceRDD(RDD<T>* input, size_t estimation) : table(new Entry*[estimation]), size(estimation) {
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

    ~MapReduceRDD() { 
        deleteHash();        
    }
  private:
    struct Entry {
        Pair<K,V> pair;
        Entry* collision;
        size_t hash;
    };
    
    Entry** const table;
    size_t  const size;
    size_t  i;
    Entry*  curr;

    void loadHash(RDD<T>* input) 
    {
        Entry* entry;
        T record;
        Pair<K,V> pair;

        memset(table, 0, size*sizeof(Entry*));
        
        size_t realSize = 0;
        while (input->next(record)) {
            map(pair, record);
            size_t hash = hashCode(pair.key);
            size_t h = hash % size;            
            for (entry = table[h]; entry != NULL && !(entry->hash == hash && pair.key == entry->pair.key); entry = entry->collision);
            if (entry == NULL) { 
                entry = new Entry();
                entry->collision = table[h];
                entry->hash = hash;
                table[h] = entry;
                entry->pair = pair;
                realSize += 1;
            } else { 
                reduce(entry->pair.value, pair.value);
            }
        }
        curr = NULL;
        i = 0;
        Queue* queue = Cluster::instance->getQueue();
        if (Cluster::instance->isCoordinator()) { 
            GatherRDD< Pair<K,V> > gather(queue);
            queue->put(Buffer::eof(queue->qid)); // do not wait for self node
            Pair<K,V> pair;
            while (gather.next(pair)) {
                size_t hash = hashCode(pair.key);
                size_t h = hash % size;            
                for (entry = table[h]; entry != NULL && !(entry->hash == hash && pair.key == entry->pair.key); entry = entry->collision);
                if (entry == NULL) { 
                    entry = new Entry();
                    entry->collision = table[h];
                    entry->hash = hash;
                    table[h] = entry;
                    entry->pair = pair;
                    realSize += 1;
                } else { 
                    reduce(entry->pair.value, pair.value);
                }                
            }            
        } else {
            sendToCoordinator< Pair<K,V> >(this, queue);            
        }
        printf("HashAggregate: estimated size=%ld, real size=%ld\n", size, realSize);
        delete input;
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
    SortRDD(RDD<T>* input, size_t estimation) {
        loadArray(input, estimation);
    }

    bool next(T& record) { 
        if (i < size) { 
            record = buf[i++];
            return true;
        }
        return false;
    }
    
    ~SortRDD() { 
        delete[] buf;
    }

  private:
    T* buf;
    size_t size;
    size_t i;

    typedef int(*comparator_t)(void const* p, void const* q);

    void loadArray(RDD<T>* input, size_t estimation) { 
        Queue* queue = Cluster::instance->getQueue();
        if (Cluster::instance->isCoordinator()) {         
            Thread loader(new FetchJob<T>(input, queue));
            GatherRDD<T> gather(queue);
            buf = new T[estimation];
            for (size = 0; gather.next(buf[size]); size++) { 
                if (size == estimation) { 
                    T* newBuf = new T[estimation *= 2];
                    memcpy(newBuf, buf, size*sizeof(T));
                    delete[] buf;
                }
            }
            qsort(buf, size, sizeof(T), (comparator_t)compare);
        } else { 
            sendToCoordinator<T>(input, queue);
            buf = NULL;
            size = 0;
        }
        delete input;
        i = 0;
    }
};

template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner)>
class HashJoinRDD : public RDD< Join<O,I> >
{
public:
    HashJoinRDD(RDD<O>* outerRDD, RDD<I>* innerRDD, size_t estimation, bool outerJoin) 
    : isOuterJoin(outerJoin), table(new Entry*[estimation]), size(estimation), inner(NULL), outer(outerRDD), scatter(NULL) {
        // First load inner relation in hash...
        queue = Cluster::instance->getQueue();
        Thread loader(new ScatterJob<I,K,innerKey>(innerRDD, queue));
        loadHash(new GatherRDD<I>(queue));
        queue = Cluster::instance->getQueue();
    }

    bool next(Join<O,I>& record)
    {
        if (scatter == NULL) { 
            // .. and then start fetching of outer relation and perform hash lookup
            scatter = new Thread(new ScatterJob<O,K,outerKey>(outer, queue));
            outer = new GatherRDD<O>(queue);
        }
        if (inner == NULL) { 
            do { 
                if (!outer->next(outerRec)) { 
                    return false;
                }
                outerKey(key, outerRec);
                hash = hashCode(key);
                size_t h = hash % size;
                for (inner = table[h]; inner != NULL && !(inner->hash == hash && key == inner->key); inner = inner->collision);
            } while (inner == NULL && !isOuterJoin);
            
            if (inner == NULL) { 
                (O&)record = outerRec;
                (I&)record = innerRec;
                return true;
            }
        }
        (O&)record = outerRec;
        (I&)record = inner->record;
        do {
            inner = inner->collision;
        } while (inner != NULL && !(inner->hash == hash && key == inner->key));

        return true;
    }

    ~HashJoinRDD() { 
        deleteHash();
        delete outer;
        delete scatter;
    }
private:
    struct Entry {
        K      key;
        I      record;
        Entry* collision;
        size_t hash;
    };
    
    bool    const isOuterJoin;
    Entry** const table;
    size_t  const size;
    O       outerRec;
    I       innerRec;
    K       key;
    size_t  hash;
    Entry*  inner;
    RDD<O>* outer;
    Queue*  queue;
    Thread* scatter;

    void loadHash(RDD<I>* gather) 
    {
        Entry* entry = new Entry();
        memset(table, 0, size*sizeof(Entry*));
        size_t realSize = 0;
        while (gather->next(entry->record)) {
            innerKey(entry->key, entry->record);
            entry->hash = hashCode(entry->key);
            size_t h = entry->hash % size;  
            entry->collision = table[h]; 
            table[h] = entry;
            entry = new Entry();
            realSize += 1;
        }
        size_t totalLen = 0;
        size_t nChains = 0;
        size_t maxLen = 0;
        for (size_t i = 0; i < size; i++) { 
            size_t chainLen = 0;
            for (Entry* entry = table[i]; entry != NULL; entry = entry->collision) chainLen += 1;
            if (chainLen > maxLen) { 
                maxLen = chainLen;
            }
            totalLen += chainLen;
            nChains += chainLen != 0;                
        }
        printf("HashJoin: estimated size=%ld, real size=%ld, collitions=(%ld max, %f avg)\n", size, realSize, maxLen, nChains != 0 ? (double)totalLen/nChains : 0.0);
        delete entry;
        delete gather;
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
class CachedRDD : public RDD<T>
{
  public:
    CachedRDD(RDD<T>* input, size_t estimation) : copy(false) { 
        cacheData(input, estimation);
    }
    bool next(T& record) { 
        if (curr == size) { 
            return false;
        }
        record = buf[curr++];
        return true;
    }
    ~CachedRDD() { 
        if (!copy) { 
            delete[] buf;
        }
    }

    CachedRDD* get() { 
        return new CachedRDD(buf, size);
    }

  private:
    CachedRDD(T* buffer, size_t bufSize) : buf(buffer), curr(0), size(bufSize), copy(true) {}

    void cacheData(RDD<T>* input, size_t estimation) { 
        buf = new T[estimation];
        size_t i = 0;
        while (input->next(buf[i])) { 
            if (++i == estimation) {
                T* newBuf = new T[estimation *= 2];
                memcpy(newBuf, buf, i*sizeof(T));
                delete[] buf;
                buf = newBuf;
            }
        }
        size = i;
        curr = 0;
        delete input;
    }

    T* buf;
    size_t curr;
    size_t size;
    bool copy;
};

template<class T>
void RDD<T>::print(FILE* out) 
{
    Cluster* cluster = Cluster::instance;
    Queue* queue = cluster->getQueue();
    if (cluster->isCoordinator()) {         
        Thread fetch(new FetchJob<T>(this, queue));
        GatherRDD<T> gather(queue);
        T record;
        while (gather.next(record)) { 
            print(record, out);
        }
    } else {         
        sendToCoordinator<T>(this, queue);
    }
    cluster->barrier();
}

template<class T>
template<bool (*predicate)(T const&)>
inline RDD<T>* RDD<T>::filter() { 
    return new FilterRDD<T,predicate>(this);
}

template<class T>
template<class S,void (*accumulate)(S& state, T const& in)>
inline RDD<S>* RDD<T>::reduce(S const& initState) {
    return new ReduceRDD<S,accumulate>(this, initState);
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
RDD< Join<T,I> >* RDD<T>::join(RDD<I>* with, size_t estimation, bool outerJoin) {
    return new HashJoinRDD<T,I,K,outerKey,innerKey>(this, with, estimation, outerJoin);
}
    
