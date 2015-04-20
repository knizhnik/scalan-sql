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
};

template<class Outer, class Inner>
struct Join : Outer, Inner {};

inline size_t hashCode(int key) { 
    return (size_t)key;
}

inline size_t hashCode(long key) { 
    return (size_t)key;
}

inline size_t hashCode(char const* key) { 
    size_t h = 0;
    while (*key != '\0') { 
        h = h*31 + (*key++ & 0xFF);
    }
    return h;
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

    template<class P, void (*projection)(P& out, T const& in)>
    RDD<P>* project();

    template<int (*compare)(T const* a, T const* b)> 
    RDD<T>* sort(size_t estimation);

    template<class I, class K, void (*outerKey)(K& key, T const& outer), void (*innerKey)(K& key, I const& inner)>
    RDD< Join<T,I> >* join(RDD<I>* with, size_t estimation, bool outerJoin = false);

    void print(FILE* out);

    virtual~RDD() {}
};


template<class T>
inline void sendToCoordinator(RDD<T>* input, Queue* queue) 
{
    Cluster* cluster = Cluster::instance;
    size_t size = cluster->bufferSize/sizeof(T);
    Buffer* buf = Buffer::create(queue->qid, size*sizeof(T));
    T* data = (T*)buf->data;
    size_t used = 0;
    
    while (input->next(data[used])) { 
        if (++used == size) { 
            cluster->sockets[COORDINATOR]->write(buf, BUF_HDR_SIZE + buf->size);
            used = 0;
        }
    }
    if (used != 0) { 
        buf->size = used*sizeof(T);
        cluster->sockets[COORDINATOR]->write(buf, BUF_HDR_SIZE + buf->size);
    } 
    buf->size = 0;
    cluster->sockets[COORDINATOR]->write(buf, BUF_HDR_SIZE); // send EOF    
}

template<class T>
class FetchJob : public Job
{
public:
    FetchJob(RDD<T>* in,  Queue* q) : input(in), queue(q) {}

    void run()
    {        
        size_t size = Cluster::instance->bufferSize/sizeof(T);
        Buffer* buf = Buffer::create(queue->qid, size*sizeof(T));
        size_t used = 0;
        while (input->next(*((T*)buf->data + used))) { 
            if (++used == size) { 
                queue->put(buf);
                buf = Buffer::create(queue->qid, size*sizeof(T));
                used = 0;
            }
        }
        buf->size = used*sizeof(T);
        queue->put(buf);
        if (used != 0) { 
            buf = Buffer::create(queue->qid, 0); // EOF
            queue->put(buf);
        }
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
        for (size_t i = 0; i < nNodes; i++) { 
            buffers[i] = Buffer::create(queue->qid, bufferSize);
            buffers[i]->size = 0;
        }

        while (input->next(record)) { 
            dist_key(key, record);
            size_t hash = hashCode(key);
            size_t node = hash % nNodes;
            if (buffers[node]->size + sizeof(T) > bufferSize) {
                if (node == nodeId) { 
                    queue->put(buffers[node]);
                    buffers[node] = Buffer::create(queue->qid, bufferSize);
                } else { 
                    cluster->sockets[node]->write(buffers[node], BUF_HDR_SIZE + buffers[node]->size);
                }
                buffers[node]->size = 0;
            }
            memcpy(buffers[node]->data + buffers[node]->size, &record, sizeof(T));
            buffers[node]->size += sizeof(T);
        }

        for (size_t node = 0; node < nNodes; node++) {
            if (node != nodeId) { 
                if (buffers[node]->size != 0) { 
                    cluster->sockets[node]->write(buffers[node], BUF_HDR_SIZE + buffers[node]->size);
                    buffers[node]->size = 0;
                }
                cluster->sockets[node]->write(buffers[node], BUF_HDR_SIZE);
                delete buffers[node];
            } else { 
                queue->put(buffers[node]);
                if (buffers[node]->size != 0) { 
                    queue->put(Buffer::create(queue->qid, 0));
                }
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
        if (used == size) { 
            delete buf;
            buf = queue->get();
            if (buf->isEof()) {
                return false;
            }
            used = 0;
            size = buf->size / sizeof(T);
            assert(size*sizeof(T) == buf->size);
        }
        record = *((T*)buf->data + used);
        used += 1;
        return true;
    }

    GatherRDD(Queue* q) : buf(NULL), used(0), size(0), queue(q) {}
    ~GatherRDD() { delete[] buf; }
private:
    Buffer* buf;
    size_t used;
    size_t size;
    Queue* queue;
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
        Queue* queue = Cluster::getQueue();
        if (!Cluster::isCoordinator()) { 
            GatherRDD< Pair<K,V> > gather(queue);
            queue->put(Buffer::create(queue->qid, 0)); // do not wait for this node
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
        Cluster::freeQueue(queue);
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
        Queue* queue = Cluster::getQueue();
        if (Cluster::isCoordinator()) {         
            Thread(new FetchJob<T>(input, queue));
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
        Cluster::freeQueue(queue);
        delete input;
        i = 0;
    }
};

template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner)>
class HashJoinRDD : public RDD< Join<O,I> >
{
public:
    HashJoinRDD(RDD<O>* outerRDD, RDD<I>* innerRDD, size_t estimation, bool outerJoin) 
    : isOuterJoin(outerJoin), table(new Entry*[estimation]), size(estimation), inner(NULL) {
        queue = Cluster::getQueue();
        Thread(new ScatterJob<I,K,innerKey>(innerRDD, queue));
        loadHash(new GatherRDD<I>(queue));
        scatter = new Thread(new ScatterJob<O,K,outerKey>(outerRDD, queue));
        outer = new GatherRDD<O>(queue);
    }

    bool next(Join<O,I>& record)
    {
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
        Cluster::freeQueue(queue);
    }
private:
    struct Entry {
        K      key;
        I      record;
        Entry* collision;
        size_t hash;
    };
    
    RDD<O>* outer;
    Queue*  queue;
    Thread* scatter;
    bool    const isOuterJoin;
    Entry** const table;
    size_t  const size;
    O       outerRec;
    I       innerRec;
    K       key;
    size_t  hash;
    Entry*  inner;

    void loadHash(RDD<I>* gather) {
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
        }
        printf("HashJoin: estimated size=%ld, real size=%ld\n", size, realSize);
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
void RDD<T>::print(FILE* out) 
{
    Queue* queue = Cluster::getQueue();
    if (Cluster::isCoordinator()) {         
        Thread(new FetchJob<T>(this, queue));
        GatherRDD<T> gather(queue);
        T record;
        while (gather.next(record)) { 
            record.print(out);
        }
    } else {         
        sendToCoordinator<T>(this, queue);
    }
}

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
RDD< Join<T,I> >* RDD<T>::join(RDD<I>* with, size_t estimation, bool outerJoin) {
    return new HashJoinRDD<T,I,K,outerKey,innerKey>(this, with, estimation, outerJoin);
}
    
