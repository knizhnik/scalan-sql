#ifndef __RDD_H__
#define __RDD_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "cluster.h"
#include "pack.h"
#include "hash.h"

#ifndef PARALLEL_INNER_OUTER_TABLES_LOAD
#define PARALLEL_INNER_OUTER_TABLES_LOAD 1
#endif

#ifndef UNKNOWN_ESTIMATION
#define UNKNOWN_ESTIMATION (1024*1024)
#endif

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

/**
 *  Result of join
 */
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
    friend void print(Join const& r, FILE* out)
    {
        print((Outer&)r, out);
        fputs(", ", out);
        print((Inner&)r, out);
    }
};

/**
 * Fixed size string key (used to wrap C char arrays)
 */
template<class T>
struct Key
{
    T val;
    
    bool operator==(Key const& other) const
    {
        return strncmp(val, other.val, sizeof(val)) == 0;
    }
    
    friend size_t hashCode(Key const& key)
    {
        return ::hashCode(key.val);
    }
    
    friend void print(Key const& key, FILE* out) 
    {
        fprintf(out, "%.*s", (int)sizeof(key.val), key.val);
    }
    friend size_t unpack(Key& dst, char const* src)
    {
        return strcopy(dst.val, src, sizeof(dst.val));
    }

    friend size_t pack(Key const& src, char* dst)
    {
        return strcopy(dst, src.val, sizeof(src.val));
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

/**
 * Join kind
 */
enum JoinKind 
{
    InnerJoin,
    OuterJoin, 
    AntiJoin
};

#define typeof(rdd) decltype((rdd)->elem)

/**
 * Abstract RDD (Resilient Distributed Dataset)
 */
template<class T>
class RDD
{
  public:
    /**
     * RDD element. It is used for getting RDD elementtype using typeof macro
     */
    T elem;

    /**
     * Main RDD method for iterating though records.
     * To avoid virtual call overhead and make it possible for compiler to inline method calls implementation
     * of each RDD defines its own version of next() method which is atatically invoked using template mechanism.
     * @param record [out] placeholder for the next record
     * @return true if there is next record, false otherwise
     */
    bool next(T& record) 
    {
        return getNext(record);
    }

    /**
     * Virtual iteration method which should be overriden by derived classes.
     * Because of virtual call overhead getNext method is not actually used in RDD implementations, instead of it them
     * invokes next() method using template mechanism. But appliation can use this method to access arbitrary RDD.
     * @param record [out] placeholder for the next record
     * @return true if there is next record, false otherwise
     */
    virtual bool getNext(T& record) = 0;

    /**
     * Filter input RDD
     * @return RDD with records matching predicate 
     */
    template<bool (*predicate)(T const&)>
    RDD<T>* filter();

    /**
     * Perfrom map-reduce
     * @param estimation esimation for number of pairs
     * @return RDD with &lt;key,value&gt; pairs
     */
    template<class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src)>
    RDD< Pair<K,V> >* mapReduce(size_t estimation = UNKNOWN_ESTIMATION);

    /**
     * Perform aggregation of input RDD 
     * @param initState initial aggregate value
     * @return RDD with aggregated value
     */
    template<class S,void (*accumulate)(S& state,  T const& in),void (*combine)(S& state, S const& in)>
    RDD<S>* reduce(S const& initState);

    /**
     * Map records of input RDD
     * @return projection of the input RDD. 
     */
    template<class P, void (*projection)(P& out, T const& in)>
    RDD<P>* project();

    /**
     * Sort input RDD
     * @param estimation estimation for number of records in RDD
     * @return RDD with sorted records
     */
    template<int (*compare)(T const* a, T const* b)> 
    RDD<T>* sort(size_t estimation = UNKNOWN_ESTIMATION);

    /**
     * Find top N records according to provided comparison function
     * @param n number of returned top records
     * @return RDD with up to N top records
     */
    template<int (*compare)(T const* a, T const* b)> 
    RDD<T>* top(size_t n);

    /**
     * Join of two RDDs. Inner join returns pairs of matches records in outer and inner table.
     * Outer join also returns records from outer table for which there are matching in inner table.
     * @param with inner join table
     * @param estimation estimation for number of joined records
     * @param kind join kind (inner/outer join)
     */
    template<class I, class K, void (*outerKey)(K& key, T const& outer), void (*innerKey)(K& key, I const& inner)>
    RDD< Join<T,I> >* join(RDD<I>* with, size_t estimation = UNKNOWN_ESTIMATION, JoinKind kind = InnerJoin);

    /**
     * Semijoin of two RDDs. Semijoin find matched records in both tables but returns only records from outer table.
     * Antijoin returns only this records of outer table for which there are no matching in inner table.
     * @param with inner join table
     * @param estimation estimation for number of joined records
     * @param kind join kind (inner/anti join)
     */
    template<class I, class K, void (*outerKey)(K& key, T const& outer), void (*innerKey)(K& key, I const& inner)>
    RDD<T>* semijoin(RDD<I>* with, size_t estimation = UNKNOWN_ESTIMATION, JoinKind kind = InnerJoin);

    /**
     * Replicate data between all nodes.
     * Broadcast local RDD data to all nodes and gather data from all nodes.
     * As a result all nodes get the same replicas of input data
     * @return replicated RDD, combining data from all nodes
     */
    virtual RDD<T>* replicate();

    /**
     * Get single record from input RDD or substitute it with default value of RDD is empty.
     * This method is usful for obtaining aggregation result
     * @return Single record from input RDD or substitute it with default value of RDD is empty.
     */
    T result(T const& defaultValue) {
        T record;
        return next(record) ? record : defaultValue;
    }

    /**
     * Print RDD records to the stream
     * @param out output stream
     */
    void output(FILE* out);

    virtual~RDD() {}
};

template<class T, class Rdd>
auto replicate(Rdd* in);

/**
 * Tranfer data from RDD to queue
 */
template<class T, class Rdd = RDD<T> >
inline void enqueue(Rdd* input, size_t node, Queue* queue) 
{
    Cluster* cluster = Cluster::instance.get();
    qid_t qid = queue->qid;
    size_t bufferSize = cluster->bufferSize;
    Buffer* buf = Buffer::create(qid, bufferSize);
    size_t size, used = 0;
    T record;
    while (input->next(record)) { 
        if (used + sizeof(T) > bufferSize) { 
            buf->size = used;
            cluster->send(node, queue, buf);
            buf = Buffer::create(qid, bufferSize);
            used = 0;
        }
        size = pack(record, buf->data + used);
        assert(size <= sizeof(T));
        used += size;
    }
    buf->size = used;
    if (used != 0) { 
        cluster->send(node, queue, buf);
        cluster->send(node, queue, Buffer::eof(qid));
    } else { 
        buf->kind = MSG_EOF;
        cluster->send(node, queue, buf);
    }
}

/**
 * Send data to coordinator
 */
template<class T, class Rdd = RDD<T> >
inline void sendToCoordinator(Rdd* input, Queue* queue) 
{
    enqueue<T,Rdd>(input, COORDINATOR, queue);
}

/**
 * Fetch data from RDD and place it in queue
 */
template<class T, class Rdd = RDD<T> >
class FetchJob : public Job
{
public:
    FetchJob(Rdd* in, Queue* q) : input(in), queue(q) {}

    void run()
    {        
        enqueue<T,Rdd>(input, Cluster::instance->nodeId, queue);
    }
private:
    Rdd* const input;
    Queue* const queue;
};

/**
 * Scatter RDD data between nodes using provided distribution key and hash function
 */
template<class T, class K, void (*dist_key)(K& key, T const& record), class Rdd = RDD<T> >
class ScatterJob : public Job
{
public:
    ScatterJob(Rdd* in, Queue* q) : input(in), queue(q) {}
    ~ScatterJob() { delete input; }
    
    void run()
    {
        K key;
        T record;
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
                sent += buffers[node]->size;
                cluster->send(node, queue, buffers[node]);
                buffers[node] = Buffer::create(queue->qid, bufferSize);
                buffers[node]->size = 0;
                
                if (sent > cluster->syncInterval) { 
                    for (size_t node = 0; node < nNodes; node++) {
                        if (node != nodeId) { 
                            cluster->send(node, queue, Buffer::ping(queue->qid));
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
            if (buffers[node]->size != 0) { 
                sent += buffers[node]->size;
                cluster->send(node, queue, buffers[node]);
                cluster->send(node, queue, Buffer::eof(queue->qid));
            } else { 
                buffers[node]->kind = MSG_EOF;
                cluster->send(node, queue, buffers[node]);
            }                
        }
        delete[] buffers;
    }
private:
    Rdd* const input;
    Queue* const queue;
};


/**
 * RDD representing result of gathering data from multiple nodes (opposite to Scatter)
 */
template<class T>
class GatherRDD : public RDD<T>
{
public:
    bool next(T& record) {
        Cluster* cluster = Cluster::instance.get();
        while (used == size) { 
            if (buf != NULL) { 
                buf->release();
            }
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

    bool getNext(T& record) override { 
        return next(record);
    }

    GatherRDD(Queue* q) : buf(NULL), used(0), size(0), queue(q), nWorkers(Cluster::instance->nNodes) {}
    ~GatherRDD() { 
        if (buf != NULL) { 
            buf->release(); 
        }
    }
private:
    Buffer* buf;
    size_t used;
    size_t size;
    Queue* queue;
    size_t nWorkers;
};

/**
 * RDD for fetching elements from buffer, used in message handlers
 */
template<class T>
class BufferRDD : public RDD<T>
{
  public:
    bool next(T& record) {
        if (pos == buf->size) { 
            return false;
        }
        pos += unpack(record, buf->data + pos);
        return true;
    }
    
    bool getNext(T& record) override { 
        return next(record);
    }

    BufferRDD(Buffer* buffer) : buf(buffer), pos(0) {}
    ~BufferRDD() { buf->release(); }

  private:
    Buffer* buf;
    size_t pos;
};

/**
 * Replicate RDD data to all nodes
 */
template<class T, class Rdd = RDD<T> >
class BroadcastJob : public Job
{
public:
    BroadcastJob(Rdd* in, Queue* q) : input(in), queue(q) {}
    ~BroadcastJob() { delete input; }
    
    void run()
    {
        T record;
        size_t nNodes = cluster->nNodes;
        size_t nodeId = cluster->nodeId;
        size_t bufferSize = cluster->bufferSize;
        Buffer* buffer = Buffer::create(queue->qid, bufferSize);        
        size_t size = 0;
        size_t sent = 0;

        while (input->next(record)) { 
            if (size + sizeof(T) > bufferSize) {
                sent += size;
                buffer->size = size;
                buffer->refCount = nNodes;
                for (size_t node = 0; node < nNodes; node++) { 
                    cluster->send(node, queue, buffer);
                    if (sent > cluster->syncInterval && node != nodeId) { 
                        cluster->sendQueues[node]->put(Buffer::ping(queue->qid));
                    }
                }
                if (sent > cluster->syncInterval) { 
                    queue->wait(nNodes-1);
                    sent = 0;
                }
                buffer = Buffer::create(queue->qid, bufferSize);
                size = 0;
            }
            size += pack(record, buffer->data + size);
            assert(size <= bufferSize);
        }
            
        if (size != 0) { 
            buffer->size = size;
            buffer->refCount = nNodes;
        } else { 
            buffer->release();
        }
        for (size_t node = 0; node < nNodes; node++) {
            if (size != 0) { 
                cluster->send(node, queue, buffer);
            }
            cluster->send(node, queue, Buffer::eof(queue->qid));
        }
    }
private:
    Rdd* const input;
    Queue* const queue;
};

/**
 * RDD representing result of replication
 */
template<class T, class Rdd = RDD<T> >
class ReplicateRDD : public GatherRDD<T>
{
public:
    ReplicateRDD(Rdd* input, Queue* queue) : GatherRDD<T>(queue), thread(new BroadcastJob<T,Rdd>(input, queue)) {}
private:
    Thread thread;
};
    

/**
 * Read data from OS plain file.
 * File can be used in two modes. In shared-nothing mode each node is expected to access its own local file.
 * Alternatively, file is present in some distributed file system and accessible by all nodes.
 * In this case file sis logically splited into parts, according to number of nodes in ther cluster, 
 * and each node access its own part of the file.
 */
template<class T>
class FileRDD : public RDD<T>
{
  public:
    FileRDD(char* path) : f(fopen(path, "rb")), segno(Cluster::instance->nodeId), split(Cluster::instance->sharedNothing ? 1 : Cluster::instance->nNodes) {
        assert(f != NULL);
        delete[] path;
        fseek(f, 0, SEEK_END);
        nRecords = (ftell(f)/sizeof(T)+split-1)/split;
        recNo = fseek(f, nRecords*segno*sizeof(T), SEEK_SET) == 0 ? 0 : nRecords;
    }

    RDD<T>* replicate() { 
        if (split != 1) { 
            nRecords *= split;
            recNo = 0;
            fseek(f, 0, SEEK_SET);
            return this;
        }
        return RDD<T>::replicate();
    }

    bool next(T& record) {
        return ++recNo <= nRecords && fread(&record, sizeof(T), 1, f) == 1;
    }
    
    bool getNext(T& record) override { 
        return next(record);
    }

    ~FileRDD() { fclose(f); }

  private:
    FILE* const f;    
    size_t segno;
    size_t split;
    long recNo;
    long nRecords;
};

/**
 * Read data from set of OS plain files located in specified directory.
 * Each node is given its own set of fileds.
 */
template<class T>
class DirRDD : public RDD<T>
{
  public:
    DirRDD(char* path) : dir(path), segno(Cluster::instance->nodeId), step(Cluster::instance->nNodes), split(Cluster::instance->split), f(NULL) {}

    RDD<T>* replicate() {
        if (Cluster::instance->sharedNothing) {
            return RDD<T>::replicate();
        }
        nRecords *= split;
        segno = 0;
        step = 1;
        split = 1;
        recNo = 0;
        return this;
    }

    bool next(T& record) {
        while (true) {
            if (f == NULL) { 
                char path[MAX_PATH_LEN];
                sprintf(path, "%s/%ld.rdd", dir, segno/split);
                f = fopen(path, "rb");
                if (f == NULL) { 
                    return false;
                }
                fseek(f, 0, SEEK_END);
                nRecords = (ftell(f)/sizeof(T)+split-1)/split;
                recNo = 0;
                int rc = fseek(f, nRecords*(segno%split)*sizeof(T), SEEK_SET);
                assert(rc == 0);
            }
            if (++recNo <= nRecords && fread(&record, sizeof(T), 1, f) == 1) { 
                return true;
            } else { 
                fclose(f);
                segno += step;
                f = NULL;
            }
        }
    }

    bool getNext(T& record) override { 
        return next(record);
    }

    ~DirRDD() {
        delete[] dir;
    }
    
  private:
    char* dir;
    size_t segno;
    size_t step;
    size_t split;
    long recNo;
    long nRecords;
    FILE* f;    
};

#if USE_PARQUET
#include "parquet.h"
#endif

/**
 * File manager to created proper file RDD based on file name
 */
class FileManager
{
public:
    template<class T>
    static RDD<T>* load(char* fileName) { 
        size_t len = strlen(fileName);
        
        return (strcmp(fileName + len - 4, ".rdd") == 0) 
            ? (RDD<T>*)new FileRDD<T>(fileName)
#if USE_PARQUET
            : (strcmp(fileName + len - 8, ".parquet") == 0) 
              ? Cluster::instance->sharedNothing 
                ? (RDD<T>*)new ParquetLocalRDD<T>(fileName)
                : (RDD<T>*)new ParquetRoundRobinRDD<T>(fileName)
#endif
              : (RDD<T>*)new DirRDD<T>(fileName);
    }
};

/**
 * Filter resutls using provided condition
 */
template<class T, bool (*predicate)(T const&), class Rdd = RDD<T> >
class FilterRDD : public RDD<T>
{
  public:
    FilterRDD(Rdd* input) : in(input) {}

    bool next(T& record) {
        while (in->next(record)) { 
            if (predicate(record)) {
                return true;
            }
        }
        return false;
    }

    bool getNext(T& record) override { 
        return next(record);
    }

    ~FilterRDD() { delete in; }

  private:
    Rdd* const in;
};
    
/**
 * Perform aggregation of input data (a-la Scala fold)
 */
template<class T, class S,void (*accumulate)(S& state, T const& in),void (*combine)(S& state, S const& in), class Rdd = RDD<T> >
class ReduceRDD : public RDD<S> 
{    
  public:
    ReduceRDD(Rdd* input, S const& initState) : state(initState), first(true) {
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

    bool getNext(S& record) override { 
        return next(record);
    }


  private:
    void aggregate(Rdd* input) { 
        T record;
        while (input->next(record)) { 
            accumulate(state, record);
        }
        Cluster* cluster = Cluster::instance.get();
        Queue* queue = cluster->getQueue();
        if (cluster->isCoordinator()) {
            S partialState;
            GatherRDD<S> gather(queue);
            queue->putFirst(Buffer::eof(queue->qid)); // do not wait for self node
            while (gather.next(partialState)) {
                combine(state, partialState);
            }
        } else {
            sendToCoordinator<S,ReduceRDD>(this, queue);            
        }
        delete input;
    }
    
    S state;
    bool first;
};
        
/**
 * Classical Map-Reduce
 */
template<class T,class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src), class Rdd = RDD<T> >
class MapReduceRDD : public RDD< Pair<K,V> > 
{    
  public:
    MapReduceRDD(Rdd* input, size_t estimation) : size(hashTableSize(estimation)), table(new Entry*[size]) {
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

    bool getNext(Pair<K,V>& record) override { 
        return next(record);
    }

    ~MapReduceRDD() { 
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
             

    void loadHash(Rdd* input) 
    {
        Entry* entry;
        T record;
        Pair<K,V> pair;

        memset(table, 0, size*sizeof(Entry*));
        
        size_t realSize = 0;
        while (input->next(record)) {
            map(pair, record);
            size_t hash = hashCode(pair.key);
            size_t h = MOD(hash, size);            
            for (entry = table[h]; !(entry == NULL || pair.key == entry->pair.key); entry = entry->collision);
            if (entry == NULL) { 
                entry = allocator.alloc();
                entry->collision = table[h];
                table[h] = entry;
                entry->pair = pair;
                if (++realSize > size) { 
                    extendHash();
                }
            } else { 
                reduce(entry->pair.value, pair.value);
            }
        }
        curr = NULL;
        i = 0;
        Cluster* cluster = Cluster::instance.get();
        Queue* queue = cluster->getQueue();
        if (cluster->isCoordinator()) { 
            GatherRDD< Pair<K,V> > gather(queue);
            queue->putFirst(Buffer::eof(queue->qid)); // do not wait for self node
            Pair<K,V> pair;
            while (gather.next(pair)) {
                size_t hash = hashCode(pair.key);
                size_t h = MOD(hash, size);            
                for (entry = table[h]; !(entry == NULL || pair.key == entry->pair.key); entry = entry->collision);
                if (entry == NULL) { 
                    entry = allocator.alloc();
                    entry->collision = table[h];
                    table[h] = entry;
                    entry->pair = pair;
                    if(++realSize > size) { 
                        extendHash();
                    }
                } else { 
                    reduce(entry->pair.value, pair.value);
                }                
            }            
        } else {
            sendToCoordinator<Pair<K,V>,MapReduceRDD>(this, queue);            
        }
        printf("HashAggregate: estimated size=%ld, real size=%ld\n", size, realSize);
        delete input;
    }

    void deleteHash() {
        delete[] table;
    }
};

/**
 * Project (map) RDD records
 */
template<class T, class P, void project(P& out, T const& in), class Rdd = RDD<T> >
class ProjectRDD : public RDD<P>
{
  public:
    ProjectRDD(Rdd* input) : in(input) {}

    bool next(P& projection) { 
        T record;
        if (in->next(record)) { 
            project(projection, record);
            return true;
        }
        return false;
    }

    bool getNext(P& record) override {
        return next(record);
    }

    ~ProjectRDD() { delete in; }

  private:
    Rdd* const in;
};

/**
 *  Sort using given comparison function
 */
template<class T, int compare(T const* a, T const* b), class Rdd = RDD<T> >
class SortRDD : public RDD<T>
{
  public:
    SortRDD(Rdd* input, size_t estimation) {
        loadArray(input, estimation);
    }

    bool next(T& record) { 
        if (i < size) { 
            record = buf[i++];
            return true;
        }
        return false;
    }
    
    bool getNext(T& record) override {
        return next(record);
    }

    ~SortRDD() { 
        delete[] buf;
    }

  private:
    T* buf;
    size_t size;
    size_t i;

    typedef int(*comparator_t)(void const* p, void const* q);

    void loadArray(Rdd* input, size_t estimation) { 
        Cluster* cluster = Cluster::instance.get();
        Queue* queue = cluster->getQueue();
        if (cluster->isCoordinator()) {         
            Thread loader(new FetchJob<T,Rdd>(input, queue));
            GatherRDD<T> gather(queue);
            buf = new T[estimation];
            for (size = 0; gather.next(buf[size]);) { 
                if (++size == estimation) { 
                    T* newBuf = new T[estimation *= 2];
                    printf("Extend sort buffer to %ld\n", estimation);
                    memcpy(newBuf, buf, size*sizeof(T));
                    delete[] buf;
                    buf = newBuf;
                }
            }
            qsort(buf, size, sizeof(T), (comparator_t)compare);
        } else { 
            sendToCoordinator<T,Rdd>(input, queue);
            buf = NULL;
            size = 0;
        }
        delete input;
        i = 0;
    }
};

/**
 * Get top N records using given comparison function
 */
template<class T, int compare(T const* a, T const* b), class Rdd = RDD<T> >
class TopRDD : public RDD<T>
{
  public:
    TopRDD(Rdd* input, size_t top) : i(0) {
        buf = new T[top];
        size_t n = loadTop(input, 0, top);
        delete input;
        
        Cluster* cluster = Cluster::instance.get();
        Queue* queue = cluster->getQueue();
        if (cluster->isCoordinator()) {         
            GatherRDD<T> gather(queue);
            queue->putFirst(Buffer::eof(queue->qid)); // Coordinator already finished it's part of work
            size = loadTop(&gather, n, top);
        } else {
            assert(n*sizeof(T) < cluster->bufferSize);
            Buffer* msg = Buffer::create(queue->qid, n*sizeof(T));
            size_t used = 0;
            for (size_t j = 0; j < n; j++) {
                used += pack(buf[j], msg->data + used);
            }
            assert(used <= n*sizeof(T));
            msg->size = used;
            cluster->send(COORDINATOR, queue, msg);
            cluster->send(COORDINATOR, queue, Buffer::eof(queue->qid));
            size = 0;
        }
    }

    bool next(T& record) { 
        if (i < size) { 
            record = buf[i++];
            return true;
        }
        return false;
    }
    
    bool getNext(T& record) override {
        return next(record);
    }

    ~TopRDD() { 
        delete[] buf;
    }

  private:
    T* buf;
    size_t size;
    size_t i;
    
    template<class IRdd>
    size_t loadTop(IRdd* input, size_t n, size_t top) { 
        T record;
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
        return n;
    }
};

/**
 * Join two RDDs using hash table.
 * Depending on estimated result size and broadcastJoinThreshold, this RDD either broadcast inner table, 
 * either shuffle inner table 
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner), class ORdd = RDD<O>, class IRdd = RDD<I> >
class HashJoinRDD : public RDD< Join<O,I> >, MessageHandler
{
public:
    HashJoinRDD(ORdd* outerRDD, IRdd* innerRDD, size_t estimation, JoinKind joinKind) 
    : kind(joinKind), table(NULL), size(hashTableSize(estimation)), innerSize(0), entry(NULL), inner(innerRDD), outer(outerRDD), scatter(NULL), gather(NULL), innerIsShuffled(false) 
    {
        assert(kind != AntiJoin);

        Cluster* cluster = Cluster::instance.get();
#ifdef USE_MESSAGE_HANDLER
        innerQueue = cluster->getQueue(this);
#else
        innerQueue = cluster->getQueue();
#endif
        outerQueue = Cluster::instance->getQueue();
    }

    bool next(Join<O,I>& record)
    {
        Cluster* cluster = Cluster::instance.get();
        if (table == NULL) { 
            bool replicateInner = size <= cluster->broadcastJoinThreshold;
#if 0
            if (replicateInner) {             
                size *= cluster->nNodes; // adjust hash table size 
            }
#endif
            table = new Entry*[size];
            memset(table, 0, size*sizeof(Entry*));
            
#if PARALLEL_INNER_OUTER_TABLES_LOAD
            loadOuterTable(replicateInner);
#endif
            if (replicateInner) {             
                loadHash(replicate<I,IRdd>(inner));
            } else { 
                Thread loader(new ScatterJob<I,K,innerKey,IRdd>(inner, innerQueue));
                loadHash(new GatherRDD<I>(innerQueue));
                innerIsShuffled = true;
            }                
#if !PARALLEL_INNER_OUTER_TABLES_LOAD
            loadOuterTable(replicateInner);
#endif
        }
        if (entry == NULL) { 
            do { 
                if (!(scatter ? gather->next(outerRec) : outer->next(outerRec))) { 
                    entry = NULL;
                    return false;
                }
                outerKey(key, outerRec);
                size_t h = MOD(hashCode(key), size);
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

    bool getNext(Join<O,I>& record) override 
    {
        return next(record);
    }

    ~HashJoinRDD() { 
        deleteHash();
        delete outer;
        delete gather;
        delete scatter;
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
    size_t  innerSize;
    O       outerRec;
    I       innerRec;
    K       key;
    size_t  hash;
    Entry*  entry;
    IRdd*   inner;
    ORdd*   outer;
    Queue*  innerQueue;
    Queue*  outerQueue;
    Thread* scatter;
    GatherRDD<O>* gather;
    bool    innerIsShuffled;
    BlockAllocator<Entry> allocator;

    void loadOuterTable(bool replicateInner)
    {
        if (!replicateInner) {
            scatter = new Thread(new ScatterJob<O,K,outerKey,ORdd>(outer, outerQueue));
            gather = new GatherRDD<O>(outerQueue);
            outer = NULL;
        }
    }

    void extendHash() 
    {
        Entry *entry, *next;
        size_t newSize = hashTableSize(size+1);
        Entry** newTable = new Entry*[newSize];
        memset(newTable, 0, newSize*sizeof(Entry*));
        for (size_t i = 0; i < size; i++) { 
            for (entry = table[i]; entry != NULL; entry = next) {
                K key;
                innerKey(key, entry->record);
                size_t h = MOD(hashCode(key), newSize);
                next = entry->collision;
                entry->collision = newTable[h];
                newTable[h] = entry;
            }
        }
        delete[] table;
        table = newTable;
        size = newSize;
    }
             
    void handle(Buffer* buf) 
    {
        BufferRDD<I> input(buf);
        Entry* entry = new Entry();       
        size_t n = 0;
        while (input.next(entry->record)) { 
            K key;
            innerKey(key, entry->record);
            size_t h = MOD(hashCode(key), size);  
            Entry* oldValue;
            do {
                oldValue = table[h];
                entry->collision = oldValue; 
            } while (!__sync_bool_compare_and_swap(&table[h], oldValue, entry));
            entry = new Entry();
            n += 1;
        }
        __sync_add_and_fetch(&innerSize, n);
        delete entry;
    }   

    template<class Rdd>
    void loadHash(Rdd* gather) 
    {
        Entry* entry = allocator.alloc();
        size_t realSize = 0;
        while (gather->next(entry->record)) {
            K key;
            innerKey(key, entry->record);
            size_t h = MOD(hashCode(key), size);  
            entry->collision = table[h]; 
            table[h] = entry;
            entry = allocator.alloc();
            if (++realSize == size) {
                extendHash();
            }
        }
        innerSize += realSize;
#ifdef SHOW_HASH_STATISTIC
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
        printf("HashJoin: estimated size=%ld, real size=%ld, collisions=(%ld max, %f avg)\n", size, realSize, maxLen, nChains != 0 ? (double)totalLen/nChains : 0.0);
#endif
        delete gather;
    }
    void deleteHash() {
#ifdef USE_MESSAGE_HANDLER
        if (innerIsShuffled) { 
            for (size_t i = 0; i < size; i++) { 
                Entry *curr, *next;
                for (curr = table[i]; curr != NULL; curr = next) { 
                    next = curr->collision;
                    delete curr;
                }
            }
        }
#endif
        delete[] table;
    }
};
    
/**
 * Semijoin two RDDs using hash table
 * Depending on estimated result size and broadcastJoinThreshold, this RDD either broadcast inner table, 
 * either shuffle inner table 
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner), class ORdd = RDD<O>, class IRdd = RDD<I> >
class HashSemiJoinRDD : public RDD<O>, MessageHandler
{
public:
    HashSemiJoinRDD(ORdd* outerRDD, IRdd* innerRDD, size_t estimation, JoinKind joinKind) 
    : kind(joinKind), table(NULL), size(hashTableSize(estimation)), inner(innerRDD), outer(outerRDD), scatter(NULL), gather(NULL), innerIsShuffled(false)
    {
        assert(kind != OuterJoin);
        Cluster* cluster = Cluster::instance.get();
#ifdef USE_MESSAGE_HANDLER
        innerQueue = cluster->getQueue(this);
#else
        innerQueue = cluster->getQueue();
#endif
        outerQueue = Cluster::instance->getQueue();
    }

    bool next(O& record)
    {
        Cluster* cluster = Cluster::instance.get();
        if (table == NULL) { 
            bool replicateInner = size <= cluster->broadcastJoinThreshold;
#if 0
            if (replicateInner) {             
                size *= cluster->nNodes; // adjust hash table size 
            }
#endif
            table = new Entry*[size];
            memset(table, 0, size*sizeof(Entry*));
            
#if PARALLEL_INNER_OUTER_TABLES_LOAD
            loadOuterTable(replicateInner);
#endif
            if (replicateInner) {             
                loadHash(replicate<I,IRdd>(inner));
            } else { 
                Thread loader(new ScatterJob<I,K,innerKey,IRdd>(inner, innerQueue));
                loadHash(new GatherRDD<I>(innerQueue));
                innerIsShuffled = true;
            }                
#if !PARALLEL_INNER_OUTER_TABLES_LOAD
            loadOuterTable(replicateInner);
#endif
        }
        while (scatter ? gather->next(record) : outer->next(record)) { 
            K key;
            outerKey(key, record);
            Entry* entry;            
            for (entry = table[MOD(hashCode(key), size)]; !(entry == NULL || entry->equalsKey(key)); entry = entry->collision);
            if ((entry != NULL) ^ (kind == AntiJoin)) {
                return true;
            }
        }
        return false;
    }

    bool getNext(O& record) override
    {
        return next(record);
    }

    ~HashSemiJoinRDD() { 
        deleteHash();
        delete outer;
        delete gather;
        delete scatter;
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
    size_t  innerSize;
    IRdd*   inner;
    ORdd*   outer;
    Queue*  innerQueue;
    Queue*  outerQueue;
    Thread* scatter;
    GatherRDD<O>* gather;
    bool    innerIsShuffled;
    BlockAllocator<Entry> allocator;

    void loadOuterTable(bool replicateInner)
    {
        if (!replicateInner) {
            scatter = new Thread(new ScatterJob<O,K,outerKey,ORdd>(outer, outerQueue));
            gather = new GatherRDD<O>(outerQueue);
            outer = NULL;
        }
    }

    void extendHash() 
    {
        Entry *entry, *next;
        size_t newSize = hashTableSize(size+1);
        Entry** newTable = new Entry*[newSize];
        memset(newTable, 0, newSize*sizeof(Entry*));
        for (size_t i = 0; i < size; i++) { 
            for (entry = table[i]; entry != NULL; entry = next) { 
                K key;
                innerKey(key, entry->record);
                size_t h = MOD(hashCode(key), newSize);
                next = entry->collision;
                entry->collision = newTable[h];
                newTable[h] = entry;
            }
        }
        delete[] table;
        table = newTable;
        size = newSize;
    }
             
    void handle(Buffer* buf) 
    {
        BufferRDD<I> input(buf);
        Entry* entry = new Entry();       
        size_t n = 0;
        while (input.next(entry->record)) { 
            K key;
            innerKey(key, entry->record);
            size_t h = MOD(hashCode(key), size);  
            Entry* oldValue;
            do {
                oldValue = table[h];
                entry->collision = oldValue; 
            } while (!__sync_bool_compare_and_swap(&table[h], oldValue, entry));
            entry = new Entry();
            n += 1;
        }
        __sync_add_and_fetch(&innerSize, n);
        delete entry;
    }   


    template<class Rdd>
    void loadHash(Rdd* gather) 
    {
        Entry* entry = allocator.alloc();
        size_t realSize = 0;
        while (gather->next(entry->record)) {
            K key;
            innerKey(key, entry->record);
            size_t h = MOD(hashCode(key), size);  
            entry->collision = table[h]; 
            table[h] = entry;
            entry = allocator.alloc();
            if (++realSize == size) { 
                extendHash();
            }
        }
        innerSize += realSize;
#ifdef SHOW_HASH_STATISTIC
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
        printf("HashSemiJoin: estimated size=%ld, real size=%ld, collisions=(%ld max, %f avg)\n", size, realSize, maxLen, nChains != 0 ? (double)totalLen/nChains : 0.0);
#endif
        delete gather;
    }

    void deleteHash() {
#ifdef USE_MESSAGE_HANDLER
        if (innerIsShuffled) { 
            for (size_t i = 0; i < size; i++) { 
                Entry *curr, *next;
                for (curr = table[i]; curr != NULL; curr = next) { 
                    next = curr->collision;
                    delete curr;
                }
            }
        }
#endif
        delete[] table;
    }
};
  

/**
 * Join two RDDs using shuffle join.
 * This join method is used when estimated result size is larger than inmemJoinThreshold.
 * In this case inner and outer tables are shuffled into N files, where N=estimation/inmemJoinThreshold.
 * The inner table files are one-by-one loaded in memory (placed in hash table and records frim corresponding 
 * outer table files are located in this hash table.
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner), class ORdd = RDD<O>, class IRdd = RDD<I> >
class ShuffleJoinRDD : public RDD< Join<O,I> >
{
public:
    ShuffleJoinRDD(ORdd* outerRDD, IRdd* innerRDD, size_t nShuffleFiles, size_t estimation, JoinKind joinKind) 
    : kind(joinKind), size(hashTableSize(estimation)), table(new Entry*[size]), nFiles(nShuffleFiles), outerFile(NULL), inner(NULL) {
        assert(kind != AntiJoin);
        Cluster* cluster = Cluster::instance.get();
        Queue* innerQueue = cluster->getQueue();
        Queue* outerQueue = cluster->getQueue();
        qid = innerQueue->qid;
        {
            // shuffle inner RDD
            Thread loader(new ScatterJob<I,K,innerKey,IRdd>(innerRDD, innerQueue));
            saveInnerFiles(new GatherRDD<I>(innerQueue));
        }
        {
            // shuffle outer RDD
            Thread loader(new ScatterJob<O,K,outerKey,ORdd>(outerRDD, outerQueue));
            saveOuterFiles(new GatherRDD<O>(outerQueue));
        }
        fileNo = 0;
    }

    bool next(Join<O,I>& record)
    {
        if (inner == NULL) { 
            do { 
                if (outerFile == NULL) {
                    if (fileNo == nFiles) { 
                        return false;
                    }
                    fileNo += 1;
                    Cluster* cluster = Cluster::instance.get();
                    FILE* innerFile = cluster->openTempFile("inner", qid, fileNo);
                    outerFile = cluster->openTempFile("outer", qid, fileNo);

                    memset(table, 0, size*sizeof(Entry*));
                    allocator.reset();

                    Entry* entry = allocator.alloc();
                    while (fread(&entry->record, sizeof(I), 1, innerFile) == 1) { 
                        K key;
                        innerKey(key, entry->record);
                        size_t h = MOD(hashCode(key), size);  
                        entry->collision = table[h]; 
                        table[h] = entry;
                        entry = allocator.alloc();
                    }
                    fclose(innerFile);
                }
                if (fread(&outerRec, sizeof(O), 1, outerFile) != 1) { 
                    fclose(outerFile);
                    outerFile = NULL;
                } else { 
                    outerKey(key, outerRec);
                    size_t h = MOD(hashCode(key), size);
                    for (inner = table[h]; !(inner == NULL || inner->equalsKey(key)); inner = inner->collision);
                }
            } while (outerFile == NULL || (inner == NULL && kind == InnerJoin));
            
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
        } while (!(inner == NULL || inner->equalsKey(key)));

        return true;
    }

    bool getNext(Join<O,I>& record) override
    {
        return next(record);
    }

    ~ShuffleJoinRDD() { 
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
    size_t  const size;
    Entry** const table;
    size_t  const nFiles;
    FILE*   outerFile;
    O       outerRec;
    I       innerRec;
    K       key;
    size_t  hash;
    Entry*  inner;
    qid_t   qid;
    size_t  fileNo;
    BlockAllocator<Entry> allocator;
    
    void saveOuterFiles(ORdd* input)
    {
        FILE** files = new FILE*[nFiles];
        Cluster* cluster = Cluster::instance.get();
        for (size_t i = 0; i < nFiles; i++) {
            files[i] = cluster->openTempFile("outer", qid, i+1, "w");
        }
        O record;
        size_t nNodes = cluster->nNodes;
        while (input->next(record)) { 
            K key;
            outerKey(key, record);
            size_t h = hashCode(key) / nNodes % nFiles;
            fwrite(&record, sizeof record, 1, files[h]);
        }
        for (size_t i = 0; i < nFiles; i++) {
            fclose(files[i]);
        }
        delete[] files;
        delete input;
    }
            
    void saveInnerFiles(IRdd* input)
    {
        FILE** files = new FILE*[nFiles];
        Cluster* cluster = Cluster::instance.get();
        for (size_t i = 0; i < nFiles; i++) {
            files[i] = cluster->openTempFile("inner", qid, i+1, "w");
        }
        I record;
        size_t nNodes = cluster->nNodes;
        while (input->next(record)) { 
            K key;
            innerKey(key, record);
            size_t h = hashCode(key) / nNodes % nFiles;
            fwrite(&record, sizeof record, 1, files[h]);
        }
        for (size_t i = 0; i < nFiles; i++) {
            fclose(files[i]);
        }
        delete[] files;
        delete input;
    }
};
    
  
/**
 * Semijoin two RDDs using shuffle join.
 * This join method is used when estimated result size is larger than inmemJoinThreshold.
 * In this case inner and outer tables are shuffled into N files, where N=estimation/inmemJoinThreshold.
 * The inner table files are one-by-one loaded in memory (placed in hash table and records frim corresponding 
 * outer table files are located in this hash table.
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner), class ORdd = RDD<O>, class IRdd = RDD<I> >
class ShuffleSemiJoinRDD : public RDD<O>
{
public:
    ShuffleSemiJoinRDD(ORdd* outerRDD, IRdd* innerRDD, size_t nShuffleFiles, size_t estimation, JoinKind joinKind) 
    : kind(joinKind), size(estimation), table(new Entry*[size]), nFiles(nShuffleFiles), outerFile(NULL) {
        assert(kind != OuterJoin);
        Cluster* cluster = Cluster::instance.get();
        Queue* innerQueue = cluster->getQueue();
        Queue* outerQueue = cluster->getQueue();
        qid = innerQueue->qid;
        {
            // shuffle inner RDD
            Thread loader(new ScatterJob<I,K,innerKey,IRdd>(innerRDD, innerQueue));
            saveInnerFiles(new GatherRDD<I>(innerQueue));
        }
        {
            // shuffle outer RDD
            Thread loader(new ScatterJob<O,K,outerKey,ORdd>(outerRDD, outerQueue));
            saveOuterFiles(new GatherRDD<O>(outerQueue));
        }
        fileNo = 0;
    }

    bool next(Join<O,I>& record)
    {
        Cluster* cluster = Cluster::instance.get();
        while (true) { 
            if (outerFile == NULL) {
                if (fileNo == nFiles) { 
                    return false;
                }
                fileNo += 1;
                FILE* innerFile = cluster->openTempFile("inner", qid, fileNo);
                outerFile = cluster->openTempFile("outer", qid, fileNo);
                
                memset(table, 0, size*sizeof(Entry*));
                allocator.reset();                

                Entry* entry = allocator.alloc();

                while (fread(&entry->record, sizeof(I), 1, innerFile) == 1) { 
                    K key;
                    innerKey(key, entry->record);
                    size_t h = MOD(hashCode(key), size);  
                    entry->collision = table[h]; 
                    table[h] = entry;
                    entry = allocator.alloc();
                }
                fclose(innerFile);
            }
            if (fread(&record, sizeof(O), 1, outerFile) != 1) { 
                fclose(outerFile);
                outerFile = NULL;
            } else { 
                K key;
                outerKey(key, record);
                size_t h = MOD(hashCode(key), size);
                Entry* inner;            
                for (inner = table[h]; !(inner == NULL || inner->equalsKey(key)); inner = inner->collision);
                if ((inner != NULL) ^ (kind == AntiJoin)) {
                    return true;
                }                
            }
        }
    }

    bool getNext(Join<O,I>& record) override
    {
        return next(record);
    }

    ~ShuffleSemiJoinRDD() { 
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
    Entry** const table;
    size_t  const nFiles;
    size_t  const size;
    FILE*   outerFile;
    qid_t   qid;
    size_t  fileNo;
    BlockAllocator<Entry> allocator;
    
    void saveOuterFiles(ORdd* input)
    {
        FILE** files = new FILE*[nFiles];
        Cluster* cluster = Cluster::instance.get();
        for (size_t i = 0; i < nFiles; i++) {
            files[i] = cluster->openTempFile("outer", qid, i+1, "w");
        }
        O record;
        size_t nNodes = cluster->nNodes;
        while (input->next(record)) { 
            K key;
            outerKey(key, record);
            size_t h = hashCode(key) / nNodes % nFiles;
            fwrite(&record, sizeof record, 1, files[h]);
        }
        for (size_t i = 0; i < nFiles; i++) {
            fclose(files[i]);
        }
        delete[] files;
        delete input;
    }
            
    void saveInnerFiles(IRdd* input)
    {
        FILE** files = new FILE*[nFiles];
        Cluster* cluster = Cluster::instance.get();
        for (size_t i = 0; i < nFiles; i++) {
            files[i] = cluster->openTempFile("inner", qid, i+1, "w");
        }
        I record;
        size_t nNodes = cluster->nNodes;
        while (input->next(record)) { 
            K key;
            innerKey(key, record);
            size_t h = hashCode(key) / nNodes % nFiles;
            fwrite(&record, sizeof record, 1, files[h]);
        }
        for (size_t i = 0; i < nFiles; i++) {
            fclose(files[i]);
        }
        delete[] files;
        delete input;
    }
};
    
  
/**
 * Cache RDD in memory
 */
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

    bool getNext(T& record) override { 
        return next(record);
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
                printf("Extend cache to %ld\n", estimation);
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

/**
 * In-memory columnar store RDD
 */
template<class H, class V, class C>
class ColumnarRDD : public RDD<V>
{
  public:
    ColumnarRDD(RDD<T>* input, size_t estimation) : cache(estimation), curr(0) { 
        H record;
        while (input->next(record)) { 
            cache.append(record);
        }
        delete input;
    }
    bool next(V& record) { 
        if (curr == cache.used) { 
            return false;
        }
        record.data = &cache;
        record.pos = curr++;
        return true;
    }

    ColumnarRDD* get() { 
        return new ColumnarRDD(cache);
    }

  private:
    ColumnarRDD(C const& _cache) : cache(_cache), curr(0) {}
    C cache;
};

template<class T>
void RDD<T>::output(FILE* out) 
{
    Cluster* cluster = Cluster::instance.get();
    Queue* queue = cluster->getQueue();
    if (cluster->isCoordinator()) {         
        Thread fetch(new FetchJob<T>(this, queue));
        GatherRDD<T> gather(queue);
        T record;
        while (gather.next(record)) { 
            print(record, out);
            fputc('\n', out);
        }
    } else {         
        sendToCoordinator<T>(this, queue);
    }
    cluster->barrier();
}

template<class T>
inline RDD<T>* RDD<T>::replicate() { 
    Queue* queue = Cluster::instance->getQueue();
    return new ReplicateRDD<T>(this, queue);
}

template<class T>
template<bool (*predicate)(T const&)>
inline RDD<T>* RDD<T>::filter() { 
    return new FilterRDD<T,predicate>(this);
}

template<class T>
template<class S,void (*accumulate)(S& state, T const& in), void(*combine)(S& state, S const& partial)>
inline RDD<S>* RDD<T>::reduce(S const& initState) {
    return new ReduceRDD<T,S,accumulate,combine>(this, initState);
}

template<class T>
template<class K,class V,void (*map_f)(Pair<K,V>& out, T const& in), void (*reduce_f)(V& dst, V const& src)>
inline RDD< Pair<K,V> >* RDD<T>::mapReduce(size_t estimation) {
    return new MapReduceRDD<T,K,V,map_f,reduce_f>(this, estimation);
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
template<int (*compare)(T const* a, T const* b)> 
inline RDD<T>* RDD<T>::top(size_t n) {
    return new TopRDD<T,compare>(this, n);
}

template<class T>
template<class I, class K, void (*outerKey)(K& key, T const& outer), void (*innerKey)(K& key, I const& inner)>
RDD< Join<T,I> >* RDD<T>::join(RDD<I>* with, size_t estimation, JoinKind kind) {
    Cluster* cluster = Cluster::instance.get();
    if (estimation <= cluster->inmemJoinThreshold) { 
        return new HashJoinRDD<T,I,K,outerKey,innerKey>(this, with, estimation, kind);
    }
    size_t nFiles = (estimation + cluster->inmemJoinThreshold - 1) / cluster->inmemJoinThreshold;
    return new ShuffleJoinRDD<T,I,K,outerKey,innerKey>(this, with, nFiles, estimation/nFiles, kind);
}

template<class T>
template<class I, class K, void (*outerKey)(K& key, T const& outer), void (*innerKey)(K& key, I const& inner)>
RDD<T>* RDD<T>::semijoin(RDD<I>* with, size_t estimation, JoinKind kind) 
{
    Cluster* cluster = Cluster::instance.get();
    if (estimation <= cluster->inmemJoinThreshold) { 
        return new HashSemiJoinRDD<T,I,K,outerKey,innerKey>(this, with, estimation, kind);
    }
    size_t nFiles = (estimation + cluster->inmemJoinThreshold - 1) / cluster->inmemJoinThreshold;
    return new ShuffleSemiJoinRDD<T,I,K,outerKey,innerKey>(this, with, nFiles, estimation/nFiles, kind);
}

template<class T, class Rdd>
inline void output(Rdd* in, FILE* out) 
{
    Cluster* cluster = Cluster::instance.get();
    Queue* queue = cluster->getQueue();
    if (cluster->isCoordinator()) {         
        Thread fetch(new FetchJob<T,Rdd>(in, queue));
        GatherRDD<T> gather(queue);
        T record;
        while (gather.next(record)) { 
            print(record, out);
            fputc('\n', out);
        }
    } else {         
        sendToCoordinator<T,Rdd>(in, queue);
    }
    cluster->barrier();
}

/**
 * Replicate data between all nodes.
 * Broadcast local RDD data to all nodes and gather data from all nodes.
 * As a result all nodes get the same replicas of input data
 * @return replicated RDD, combining data from all nodes
 */
template<class T, class Rdd>
inline auto replicate(Rdd* in) { 
    Queue* queue = Cluster::instance->getQueue();
    return new ReplicateRDD<T,Rdd>(in, queue);
}

/**
 * Filter input RDD
 * @return RDD with records matching predicate 
 */
template<class T, bool (*predicate)(T const&), class Rdd>
inline auto filter(Rdd* in) { 
    return new FilterRDD<T,predicate,Rdd>(in);
}

/**
 * Perform aggregation of input RDD 
 * @param initState initial aggregate value
 * @return RDD with aggregated value
 */
template<class T,class S,void (*accumulate)(S& state, T const& in), void(*combine)(S& state, S const& partial), class Rdd>
inline auto reduce(Rdd* in, S const& initState) {
    return new ReduceRDD<T,S,accumulate,combine,Rdd>(in, initState);
}

/**
 * Perfrom map-reduce
 * @param estimation esimation for number of pairs
 * @return RDD with &lt;key,value&gt; pairs
 */
template<class T,class K,class V,void (*map_f)(Pair<K,V>& out, T const& in), void (*reduce_f)(V& dst, V const& src), class Rdd>
inline auto mapReduce(Rdd* in, size_t estimation = UNKNOWN_ESTIMATION) {
    return new MapReduceRDD<T,K,V,map_f,reduce_f,Rdd>(in, estimation);
}

/**
 * Map records of input RDD
 * @return projection of the input RDD. 
 */
template<class T,class P, void (*projection)(P& out, T const& in), class Rdd>
inline auto project(Rdd* in) {
    return new ProjectRDD<T,P,projection,Rdd>(in);
}

/**
 * Sort input RDD
 * @param estimation estimation for number of records in RDD
 * @return RDD with sorted records
 */
template<class T, int (*compare)(T const* a, T const* b), class Rdd> 
inline auto sort(Rdd* in, size_t estimation = UNKNOWN_ESTIMATION) {
    return new SortRDD<T,compare,Rdd>(in, estimation);
}

/**
 * Find top N records according to provided comparison function
 * @param n number of returned top records
 * @return RDD with up to N top records
 */
template<class T, int (*compare)(T const* a, T const* b), class Rdd> 
inline auto top(Rdd* in, size_t n = UNKNOWN_ESTIMATION) {
    return new TopRDD<T,compare,Rdd>(in, n);
}

/**
 * In-memory hash join of two RDDs. Inner join returns pairs of matches records in outer and inner table.
 * Outer join also returns records from outer table for which there are matching in inner table.
 * @param with inner join table
 * @param estimation estimation for number of joined records
 * @param kind join kind (inner/outer join)
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner), class ORdd, class IRdd>
inline auto join(ORdd* outer, IRdd* inner, size_t estimation = UNKNOWN_ESTIMATION, JoinKind kind = InnerJoin) 
{
    return new HashJoinRDD<O,I,K,outerKey,innerKey,ORdd,IRdd>(outer, inner, estimation, kind);
}

/**
 * Disk shuffle join of two RDDs. Inner join returns pairs of matches records in outer and inner table.
 * Outer join also returns records from outer table for which there are matching in inner table.
 * @param with inner join table
 * @param estimation estimation for number of joined records
 * @param kind join kind (inner/outer join)
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner), class ORdd, class IRdd>
inline auto shuffleJoin(ORdd* outer, IRdd* inner, size_t estimation = UNKNOWN_ESTIMATION, JoinKind kind = InnerJoin) 
{
    Cluster* cluster = Cluster::instance.get();
    size_t nFiles = (estimation + cluster->inmemJoinThreshold - 1) / cluster->inmemJoinThreshold;
    return new ShuffleJoinRDD<O,I,K,outerKey,innerKey,ORdd,IRdd>(outer, inner, nFiles, estimation/nFiles, kind);
}

/**
 * In-memory hash semijoin of two RDDs. Semijoin find matched records in both tables but returns only records from outer table.
 * Antijoin returns only this records of outer table for which there are no matching in inner table.
 * @param with inner join table
 * @param estimation estimation for number of joined records
 * @param kind join kind (inner/anti join)
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner), class ORdd, class IRdd>
inline auto semijoin(ORdd* outer, IRdd* inner, size_t estimation = UNKNOWN_ESTIMATION, JoinKind kind = InnerJoin) 
{
    return new HashSemiJoinRDD<O,I,K,outerKey,innerKey,ORdd,IRdd>(outer, inner, estimation, kind);
}

/**
 * Disk shuffle  semijoin of two RDDs. Semijoin find matched records in both tables but returns only records from outer table.
 * Antijoin returns only this records of outer table for which there are no matching in inner table.
 * @param with inner join table
 * @param estimation estimation for number of joined records
 * @param kind join kind (inner/anti join)
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner), class ORdd, class IRdd>
inline auto shuffleSemijoin(ORdd* outer, IRdd* inner, size_t estimation = UNKNOWN_ESTIMATION, JoinKind kind = InnerJoin) 
{
    Cluster* cluster = Cluster::instance.get();
    size_t nFiles = (estimation + cluster->inmemJoinThreshold - 1) / cluster->inmemJoinThreshold;
    return new ShuffleSemiJoinRDD<O,I,K,outerKey,innerKey,ORdd,IRdd>(outer, inner, nFiles, estimation/nFiles, kind);
}

#endif
