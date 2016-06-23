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

/**
 * Join kind
 */
enum JoinKind 
{
    InnerJoin,
    OuterJoin, 
    AntiJoin
};

#define typeof(node) decltype((node)->elem)

/**
 * Abstract Node (Resilient Distributed Dataset)
 */
template<class T>
class Node
{
    /**
     * Node element. It is used for getting Node elementtype using typeof macro
     */
    T elem;

    /**
     * Main Node method for iterating though records.
     * To avoid virtual call overhead and make it possible for compiler to inline method calls implementation
     * of each Node defines its own version of next() method which is atatically invoked using template mechanism.
     * @param record [out] placeholder for the next record
     * @return true if there is next record, false otherwise
     */
    bool next(T& record) 
    {
        return getNext(record);
    }

    /**
     * Virtual iteration method which should be overriden by derived classes.
     * Because of virtual call overhead getNext method is not actually used in Node implementations, instead of it them
     * invokes next() method using template mechanism. But appliation can use this method to access arbitrary Node.
     * @param record [out] placeholder for the next record
     * @return true if there is next record, false otherwise
     */
    virtual bool getNext(T& record) = 0;
};

/**
 * Filter resutls using provided condition
 */
template<class T, bool (*predicate)(T const&), class Node = Node<T> >
class FilterNode : public Node<T>
{
  public:
    FilterNode(Node* input) : in(input) {}

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

    ~FilterNode() { delete in; }

  private:
    Node* const in;
};

    
/**
 * Perform aggregation of input data (a-la Scala fold)
 */
template<class T, class S,void (*accumulate)(S& state, T const& in),void (*combine)(S& state, S const& in), class Node = Node<T> >
class ReduceNode : public Node<S> 
{    
  public:
    ReduceNode(Node* input, S const& initState) : state(initState), first(true) {
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
    void aggregate(Node* input) { 
        T record;
        while (input->next(record)) { 
            accumulate(state, record);
        }
        Cluster* cluster = Cluster::instance.get();
        Queue* queue = cluster->getQueue();
        if (cluster->isCoordinator()) {
            S partialState;
            GatherNode<S> gather(queue);
            queue->putFirst(Buffer::eof(queue->qid)); // do not wait for self node
            while (gather.next(partialState)) {
                combine(state, partialState);
            }
        } else {
            sendToCoordinator<S,ReduceNode>(this, queue);            
        }
        delete input;
    }
    
    S state;
    bool first;
};
        
/**
 * Classical Map-Reduce
 */
template<class T,class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src), class Node = Node<T> >
class MapReduceNode : public Node< Pair<K,V> > 
{    
  public:
    MapReduceNode(Node* input, size_t estimation) : size(hashTableSize(estimation)), table(new Entry*[size]) {
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
             

    void loadHash(Node* input) 
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
            GatherNode< Pair<K,V> > gather(queue);
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
            sendToCoordinator<Pair<K,V>,MapReduceNode>(this, queue);            
        }
        printf("HashAggregate: estimated size=%ld, real size=%ld\n", size, realSize);
        delete input;
    }

    void deleteHash() {
        delete[] table;
    }
};

/**
 * Project (map) Node records
 */
template<class T, class P, void project(P& out, T const& in), class Node = Node<T> >
class ProjectNode : public Node<P>
{
  public:
    ProjectNode(Node* input) : in(input) {}

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

    ~ProjectNode() { delete in; }

  private:
    Node* const in;
};

/**
 *  Sort using given comparison function
 */
template<class T, int compare(T const* a, T const* b), class Node = Node<T> >
class SortNode : public Node<T>
{
  public:
    SortNode(Node* input, size_t estimation) {
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

    ~SortNode() { 
        delete[] buf;
    }

  private:
    T* buf;
    size_t size;
    size_t i;

    typedef int(*comparator_t)(void const* p, void const* q);

    void loadArray(Node* input, size_t estimation) { 
        Cluster* cluster = Cluster::instance.get();
        Queue* queue = cluster->getQueue();
        if (cluster->isCoordinator()) {         
            Thread loader(new FetchJob<T,Node>(input, queue));
            GatherNode<T> gather(queue);
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
            sendToCoordinator<T,Node>(input, queue);
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
template<class T, int compare(T const* a, T const* b), class Node = Node<T> >
class TopNode : public Node<T>
{
  public:
    TopNode(Node* input, size_t top) : i(0) {
        buf = new T[top];
        size_t n = loadTop(input, 0, top);
        delete input;
        
        Cluster* cluster = Cluster::instance.get();
        Queue* queue = cluster->getQueue();
        if (cluster->isCoordinator()) {         
            GatherNode<T> gather(queue);
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

    ~TopNode() { 
        delete[] buf;
    }

  private:
    T* buf;
    size_t size;
    size_t i;
    
    template<class INode>
    size_t loadTop(INode* input, size_t n, size_t top) { 
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
 * Join two Nodes using hash table.
 * Depending on estimated result size and broadcastJoinThreshold, this Node either broadcast inner table, 
 * either shuffle inner table 
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner), class ONode = Node<O>, class INode = Node<I> >
class HashJoinNode : public Node< Join<O,I> >, MessageHandler
{
public:
    HashJoinNode(ONode* outerNode, INode* innerNode, size_t estimation, JoinKind joinKind) 
    : kind(joinKind), table(NULL), size(hashTableSize(estimation)), innerSize(0), entry(NULL), inner(innerNode), outer(outerNode), scatter(NULL), gather(NULL), innerIsShuffled(false) 
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
                loadHash(replicate<I,INode>(inner));
            } else { 
                Thread loader(new ScatterJob<I,K,innerKey,INode>(inner, innerQueue));
                loadHash(new GatherNode<I>(innerQueue));
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

    ~HashJoinNode() { 
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
    INode*   inner;
    ONode*   outer;
    Queue*  innerQueue;
    Queue*  outerQueue;
    Thread* scatter;
    GatherNode<O>* gather;
    bool    innerIsShuffled;
    BlockAllocator<Entry> allocator;

    void loadOuterTable(bool replicateInner)
    {
        if (!replicateInner) {
            scatter = new Thread(new ScatterJob<O,K,outerKey,ONode>(outer, outerQueue));
            gather = new GatherNode<O>(outerQueue);
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
        BufferNode<I> input(buf);
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

    template<class Node>
    void loadHash(Node* gather) 
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
 * Semijoin two Nodes using hash table
 * Depending on estimated result size and broadcastJoinThreshold, this Node either broadcast inner table, 
 * either shuffle inner table 
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner), class ONode = Node<O>, class INode = Node<I> >
class HashSemiJoinNode : public Node<O>, MessageHandler
{
public:
    HashSemiJoinNode(ONode* outerNode, INode* innerNode, size_t estimation, JoinKind joinKind) 
    : kind(joinKind), table(NULL), size(hashTableSize(estimation)), inner(innerNode), outer(outerNode), scatter(NULL), gather(NULL), innerIsShuffled(false)
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
                loadHash(replicate<I,INode>(inner));
            } else { 
                Thread loader(new ScatterJob<I,K,innerKey,INode>(inner, innerQueue));
                loadHash(new GatherNode<I>(innerQueue));
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

    ~HashSemiJoinNode() { 
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
    INode*   inner;
    ONode*   outer;
    Queue*  innerQueue;
    Queue*  outerQueue;
    Thread* scatter;
    GatherNode<O>* gather;
    bool    innerIsShuffled;
    BlockAllocator<Entry> allocator;

    void loadOuterTable(bool replicateInner)
    {
        if (!replicateInner) {
            scatter = new Thread(new ScatterJob<O,K,outerKey,ONode>(outer, outerQueue));
            gather = new GatherNode<O>(outerQueue);
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
        BufferNode<I> input(buf);
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


    template<class Node>
    void loadHash(Node* gather) 
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
  
template<class T>
void Node<T>::output(FILE* out) 
{
    Cluster* cluster = Cluster::instance.get();
    Queue* queue = cluster->getQueue();
    if (cluster->isCoordinator()) {         
        Thread fetch(new FetchJob<T>(this, queue));
        GatherNode<T> gather(queue);
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

/**
 * Filter input Node
 * @return Node with records matching predicate 
 */
template<class T, bool (*predicate)(T const&), class Node>
inline auto filter(Node* in) { 
    return new FilterNode<T,predicate,Node>(in);
}

/**
 * Perform aggregation of input Node 
 * @param initState initial aggregate value
 * @return Node with aggregated value
 */
template<class T,class S,void (*accumulate)(S& state, T const& in), void(*combine)(S& state, S const& partial), class Node>
inline auto reduce(Node* in, S const& initState) {
    return new ReduceNode<T,S,accumulate,combine,Node>(in, initState);
}

/**
 * Perfrom map-reduce
 * @param estimation esimation for number of pairs
 * @return Node with &lt;key,value&gt; pairs
 */
template<class T,class K,class V,void (*map_f)(Pair<K,V>& out, T const& in), void (*reduce_f)(V& dst, V const& src), class Node>
inline auto mapReduce(Node* in, size_t estimation = UNKNOWN_ESTIMATION) {
    return new MapReduceNode<T,K,V,map_f,reduce_f,Node>(in, estimation);
}

/**
 * Map records of input Node
 * @return projection of the input Node. 
 */
template<class T,class P, void (*projection)(P& out, T const& in), class Node>
inline auto project(Node* in) {
    return new ProjectNode<T,P,projection,Node>(in);
}

/**
 * Sort input Node
 * @param estimation estimation for number of records in Node
 * @return Node with sorted records
 */
template<class T, int (*compare)(T const* a, T const* b), class Node> 
inline auto sort(Node* in, size_t estimation = UNKNOWN_ESTIMATION) {
    return new SortNode<T,compare,Node>(in, estimation);
}

/**
 * Find top N records according to provided comparison function
 * @param n number of returned top records
 * @return Node with up to N top records
 */
template<class T, int (*compare)(T const* a, T const* b), class Node> 
inline auto top(Node* in, size_t n = UNKNOWN_ESTIMATION) {
    return new TopNode<T,compare,Node>(in, n);
}

/**
 * In-memory hash join of two Nodes. Inner join returns pairs of matches records in outer and inner table.
 * Outer join also returns records from outer table for which there are matching in inner table.
 * @param with inner join table
 * @param estimation estimation for number of joined records
 * @param kind join kind (inner/outer join)
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner), class ONode, class INode>
inline auto join(ONode* outer, INode* inner, size_t estimation = UNKNOWN_ESTIMATION, JoinKind kind = InnerJoin) 
{
    return new HashJoinNode<O,I,K,outerKey,innerKey,ONode,INode>(outer, inner, estimation, kind);
}

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
};

template<class T, class Node>
inline void output(Node* in, FILE* out) 
{
    Cluster* cluster = Cluster::instance.get();
    Queue* queue = cluster->getQueue();
    if (cluster->isCoordinator()) {         
        Thread fetch(new FetchJob<T,Node>(in, queue));
        GatherNode<T> gather(queue);
        T record;
        while (gather.next(record)) { 
            print(record, out);
            fputc('\n', out);
        }
    } else {         
        sendToCoordinator<T,Node>(in, queue);
    }
    cluster->barrier();
}
