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

// Do not use virtual functions here: let derived template classes be inlined
template<class T>
class Reactor
{
  public:
    void react(T const& record) {}
	
	virtual~Reactor() {} 
};

template<class R>
class ReactorFactory
{
    R* getReactor();
};

class StagedScheduler : public Scheduler
{
    vector< vector<Job> > jobs;
    Mutex    mutex;
    Event    stageDone;
    stage_t currStage;
    size_t   currJob;
    size_t   activeJobs;
    time_t   stageStartTime;
    
  public:
    StagedScheduler() : currStage(0), currJob(0), activeJobs(0), stageStartTime(getCurrentTime()) {}
    
    void schedule(stage_t stage, Job* job)
    {
        assert(stage > 0);
        if (stage >= jobs.size()) {
            jobs.resize(stage+1);
        }
        jobs[stage].push_back(job);
    }

    Job* getJob() { 
        CriticalSection cs(mutex);
        while (currStage < jobs.size()) {
            if (currJob < jobs[currStage].size()) {
                nActiveJobs += 1;
                return jobs[currStage][currJob++];
            }
            while (nActiveJobs != 0) {
                stageDone.wait(mutex);
            }
            stageStartTime = getCurrentTime();
            currStage += 1;
            currJob = 0;
        }
        return NULL;
    }

    void jobFinished(Job* job) {
        CriticalSection cs(mutex);
        assert(nActiveJobs == 0);
        if (--nActiveJobs == 0) {
            printf("Stage %u finisihed in %ld microseconds\n", currStage, stageStartTime - getCurrentTime());
            event.signal();
        }
    }
};
        

/**
 * Abstract RDD (Resilient Distributed Dataset)
 */
template<class T>
class RDD
{
  public:
    template<class R>
    virtual stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& rf);

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

    class Singleton : public Reactor<T>
    {
      public:
        T value;
        Singleton(T const& defaultValue) : value(defautlValue) {}
        void react(T const& record) {
            value = record;
        }
    };
	
	class SingletonFactory : public ReactorFactory< Reactor<T> >
	{
	  public:
		Singleton* getReactor(T const& defaultValue) {
			return new Singleton(defaultValue);
		}
	};

    
    /**
     * Get single record from input RDD or substitute it with default value of RDD is empty.
     * This method is usful for obtaining aggregation result
     * @return Single record from input RDD or substitute it with default value of RDD is empty.
     */
    T result(T const& defaultValue)
    {
        SingletonFactory factoryu(defaultValue);
        StageByStageSheduler scheduler;
        Cluster* cluster = Cluster::instance.get();
        schedule(scheduler, 1, singleton);
        cluster->threadPool.run(scheduler);
        return singleton.value;
    }

    /**
     * Print RDD records to the stream
     * @param out output stream
     */
    void output(FILE* out);

    virtual~RDD() {}
};


template<class T, void (*dist_key)(K& key, T const& record)>
class Scatter : public Reactor
{
public:
	Scatter(Channel* chan) : channel(chan), cluster(Cluster::instance.get()), nNodes(cluster->nNodes), bufferSize(cluster->bufferSize), buffers(nNodes) {} 
	{
		for (size_t i = 0; i < nNodes; i++) { 
			buffers[i] = Buffer::create(channel->cid, bufferSize);
			buffers[i]->size = 0;
		}
	}
	void react(T const& record)
	{
		K key;
		dist_key(key, record);
		size_t hash = hashCode(key);
		size_t node = hash % nNodes;
		if (buffers[node]->size + sizeof(T) > bufferSize) {
			sent += buffers[node]->size;
			cluster->send(node, channel, buffers[node]);
			buffers[node]->size = 0;
		}
		size_t size = pack(record, buffers[node]->data + buffers[node]->size);
		assert(size <= sizeof(T));
		buffers[node]->size += size;
	}
	
	~Scatter() { 
		for (size_t i = 0; i < nNodes; i++) { 
			if (buffers[i]->size != 0) { 
				cluster->send(node, channel, buffers[node]);
			}
			delete buffers[node];
		}
		if (channel->done()) { 
			for (size_t i = 0; i < nNodes; i++) { 
				cluster->sendEof(node);
			}
		}
	}
  private:
	Channel* channel;
	Cluster* cluster;
	size_t nNodes;
	size_t bufferSize;
	vector<Buffer*> buffers;
};


template<class T>
class Sender : public Reactor<T>
{
public:
	void react(T const& record) { 
		if (msg->size + sizeof(T) > cluster->bufferSize) { 
			cluster->send(node, channel, msg);
			msg->size = 0;
		}
		msg->size += pack(record, msg->data + msg->size);
	}

	Sender(Channel* chan, size_t destination = COORDINATOR) : cluster(Cluster::instance.get()), channe(chan), node(destination) { 
		msg = Buffer::create(channel->cid, cluster->bufferSize);
		msg->size = 0;
	}

	~Sender() {
		if (msg->size != 0) { 
			cluster->send(node, channel, msg);
		}
		cluster->sendEof(node, channel);
		delete msg;
	}
	
 private:
	Cluster* cluster;
	Channel* channel;
	Buffer* msg;
	size_t node;
};

template<class T>
class Broadcaster : public Reactor<T>
{
public:
	void react(T const& record) { 
		if (msg->size + sizeof(T) > cluster->bufferSize) { 
			for (size_t i = 0; i < cluster->nNodes; i++) { 
				if (i != cluster->nodeId) { 
					cluster->send(i, channel, msg);
				}
			}
			msg->size = 0;
		}
		msg->size += pack(record, msg->data + msg->size);
	}

	Broadcaster(Channel* chan) : cluster(Cluster::instance.get()), channe(chan) { 
		msg = Buffer::create(channel->cid, cluster->bufferSize);
		msg->size = 0;
	}

	~Broadcaster() {
		if (msg->size != 0) { 
			for (size_t i = 0; i < cluster->nNodes; i++) { 
				if (i != cluster->nodeId) { 
					cluster->send(i, channel, msg);
				}
			}
		}
		for (size_t i = 0; i < cluster->nNodes; i++) { 
			if (i != cluster->nodeId) { 
				cluster->sendEof(i, channel);
			}
		}
		delete msg;
	}
	
 private:
	Cluster* cluster;
	Channel* channel;
	Buffer* msg;
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
    FileRDD(char* filePath) : path(filePath), segno(Cluster::instance->nodeId), split(Cluster::instance->sharedNothing ? 1 : Cluster::instance->nNodes)
    {
    }

    template<class R>
    class ReadJob : public Job {        
        FILE* f;
        R* reactor;
      public:
        ReadJob(FileRDD& file, R* jobReactor, size_t id, size_t concurrency) : reactor(jobReactor)
        {
            f = fopen(file.path, "rb");
            assert(f != NULL);
            fseek(f, 0, SEEK_END);
            size_t segmentSize = (ftell(f)/sizeof(T)+file.split-1)/file.split;
            size_t segmentOffs = segmentSize*file.segno*sizeof(T);
            jobSize = (segmentSize + concurrency - 1) / concurrency;
            if (jobSize*(id+1) > segmentSize) {
                jobSize = segmentSize - jobSize*id;
            }
            jobOffs = fseek(f, segmentOffs + jobSize, SEEK_SET) == 0 ? 0 : jobSize;
        }
            
        void run() {
            T record;
            while (++jobOffs <= jobSize && fread(&record, sizeof(T), 1, f) == 1) {
                reactor->react(record);
            }
			delete reactor;
        }
        ~ReadJob() {
            fclose(f);
        }
      private:
        FILE* f;
        size_t jobOffs;
        size_t jobSize;
    };   
    
    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, R* reactor) {
        size_t concurrecy = Cluster::instance()->threadPool.defaultConcurrency();
        for (size_t i = 0; i < concurrency; i++) {
            scheduler.schedule(stage, new ReadJob<R>(*this, reactor, i, concurrency));
        }
		return stage;
    }

    ~FileRDD() {
        delete[] path;
    }

  private:
    char* path;    
    size_t segno;
    size_t split;
};

/**
 * Read data from set of OS plain files located in specified directory.
 * Each node is given its own set of fileds.
 */
template<class T>
class DirRDD : public RDD<T>
{
  public:
    DirRDD(char* dirPath)
    : path(dirPath), segno(Cluster::instance->nodeId), step(Cluster::instance->nNodes), split(Cluster::instance->split) {}

    template<class R>
    class ReadJob : public Job {        
        char*  path;
        R*     reactor;
        size_t segno;
        size_t split;
        size_t step;
      public:
        ReadJob(DirRDD& dir, R* jobReactor, size_t id, size_t concurrency)
        : path(dir.path), reactor(jobReactor), segno(dir.segno*concurrency + id), split(dir.split*concurrency), step(dir.step*concurrency) {}

        void run() {
            T record;
            
            while (true) {
                char path[MAX_PATH_LEN];
                sprintf(path, "%s/%ld.rdd", dir, segno/split);
                FILE* f = fopen(path, "rb");
                if (f == NULL) { 
                    break;
                }
                fseek(f, 0, SEEK_END);
                size_t nRecords = (ftell(f)/sizeof(T)+split-1)/split;
                size_t recNo = 0;
                int rc = fseek(f, nRecords*(segno%split)*sizeof(T), SEEK_SET);
                assert(rc == 0);

                while (nRecords-- != 0 && fread(&record, sizeof(T), 1, f) == 1) {
                    reactor->react(record);
                }
                fclose(f);
                segno += step;
            }
			delete reactor;
        }
        ~ReadJob() {
            fclose(f);
        }
      private:
        FILE* f;
        size_t jobOffs;
        size_t jobSize;
    };   
    
    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, R* reactor) {
        size_t concurrecy = Cluster::instance()->threadPool.defaultConcurrency();
        for (size_t i = 0; i < concurrency; i++) {
            scheduler.schedule(stage, new ReadJob(*this, reactor, i, concurrency));
        }
		return stage;
    }

    ~DirRDD() {
        delete[] path;
    }
    
  private:
    char* path;
    size_t segno;
    size_t step;
    size_t split;
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
template<class T, bool (*predicate)(T const&)>
class FilterRDD : public RDD<T>
{
  public:
    FilterRDD(RDD<T>* input) : in(input) {}

    template<class R>
    class FilterReactor {
        R* reactor;

	  public:
		FilterReactor(R* r) : reactor(r) {}

        void react(T const& record) {
            if (predicate(record)) {
                reactor->react(record);
            }
        }

		~FilterReactor() { 
			delete reactor;
		}
    };
        
    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, R* reactor) {
        return in->shedule<FilterReactor>(scheduler, stage, new FilterReactor(reactor));
    }

    ~FilterRDD() { delete in; }

  private:
    RDD<T>* const in;
};


class BarrierJob : public Job
{
  public:
	Barrier(size_t n) : count(n) {}

	void reach() { 
		semaphore.signal();
	}

	void run() { 
		CriticalSection cs(mutex);
		semaphore.wait(count);
	}
  private:
	Mutex mutex;
	Semaphore semaphore;
};
    
/**
 * Perform aggregation of input data (a-la Scala fold)
 */
template<class T, class S, void (*accumulate)(S& state, T const& in), void (*combine)(S& state, S const& in) >
class ReduceRDD : public RDD<S> 
{    
    template<class R>
    class ReduceReactor {
        ReduceRDD& rdd;
		S state;

	  public:
		ReduceReactor(ReduceRDD& reduce) : rdd(reduce), state(reduce.state) {}

        void react(T const& record) {
			accumulate(state, record);
        }

		~ReduceReactor() { 
			CriticalSection cs(rdd.mutex);
			combine(rdd.state, state);
		}
    };

    template<class R>
	class ReduceProcessor { 
	public:
		void process(Buffer* buf) { 
			if (buf->kind == MSG_EOF) { 
				rdd.barrier->reach();
			} else {
				S state;
				unpack(state, buf->data);
				combine(rdd.state, state);
			}
		}

		ReduceProcessor(ReduceRDD& redure) : rdd(reduce) {}

	private:
		ReduceRDD& reduce;
	};	
		
	template<class R>
	class SendJob : public Job
	{
	public:
		void run() {
			reactor->react(result);
			delete reactor;
		}

		SendJob(S& state, R* to) : result(state), reactor(to) {}
	private:
		R* reactor;
	};
		

  public:
    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, R* reactor) {
        stage = in->shedule(scheduler, stage, new ReduceReactor(*this));
        Cluster* cluster = Cluster::instance.get();
		Channel* channel = cluster->getChannel(new ReduceProcessor(*this));
		if (!Cluster::instance->isCoordinator()) {
			scheduler.schedule(++stage, new SendJob<S, Sender<S> >(state, new Sender<S>(channel)));
		} else { 
			barrier = new BarrierJob(cluster->nNodes-1);
			scheduler.schedule(++stage, barrier);		
			scheduler.schedule(++stage, new SendJob<S,R>(state, reactor));
		}		
		return stage;
    }

    ReduceRDD(RDD<T>* input, S const& initState) : state(initState), in(input) {}
  private:
    S state;
	RDD<T>* in;
	Mutex mutex;
};
   


/**
 * Classical Map-Reduce
 */
template<class T,class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src) >
class MapReduceRDD : public RDD< Pair<K,V> > 
{    
  public:
    MapReduceRDD(RDD<T>* input, size_t estimation) : initSize(hashTableSize(estimation)), in(input) {}

    template<class R>
    class MapReduceReactor {
		BlockAllocator<Entry> allocator;
		MapReduceRDD& rdd;
		KeyValueMap<K,V,reduce> hashMap;

	  public:
		MapReduceReator(MapReduceRDD& reduce) : rdd(reduce), hashMap(rdd.initSize) {}

        void react(T const& record) {
			Pair<K,V> pair;
            map(pair, record);
			hashMap.add(pair);
        }

		~MapReduceReator() { 
			CriticalSection cs(rdd.mutex);
			rdd.hashMap.merge(hashMap);
		}
    };

    template<class R>
	class MapReduceProcessor { 
	public:
		void process(Buffer* buf) { 
			if (buf->kind == MSG_EOF) { 
				rdd.barrier->reach();
			} else {
				char* src = buf->data;
				char* end = src + buf->size;
				Pair<K,V> pair;
				while (src < end) { 
					src += unpack(pair, src);
					rdd.hashMap.add(pair);
				}
			}
		}

		MapReduceProcessor(MapReduce& reduce) : rdd(reduce) {}

	private:
		MapReduce& rdd;
	};	
		
	friend void key(K& k, Pair<K,V> const& pair) { 
		k = pair.key;
	}

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, R* reactor) {
        stage = in->shedule(scheduler, stage, new MapReduceReator(*this));
        Cluster* cluster = Cluster::instance.get();
		Channel* channel = cluster->getChannel(new MapReduceProcessor(*this));
		if (!Cluster::instance->isCoordinator()) {
			scheduler.schedule(++stage, hashMap.iterate<Sender<Pair<K,V> >(new Sender<Pair<K,V> >(channel))));
		} else { 
			barrier = new BarrierJob(cluster->nNodes-1);
			scheduler.schedule(++stage, barrier);		
			stage += 1;
			size_t nPartitions = cluster->threadPool.deafultConcurrency();
			for (size_t i = 0; i < nPartitions; i++) { 				
				scheduler.schedule(stage, hashMap.iterate<R>(reactor, i, nPartitions)); // !!!! Fix iteator in utils
			}
		}
		return stage;
	}

  private:
	size_t const initSize;
	RDD<T>* const in;
	KeyValueMap<K,V> hashMap;	
	Scatter<T,key>* scatter; 
	BarrierJob* barrier;
	Mutex mutex;	
};

/**
 * Project (map) RDD records
 */
template<class T, class P, void project(P& out, T const& in), class Rdd = RDD<T> >
class ProjectRDD : public RDD<P>
{
  public:
    ProjectRDD(Rdd* input) : in(input) {}

    template<class R>
    class ProjectReactor {
	  public:
		ProjectReactor(R* r) : reactor(r) {}

        void react(T const& record) {
			P projection;
			project(projection, record);
			reactor->react(projection);
        }

		~ProjectReactor() { 
			delete reactor;
		}

	private:
		R* reactor;
    };
 
    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, R& reactor) {
        return in->shedule(scheduler, stage, new ProjectReactor<R>(reactor));
    }

    ~ProjectRDD() { delete in; }

  private:
    RDD<T>* const in;
};

// Move to utils.h
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

// Move to utils.h

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
 *  Sort using given comparison function
 */
template<class T, int compare(T const* a, T const* b)>
class SortRDD : public RDD<T>
{
    template<class R>
	class AppendProcessor { 
	public:
		void process(Buffer* buf) { 
			if (buf->kind == MSG_EOF) { 
				rdd.barrier->reach();
			} else {
				char* src = buf->data;
				char* end = src + buf->size;
				T record;
				while (src < end) { 
					src += unpack(record, src);
					rdd.array.push_back(record);
				}
			}
		}

		AppendProcessor(SortRDD& sort) : rdd(sort) {}

	private:
		SortRDD& rdd;
	};	

    typedef int(*comparator_t)(void const* p, void const* q);

	class SortJob : public Job
	{
	public:
		SortJob(SortRDD& sort) : rdd(sort) {}

		public void run() { 
            qsort(&rdd.array[0], rdd.size, sizeof(T), (comparator_t)compare);
		}			
	};

  public:
    SortRDD(RDD<T>* input, size_t estimation) : in(input){
        array.reserve(estimation);
    }

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, R* reactor) {
		Channel* channel = cluster->getChannel(new AppendProcessor(*this));
        stage = in->shedule(scheduler, stage, new Sender<T>(channel), array);
		if (Cluster::instance->isCoordinator()) {
			barrier = new BarrierJob(cluster->nNodes-1);
			scheduler.schedule(++stage, barrier);		
			scheduler.schedule(++stage, new SortJob(*this));
			scheduler.schedule(++stage, new IterateArrayJob<T,R>(reactor, array));
		}
		return stage;
    }
	
	~SortRDD() { 
		delete in;
	}

private:
	RDD<T>* in;
	vector<T> array;
    BarrierJob* barrier;
};

/**
 * Get top N records using given comparison function
 */
template<class T, int compare(T const* a, T const* b)>
class TopRDD : public RDD<T>
{
  public:
    TopRDD(Rdd* input, size_t topN) : in(input), top(topN) {}

    template<class R>
	class MergeSortProcessor { 
	public:
		void process(Buffer* buf) { 
			if (buf->kind == MSG_EOF) { 
				rdd.barrier->reach();
			} else {
				char* src = buf->data;
				char* end = src + buf->size;
				T record;
				while (src < end) { 
					src += unpack(record, src);
					rdd.heap.add(record);
				}
			}
		}

		MergeSortProcessor(TopRDD& sort) : rdd(sort) {}

	private:
		TopRDD& rdd;
	};	

    class SortedArray
    {
      public:
        void add(T const& record) {
            size_t l = 0, r = buf.size();
            while (l < r) {
                size_t m = (l + r) >> 1;
                if (compare(&buf[m], &record) <= 0) {
                    l = m + 1;
                } else {
                    r = m;
                }
            }
            if (r < top) {
                if (buf.size() < top) {
                    buf.insert(r, record);
                } else {
                    memmove(&buf[r+1], &buf[r], (size-r-1)*sizeof(T));
                    buf[r] = record;
                }
            }
        }

        void merge(SortedArray const& other) {
            for (size_t i = 0; i < other.buf.size(); i++) {
                add(other.buf[i]);
            }
        }        
        
        SortedArray(size_t n) : top(n) 
        
        vector<T> buf;
        size_t top;
    };

    class TopReactor : public Reactor, public SortedArray
    {
      public:
        TopReactor(TopRDD& r) : SortedArray(r.top), rdd(r) {}
        ~TopReactor() {
            CriticalSection cs(rdd.mutex);
            rdd.heap.merge(*this);
        }

        void react(T const& record) {
            add(record);
        }
        
      private:
        TopRDD& rdd;
    };

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, R* reactor) {
		Channel* channel = cluster->getChannel(new MergeSortAppendProcessor(*this));
        stage = in->shedule(scheduler, stage, new TopReactor(*this));
        scheduler.schedule(++state, new IterateArrayJob<T,Sender<T> >(new Sender<T>(channel), heap.buf));
		if (Cluster::instance->isCoordinator()) {
			barrier = new BarrierJob(cluster->nNodes-1);
			scheduler.schedule(++stage, barrier);		
			scheduler.schedule(++stage, new IterateArrayJob<T,R>(reactor, heap.buf));
		}
		return stage;
    }
	
	~TopRDD() { 
		delete in;
	}

  private:
    SortedArray heap;
    Mutex mutex;
    BarrierJob* barrier;
};

/**
 * Join two RDDs using hash table.
 * Depending on estimated result size and broadcastJoinThreshold, this RDD either broadcast inner table, 
 * either shuffle inner table 
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner)>
class HashJoinRDD : public RDD< Join<O,I> >
{
public:
    HashJoinRDD(RDD<O>* outerRDD, RDD<I>* innerRDD, size_t estimation, JoinKind joinKind) 
    : kind(joinKind), outer(outerRDD), inner(innerRDD), hashTable(estimation) {}

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, R* reactor) {
        Cluster* cluster = Cluster::instance.get();
		Channel* channel = cluster->getChannel(new InnerProcessor(*this));
        innerBarrier = new BarrierJob(cluster->nNodes-1);
        if (estimation <= cluster->broadcastJoinThreshold) {
            // broadcast inner table
            stage = inner->shedule(scheduler, stage, new Broadcaster<I> >(channel));
            scheduler.schedule(++stage, innerBarrier);
            stage = outer->schedule(scheduler, ++stage, new JoinReactor(*this, reactor));
        } else {
            // shuffle both tables
            stage = inner->shedule(scheduler, stage, new Scatter<I, innerKey> >(channel));
            scheduler.schedule(++stage, innerBarrier);

            outerBarrier = new BarrierJob(cluster->nNodes-1);
            channel = cluster->getChannel(new OuterProcessor<R>(*this, reactor));
            stage = outer->schedule(scheduler, ++stage, new Scatter<O, outerKey> >(channel));
            scheduler.schedule(++stage, outerBarrier);
        }
        return stage;
    }

  private:
    template<class R>
	class InnerProcessor { 
	public:
		void process(Buffer* buf) { 
			if (buf->kind == MSG_EOF) { 
				rdd.innerBarrier->reach();
			} else {
				char* src = buf->data;
				char* end = src + buf->size;
				I record;
				while (src < end) { 
					src += unpack(record, src);
					rdd.hashTable.add(record);
				}
			}
		}

		InnerProcessor(HashJoinRDD& join) : rdd(join) {}

	private:
		HashJoinRDD& rdd;
	};	
    
    template<class R>
	class OuterProcessor { 
	public:
		void process(Buffer* buf) { 
			if (buf->kind == MSG_EOF) { 
				rdd.outerBarrier->reach();
                delete reactor;
			} else {
				char* src = buf->data;
				char* end = src + buf->size;
				Pair<O,I> pair;
				while (src < end) {
                    K key;
                    size_t nMatches = 0;
					src += unpack(pair.key, src);
                    outerKey(key, pair.key);                    
                    for (HashTable<I,innerKey>::Entry* entry = rdd.hashTable.get(key); entry != NULL; entry = entry->collision) {
                        if (entry->equalsKey(key)) {
                            pair.value = entry->record;
                            reactor->react(pair);
                            nMatches += 1;
                        }
                    }
                    if (nMatches == 0 && rdd.kind == OuterJoin){ 
                        reactor->react(pair);
                    }
				}
			}
		}

		OuterProcessor(HashJoinRDD& join, R* reactor) : rdd(join) {}

	private:
		HashJoinRDD& rdd;
        R* reactor;
	};	

    template<class R>
    class JoinReactor : public Reactor< Pair<O,I> >
    {
      public:
        void react(O const& outer) {
            K key;
            Pair<O,I> pair;
            size_t nMatches = 0;
            outerKey(key, outer);                    
            pair.key = outer;
            for (HashTable<I,innerKey>::Entry* entry = rdd.hashTable.get(key); entry != NULL; entry = entry->collision) {
                if (entry->equalsKey(key)) {
                    pair.value = entry->record;
                    reactor->react(pair);
                    nMatches += 1;
                }
            }
            if (nMatches == 0 && rdd.kind == OuterJoin){ 
                reactor->react(pair);
            }
        }

        JoinReactor(HashJoinRDD& join, R* r) : rdd(join), reactor(r) {}
        ~JoinReactor() {
            delete reactor;
        }
      private:
        HashJoinRDD& rdd;
        R* reactor;
    };
        
    JoinKind const kind;
    HashTable<I, innerKey> hashTable;
    RDD<I>* const outer;
    RDD<I>* const inner;
    BarrierJob* outerBarrier;
    BarrierJob* innerBarrier;
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
    ColumnarRDD(RDD<H>* input, size_t estimation) : cache(estimation), curr(0) { 
        H record;
        while (input->next(record)) { 
            cache.append(record);
        }
        delete input;
    }
    bool next(V& record) { 
        if (curr == cache._used) { 
            return false;
        }
        record._data = &cache;
        record._pos = curr++;
        return true;
    }
    bool getNext(V& record) override { 
        return next(record);
    }

    ColumnarRDD* get() { 
        return new ColumnarRDD(cache);
    }

  private:
    ColumnarRDD(C const& _cache) : cache(_cache), curr(0) {}
    C cache;
    size_t curr;
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
