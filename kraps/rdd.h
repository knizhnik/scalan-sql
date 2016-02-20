#ifndef __RDD_H__
#define __RDD_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "cluster.h"
#include "pack.h"
#include "hash.h"
#include "util.h"

#ifndef UNKNOWN_ESTIMATION
#define UNKNOWN_ESTIMATION (1024*1024)
#endif

// Do not use virtual functions: let's functions be inlined by templates
#define VIRTUAL(x)

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
 * Join kind
 */
enum JoinKind 
{
    InnerJoin,
    OuterJoin, 
    AntiJoin
};

/**
 * Main interface for PUSH mode: react on pushed events
 */
template<class T>
class Reactor
{
  public:
    /**
	 * Process record	 
	 */
    VIRTUAL(void react(T const& record));
	
	/**
	 * Destructor is used to inform consumer that end of data is reached 
	 */
	virtual~Reactor() {} 
};

/**
 * Construct instance of Reactor class.
 * Each job has its own reactor.
 */
template<class R>
class ReactorFactory
{
  public:
    virtual R* getReactor() = 0;
};

/**
 * Reactor factory for chained reactors (reactor of child RDD calls reactor of parent RDD)
 */
template<class Parent,class Child>
class ChainReactorFactory : public ReactorFactory<Child>
{
  public:
	ChainReactorFactory(ReactorFactory<Parent>& factory) : parent(factory) {}
		
	Child* getReactor() { 
		return new Child(parent.getReactor());
	}
  private:
	ReactorFactory<Parent>& parent;
};

/**
 * Construct reactor depending on owning RDD
 */
template<class Rdd,class R>
class RddReactorFactory : public ReactorFactory<R>
{
  public:
	RddReactorFactory(Rdd& owner) : rdd(owner) {}

	R* getReactor() { 
		return new R(rdd);
	}

  private:
	Rdd& rdd;
};

/**
 * Combination of ChainReactorFactory and RddReactorFactory
 */
template<class Rdd,class Parent,class Child>
class ChainRddReactorFactory : public ReactorFactory<Child>
{
  public:
	ChainRddReactorFactory(Rdd& owner, ReactorFactory<Parent>& factory) : rdd(owner), parent(factory) {}

	Child* getReactor() { 
		return new Child(rdd, parent.getReactor());
	}

  private:
	Rdd& rdd;
	ReactorFactory<Parent>& parent;
};

/**
 * Reactor factory for network reactors
 */
template<class R>
class ChannelReactorFactory : public ReactorFactory<R> 
{
  public:
	ChannelReactorFactory(Channel* chan) : channel(chan) {}

    R* getReactor() { 
		return new R(channel);
	}
	
  private:
	Channel* const channel;
};


/**
 * Schedule executing in parallel jobs of one stage and waiting their completion before staring new stage
 */
class StagedScheduler : public Scheduler
{
  public:
    StagedScheduler() : currStage(0), currJob(0), nActiveJobs(0), stageStartTime(getCurrentTime()) {}
    
	size_t getDefaultConcurrency() 
	{
		return Cluster::instance->threadPool.getDefaultConcurrency();
	}
		
    void schedule(stage_t stage, Job* job)
    {
        assert(stage > 0);
        if (stage >= jobs.size()) {
            jobs.resize(stage+1);
        }
        jobs[stage].push_back(job);
    }

    Job* getJob() 
	{ 
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

    void jobFinished(Job* job) 
	{
        CriticalSection cs(mutex);
        assert(nActiveJobs > 0);
        if (--nActiveJobs == 0) {
            printf("Stage %u finisihed in %ld microseconds\n", currStage, getCurrentTime() - stageStartTime);
            stageDone.signal();
        }
    }

  private:
    vector< vector<Job*> > jobs;
    Mutex    mutex;
    Event    stageDone;
    stage_t  currStage;
    size_t   currJob;
    size_t   nActiveJobs;
    time_t   stageStartTime;    
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
	 * Mutex to synchronize access to RDD shared state
	 */
    Mutex mutex;

	/**
	 * Schedule jobs for this RDD
	 */
    VIRTUAL(template<class R> stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory));
	
	/**
	 * Release RDD: delete dynamically created RDDs
	 */
	virtual void release() {
		delete this;
	}
	
	/**
	 * Vertual destructor
	 */
    virtual~RDD() {}
};

/**
 * Reactor for shuffling records by distribution key among cluster nodes
 */
template<class T, class K, void (*dist_key)(K& key, T const& record)>
class Scatter : public Reactor<T>
{
public:
	Scatter(Channel* chan) 
	: channel(chan), cluster(Cluster::instance.get()), nNodes(cluster->nNodes), bufferSize(cluster->bufferSize), buffers(nNodes)
	{
		channel->attach();
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
			cluster->send(node, channel, buffers[node]);
			buffers[node]->size = 0;
		}
		size_t size = pack(record, buffers[node]->data + buffers[node]->size);
		assert(size <= sizeof(T));
		buffers[node]->size += size;
	}
	
	~Scatter() 
	{ 
		for (size_t i = 0; i < nNodes; i++) { 
			if (buffers[i]->size != 0) { 
				cluster->send(i, channel, buffers[i]);
			}
			delete buffers[i];
		}
		if (channel->detach()) { 
			for (size_t i = 0; i < nNodes; i++) { 
				cluster->sendEof(i, channel);
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


/**
 * Reactor for sending data to the specific node (usually coordinator)
 */
template<class T>
class Sender : public Reactor<T>
{
public:
	Sender(Channel* chan, size_t destination = COORDINATOR) 
	: cluster(Cluster::instance.get()), channel(chan), node(destination) 
	{ 
		msg = Buffer::create(channel->cid, cluster->bufferSize);
		msg->size = 0;
		channel->attach();
	}

	void react(T const& record) 
	{ 
		if (msg->size + sizeof(T) > cluster->bufferSize) { 
			cluster->send(node, channel, msg);
			msg->size = 0;
		}
		size_t size = pack(record, msg->data + msg->size);
		assert(size <= sizeof(T));
		msg->size += size;
	}

	~Sender() 
	{
		if (msg->size != 0) { 
			cluster->send(node, channel, msg);
		}
		delete msg;
		if (channel->detach()) { 
			cluster->sendEof(node, channel);
		}
	}
	
  private:
	Cluster* cluster;
	Channel* channel;
	Buffer* msg;
	size_t node;
};


/**
 * Reactor for broadcastig data to all cluster nodes
 */
template<class T>
class Broadcaster : public Reactor<T>
{
  public:
	void react(T const& record) 
	{ 
		if (msg->size + sizeof(T) > cluster->bufferSize) { 
			for (size_t i = 0; i < cluster->nNodes; i++) { 
				if (i != cluster->nodeId) { 
					cluster->send(i, channel, msg);
				}
			}
			msg->size = 0;
		}
		size_t size = pack(record, msg->data + msg->size);
		assert(size <= sizeof(T));
		msg->size += size;
	}

	Broadcaster(Channel* chan) : cluster(Cluster::instance.get()), channel(chan) 
	{ 
		msg = Buffer::create(channel->cid, cluster->bufferSize);
		msg->size = 0;
		channel->attach();
	}

	~Broadcaster() 
	{
		if (msg->size != 0) { 
			for (size_t i = 0; i < cluster->nNodes; i++) { 
				if (i != cluster->nodeId) { 
					cluster->send(i, channel, msg);
				}
			}
		}
		delete msg;

		if (channel->detach()) { 
			for (size_t i = 0; i < cluster->nNodes; i++) { 
				if (i != cluster->nodeId) { 
					cluster->sendEof(i, channel);
				}
			}
		}
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
    FileRDD(char* filePath) 
	: path(filePath),
	  segno(Cluster::instance->nodeId), 
	  split(Cluster::instance->sharedNothing ? 1 : Cluster::instance->nNodes) {}

    ~FileRDD() {
        delete[] path;
    }

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) {
        size_t concurrency = scheduler.getDefaultConcurrency();
        for (size_t i = 0; i < concurrency; i++) {
            scheduler.schedule(stage, new ReadJob<R>(*this, factory.getReactor(), i, concurrency));
        }
		return stage;
    }

  private:
    template<class R>
    class ReadJob : public Job {        
      public:
        ReadJob(FileRDD& file, R* jobReactor, size_t id, size_t concurrency) : reactor(jobReactor)
        {
            f = fopen(file.path, "rb");
            assert(f != NULL);
            fseek(f, 0, SEEK_END);
            jobSize = (ftell(f)/sizeof(T)+file.split*concurrency-1)/(file.split*concurrency);
            size_t segmentOffs = jobSize*(file.segno*concurrency + id)*sizeof(T);
            jobOffs = fseek(f, segmentOffs, SEEK_SET) == 0 ? 0 : jobSize;
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
        R* reactor;
        FILE* f;
        size_t jobOffs;
        size_t jobSize;
    };   
    
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

    ~DirRDD() {
        delete[] path;
    }

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) {
        size_t concurrency = scheduler.getDefaultConcurrency();
        for (size_t i = 0; i < concurrency; i++) {
            scheduler.schedule(stage, new ReadJob<R>(*this, factory.getReactor(), i, concurrency));
        }
		return stage;
    }
	
  private:
    template<class R>
    class ReadJob : public Job {        
      public:
        ReadJob(DirRDD& dir, R* jobReactor, size_t id, size_t concurrency)
        : dirPath(dir.path), reactor(jobReactor), segno(dir.segno*concurrency + id), split(dir.split*concurrency), step(dir.step*concurrency) {}

        void run() {
            T record;
            
            while (true) {
                char path[MAX_PATH_LEN];
                sprintf(path, "%s/%ld.rdd", dirPath, segno/split);
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
      private:
        char*  dirPath;
        R*     reactor;
        size_t segno;
        size_t const split;
        size_t const step;
    };   
    
  private:
    char* path;
    size_t segno;
    size_t step;
    size_t split;
};

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
			: (RDD<T>*)new DirRDD<T>(fileName);
    }
};

/**
 * Filter resutls using provided condition
 */
template<class T, bool (*predicate)(T const&), class Rdd>
class FilterRDD : public RDD<T>
{
  public:
    FilterRDD(Rdd* input) : in(input) {}
    ~FilterRDD() { in->release(); }

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) {
		ChainReactorFactory< R,FilterReactor<R> > filter(factory);
        return in->schedule(scheduler, stage, filter);
    }

  private:
    template<class R>
    class FilterReactor {
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
	  private:
        R* reactor;
    };

    Rdd* const in;
};


class BarrierJob : public Job
{
  public:
	BarrierJob(size_t n) : count(n) {}

	void reach() { 
		CriticalSection cs(mutex);
		semaphore.signal(mutex);
	}

	void run() { 
		CriticalSection cs(mutex);
		semaphore.wait(mutex, count);
	}

  private:
	size_t const count;
	Mutex mutex;
	Semaphore semaphore;
};
    
/**
 * Perform aggregation of input data (a-la Scala fold)
 */
template<class T, class S, void (*accumulate)(S& state, T const& in), void (*combine)(S& state, S const& in), class Rdd >
class ReduceRDD : public RDD<S> 
{     
  public:
    ReduceRDD(Rdd* input, S const& initState) : state(initState), in(input) {}
    ~ReduceRDD() { in->release(); }

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) 
	{
		RddReactorFactory<ReduceRDD,ReduceReactor> reduce(*this);
        stage = in->schedule(scheduler, stage, reduce);
        Cluster* cluster = Cluster::instance.get();
		Channel* channel = cluster->getChannel(new ReduceProcessor(*this));
		if (Cluster::instance->isCoordinator()) {
			barrier = new BarrierJob(cluster->nNodes-1);
			scheduler.schedule(++stage, barrier);		
			scheduler.schedule(++stage, new SendJob<R>(state, factory.getReactor()));
		} else { 
			scheduler.schedule(++stage, new SendJob< Sender<S> >(state, new Sender<S>(channel)));
		}		
		return stage;
    }

  private:
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

	class ReduceProcessor : public ChannelProcessor { 
	  public:
		void process(Buffer* buf, size_t node) { 
			if (buf->kind == MSG_EOF) { 
				rdd.barrier->reach();
			} else {
				S state;
				size_t size = unpack(state, buf->data);
				assert(size == buf->size);
				{
					CriticalSection cs(rdd.mutex);
					combine(rdd.state, state);
				}
			}
		}

		ReduceProcessor(ReduceRDD& reduce) : rdd(reduce) {}

	  private:
		ReduceRDD& rdd;
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
		S& result;
		R* reactor;
	};
		
  private:
    S state;
	Rdd* in;
	BarrierJob* barrier;
};
   


/**
 * Classical Map-Reduce
 */
template<class T,class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src), class Rdd >
class MapReduceRDD : public RDD< Pair<K,V> > 
{    
  public:
    MapReduceRDD(Rdd* input, size_t estimation) : initSize(estimation), hashMap(estimation), in(input) {}
	~MapReduceRDD() { in->release(); }
			
    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) 
	{
		RddReactorFactory<MapReduceRDD,MapReduceReactor> reducer(*this);
        stage = in->schedule(scheduler, stage, reducer);
        Cluster* cluster = Cluster::instance.get();
		Channel* channel = cluster->getChannel(new MapReduceProcessor(*this));
		if (Cluster::instance->isCoordinator()) {
			barrier = new BarrierJob(cluster->nNodes-1);
			scheduler.schedule(++stage, barrier);		
			stage += 1;
			size_t nPartitions = scheduler.getDefaultConcurrency();
			for (size_t i = 0; i < nPartitions; i++) { 				
				scheduler.schedule(stage, hashMap.iterate(factory.getReactor(), i, nPartitions));
			}
		} else { 
			scheduler.schedule(++stage, hashMap.iterate(new Sender< Pair<K,V> >(channel)));
		}
		return stage;
	}
	
  private:
    class MapReduceReactor : public Reactor<T> {
		MapReduceRDD& rdd;
		KeyValueMap<K,V,reduce> hashMap;

	  public:
		MapReduceReactor(MapReduceRDD& owner) : rdd(owner), hashMap(rdd.initSize) {}

        void react(T const& record) {
			Pair<K,V> pair;
            map(pair, record);
			hashMap.add(pair);
        }

		~MapReduceReactor() { 
			CriticalSection cs(rdd.mutex);
			rdd.hashMap.merge(hashMap);
		}
    };

	class MapReduceProcessor : public ChannelProcessor { 
	public:
		void process(Buffer* buf, size_t node) { 
			if (buf->kind == MSG_EOF) { 
				rdd.barrier->reach();
			} else {
				char* src = buf->data;
				char* end = src + buf->size;
				Pair<K,V> pair;
				{
					CriticalSection cs(rdd.mutex);
					while (src < end) { 
						src += unpack(pair, src);
						rdd.hashMap.add(pair);
					}
				}
			}
		}

		MapReduceProcessor(MapReduceRDD& owner) : rdd(owner) {}

	private:
		MapReduceRDD& rdd;
	};	

  private:
	size_t const initSize;
    KeyValueMap<K,V,reduce> hashMap;	
	Rdd* const in;
	BarrierJob* barrier;
};

/**
 * Project (map) RDD records
 */
template<class T, class P, void project(P& out, T const& in), class Rdd>
class ProjectRDD : public RDD<P>
{
  public:
    ProjectRDD(Rdd* input) : in(input) {}
    ~ProjectRDD() { in->release(); }

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) {
		ChainReactorFactory< R,ProjectReactor<R> > project(factory);
        return in->schedule(scheduler, stage, project);
    }

  private:
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
 
  private:
    Rdd* const in;
};

/**
 *  Sort using given comparison function
 */
template<class T, int compare(T const* a, T const* b), class Rdd>
class SortRDD : public RDD<T>
{
  public:
    SortRDD(Rdd* input, size_t estimation) : in(input){}
	~SortRDD() { in->release();	}

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) {
        Cluster* cluster = Cluster::instance.get();
		Channel* channel = cluster->getChannel(new AppendProcessor(*this));
		ChannelReactorFactory< Sender<T> > sender(channel);
        stage = in->schedule(scheduler, stage, sender);
		if (Cluster::instance->isCoordinator()) {
			barrier = new BarrierJob(cluster->nNodes-1);
			scheduler.schedule(++stage, barrier);		
			scheduler.schedule(++stage, new SortJob(*this));
			// Create just one job because most likely we want ot preserve sort order
			scheduler.schedule(++stage, new IterateArrayJob<T,R>(factory.getReactor(), array));
		}
		return stage;
    }
	
  private:
	class AppendProcessor : public ChannelProcessor { 
	public:
		void process(Buffer* buf, size_t node) { 
			if (buf->kind == MSG_EOF) { 
				rdd.barrier->reach();
			} else {
				CriticalSection cs(rdd.mutex);
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

		void run() { 
            qsort(&rdd.array[0], rdd.array.size(), sizeof(T), (comparator_t)compare);
		}	
	  private:
		SortRDD& rdd;
	};

  private:
	Rdd* in;
	vector<T> array;
    BarrierJob* barrier;
};

/**
 * Get top N records using given comparison function
 */
template<class T, int compare(T const* a, T const* b), class Rdd>
class TopRDD : public RDD<T>
{
  public:
    TopRDD(Rdd* input, size_t top) : in(input), heap(top) {}
	~TopRDD() { in->release(); }

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) {
        Cluster* cluster = Cluster::instance.get();
		Channel* channel = cluster->getChannel(new MergeSortProcessor(*this));
		RddReactorFactory<TopRDD,TopReactor> topReactor(*this);
        stage = in->schedule(scheduler, stage, topReactor);
		if (Cluster::instance->isCoordinator()) {
			barrier = new BarrierJob(cluster->nNodes-1);
			scheduler.schedule(++stage, barrier);		
			// Create just one job because most likely we want ot preserve sort order
			scheduler.schedule(++stage, new IterateArrayJob<T,R>(factory.getReactor(), heap.buf));
		} else { 
			scheduler.schedule(++stage, new IterateArrayJob<T,Sender<T> >(new Sender<T>(channel), heap.buf));
		}
		return stage;
    }
	
	vector<T> buf;
  private:
	class MergeSortProcessor : public ChannelProcessor { 
	  public:
		void process(Buffer* buf, size_t node) { 
			if (buf->kind == MSG_EOF) { 
				rdd.barrier->reach();
			} else {
				CriticalSection cs(rdd.mutex);
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
                    buf.insert(buf.begin() + r, record);
                } else {
                    memmove(&buf[r+1], &buf[r], (top-r-1)*sizeof(T));
                    buf[r] = record;
                }
            }
        }

        void merge(SortedArray const& other) {
            for (size_t i = 0; i < other.buf.size(); i++) {
                add(other.buf[i]);
            }
        }        
        
        SortedArray(size_t n) : top(n) {}

		vector<T> buf;
        size_t top;
    };

    class TopReactor : public Reactor<T>
    {
      public:
        TopReactor(TopRDD& r) : heap(r.heap.top), rdd(r) {}
        ~TopReactor() {
            CriticalSection cs(rdd.mutex);
            rdd.heap.merge(heap);
        }

        void react(T const& record) {
            heap.add(record);
        }
        
      private:
		SortedArray heap;
        TopRDD& rdd;
    };

  private:
	Rdd* in;
    SortedArray heap;
    BarrierJob* barrier;
};

/**
 * Join two RDDs using hash table.
 * Depending on estimated result size and broadcastJoinThreshold, this RDD either broadcast inner table, 
 * either shuffle inner table 
 */
template<class O, class I, class K, void (*outerKey)(K& key, O const& outer), void (*innerKey)(K& key, I const& inner), class ORdd, class IRdd>
class HashJoinRDD : public RDD< Join<O,I> >
{
public:
    HashJoinRDD(ORdd* outerRDD, IRdd* innerRDD, size_t estimation, JoinKind joinKind) 
    : kind(joinKind), outer(outerRDD), inner(innerRDD), initSize(estimation), hashTable(estimation) {}
	
	~HashJoinRDD() { 
		inner->release();
		outer->release();
	}

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) {
        Cluster* cluster = Cluster::instance.get();
		Channel* channel = cluster->getChannel(new InnerProcessor(*this));
        innerBarrier = new BarrierJob(cluster->nNodes-1);

        if (initSize <= cluster->broadcastJoinThreshold) {
            // broadcast inner table
			ChannelReactorFactory< Broadcaster<I> > broadcast(channel);
            stage = inner->schedule< Broadcaster<I> >(scheduler, stage, broadcast);

            scheduler.schedule(++stage, innerBarrier);

			ChainRddReactorFactory< HashJoinRDD,R,JoinReactor<R> > join(*this, factory);
            stage = outer->schedule(scheduler, ++stage, join);
        } else {
            // shuffle both tables
			ChannelReactorFactory< Scatter<I, K, innerKey> > innerScatter(channel);
            stage = inner->schedule(scheduler, stage, innerScatter);

            scheduler.schedule(++stage, innerBarrier);

            outerBarrier = new BarrierJob(cluster->nNodes-1);
            channel = cluster->getChannel(new OuterProcessor<R>(*this, factory));

			ChannelReactorFactory< Scatter<O, K, outerKey> > outerScatter(channel);
            stage = outer->schedule(scheduler, ++stage, outerScatter);

            scheduler.schedule(++stage, outerBarrier);
        }
        return stage;
    }

  private:
	class InnerProcessor : public ChannelProcessor { 
	  public:
		void process(Buffer* buf, size_t node) { 
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
	class OuterProcessor : public ChannelProcessor { 
	  public:
		void process(Buffer* buf, size_t node) { 
			if (buf->kind == MSG_EOF) { 
				rdd.outerBarrier->reach();
                delete reactors[node];
			} else {
				char* src = buf->data;
				char* end = src + buf->size;
				Join<O,I> join;
				while (src < end) {
                    K key;
                    size_t nMatches = 0;
					src += unpack(join, src);
                    outerKey(key, (O&)join);                    
                    for (typename HashTable<I,K,innerKey>::Entry* entry = rdd.hashTable.get(key); 
						 entry != NULL; 
						 entry = entry->collision) 
					{
                        if (entry->equalsKey(key)) {
                            (I&)join = entry->record;
                            reactors[node]->react(join);
                            nMatches += 1;
                        }
                    }
                    if (nMatches == 0 && rdd.kind == OuterJoin){ 
                        reactors[node]->react(join);
                    }
				}
			}
		}

		OuterProcessor(HashJoinRDD& join, ReactorFactory<R>& factory) : rdd(join), reactors(Cluster::instance->nNodes) {
			for (size_t i = 0; i != reactors.size(); i++) { 
				reactors[i] = factory.getReactor();
			}
		}

	  private:
		HashJoinRDD& rdd;
        vector<R*> reactors;
	};	

    template<class R>
    class JoinReactor : public Reactor< Join<O,I> >
    {
      public:
        void react(O const& outer) {
            K key;
            Join<O,I> join;
            size_t nMatches = 0;
            outerKey(key, outer);                    
            (O&)join = outer;
            for (typename HashTable<I,K,innerKey>::Entry* entry = rdd.hashTable.get(key); 
				 entry != NULL; 
				 entry = entry->collision) 
			{
                if (entry->equalsKey(key)) {
                    (I&)join = entry->record;
                    reactor->react(join);
                    nMatches += 1;
                }
            }
            if (nMatches == 0 && rdd.kind == OuterJoin) { 
				(I&)join = I();
                reactor->react(join);
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
    ORdd* const outer;
    IRdd* const inner;
	size_t  const initSize;
    HashTable<I,K,innerKey> hashTable;
    BarrierJob* outerBarrier;
    BarrierJob* innerBarrier;
};
  
class SequentialScheduler : public Scheduler
{
  public:
	virtual size_t getDefaultConcurrency() { 
		return 1;
	}

    virtual void   schedule(stage_t stage, Job* job) { 
		jobs.push_back(job);
	}

    virtual Job*   getJob() { 
        CriticalSection cs(mutex);
		while (busy) { 
			event.wait(mutex);
		}
		return currJob == jobs.size() ? NULL : jobs[currJob++];
	}

    virtual void   jobFinished(Job* job) { 
		busy = false;
		event.signal();
	}

	void execute() { 
		for (size_t i = 0; i < jobs.size(); i++) {
			jobs[i]->run();
			delete jobs[i];
		}
		jobs.clear();
	}

	SequentialScheduler() : busy(false), currJob(0) {}

  private:
	vector<Job*> jobs;
	Mutex  mutex;
	Event  event;
	bool   busy;
	size_t currJob;
};
	

/**
 * Cache RDD in memory
 */
template<class T>
class CachedRDD : public RDD<T>
{
  public:
    CachedRDD(DirRDD<T>* input, size_t estimation) { 
		array.reserve(estimation);
		SequentialScheduler scheduler;
		RddReactorFactory<CachedRDD,AppendArray> factory(*this);
		input->schedule(scheduler, 1, factory);
		scheduler.execute();
		input->release();
    }

	template<class R>
	stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory)
	{
        size_t concurrency = scheduler.getDefaultConcurrency();
        for (size_t i = 0; i < concurrency; i++) {
            scheduler.schedule(stage, new IterateArrayJob<T,R>(factory.getReactor(), array, i, concurrency));
        }
		return stage;
	}

	virtual void release() {}

  private:
	class AppendArray : Reactor<T> { 
	  public:
		void react(T const& record) { 
			rdd.array.push_back(record);
		}

		AppendArray(CachedRDD& cache) : rdd(cache) {}
	  private:
		CachedRDD& rdd;
	};

  private:
	vector<T> array;
};

/**
 * In-memory columnar store RDD
 */
template<class H, class V, class C>
class ColumnarRDD : public RDD<V>
{
  public:
    ColumnarRDD(DirRDD<H>* input, size_t estimation) : cache(estimation) { 
		SequentialScheduler scheduler;
		RddReactorFactory<ColumnarRDD,AppendCache> factory(*this);
		input->schedule(scheduler, 1, factory);
		scheduler.execute();
		input->release();
    }

	template<class R>
	stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory)
	{
        size_t concurrency = scheduler.getDefaultConcurrency();
		size_t size = cache._used;
		size_t partitionSize = (size + concurrency - 1)/concurrency;
        for (size_t i = 0; i < concurrency; i++) {
            scheduler.schedule(stage, new IterateCacheJob<R>(factory.getReactor(), *this, 
															 i*partitionSize, (i+1)*partitionSize > size ? size : (i+1)*partitionSize));
        }
		return stage;
	}

	virtual void release() {}

  private:

	template<class R>
	class IterateCacheJob : public Job
	{
	  public:
		void run() { 
			V record;
			for (size_t i = from; i < till; i++) { 
				record._data = &rdd.cache;
				record._pos = i;			
				reactor->react(record);				
			}
			delete reactor;
		}
		
		IterateCacheJob(R* r, ColumnarRDD& cs, size_t begin, size_t end) 
		: reactor(r), rdd(cs), from(begin), till(end) {}

	  private:
		R* reactor;
		ColumnarRDD& rdd;
		size_t const from;
		size_t const till;
	};

	class AppendCache : Reactor<H> { 
	  public:
		void react(H const& record) {
			rdd.cache.append(record);
		}

		AppendCache(ColumnarRDD& cache) : rdd(cache) {}

	  private:
		ColumnarRDD& rdd;
	};

  private:
    C cache;
};

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
 * Output results
 */
template<class T, class Rdd>
class OutputReactor : public Reactor<T>
{
public:
	OutputReactor(Rdd& owner, FILE* output) : rdd(owner), out(output) {}
	
	void react(T const& record) { 
		CriticalSection cs(rdd.mutex);
		print(record, out);
	}
	
private:
	Rdd&  rdd;
	FILE* out;
};


template<class T, class Rdd>
class OutputReactorFactory : public ReactorFactory< OutputReactor<T,Rdd> >
{
public:
	OutputReactorFactory(Rdd& owner, FILE* output) : rdd(owner), out(output) {}
	
	OutputReactor<T,Rdd>* getReactor() { 
		return new OutputReactor<T,Rdd>(rdd, out);
	}
	
private:
	Rdd&  rdd;
	FILE* out;
};

/**
 * Print RDD records to the stream
 * @param out output stream
 */
template<class T, class Rdd>
inline void output(Rdd* in, FILE* out)
{
	OutputReactorFactory<T,Rdd> factory(*in, out);
	StagedScheduler scheduler;
	in->schedule(scheduler, 1, factory);
	Cluster::instance->threadPool.run(scheduler);
}


#endif

