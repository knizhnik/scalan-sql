#ifndef __RDD_H__
#define __RDD_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include "cluster.h"
#include "pack.h"
#include "hash.h"

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

/**
 * Main interface for PUSH mode: react on pushed events
 */
template<class T>
class Reactor
{
  public:
    /**
	 * Proceed record	 
	 * Do not use virtual functions here: let derived template classes be inlined
	 */
    void react(T const& record) {}
	
	/**
	 * Destructor is used to inform consumer that end of data is reached */
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
 * Compbination of ChainReactorFactory and RddReactporFactory
 */
template<class Rdd,class Parent,class Child>
class ChainRddReactorFactory : public ReactorFactory<Child>
{
  public:
	ChainRddReactorFactory(Rdd& owner, ReactorFactory<Parent>& factory) : rdd(owner), parent(factory) {}

	R* getReactor() { 
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
 * Schedule executing in parallel jobs of one stage and waiting there completion before staring new stage
 */
class StagedScheduler : public Scheduler
{
  public:
    StagedScheduler() : currStage(0), currJob(0), activeJobs(0), stageStartTime(getCurrentTime()) {}
    
	size_t getDefaultConcurrency() 
	{
		return Cluster::instance.threadPool.getDefaultConcurrency();
	}
		
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

  private:
    vector< vector<Job> > jobs;
    Mutex    mutex;
    Event    stageDone;
    stage_t  currStage;
    size_t   currJob;
    size_t   activeJobs;
    time_t   stageStartTime;    
};
        

/**
 * Abstract RDD (Resilient Distributed Dataset)
 */
template<class T>
class RDD
{
  public:
	/**
	 * Schedule jobs for this RDD
	 */
    template<class R>
    virtual stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory);

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
     * Print RDD records to the stream
     * @param out output stream
     */
    void output(FILE* out)
	{
        OutputFactory factory(*this, out);
        StagedScheduler scheduler;
        schedule<Output>(scheduler, 1, factory);
        Cluster::instance->threadPool.run(scheduler);
    }

	
	virtual void release() {
		delete this;
	}
		
    virtual~RDD() {}

  private:
	class OutputReactor : public Reactor<T>
	{
	  public:
		OutputReactor(RDD& owner, FILE* output) : rdd(owner), out(output) {}

		void react(T const& record) { 
			CriticalSection cs(rdd.mutex);
			print(record, out);
		}

	  private:
		RDD&  rdd;
		FILE* out;
	};

	class OutputFactory : public ReactorFactory<OutputReacto>
	{
	  public:
		OutputFactory(RDD& owner, FILE* output) : rdd(owner), out(output) {}

		OutputReactor* getFactory() { 
			return new OutputReactor(rdd, out);
		}

	  private:
		RDD&  rdd;
		FILE* out;
	};

  protected:
    Mutex mutex;
};

/**
 * Reactor for shufling records for distribution key among cluster nodes
 */
template<class T, void (*dist_key)(K& key, T const& record)>
class Scatter : public Reactor
{
public:
	Scatter(Channel* chan) 
	: channel(chan), cluster(Cluster::instance.get()), nNodes(cluster->nNodes), bufferSize(cluster->bufferSize), buffers(nNodes) {} 
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


/**
 * Reactor for sending data to the specific node (usually coordinator)
 */
template<class T>
class Sender : public Reactor<T>
{
public:
	Sender(Channel* chan, size_t destination = COORDINATOR) : cluster(Cluster::instance.get()), channe(chan), node(destination) { 
		msg = Buffer::create(channel->cid, cluster->bufferSize);
		msg->size = 0;
	}

	void react(T const& record) { 
		if (msg->size + sizeof(T) > cluster->bufferSize) { 
			cluster->send(node, channel, msg);
			msg->size = 0;
		}
		msg->size += pack(record, msg->data + msg->size);
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


/**
 * Reactor for broadcastig data to all cluster nodes
 */
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
    FileRDD(char* filePath) 
	: path(filePath),
	  segno(Cluster::instance->nodeId), 
	  split(Cluster::instance->sharedNothing ? 1 : Cluster::instance->nNodes) {}

    ~FileRDD() {
        delete[] path;
    }

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) {
        size_t concurrecy = scheduler.getDefaultConcurrency();
        for (size_t i = 0; i < concurrency; i++) {
            scheduler.schedule(stage, new ReadJob<R>(*this, factory.getReactor(), i, concurrency));
        }
		return stage;
    }

  private:
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
            jobSize = (ftell(f)/sizeof(T)+file.split*concurrency-1)/(file.split*concurrency);
            size_t segmentOffs = segmentSize*(file.segno*concurrency + id)*sizeof(T);
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
        size_t concurrecy = scheduler.getDefaultConcurrency();
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
        ~ReadJob() {
            fclose(f);
        }
      private:
        char*        dirPath;
        R*           reactor;
        size_t const segno;
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
template<class T, bool (*predicate)(T const&)>
class FilterRDD : public RDD<T>
{
  public:
    FilterRDD(RDD<T>* input) : in(input) {}
    ~FilterRDD() { in->release(); }

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) {
		ChainReactorFactory< R,FilterReactor<R> > filter(factory);
        return in->shedule<FilterReactor>(scheduler, stage, filter);
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
  public:
    ReduceRDD(RDD<T>* input, S const& initState) : state(initState), in(input) {}
    ~ReduceRDD() { in->release(); }

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) 
	{
		RddReactorFactory<ReduceRDD,ReduceReactor> reduce(*this);
        stage = in->shedule(scheduler, stage, reduce);
        Cluster* cluster = Cluster::instance.get();
		Channel* channel = cluster->getChannel(new ReduceProcessor(*this));
		if (Cluster::instance->isCoordinator()) {
			barrier = new BarrierJob(cluster->nNodes-1);
			scheduler.schedule(++stage, barrier);		
			scheduler.schedule(++stage, new SendJob<S,R>(state, factory.getReactor()));
		} else { 
			scheduler.schedule(++stage, new SendJob<S, Sender<S> >(state, new Sender<S>(channel)));
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

    template<class R>
	class ReduceProcessor { 
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
		
  private:
    S state;
	RDD<T>* in;
	BarrierJob* barrier;
};
   


/**
 * Classical Map-Reduce
 */
template<class T,class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src) >
class MapReduceRDD : public RDD< Pair<K,V> > 
{    
  public:
    MapReduceRDD(RDD<T>* input, size_t estimation) : initSize(hashTableSize(estimation)), in(input) {}
	~MapReduceRDD() { in->release(); }
			
    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) 
	{
		RddReactorFactory<MapReduceRDD,MapReduceReator> reduce(*this);
        stage = in->shedule(scheduler, stage, reduce);
        Cluster* cluster = Cluster::instance.get();
		Channel* channel = cluster->getChannel(new MapReduceProcessor(*this));
		if (Cluster::instance->isCoordinator()) {
			barrier = new BarrierJob(cluster->nNodes-1);
			scheduler.schedule(++stage, barrier);		
			stage += 1;
			size_t nPartitions = cluster->threadPool.deafultConcurrency();
			for (size_t i = 0; i < nPartitions; i++) { 				
				scheduler.schedule(stage, hashMap.iterate<R>(factory.getReactor(), i, nPartitions));
			}
		} else { 
			scheduler.schedule(++stage, hashMap.iterate<Sender< Pair<K,V> >(new Sender<Pair<K,V> >(channel))));
		}
		return stage;
	}
	
  private:
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

		MapReduceProcessor(MapReduce& reduce) : rdd(reduce) {}

	private:
		MapReduce& rdd;
	};	

  private:
	size_t const initSize;
	RDD<T>* const in;
	KeyValueMap<K,V> hashMap;	
	BarrierJob* barrier;
};

/**
 * Project (map) RDD records
 */
template<class T, class P, void project(P& out, T const& in), class Rdd = RDD<T> >
class ProjectRDD : public RDD<P>
{
  public:
    ProjectRDD(Rdd* input) : in(input) {}
    ~ProjectRDD() { in->release(); }

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) {
		ChainReactorFactory< R,ProjectReactor<R> > project(factory);
        return in->shedule(scheduler, stage, project);
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
    RDD<T>* const in;
};

/**
 *  Sort using given comparison function
 */
template<class T, int compare(T const* a, T const* b)>
class SortRDD : public RDD<T>
{
  public:
    SortRDD(RDD<T>* input, size_t estimation) : in(input){}
	~SortRDD() { in->release();	}

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) {
		Channel* channel = cluster->getChannel(new AppendProcessor(*this));
		ChannelReactorFactory< Sender<T> > sender(channel);
        stage = in->shedule(scheduler, stage, sender);
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
    template<class R>
	class AppendProcessor { 
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
    TopRDD(Rdd* input, size_t n) : in(input), top(n) {}
	~TopRDD() { in->release(); }

    template<class R>
    stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory) {
		Channel* channel = cluster->getChannel(new MergeSortProcessor(*this));
		RddReactorFactory<TopRDD<TopReactor> topReactor(*this);
        stage = in->shedule(scheduler, stage, topReactor);
		if (Cluster::instance->isCoordinator()) {
			barrier = new BarrierJob(cluster->nNodes-1);
			scheduler.schedule(++stage, barrier);		
			// Create just one job because most likely we want ot preserve sort order
			scheduler.schedule(++stage, new IterateArrayJob<T,R>(factory.getReactor(), heap.buf));
		} else { 
			scheduler.schedule(++state, new IterateArrayJob<T,Sender<T> >(new Sender<T>(channel), heap.buf));
		}
		return stage;
    }
	
  private:
    template<class R>
	class MergeSortProcessor { 
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
        
        SortedArray(size_t n) : top(n) {}

	  private:
        vector<T> buf;
        size_t top;
    };

    class TopReactor : public Reactor
    {
      public:
        TopReactor(TopRDD& r) : heap(r.top), rdd(r) {}
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
    SortedArray heap;
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

        if (estimation <= cluster->broadcastJoinThreshold) {
            // broadcast inner table
			ChannelReactorFactory< Broadcaster<I> > broadcast(channel);
            stage = inner->shedule< Broadcaster<I> >(scheduler, stage, broadcast);

            scheduler.schedule(++stage, innerBarrier);

			ChainRddReactorFactory<HashJoinRDD,R,JoinReactor> join(*this, factory);
            stage = outer->schedule(scheduler, ++stage, join);
        } else {
            // shuffle both tables
			ChannelReactorFactory< Scatter<I, innerKey> > innerScatter(channel);
            stage = inner->shedule(scheduler, stage, innerScatter);

            scheduler.schedule(++stage, innerBarrier);

            outerBarrier = new BarrierJob(cluster->nNodes-1);
            channel = cluster->getChannel(new OuterProcessor<R>(*this, factory));

			ChannelReactorFactory< Scatter<O, outerKey> > outerScatter(channel);
            stage = outer->schedule(scheduler, ++stage, outerScatter);

            scheduler.schedule(++stage, outerBarrier);
        }
        return stage;
    }

  private:
    template<class R>
	class InnerProcessor { 
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
	class OuterProcessor { 
	  public:
		void process(Buffer* buf, size_t node) { 
			if (buf->kind == MSG_EOF) { 
				rdd.outerBarrier->reach();
                delete reactors[node];
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
                            reactors[node]->react(pair);
                            nMatches += 1;
                        }
                    }
                    if (nMatches == 0 && rdd.kind == OuterJoin){ 
                        reactors[node]->react(pair);
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
    RDD<I>* const outer;
    RDD<I>* const inner;
	size_t  const initSize;
    HashTable<I, innerKey> hashTable;
    BarrierJob* outerBarrier;
    BarrierJob* innerBarrier;
};
  
class SequentialScheduler : Scheduler
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
    CachedRDD(RDD<T>* input, size_t estimation) { 
		array.reserve(estimation);
		SequentialScheduler scheduler;
		RddReactorFactory<CachedRDD,AppendArray> factory(*this);
		input->schedule(scheduler, 1, factory);
		scheduler.execute();
		input->release();
    }

	template<class T>
	stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory)
	{
        size_t concurrecy = scheduler.getDefaultConcurrency();
        for (size_t i = 0; i < concurrency; i++) {
            scheduler.schedule(stage, new IterateArrayJob<R>(factory.getReactor(), array, i, concurrency));
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

		AppendArray(cachedRDD& cache) : rdd(cache) {}
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
    ColumnarRDD(RDD<H>* input, size_t estimation) : cache(estimation) { 
		SequentialScheduler scheduler;
		RddReactorFactory<ColumnarRDD,AppendCache> factory(*this);
		input->schedule(scheduler, 1, factory);
		scheduler.execute();
		input->release();
    }

	template<class T>
	stage_t schedule(Scheduler& scheduler, stage_t stage, ReactorFactory<R>& factory)
	{
        size_t concurrecy = scheduler.getDefaultConcurrency();
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
				record._data = &cache;
				record._pos = i;			
				reactor->react(record);				
			}
			delete reactor;
		}
		
		IterateCacheJob(R* r, ColumnarRDD& cs, size_t begin, size_t end) 
		: reactor(r), rdd(cs), from(begin), end(till) {}

	  private:
		R* reactor;
		ColumnarRDD& rdd;
		size_t const from;
		size_t const step;
	};

	class AppendCache : Reactor<H> { 
	  public:
		void react(H const& record) {
			rdd.cache.append(record);
		}

		AppendArray(cachedRDD& cache) : rdd(cache) {}

	  private:
		ColumnarRDD& rdd;
	};

  private:
    C cache;
};

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
	return new HashJoinRDD<T,I,K,outerKey,innerKey>(this, with, estimation, kind);
}

#endif
