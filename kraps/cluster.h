#ifndef __CLUSTER_H__
#define __CLUSTER_H__

#include <inttypes.h>
#include "sync.h"
#include "sockio.h"

const size_t COORDINATOR = 0;
const size_t BUF_HDR_SIZE = 16;
const size_t MAX_PATH_LEN = 1024;
const size_t MAX_SIZE_T = (size_t)~0;

class Cluster;
class ReceiveJob;

typedef uint32_t cid_t;

enum MessageKind 
{ 
    MSG_DATA,
    MSG_EOF,
	MSG_BARRIER,
    MSG_SHUTDOWN
};

/**
 * Buffer is the main unit of echanging data between node in Flint. 
 * Although RDD is processning and traversing individual records, when data needs to be sent through network, 
 * records are packed into buffer and buffer is delivier to the destination node.
 */
struct Buffer 
{ 
    uint32_t    size;  // size without header
    uint32_t    node;  // sender nodeId
    MessageKind kind;  // message kind
	cid_t       cid;   // identifier of destination channel
    char        data[1];
    
    /**
     * Create new buffer of specified kind and size
     * @param cid channel ID (needed to locate recipient channel at target node)
     * @param size buffer data size (not including header)
     * @param type message type 
     * @return created buffer
     */
    static Buffer* create(cid_t cid, size_t size, MessageKind type = MSG_DATA) {
        return new (size) Buffer(type, cid, size);
    }
    /**
     * Buffer constructor
     * @param type message type 
     * @param id channel ID (needed to locate recipient channel at target node)
     * @param len buffer data size (not including header)
     */
    Buffer(MessageKind type, cid_t id, size_t len = 0) : size((uint32_t)len), kind(type), cid(id) {}

    void* operator new(size_t hdrSize, size_t bufSize) {
        return malloc(BUF_HDR_SIZE + bufSize);
    }
            
    void operator delete(void* ptr, size_t size) { 
        free(ptr);
    }

    void operator delete(void* ptr) { 
        free(ptr);
    }
};

class Channel;

class ChannelProcessor
{
  public:
	Channel* channel;

	virtual void process(Buffer* buf, size_t node) = 0;
	virtual ~ChannelProcessor() {}
};
	
class Queue
{
  public:
	void put(Buffer* buf) 
	{ 
		CriticalSection cs(mutex);
		if (buf->kind == MSG_EOF) { 
			delete buf;
			if (--nProducers == 0) { 
				ready.broadcast();
			}
		} else { 				
			Elem* elem = new Elem();
			*tail = elem;
			elem->next = NULL;
			elem->buf = buf;
			tail = &elem->next;
			ready.signal();
		}
	}

	Buffer* get() 
	{ 
		CriticalSection cs(mutex);
		Buffer* buf = NULL;
		Elem* elem;
		while ((elem = head) == NULL && nProducers != 0) {
			ready.wait(mutex);
		}
		if (elem != NULL) { 
			head = elem->next;
			if (head == NULL) { 
				tail = &head;
			}
			buf = elem->buf;
			delete elem;
		}
		return buf;
	}
	
	Queue(size_t n) : nProducers(n), head(NULL), tail(&head) {}

  private:
	struct Elem { 
		Elem* next;
		Buffer* buf;
	};

	size_t nProducers;
	Elem*  head;
	Elem** tail;	
	Mutex  mutex;
	Event  ready;
};
		
class Channel
{
  public:
	cid_t const cid;
	Cluster* const cluster;
	ChannelProcessor* processor;
	Queue queue;

	void attachProducer() {
		nProducers += 1;
	}

	void attachConsumer() {
		nConsumers += 1;
	}

	bool detachProducer() { 
		return __sync_add_and_fetch(&nProducers, -1) == 0;
	}

	void detachConsumer() { 
		if (__sync_add_and_fetch(&nConsumers, -1) == 0) { 
			delete processor;
			processor = NULL;
		}
	}
		
	void gather(stage_t stage, Scheduler& scheduler);

	Channel(cid_t cid, Cluster* cluster, ChannelProcessor* processor);
	~Channel() { delete processor; }

  private:	
	int nProducers;
	int nConsumers;
};

class GatherJob : public Job
{
  public:
	GatherJob(Channel* chan) : channel(chan) {
		channel->attachConsumer();
	}

	void run();

  protected:
	Channel* const channel;
};
    

/**
 * Main cluster class.
 */
class Cluster 
{
  public:
    /**
     * Check if this node is coordinator. Coordinator is forst node in the cluster and it is used to collect all
     * local results from other nodes
     */
    bool isCoordinator() { return nodeId == COORDINATOR; }

    /** 
     * Get new channel. It is expected that channels are created in the same order by all nodes, so
     * them are given same identifiers. This is why it is possible to use CID to identify recipient at target node.
     * @param hnd optional message handler used for push-style processing for this channel
     */
    Channel* getChannel(ChannelProcessor* proc);

    /**
     * Send message to the node or process it at local node
     * @param node destination node
     * @param channel if node is self node, then message is processed directly, otherwise it is sent to 
     * the remote node with cid (channel identifier) taken from this channel
     * @param buf message to be delivered
     */
    void send(size_t node, Channel* channel, Buffer* buf);
    
    /**
     * Send EOF message to the node
     * @param node destination node
     * @param channel if node is self node, then do nothing, otherwise it is sent to 
     * the remote node with cid (channel identifier) taken from this channel
     */
	void sendEof(size_t node, Channel* channel);
	
	/**
	 * Wait untillall nodes of the cluster reach barrier
	 */
	bool barrier(bool vote = true);

	/**
	 * Reset set of cluster: clear channels
	 */
	void reset();

    /**
     * Cluster constructor 
     * @param nodeId identifier of this cluster node (nodes are enumerated from 0)
     * @param nHosts number of nodes in clisters
     * @param hosts addresses of cluster node. Each address includes host name and port separated by column, i.e. "localhost:5011"
     * @param nThreads concurrency level (8)
     * @param bufferSize size of buffer used for internode communication  (256kb)
     * @param broadcastJoinThreshold threshold for choosing broadcast of inner table for join rather than shuffle method (10 000)
     * @param sharedNothing trues if each nodes is given its own local files, false if files in DFS are shared by all nodes
     * @param split split factor. Using split factor greater than 1 it is posisble to spawn more cluster nodes than there are 
     * physical partitions (files)
     */
    Cluster(size_t nodeId, size_t nHosts, char** hosts, size_t nThreads = 8, size_t bufferSize = 64*1024, size_t socketBufferSize = 64*1024*1024, size_t broadcastJoinThreshold = 10000, bool sharedNothing = true, size_t split = 1, bool verbose=false);
    ~Cluster();

  public:
    size_t const nNodes;
    size_t const nodeId;
    size_t const bufferSize;
    size_t const broadcastJoinThreshold;
    size_t const split;
    bool   const sharedNothing;
	bool   const verbose;
	bool verdict;
    bool shutdown;
    void* userData;
    ThreadPool threadPool;
    ThreadPool streamingThreadPool;

    static ThreadLocal<Cluster> instance;

  private:
	friend class ReceiveJob;

	struct Node { 
		Mutex   mutex;
		Socket* socket;
        Thread* receiver;
		
		Node() : socket(NULL), receiver(NULL) {}
		~Node() { delete socket; }		
	};

	vector<Node> nodes;
    char** hosts;
	vector<Channel*> channels;
	Semaphore semaphore;
	Mutex mutex;
	
	/**
	 * Handle BARRIER message
	 */
	void sync(bool vote);

    /**
     * Check if sepcified address corresponds to local node
     * @param host host address
     * @return true if host is localhost or matchs with name returned by uname
     */
    bool isLocalNode(char const* host);
};


#endif
