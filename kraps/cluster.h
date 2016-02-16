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

typedef size_t qid_t;

enum MessageKind 
{ 
    MSG_DATA,
    MSG_PING,
    MSG_PONG,
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
    uint32_t compressedSize; // compressed size 
    uint32_t size;  // size without header
    uint16_t node;  // sender
    uint16_t qid;   // identifier of destination queue
    uint16_t kind;  // message kind
    uint16_t refCount; // reference count
    char     data[1];
    
    /**
     * Create new buffer of specified kind and size
     * @param qid queue ID (needed to locate recipient queue at target node)
     * @param size buffer data size (not including header)
     * @param type message type 
     * @return created buffer
     */
    static Buffer* create(qid_t qid, size_t size, MessageKind type = MSG_DATA) {
        return new (size) Buffer(type, qid, size);
    }

    /**
     * Create buffer with EOF (End-Of-File) message
     * @param qid queue ID (needed to locate recipient queue at target node)
     * @return new EOF message
     */
    static Buffer* eof(qid_t qid) { 
        return create(qid, 0, MSG_EOF);
    }

    /**
     * Create buffer with barries message
     * @param qid queue ID (needed to locate recipient queue at target node)
     * @return new barrier message
     */
    static Buffer* barrier(qid_t qid) { 
        return create(qid, 0, MSG_BARRIER);
    }

    /**
     * Create buffer with ping message. Ping messages are used to synchronize data consumers and produces and so
     * avoid queueus overflow
     * @param qid queue ID (needed to locate recipient queue at target node)
     * @return new ping message
     */
    static Buffer* ping(qid_t qid) { 
        return create(qid, 0, MSG_PING);
    }

    /**
     * Buffer constructor
     * @param type message type 
     * @param id queue ID (needed to locate recipient queue at target node)
     * @param len buffer data size (not including header)
     */
    Buffer(MessageKind type, qid_t id, size_t len = 0) : compressedSize((uint32_t)len), size((uint32_t)len), qid((uint16_t)id), kind((uint16_t)type), refCount(1) {}

    void* operator new(size_t hdrSize, size_t bufSize) {
        return malloc(BUF_HDR_SIZE + bufSize);
    }
            
    void operator delete(void* ptr, size_t size) { 
        free(ptr);
    }

    void operator delete(void* ptr) { 
        free(ptr);
    }

    /**
     * Release buffer. Flint associates reference counter with each buffer to impkement shared pointers to the buffers.
     * This method allows to delete buffer when it is not needed to anybody (there are no more references to the buffer).
     */
    void release() { 
        if (__sync_add_and_fetch(&refCount, -1) == 0) {
            free(this);
        }
    }
};

/**
 * Message handler inteface is needed to implement push-style handling of buffers (used only for local multithreaded
 * configuration of Flint).
 */
class MessageHandler
{
  public:
    virtual void handle(Buffer* buf) = 0;
    virtual ~MessageHandler() {}
};

/**
 * FIFO blocking queue, multiple consumers/producers
 */
class Queue
{
    friend class Cluster;
  public:
    qid_t const qid;

    /**
     * Place message at the queue head
     * @param buf buffer to be prepended to queue
     */
    void putFirst(Buffer* buf);

    /** 
     * Place message at queue tail
     * @param buf buffer to be appended to queue
     */
    void put(Buffer* buf);
    
    /** 
     * Get message from queue head
     * @return head of queue
     */
    Buffer* get();

    /**
     * Wait N notifications
     * @param n number of signals to wait for
     */
    void wait(size_t n) { 
        semaphore.wait(mutex, n);
    }
    
    /**
     * Send notification
     */
    void signal() { 
        semaphore.signal(mutex);
    }
    
    /** 
     * Send message handler for this queue. 
     * It is used by push-style processing 
     */
    void setHandler(MessageHandler* hnd) { 
        handler = hnd;
    }

    /**
     * Queue onstructor
     * @param id Identifer of queue. It is expected that queues are created in the same order by all nodes, so
     * them are given same queue identifiers. This is why it is possible to use QID to identify recipient at target node.
     * @param maxSize maximal queue size, when amounbt of pending message exceeds this value put will be blocked
     */
    Queue(qid_t id, size_t maxSize) 
    : qid(id), head(NULL), tail(&head), size(0), limit(maxSize), nSignals(0), blockedPut(false), blockedGet(false), handler(NULL) {}

  private:
    struct Message { 
        Message* next;
        Buffer* buf;
        
        Message(Buffer* msgBuf) : next(NULL), buf(msgBuf) {}
    };

    Message* head;
    Message** tail;
    Mutex mutex;
    Event empty;
    Event full;
    Event sync;
    size_t size;
    size_t limit;
    size_t nSignals;
    bool blockedPut;
    bool blockedGet;
    Semaphore semaphore;
    MessageHandler* handler;
};
       
/**
 * Job for receiving data. There is single receive thread in process.
 */
class ReceiveJob : public Job
{
  public:
    void run();
};

/**
 * Job for sending data. There is separate send thread for each connected node with dedicated queue.
 */
class SendJob : public Job
{
public:
    void run();
    SendJob(size_t id) : node(id), sent(0) {}

private:
    size_t const node;
    size_t sent;
};

/**
 * Main cluster class.
 */
class Cluster 
{
  public:
    size_t const nNodes;
    size_t const maxQueues;
    size_t const nodeId;
    size_t const bufferSize;
    size_t const syncInterval;
    size_t const broadcastJoinThreshold;
    size_t const inmemJoinThreshold;
    size_t const split;
    bool   const sharedNothing;
    char const* tmpDir;
    Socket** sockets;
    Queue** recvQueues;
    Queue** sendQueues;
    Thread** senders;
    char** hosts;
    qid_t qid;
    Thread* receiver;
    bool shutdown;
    size_t nExecutorsPerHost;
    void* userData;
    Mutex mutex;
    Event cond;
    ThreadPool threadPool;
    static Cluster** nodes; 
    
    FILE* openTempFile(char const* prefix, qid_t qid, size_t no, char const* mode = "r");
    
    /**
     * Check if this node is coordinator. Coordinator is forst node in the cluster and it is used to collect all
     * local results from other nodes
     */
    bool isCoordinator() { return nodeId == COORDINATOR; }

    /** 
     * Get new queue. It is expected that queues are created in the same order by all nodes, so
     * them are given same queue identifiers. This is why it is possible to use QID to identify recipient at target node.
     * @param hnd optional message handler used for push-style processing for this queue
     */
    Queue* getQueue(MessageHandler* hnd = NULL);

    /**
     * Set barrier: syncronize execution of all nodes
     */
    void barrier();

    /**
     * Send message to the node or place it in locasl queue
     * @param node destination node
     * @param queue if node is self node, then message is placed directly in this queue, otherwise it is sent to 
     * the remote node with qid (queue identifier) taken from this queue
     * @param buf message to be delivered
     */
    void send(size_t node, Queue* queue, Buffer* buf);
    
    /**
     * Check if sepcified address corresponds to local node
     * @param host host address
     * @return true if host is localhost or matchs with name returned by uname
     */
    bool isLocalNode(char const* host);

    /**
     * Cluster constructor 
     * @param nodeId identifier of this cluster node (nodes are enumerated from 0)
     * @param nHosts number of nodes in clisters
     * @param hosts addresses of cluster node. Each address includes host name and port separated by column, i.e. "localhost:5011"
     * @param nQueues maximal number of receive queues  (64)
     * @param bufferSize size of buffer used for internode communication  (256kb)
     * @param recvQueueSize size of receive queue (256Mb)
     * @param sendQueueSize size of send queue (16Mb)
     * @param syncInterval synchorinization inerval (amount of bytes after which PING message is sent to synchronize data producer and consumer (64Mb)
     * @param broadcastJoinThreshold threshold for choosing broadcast of inner table for join rather than shuffle method (10 000)
     * @param inmemJoinThreshold threshold for choosing shuffling to files instead of in-memory join (10 000 000)
     * @param tmp directory  for creating shuffling files ("/tmp")
     * @param sharedNothing trues if each nodes is given its own local files, false if files in DFS are shared by all nodes
     * @param split split factor. Using split factor greater than 1 it is posisble to spawn more cluster nodes than there are 
     * physical partitions (files)
     */
    Cluster(size_t nodeId, size_t nHosts, char** hosts, size_t nQueues = 64, size_t bufferSize = 4*64*1024, size_t recvQueueSize = 4*64*1024*1024,  size_t sendQueueSize = 4*4*1024*1024, size_t syncInterval = 64*1024*1024, size_t broadcastJoinThreshold = 10000, size_t inmemJoinThreshold = 10000000, char const* tmp = "/tmp", bool sharedNothing = true, size_t split = 1);
    ~Cluster();

    static ThreadLocal<Cluster> instance;
};

    
extern uint32_t murmur_hash3_32(const void* key, const int len);

#endif
