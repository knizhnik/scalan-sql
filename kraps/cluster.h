#ifndef __CLUSTER_H__
#define __CLUSTER_H__

#include <inttypes.h>
#include "sync.h"
#include "sockio.h"

const size_t COORDINATOR = 0;
const size_t BUF_HDR_SIZE = 16;
const size_t MAX_PATH_LEN = 1024;

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

struct Buffer 
{ 
    uint32_t compressedSize; // compressed size 
    uint32_t size;  // size without header
    uint16_t node;  // sender
    uint16_t qid;   // identifier of destination queue
    uint16_t kind;  // message kind
    uint16_t refCount; // reference count
    char     data[1];
    
    static Buffer* create(qid_t qid, size_t size, MessageKind type = MSG_DATA) {
        return new (size) Buffer(type, qid, size);
    }

    static Buffer* eof(qid_t qid) { 
        return create(qid, 0, MSG_EOF);
    }

    static Buffer* barrier(qid_t qid) { 
        return create(qid, 0, MSG_BARRIER);
    }

    static Buffer* ping(qid_t qid) { 
        return create(qid, 0, MSG_PING);
    }

    Buffer(MessageKind type, qid_t id, size_t len = 0) : size((uint32_t)len), qid((uint16_t)id), kind((uint16_t)type), refCount(1) {}

    void* operator new(size_t hdrSize, size_t bufSize) {
        return malloc(BUF_HDR_SIZE + bufSize);
    }
            
    void operator delete(void* ptr, size_t size) { 
        free(ptr);
    }

    void operator delete(void* ptr) { 
        free(ptr);
    }

    void release() { 
        assert(refCount > 0);
        if (--refCount == 0) { 
            free(this);
        }
    }
};

// FIFO blocking queue, multiple consumers/producers
class Queue
{
  public:
    qid_t const qid;

    void putFirst(Buffer* buf);
    void put(Buffer* buf);
    Buffer* get();

    void wait(size_t n) { 
        semaphore.wait(mutex, n);
    }
    void signal() { 
        semaphore.signal(mutex);
    }

    Queue(qid_t id, size_t maxSize) 
    : qid(id), head(NULL), tail(&head), size(0), limit(maxSize), nSignals(0), blockedPut(false), blockedGet(false) {}

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
};
        
class ReceiveJob : public Job
{
  public:
    void run();
};

class SendJob : public Job
{
public:
    void run();
    SendJob(size_t id) : node(id), sent(0) {}

private:
    size_t const node;
    size_t sent;
};

class Cluster {
  public:
    size_t const nNodes;
    size_t const maxQueues;
    size_t const nodeId;
    size_t const bufferSize;
    size_t const syncInterval;
    size_t const broadcastJoinThreshold;
    size_t const inmemJoinThreshold;
    bool   const sharedNothing;
    char const* tmpDir;
    Socket** sockets;
    Queue** recvQueues;
    Queue** sendQueues;
    Queue*  syncQueue;
    Thread** senders;
    char** hosts;
    qid_t qid;
    Thread* receiver;
    bool shutdown;
    size_t nExecutorsPerHost;
    
    FILE* openTempFile(char const* prefix, qid_t qid, size_t no, char const* mode = "r");
    
    bool isCoordinator() { return nodeId == COORDINATOR; }
    Queue* getQueue();
    void barrier();

    bool isLocalNode(char const* host);

    Cluster(size_t nodeId, size_t nHosts, char** hosts, size_t nQueues = 64, size_t bufferSize = 4*64*1024, size_t recvQueueSize = 4*64*1024*1024,  size_t sendQueueSize = 4*4*1024*1024, size_t syncInterval = 64*1024*1024, size_t broadcastJoinThreshold = 10000, size_t inmemJoinThreshold = 10000000, char const* tmp = "/tmp", bool sharedNothing = false);
    ~Cluster();

    static ThreadLocal<Cluster> instance;
};


extern uint32_t murmur_hash3_32(const void* key, const int len);

#endif
