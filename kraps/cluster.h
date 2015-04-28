#include <inttypes.h>
#include "sync.h"
#include "sockio.h"

const size_t COORDINATOR = 0;
const size_t BUF_HDR_SIZE = 8;

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
    uint32_t size; // size without header
    uint16_t qid;  // identifier of destination queue
    uint16_t kind; // message kind
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

    Buffer(MessageKind type, qid_t id, size_t len = 0) : size((uint32_t)len), qid((uint16_t)id), kind((uint16_t)type) {}

    void* operator new(size_t hdrSize, size_t bufSize) { 
        return new char[BUF_HDR_SIZE + bufSize];
    }
    void operator delete(void* ptr, size_t size) { 
        delete[] (char*)ptr;
    }
    void operator delete(void* ptr) { 
        delete[] (char*)ptr;
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

    Queue(qid_t id, size_t maxSize) 
    : qid(id), head(NULL), tail(&head), size(0), limit(maxSize), blockedPut(false), blockedGet(false) {}

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
    size_t size;
    size_t limit;
    bool blockedPut;
    bool blockedGet;
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
    SendJob(size_t id) : node(id) {}

private:
    size_t const node;
};

class Cluster {
  public:
    size_t const nNodes;
    size_t const maxQueues;
    size_t const nodeId;
    size_t const bufferSize;
    Socket** sockets;
    Queue** recvQueues;
    Queue** sendQueues;
    Queue*  syncQueue;
    Thread** senders;
    size_t  pingPongInterval;
    qid_t qid;
    Thread* receiver;
    bool shutdown;

    bool isCoordinator() { return nodeId == COORDINATOR; }
    Queue* getQueue();
    void barrier();

    Cluster(size_t nodeId, size_t nHosts, char** hosts, size_t nQueues = 64, size_t bufferSize = 64*1024, size_t recvQueueSize = 64*1024*1024,  size_t sendQueueSize = 8*1024*1024, size_t syncInterval = 1024*1024);
    ~Cluster();

    static Cluster* instance;
};
