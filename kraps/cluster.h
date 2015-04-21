#include "sync.h"
#include "sockio.h"

const size_t COORDINATOR = 0;
const size_t BUF_HDR_SIZE = 8;

class Cluster;

typedef unsigned size32_t;
typedef size32_t qid_t;

struct Buffer 
{ 
    size32_t size;
    qid_t    qid;
    char     data[1];

    bool isEof() { 
        return size == 0;
    }
    
    static Buffer* create(qid_t qid, size32_t size) {
        return new (size) Buffer(qid, size);
    }
    
    Buffer(qid_t id, size32_t len) : size(len), qid(id) {}

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

// FIFO queue, one consumer, multipler producers
class Queue
{
  public:
    qid_t const qid;

    void put(Buffer* buf);
    Buffer* get();

    Queue(qid_t id, size_t maxSize) 
    : qid(id), head(NULL), tail(&head), size(0), limit(maxSize), nFinished(0), blockedPut(false), blockedGet(false) {}

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
    size_t nFinished;
    bool blockedPut;
    bool blockedGet;
};
        
class GatherJob : public Job
{
  public:
    void run();
};

     
class Cluster {
  public:
    size_t const nNodes;
    size_t const nodeId;
    size_t const bufferSize;
    Socket** sockets;
    Queue** queues;
    qid_t qid;
        
    bool isCoordinator() { return nodeId == COORDINATOR; }
    Queue* getQueue();
    void reset() { qid = 0; }

    Cluster(size_t nodeId, size_t nHosts, char** hosts, size_t nQueues = 16, size_t bufferSize = 64*1024, size_t queueSize = 1024);

    static Cluster* instance;
};
