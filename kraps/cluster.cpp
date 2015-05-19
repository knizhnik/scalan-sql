#include <unistd.h>
#include "rdd.h"
#include "compress.h"

const unsigned shutdownDelay = 5;
const size_t MB = 1024*1024;

Cluster* Cluster::instance;

void Queue::put(Buffer* buf) 
{ 
    CriticalSection cs(mutex);
    while (size >= limit) { 
        blockedPut = true;
        full.wait(mutex);
    }
    size += buf->size;
    Message* msg = new Message(buf);
    *tail = msg;
    tail = &msg->next;
    if (blockedGet) { 
        blockedGet = false;
        empty.broadcast();
    }
}

void Queue::putFirst(Buffer* buf) 
{ 
    CriticalSection cs(mutex);
    size += buf->size;
    Message* msg = new Message(buf);
    if (head == NULL) { 
        tail = &msg->next;
    }
    msg->next = head;
    head = msg;
    if (blockedGet) { 
        blockedGet = false;
        empty.broadcast();
    }
}

Buffer* Queue::get() 
{
    CriticalSection cs(mutex);
    while (head == NULL) { 
        blockedGet = true;
        empty.wait(mutex);
    }
    Message* msg = head;
    Buffer* buf = msg->buf;
    size -= buf->size;
    head = head->next;
    delete msg;
    if (head == NULL) { 
        tail = &head;
    }
    if (blockedPut) { 
        blockedPut = false;
        full.broadcast();
    }
    return buf;
}

Queue* Cluster::getQueue()
{
    assert(qid < maxQueues);
    return recvQueues[qid++];
}

Cluster::Cluster(size_t selfId, size_t nHosts, char** hosts, size_t nQueues, size_t bufSize, size_t recvQueueSize, size_t sendQueueSize, size_t syncPeriod) 
: nNodes(nHosts), maxQueues(nQueues), nodeId(selfId), bufferSize(bufSize), syncInterval(syncPeriod), shutdown(false)
{
    instance = this;

    sockets = new Socket*[nHosts];
    memset(sockets, 0, nHosts*sizeof(Socket*));

    qid = 0;
    syncQueue = new Queue(0, sendQueueSize);
    recvQueues = new Queue*[nQueues];
    for (size_t i = 0; i < nQueues; i++) { 
        recvQueues[i] = new Queue((qid_t)i, recvQueueSize);
    }
    sendQueues = new Queue*[nHosts];
    senders = new Thread*[nHosts];
    for (size_t i = 0; i < nHosts; i++) { 
        if (i != selfId) { 
            sendQueues[i] = new Queue((qid_t)i, sendQueueSize);
            senders[i] = new Thread(new SendJob(i));
        }
    }
    sendQueues[selfId] = NULL;

    for (size_t i = 0; i < selfId; i++) { 
        sockets[i] = Socket::connect(hosts[i]);
        sockets[i]->write(&nodeId, sizeof nodeId);
    }
    sockets[selfId] = NULL;

    char* sep = strchr(hosts[selfId], ':');
    int port = atoi(sep+1);
    Socket* localGateway = Socket::createLocal(port, nHosts);
    Socket* globalGateway = Socket::createGlobal(port, nHosts);

    for (size_t i = selfId+1; i < nHosts; i++) {
        size_t node;
        Socket* s = Socket::isLocalHost(hosts[i])
            ? localGateway->accept()
            : globalGateway->accept();
        s->read(&node, sizeof node);
        assert(sockets[node] == NULL);
        sockets[node] = s;
    }
    delete localGateway;
    delete globalGateway;

    receiver = new Thread(new ReceiveJob());
        
}

Cluster::~Cluster()
{
    Buffer shutdownMsg(MSG_SHUTDOWN, 0);
    shutdown = true;
    sleep(shutdownDelay);
    for (size_t i = 0; i < nNodes; i++) { 
        if (i != nodeId) { 
            sendQueues[i]->put(&shutdownMsg);
            delete senders[i];
            delete sendQueues[i];
        }
    }
    delete receiver;
    // at this moment all threads should finish
    
    delete[] senders;
    delete syncQueue;

    for (size_t i = 0; i < maxQueues; i++) { 
        delete recvQueues[i];
    }
    delete[] recvQueues;

    for (size_t i = 0; i < nNodes; i++) { 
        if (i != nodeId) { 
            delete sockets[i];
        }
    }
    delete[] sockets;
}

     
void Cluster::barrier()
{
    Queue* queue = getQueue();
    for (size_t i = 0; i < nNodes; i++) { 
        Queue* dst = (i == nodeId) ? queue : sendQueues[i];
        dst->put(Buffer::barrier(queue->qid));
    }
    for (size_t i = 0; i < nNodes; i++) { 
        Buffer* resp = queue->get();
        assert(resp->kind == MSG_BARRIER);
        delete resp;
    }
    qid = 0;
}

void ReceiveJob::run()
{
    Cluster* cluster = Cluster::instance;
    Buffer* ioBuf = Buffer::create(0, cluster->bufferSize);
    size_t totalReceived = 0;
    try { 
        while (true) {
            Socket* socket = Socket::select(cluster->nNodes, cluster->sockets);
            if (cluster->shutdown) {
                break;
            }
            socket->read(ioBuf, BUF_HDR_SIZE);
            totalReceived += BUF_HDR_SIZE;
            
            Buffer* buf = Buffer::create(ioBuf->qid, cluster->bufferSize);
            memcpy(buf, ioBuf, BUF_HDR_SIZE);
            
            if (ioBuf->compressedSize < ioBuf->size) { 
                socket->read(ioBuf->data, ioBuf->compressedSize);
                decompress(buf->data, ioBuf->data, ioBuf->size, ioBuf->compressedSize);
                totalReceived += ioBuf->compressedSize;
            } else if (buf->size != 0) { 
                socket->read(buf->data, buf->size);
                totalReceived += buf->size;
            }
            switch (buf->kind) {
              case MSG_PONG:
                assert(buf->qid < cluster->maxQueues);
                cluster->recvQueues[buf->qid]->signal();            
                delete buf;
                continue;
              case MSG_SHUTDOWN:
                delete buf;
                break;
              default:
                cluster->recvQueues[buf->qid]->put(buf);
                continue;
            }
            break;
        }
    } catch (std::exception& x) {
        printf("Receiver catch exception %s\n", x.what());
    } 
    printf("Totally received %ldMb\n", totalReceived/MB);
    delete ioBuf;
}


void SendJob::run()
{
    Cluster* cluster = Cluster::instance;
    size_t compressBufSize = cluster->bufferSize*2;
    Buffer* ioBuf = Buffer::create(0, compressBufSize);
    try {
        while (true) { 
            Buffer* buf = cluster->sendQueues[node]->get();
            buf->node = (uint32_t)cluster->nodeId;
            buf->compressedSize = buf->size ? compress(ioBuf->data, buf->data, buf->size) : 0;
            if (buf->kind == MSG_SHUTDOWN) { 
                if (node == (cluster->nodeId + 1) % cluster->nNodes) { // shutdown neighbour receiver
                    cluster->sockets[node]->write(buf, BUF_HDR_SIZE);
                }
                delete ioBuf;
                return;
            }
            if (buf->compressedSize < buf->size) {
                memcpy(ioBuf, buf, BUF_HDR_SIZE);
                cluster->sockets[node]->write(ioBuf, BUF_HDR_SIZE + ioBuf->compressedSize);
                sent += BUF_HDR_SIZE + ioBuf->compressedSize;
            } else {
                assert(buf->compressedSize <= compressBufSize);
                cluster->sockets[node]->write(buf, BUF_HDR_SIZE + buf->size);
                sent += BUF_HDR_SIZE + buf->size;
            }
            delete buf;
        }
    } catch (std::exception& x) {
        printf("Sender to node %d catch exception %s\n", (int)node, x.what());
    } 
}

#define ROTL32(x, r) ((x) << (r)) | ((x) >> (32 - (r)))
#define HASH_BITS 25
#define N_HASHES (1 << (32 - HASH_BITS))
#define MURMUR_SEED 0x5C1DB

uint32_t murmur_hash3_32(const void* key, const int len)
{
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len / 4;
    
    uint32_t h1 = MURMUR_SEED;
    
    uint32_t c1 = 0xcc9e2d51;
    uint32_t c2 = 0x1b873593;
    int i;
    uint32_t k1;
    const uint8_t* tail;
    const uint32_t* blocks = (const uint32_t *)(data + nblocks*4);
    
    for(i = -nblocks; i; i++)
    {
        k1 = blocks[i];
        
        k1 *= c1;
        k1 = ROTL32(k1,15);
        k1 *= c2;
        
        h1 ^= k1;
        h1 = ROTL32(h1,13); 
        h1 = h1*5+0xe6546b64;
    }
    
    tail = (const uint8_t*)(data + nblocks*4);
    
    k1 = 0;

    switch(len & 3)
    {
      case 3: 
        k1 ^= tail[2] << 16;
        /* no break */
      case 2: 
        k1 ^= tail[1] << 8;
        /* no break */
      case 1: 
        k1 ^= tail[0];
        k1 *= c1;
        k1 = ROTL32(k1,15); 
        k1 *= c2; 
        h1 ^= k1;
    }
    
    h1 ^= len;   
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    
    return h1;
} 
