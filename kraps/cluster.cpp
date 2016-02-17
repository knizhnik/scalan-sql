#include <unistd.h>
#include "rdd.h"
#include "compress.h"

const unsigned shutdownDelay = 5;
const size_t MB = 1024*1024;

Cluster** Cluster::nodes;
ThreadLocal<Cluster> Cluster::instance;

void Queue::put(Buffer* buf) 
{ 
#ifdef USE_MESSAGE_HANDLER
    if (handler != NULL && buf->kind == MSG_DATA) { 
        handler->handle(buf);
        return;
    }
#endif
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

void Cluster::send(size_t node, Channel* channel, Buffer* buf)
{
	CriticalSection cs(nodes[node].mutex);
    if (node == nodeId) {
        channel->processor->process(buf);
    } else {
		buf->node = (uint16_t)nodeId;
		nodes[node].socket->write(buf, BUF_HDR_SIZE + buf->size);
		sent += BUF_HDR_SIZE + buf->size;
    }
}

void Cluster::sendEof(size_t node, Channel* channel)
{
    if (node == nodeId) {
		Buffer buf(MSG_EOF, channel->cid);
        channel->processor->process(buf);
    } else {
		Buffer buf(MSG_EOF, channel->cid);
		buf.node  = (uint16_t)nodeId;
		CriticalSection cs(nodes[node].mutex);
		nodes[node].socket->write(&buf, BUF_HDR_SIZE);
		sent += BUF_HDR_SIZE;
    }
}

Channel* Cluster::getChannel(ChanelProcessor* processor)
{
    assert(cid < channels.size());
    Channel* c = channels[cid++];
	c->processor = processor;
    return q;

}

FILE* Cluster::openTempFile(char const* prefix, qid_t qid, size_t fno, char const* mode)
{
    char path[MAX_PATH_LEN];    
    sprintf(path, "%s/%s-%ld-%ld.%ld", tmpDir, prefix, nodeId, qid, fno);            
    FILE* f = fopen(path, mode);
    assert(f != NULL);
    return f;
}

bool Cluster::isLocalNode(char const* host)
{
    size_t len = strlen(host);
    char const* localNode = hosts[nodeId];
    return strncmp(localNode, host, len) == 0 && (localNode[len] == ':' || localNode[len] == '\0');
}

    
Cluster::Cluster(size_t selfId, size_t nHosts, char** hosts, size_t nChannels, size_t bufSize, size_t recvQueueSize, size_t sendQueueSize, size_t syncPeriod, size_t broadcastThreshold, size_t inmemThreshold, char const* tmp, bool sharedNothingDFS, size_t fileSplit) 
: nNodes(nHosts), nodeId(selfId), bufferSize(bufSize), broadcastJoinThreshold(broadcastThreshold), inmemJoinThreshold(inmemThreshold), split(fileSplit), sharedNothing(sharedNothingDFS), tmpDir(tmp), shutdown(false), userData(NULL), nodes(nNodes), channels(nQueues)
{
    instance.set(this);
    this->hosts = hosts;

    cid = 0;
    if (hosts == NULL) {
        nodes[nodeId] = this;
        return;
    }
    receiver = NULL;
    
    for (size_t i = 0; i < selfId; i++) { 
        nodes[i].sockets[i] = Socket::connect(hosts[i]);
        nodes[i].socket->write(&nodeId, sizeof nodeId);
    }

    char* sep = strchr(hosts[selfId], ':');
    int port = atoi(sep+1);
    Socket* localGateway = Socket::createLocal(port, nHosts);
    Socket* globalGateway = Socket::createGlobal(port, nHosts);

    size_t n, hostLen = strchr(hosts[0], ':') - hosts[0];    
    for (n = 1; n < nHosts && strncmp(hosts[0], hosts[n], hostLen) == 0 && hosts[n][hostLen] == ':'; n++);
    nExecutorsPerHost = n;
    
    for (size_t i = selfId+1; i < nHosts; i++) {
        size_t node;
        Socket* s = Socket::isLocalHost(hosts[i])
            ? localGateway->accept()
            : globalGateway->accept();
        s->read(&node, sizeof node);
        assert(nodes[node] == NULL);
        nodes[node].socket = s;
    }
    delete localGateway;
    delete globalGateway;
    
	receiver = (nHosts != 1) ? new Thread(new ReceiveJob()) : NULL;
}

Cluster::~Cluster()
{
    if (hosts != NULL) { 
        Buffer shutdownMsg(MSG_SHUTDOWN, 0);
        shutdown = true;
        sleep(shutdownDelay);
        delete receiver;
    }
    delete[] recvQueues;
}
     
void Cluster::barrier()
{
    Queue* queue = getQueue();
    for (size_t i = 0; i < nNodes; i++) { 
        send(i, queue, Buffer::barrier(queue->qid));
    }
    for (size_t i = 0; i < nNodes; i++) { 
        Buffer* resp = queue->get();
        assert(resp->kind == MSG_BARRIER);
        resp->release();
    }
    for (size_t i = 0; i < qid; i++) { 
        recvQueues[i]->setHandler(NULL);
    }
    qid = 0;
}

Job::Job()
{
    cluster = Cluster::instance.get ();
}

void* Thread::trampoline(void* arg)
{
    Job* job = (Job*)arg;
    Cluster::instance.set(job->cluster);
    job->run();
    delete job;
    return NULL;
}

void ReceiveJob::run()
{
    Buffer* buf = Buffer::create(0, cluster->bufferSize);
    size_t totalReceived = 0;
    try { 
        while (true) {
            Socket* socket = Socket::select(cluster->nNodes, cluster->sockets);
            if (cluster->shutdown) {
                break;
            }
            socket->read(buf, BUF_HDR_SIZE);
            totalReceived += BUF_HDR_SIZE + buf->size;
            
            if (buf->kind == MSG_SHUTDOWN) { 
                break;
			} else {
				CriticalSection cs(cluster->nodes[cluster->nodeId].mutex);
				cluster->channels[buf->cid]->process(buf);
			}
        }
    } catch (std::exception& x) {
        printf("Receiver catch exception %s\n", x.what());
    } 
    printf("Totally received %ldMb\n", totalReceived/MB);
    delete buf;
}

void SendJob::run()
{
    size_t compressBufSize = cluster->bufferSize*2;
    Buffer* ioBuf = Buffer::create(0, compressBufSize);
    try {
        while (true) { 
            Buffer* buf = cluster->sendQueues[node]->get();
            buf->node = (uint16_t)cluster->nodeId;
            memcpy(ioBuf, buf, BUF_HDR_SIZE);
            if (cluster->sockets[node]->isLocal()) {
                ioBuf->compressedSize = buf->size;
            } else { 
                ioBuf->compressedSize = buf->size ? compress(ioBuf->data, buf->data, buf->size) : 0;
            }
            if (buf->kind == MSG_SHUTDOWN) { 
                if (node == (cluster->nodeId + 1) % cluster->nNodes) { // shutdown neighbour receiver
                    cluster->sockets[node]->write(buf, BUF_HDR_SIZE);
                }
                delete ioBuf;
                return;
            }
            if (ioBuf->compressedSize < ioBuf->size) {
                cluster->sockets[node]->write(ioBuf, BUF_HDR_SIZE + ioBuf->compressedSize);
                sent += BUF_HDR_SIZE + ioBuf->compressedSize;
            } else {
                assert(ioBuf->compressedSize <= compressBufSize);
                cluster->sockets[node]->write(buf, BUF_HDR_SIZE + buf->size);
                sent += BUF_HDR_SIZE + buf->size;
            }
            buf->release();
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
