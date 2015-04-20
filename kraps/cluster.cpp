#include "rdd.h"

void Queue::put(Buffer* buf) 
{ 
    CriticalSecion cs(mutex);
    if (buf->size == 0) { 
        if (++nFinished != Cluster::instance->nNodes) {  / make it possible to reused queue
            nFinished = 0;
            return;
        }
    } else { 
        while (size >= limit) { 
            full.wait(mutex);
        }
        size += buf->size;
    }
    Message* msg = new Message(buf);
    *tail = msg;
    tail = &msg->next;
    if (blockedGet) { 
        blockedGet = false;
        empty.signal();
    }
}

Buffer* Queue::get() 
{
    CriticalSecion cs(mutex);
    while (head == NULL) { 
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
    Queue* queue = instance->freeQueue;
    assert(queue != NULL);
    instance->freeQueue = queue->next;
    return queue;
}

void Cluster::freeQueue(Queue* queue)
{
    queue->next = instance->freeQueue;
    freeQueue  = queue;
}

Cluster::Cluster(size_t id, size_t nHosts, char const* hosts, size_t nQueues = 16, size_t bufSize, size_t queueSize) : nNodes(nHosts), nodeId(id), bufferSize(bufSize), queue(queueSize)
{
    sockets = new Socket*[nHosts];
    memset(sockets, 0, nHosts*sizeof(Socket*));

    for (size_t i = 0; i < id; i++) { 
        sockets[i] = Socket::connect(hosts[i]);
        sockets[i]->write(&nodeId, sizeof nodeId);
    }
    sockets[id] = NULL;

    freeQueue = NULL;
    for (size_t i = 0; i < nQueues; i++) { 
        queues[i] = freeQueue = new Queue((qid_t)i, queueSize, freeQueue );
    }
    Socket* localGateway = Socket::createLocal(port);
    for (size_t i = id+1; i < nHosts && strncmp(hosts[i], "localhost:", 10) == 0) {
        size_t node;
        Socket* s = localGateway->accept();
        s->read(&node, sizeof node);
        assert(sockets[node] == NULL);
        sockets[node] = s;
    }
    delete localGateway;

    Socket* globalGateway = SOcket::createGlobal(port);
    for (size_t i = id+1; i < nHosts && strncmp(hosts[i], "localhost:", 10) != 0) {
        size_t node;
        Socket* s = globalGateway->accept();
        s->read(&node, sizeof node);
        assert(sockets[node] == NULL);
        sockets[node] = s;
    }
    delete globalGateway;
}

void GatherJob::run()
{
    Buffer* header = Buffer::create(0,0);
    Cluster* cluster = Cluster::instance;
    while (true) {
        Socket* socket = Socket::select(cluster->nNodes, cluster->socket);
        socket->read(header, BUF_HDR_SIZE);
        Buffer* buf = header;
        if (buf->size != 0) { 
            buf = Buffer::create(header->qid, header->size);
            socket->read(buf->data, buf->size);
        }
        cluster->queues[buf->qid]->put(buf);
    }
}
