#include "rdd.h"

Cluster* Cluster::instance;

void Queue::put(Buffer* buf) 
{ 
    CriticalSection cs(mutex);
    if (buf->size == 0) { 
        if (++nFinished != Cluster::instance->nNodes) {  
            delete buf;
            return;
        }
        nFinished = 0; // make it possible to reuse queue
    } else { 
        while (size >= limit) { 
            blockedPut = true;
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
    assert(qid < nNodes);
    return recvQueues[qid++];
}

Cluster::Cluster(size_t id, size_t nHosts, char** hosts, size_t nQueues, size_t bufSize, size_t queueSize) 
: nNodes(nHosts), nodeId(id), bufferSize(bufSize)
{
    instance = this;

    sockets = new Socket*[nHosts];
    memset(sockets, 0, nHosts*sizeof(Socket*));

    qid = 0;
    recvQueues = new Queue*[nQueues];
    for (size_t i = 0; i < nQueues; i++) { 
        recvQueues[i] = new Queue((qid_t)i, queueSize);
    }
    sendQueues = new Queue*[nHosts];
    for (size_t i = 0; i < nHosts; i++) { 
        sendQueues[i] = new Queue((qid_t)i, queueSize);
        new Thread(new SendJob(i));
    }

    for (size_t i = 0; i < id; i++) { 
        sockets[i] = Socket::connect(hosts[i]);
        sockets[i]->write(&nodeId, sizeof nodeId);
    }
    sockets[id] = NULL;

    char* sep = strchr(hosts[id], ':');
    int port = atoi(sep+1);
    Socket* localGateway = Socket::createLocal(port, nHosts);
    Socket* globalGateway = Socket::createGlobal(port, nHosts);
    for (size_t i = id+1; i < nHosts; i++) {
        size_t node;
        Socket* s = (strncmp(hosts[i], "localhost:", 10) == 0) 
            ? localGateway->accept()
            : globalGateway->accept();
        s->read(&node, sizeof node);
        assert(sockets[node] == NULL);
        sockets[node] = s;
    }
    delete localGateway;
    delete globalGateway;

    new Thread(new ReceiveJob());
        
}

void Cluster::barrier()
{
    Queue* queue = getQueue();
    for (size_t i = 0; i < nNodes; i++) { 
        Buffer* req = Buffer::create(queue->qid, 0);
        if (i == nodeId) { 
            queue->put(req);
        } else { 
            sendQueues[i]->put(req);
        } 
    }
    Buffer* resp = queue->get();
    delete resp;
    qid = 0;
}

void ReceiveJob::run()
{
    Buffer header(0,0);
    Cluster* cluster = Cluster::instance;
    while (true) {
        Socket* socket = Socket::select(cluster->nNodes, cluster->sockets);
        socket->read(&header, BUF_HDR_SIZE);
        Buffer* buf = Buffer::create(header.qid, header.size);
        if (buf->size != 0) { 
            socket->read(buf->data, buf->size);
        }
        cluster->sendQueues[buf->qid]->put(buf);
    }
}


void SendJob::run()
{
    Cluster* cluster = Cluster::instance;
    while (true) {
        Buffer* buf = cluster->sendQueues[node]->get();
        cluster->sockets[node]->write(buf, BUF_HDR_SIZE + buf->size);
        delete buf;
    }
}
