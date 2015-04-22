#include "rdd.h"

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

Cluster::Cluster(size_t selfId, size_t nHosts, char** hosts, size_t nQueues, size_t bufSize, size_t queueSize, size_t syncInterval) 
: nNodes(nHosts), nodeId(selfId), bufferSize(bufSize), pingPongInterval(syncInterval)
{
    instance = this;

    sockets = new Socket*[nHosts];
    memset(sockets, 0, nHosts*sizeof(Socket*));

    qid = 0;
    syncQueue = new Queue(0, queueSize);
    recvQueues = new Queue*[nQueues];
    for (size_t i = 0; i < nQueues; i++) { 
        recvQueues[i] = new Queue((qid_t)i, queueSize);
    }
    sendQueues = new Queue*[nHosts];
    for (size_t i = 0; i < nHosts; i++) { 
        if (i != selfId) { 
            sendQueues[i] = new Queue((qid_t)i, queueSize);
            new Thread(new SendJob(i));
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
    Buffer header(MSG_DATA,0);
    Cluster* cluster = Cluster::instance;
    while (true) {
        Socket* socket = Socket::select(cluster->nNodes, cluster->sockets);
        socket->read(&header, BUF_HDR_SIZE);
        Buffer* buf = Buffer::create(header.qid, header.size, (MessageKind)header.kind);
        if (buf->size != 0) { 
            socket->read(buf->data, buf->size);
        }
        if (buf->kind == MSG_PING) { 
            buf->kind = MSG_PONG;
            cluster->sendQueues[buf->qid]->put(buf);
        } else if (buf->kind == MSG_PONG) { 
            cluster->syncQueue->put(buf);
        } else { 
            cluster->recvQueues[buf->qid]->put(buf);
        }
    }
}


void SendJob::run()
{
    Cluster* cluster = Cluster::instance;
    Buffer ping(MSG_PING, node);
    size_t sent = 0;

    while (true) { 
        Buffer* buf = cluster->sendQueues[node]->get();
        sent += buf->size;
        cluster->sockets[node]->write(buf, BUF_HDR_SIZE + buf->size);
        delete buf;

        // try to avoid socket and buffer overflow 
        if (sent >= cluster->pingPongInterval) { 
            cluster->sockets[node]->write(&ping, BUF_HDR_SIZE);
            Buffer* pong = cluster->syncQueue->get();
            assert(pong->kind == MSG_PONG);
            delete pong;
            sent = 0;
        }
    }
}
