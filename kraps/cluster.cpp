#include <unistd.h>
#include "rdd.h"
#include "compress.h"

const size_t MB = 1024*1024;

ThreadLocal<Cluster> Cluster::instance;

Channel::Channel(cid_t id, Cluster* clu, ChannelProcessor* cp) 
  : cid(id), cluster(clu), processor(cp), nConnections(cluster->nNodes-1) {}


class ReceiveJob : public Job
{
  public:
    ReceiveJob(Cluster* owner, size_t i) : cluster(owner), node(i) {}
    
    void run()
    {
        Buffer* buf = Buffer::create(0, cluster->bufferSize);
        size_t totalReceived = 0;
        try { 
            while (!cluster->shutdown) {
                cluster->nodes[node].socket->read(buf, BUF_HDR_SIZE);
                totalReceived += BUF_HDR_SIZE + buf->size;
                
                if (buf->kind == MSG_SHUTDOWN) { 
                    break;
                } else {
                    cluster->channels[buf->cid]->process(buf, node);
                }
            }
        } catch (std::exception& x) {
            printf("Receiver catch exception %s\n", x.what());
        } 
        printf("Totally received %ldMb\n", totalReceived/MB);
        delete buf;
    }
  private:
    Cluster* cluster;
    size_t node;
};        

void Cluster::send(size_t node, Channel* channel, Buffer* buf)
{
    if (node == nodeId) {
        channel->processor->process(buf, node);
    } else {
        CriticalSection cs(nodes[node].mutex);
		nodes[node].socket->write(buf, BUF_HDR_SIZE + buf->size);
		sent += BUF_HDR_SIZE + buf->size;
    }
}

void Cluster::sendEof(size_t node, Channel* channel)
{
    if (node == nodeId) {
		Buffer buf(MSG_EOF, channel->cid);
        channel->processor->process(buf, node);
    } else {
		Buffer buf(MSG_EOF, channel->cid);
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

    for (size_t i = 0; i < nHosts; i++)  {
        if (i != selfId) { 
            nodes[i].receiver = new Thread(new ReceiveJob(i));
        }
    }    
}

Cluster::~Cluster()
{
    if (hosts != NULL) { 
        Buffer shutdownMsg(MSG_SHUTDOWN, 0);
        shutdown = true;
        for (size_t i = 0; i < nNodes; i++) {
            if (i != nodeID) {
                nodes[i].socket->write(shutdownMsg, BUF_HDR_SIZE);
            }
        }
        for (size_t i = 0; i < nNodes; i++) {
            delete nodes[i].receiver;
        }
    }
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

