#include <unistd.h>
#include "rdd.h"
#include "compress.h"

const size_t MB = 1024*1024;

ThreadLocal<Cluster> Cluster::instance;

Channel::Channel(cid_t id, Cluster* clu, ChannelProcessor* cp)
: cid(id), cluster(clu), processor(cp), nProducers(0), nConsumers(cluster->nNodes-1) {}



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
				} else if (buf->kind == MSG_BARRIER) { 
                    cluster->sync();
                } else {
					if (buf->size != 0) { 
						cluster->nodes[node].socket->read(buf->data, buf->size);
					}
					//printf("Receive message %d from node %ld channel %d processor=%p\n", buf->kind, node, buf->cid, cluster->channels[buf->cid]->processor);
					//fflush(stdout);
					cluster->channels[buf->cid]->processor->process(buf, node);
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
		buf->cid = channel->cid;
		nodes[node].socket->write(buf, BUF_HDR_SIZE + buf->size);
    }
}

void Cluster::sendEof(size_t node, Channel* channel)
{
    if (node != nodeId) {
		Buffer buf(MSG_EOF, channel->cid);
		CriticalSection cs(nodes[node].mutex);
		nodes[node].socket->write(&buf, BUF_HDR_SIZE);
	}		
}

Channel* Cluster::getChannel(ChannelProcessor* processor)
{
	Channel* channel = new Channel(channels.size(), this, processor);
	channels.push_back(channel);
	processor->channel = channel;
    return channel;

}

void Cluster::sync()
{
	semaphore.signal(mutex);
}

void Cluster::barrier()
{
	Buffer buf(MSG_BARRIER, 0);
	for (size_t i = 0; i < nNodes; i++) { 
		if (i != nodeId) { 
			CriticalSection cs(nodes[i].mutex);
			nodes[i].socket->write(&buf, BUF_HDR_SIZE);
		}
	}
	semaphore.wait(mutex, nNodes-1); // wait responses rfom all nodes
}

void Cluster::reset()
{
	for (size_t i = 0; i < channels.size(); i++) { 
		delete channels[i];
	}
	channels.clear();
}

bool Cluster::isLocalNode(char const* host)
{
    size_t len = strlen(host);
    char const* localNode = hosts[nodeId];
    return strncmp(localNode, host, len) == 0 && (localNode[len] == ':' || localNode[len] == '\0');
}

    
Cluster::Cluster(size_t selfId, size_t nHosts, char** hosts, size_t nThreads, size_t bufSize, size_t socketBufferSize, size_t broadcastThreshold, bool sharedNothingDFS, size_t fileSplit) 
: nNodes(nHosts), nodeId(selfId), bufferSize(bufSize), broadcastJoinThreshold(broadcastThreshold), split(fileSplit), sharedNothing(sharedNothingDFS), shutdown(false), userData(NULL), threadPool(nThreads), nodes(nNodes) 
{
    instance.set(this);
    this->hosts = hosts;

    for (size_t i = 0; i < selfId; i++) { 
        nodes[i].socket = Socket::connect(hosts[i], socketBufferSize);
        nodes[i].socket->write(&nodeId, sizeof nodeId);
    }

    char* sep = strchr(hosts[selfId], ':');
    int port = atoi(sep+1);
    Socket* localGateway = Socket::createLocal(port, socketBufferSize, nHosts);
    Socket* globalGateway = Socket::createGlobal(port, socketBufferSize, nHosts);

    size_t n, hostLen = strchr(hosts[0], ':') - hosts[0];    
    for (n = 1; n < nHosts && strncmp(hosts[0], hosts[n], hostLen) == 0 && hosts[n][hostLen] == ':'; n++);
    
    for (size_t i = selfId+1; i < nHosts; i++) {
        size_t node;
        Socket* s = Socket::isLocalHost(hosts[i])
            ? localGateway->accept()
            : globalGateway->accept();
        s->read(&node, sizeof node);
        assert(nodes[node].socket == NULL);
        nodes[node].socket = s;
    }

    delete localGateway;
    delete globalGateway;

    for (size_t i = 0; i < nHosts; i++)  {
        if (i != selfId) { 
            nodes[i].receiver = new Thread(new ReceiveJob(this, i));
        }
    }    
}

Cluster::~Cluster()
{
    if (hosts != NULL) { 
        Buffer shutdownMsg(MSG_SHUTDOWN, 0);
        shutdown = true;
        for (size_t i = 0; i < nNodes; i++) {
            if (i != nodeId) {
                nodes[i].socket->write(&shutdownMsg, BUF_HDR_SIZE);
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

