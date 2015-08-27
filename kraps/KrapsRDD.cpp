#define _JNI_IMPLEMENTATION_ 1
#include <jni.h>
#include <assert.h>
#include "KrapsRDD.h"

ThreadLocal<JNIEnv> KrapsIterator::env;
typedef KrapsIterator* (*iterator_constructor_t)(JNIEnv *env, jobjectArray* input);

struct KrapsRDD 
{
    void* dll;
    KrapsIterator* iterator;
    
    KrapsRDD(void* so, KrapsIterator* iter) : dll(so), iterator(iter) {}
    ~KrapsRDD() { 
        dlclose(dll);
        delete iterator;
    }
};

JNIEXPORT jlong Java_KrapsRDD_createIterator(JNIEnv* env, jobject self, jint queryId, jobjectArray* iterators)
{
    char buf[256];
    sprintf(buf, "libQ%d.so", queryId);
    void* dll = dlopen(buf, RTLD_LAZY);
    assert(dll != NULL);
    sprintf(buf, "getQ%dIterator", queryId);
    iterator_constructor_t constructor = (iterator_constructor_t)dlsym(dll, buf);
    assert(constructor != NULL);
    KrapsIterator::env = env;
    return (jlong)(size_t)new KrapsRDD(dll, constructor(env, iterators));
}

JNIEXPORT jlong Java_KrapsRDD_nextRow(JNIEnv* env, jobject self, jlong iterator)
{
    KrapsRdd* rdd = (KrapsRdd*)iterators;
    KrapsIterator::env = env;
    void* row = rdd->next();
    if (row != NULL) {
        return (jlong)(size_t)row;
    } 
    Cluster* cluster = Cluster::instance.get();
    cluster->barrier();
    delete rdd;
    return 0;
}

class KrapsCluster
{
	Cluster* cluster;
    char** nodes;
    int executorId;
    pthread_t mutex;

    KrapCluster() : executorId(0) {
        pthread_mutex_init(&mutex, NULL);
    }
    ~KrapsCluster() {
        pthread_mutex_destroy(&mutex);
    }

    int getExecutorId()
    {
#ifdef SMP_SUPPORT    
        pthread_mutex_lock(&mutex);
        int id = ++executorId;
        pthread_mutex_unlock(&mutex);
        return id;
#else
        return 1;
#endif
    }

    void start(jobjectArray hosts, jint nCores)
    {
        int nHosts = env->GetArrayLength(hosts);
        int nNodes = nHosts*nCores;
        nodes = new char*[nNodes];
        int nodeId = -1;
        int id = getExecutorId();
        for (int i = 0; i < nNodes; i++) { 
            hosts[i] = new char[16];
            jstring host = (jstring)env->GetObjectArrayElement(hosts, i % nHosts);
            char const* hostName = env->GetStringUTFChars(host, 0);
            sprintf(nodes[i], "%s:500%d", hostName, (i / nHosts) + 1); 
            env->ReleaseStringUTFChars(host, hostName);
            if (Socket::isLocalHost(nodes[i])) {
                if (--id == 0) { 
                    nodeId = i;
                }
            }
        }
        assert(nodeId >= 0);
        cluster = new Cluster(nodeId, nNodes, nodes);
    }

    void stop()
    {
        for (size_t i = 0; i < cluster->nNodes; i++) { 
            delete hosts[i];
        }
        delete[] hosts;
        delete cluster;
    }
};

static KrapsCluster kraps;
    
JNIEXPORT void Java_KrapsCluster_start(JNIEnv* env, jobject self, jobjectArray hosts, jint nCores)
{
    kraps.start(env, hosts, nCodes);
}

JNIEXPORT void Java_KrapsCluster_stop(JNIEnv* env, jobject self)
{
    kraps.stop();
}
