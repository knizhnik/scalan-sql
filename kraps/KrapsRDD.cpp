#define _JNI_IMPLEMENTATION_ 1
#include <jni.h>
#include <dlfcn.h>
#include <assert.h>
#include "KrapsRDD.h"
#include <unistd.h>

typedef KrapsIterator* (*iterator_constructor_t)(JNIEnv* env);

struct KrapsRDD 
{
    void* dll;
    KrapsIterator* iterator;
   
    void* next() { 
    	return iterator->next();
    }

    KrapsRDD(void* so, KrapsIterator* iter) : dll(so), iterator(iter) {}
    ~KrapsRDD() { 
        delete iterator;
        dlclose(dll);
    }
};


class KrapsCluster
{
  public:
    Cluster* cluster;
    char** nodes;
    int executorId;
    pthread_mutex_t mutex;

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

  public:
    KrapsCluster() : executorId(0) 
    {
        pthread_mutex_init(&mutex, NULL);
    }
     
    ~KrapsCluster() 
    {
        pthread_mutex_destroy(&mutex);
    }

    void start(JNIEnv* env, jobjectArray hosts, jint nCores) {
        int nHosts = env->GetArrayLength(hosts);
        int nNodes = nHosts*nCores;
        nodes = new char*[nNodes];
        int nodeId = -1;
        int id = getExecutorId();
        for (int i = 0; i < nNodes; i++) { 
            nodes[i] = new char[16];
            jstring host = (jstring)env->GetObjectArrayElement(hosts, i % nHosts);
            char const* hostName = env->GetStringUTFChars(host, 0);
            sprintf(nodes[i], "%s:%d", hostName, 5001 + i); 
            env->ReleaseStringUTFChars(host, hostName);
            if (Socket::isLocalHost(nodes[i])) {
                if (--id == 0) { 
                    nodeId = i;
                }
            }
        }
        assert(nodeId >= 0);
        cluster = new Cluster(nodeId, nNodes, nodes);
        cluster->userData = env;
    }

    void stop()
    {
        for (size_t i = 0; i < cluster->nNodes; i++) { 
            delete nodes[i];
        }
        delete[] nodes;
        delete cluster;
    }
};

static KrapsCluster kraps;

extern "C" {

JNIEXPORT jlong Java_kraps_KrapsRDD_createIterator(JNIEnv* env, jobject self, jint queryId, jobjectArray sparkInputs)
{
    char buf[256];
    sprintf(buf, "libQ%d.so", queryId);
//    fprintf(stdout, "Library name: %s\n", buf);
//
//    char cwd[1024];
//	if (getcwd(cwd, sizeof(cwd)) != NULL)
//	   fprintf(stdout, "Current working dir: %s\n", cwd);
//	else
//	   perror("getcwd() error");

    void* dll = dlopen(buf, RTLD_NOW | RTLD_GLOBAL);

    assert(dll != NULL);
    sprintf(buf, "getQ%dIterator", queryId);
    iterator_constructor_t constructor = (iterator_constructor_t)dlsym(dll, buf);
    assert(constructor != NULL);

    JavaContext ctx(env, sparkInputs);
    Cluster* cluster = Cluster::instance.get();
    if (cluster == NULL) { 
        cluster = kraps.cluster;
        Cluster::instance.set(cluster);
    }
    cluster->userData = &ctx;

    return (jlong)(size_t)new KrapsRDD(dll, constructor(env));
}

JNIEXPORT jlong Java_kraps_KrapsRDD_nextRow(JNIEnv* env, jobject self, jlong iterator, jobjectArray sparkInputs)
{
    KrapsRDD* rdd = (KrapsRDD*)iterator;
    JavaContext ctx(env, sparkInputs);
    Cluster* cluster = Cluster::instance.get();
    if (cluster == NULL) { 
        cluster = kraps.cluster;
        Cluster::instance.set(cluster);
    }
    cluster->userData = &ctx;
    void* row = rdd->next();
    if (row != NULL) {
        return (jlong)(size_t)row;
    } 
    cluster->barrier();
    delete rdd;
    return 0;
}

JNIEXPORT void Java_kraps_KrapsCluster_00024_start(JNIEnv* env, jobject self, jobjectArray hosts, jint nCores)
{
    kraps.start(env, hosts, nCores);
}

JNIEXPORT void Java_kraps_KrapsCluster_00024_stop(JNIEnv* env, jobject self)
{
    kraps.stop();
}

}
