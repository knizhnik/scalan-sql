#define _JNI_IMPLEMENTATION_ 1
#include <jni.h>
#include <dlfcn.h>
#include <pthread.h>
#include <assert.h>
#include "KrapsRDD.h"
#include <unistd.h>

static pthread_mutex_t dllMutex = PTHREAD_MUTEX_INITIALIZER;

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

  public:
    void start(JNIEnv* env, jobjectArray hosts, jint nodeId) {
        int nNodes = env->GetArrayLength(hosts);
        nodes = new char*[nNodes];
        for (int i = 0; i < nNodes; i++) { 
            jstring host = (jstring)env->GetObjectArrayElement(hosts, i % nNodes);
            char const* hostName = env->GetStringUTFChars(host, 0);
            nodes[i] = new char[strlen(hostName) + 8];
            sprintf(nodes[i], "%s:%d", hostName, 5001 + i); 
            env->ReleaseStringUTFChars(host, hostName);
        }
        cluster = new Cluster(nodeId, nNodes, nodes);
        cluster->userData = NULL;
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

extern "C" {

JNIEXPORT jlong Java_kraps_KrapsRDD_createIterator(JNIEnv* env, jobject self, jlong kraps, jint queryId, jobjectArray sparkInputs)
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

	pthread_mutex_lock(&dllMutex);
    void* dll = dlopen(buf, RTLD_NOW | RTLD_GLOBAL);

    assert(dll != NULL);
    sprintf(buf, "getQ%dIterator", queryId);
    iterator_constructor_t constructor = (iterator_constructor_t)dlsym(dll, buf);
    assert(constructor != NULL);
	pthread_mutex_unlock(&dllMutex);

    JavaContext ctx(env, sparkInputs);
    Cluster* cluster = Cluster::instance.get();
    if (cluster == NULL) { 
        cluster = ((KrapsCluster*)kraps)->cluster;
        Cluster::instance.set(cluster);
    } else {
		assert(cluster == ((KrapsCluster*)kraps)->cluster);
	}
    cluster->userData = &ctx;

    return (jlong)(size_t)new KrapsRDD(dll, constructor(env));
}

JNIEXPORT jlong Java_kraps_KrapsRDD_nextRow(JNIEnv* env, jobject self, jlong kraps, jlong iterator, jobjectArray sparkInputs)
{
    KrapsRDD* rdd = (KrapsRDD*)iterator;
    JavaContext ctx(env, sparkInputs);
    Cluster* cluster = Cluster::instance.get();
    if (cluster == NULL) { 
        cluster = ((KrapsCluster*)kraps)->cluster;
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

JNIEXPORT jlong Java_kraps_KrapsCluster_00024_start(JNIEnv* env, jobject self, jobjectArray hosts, jint nodeId)
{
    KrapsCluster* cluster = new KrapsCluster();
    cluster->start(env, hosts, nodeId);
    return (jlong)(size_t)cluster;
}

JNIEXPORT void Java_kraps_KrapsCluster_00024_stop(JNIEnv* env, jobject self, jlong kraps)
{
    KrapsCluster* cluster = (KrapsCluster*)kraps;
    cluster->stop();
    delete cluster;
}

}
