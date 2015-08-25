struct KrapsRDD 
{
    void* dll;
    KrapsIterator* iterator;
    
    KrapsRDD(void* so, KrapIterator* iter) : dll(so), iterator(iter) {}
    ~KrapsRDD() { 
        dlclose(dll);
        delete iterator;
    }
};

ThreadLocal<JNIEnv> KrapsItertor::env;
typedef (*iterator_constructor_t)(JNIEnv *env, jobjectArray* input);

JNIEXPORT jlong Java_KrapsRDD_createIterator(JNIEnv* env, jobject self, jint queryId, jint nNodes, jobjectArray iterators)
{
    char buf[256];
    sprintf(buf, "libQ%d.so", queryId);
    void* dll = dlopen(buf, RTLD_LAZY);
    sprintf(buf, "getQ%dIterator", queryId);
    iterator_constructor_t constructor = (iterator_constructor_t)dlsym(buf);
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
    delete cluster;
    return 0;
}
