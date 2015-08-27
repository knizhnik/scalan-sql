#include "rdd.h"
#include "pack.h"

class KrapsIterator
{
  public:
    static ThreadLocal<JNIEnv> env;

    virtual void* next() = 0;
    virtual ~KrapsIterator() {}
};

template<class T>
class SparkRDD : RDD<T>
{
  public:
    bool next(Lineitem& l)
    {
        JNIEnv* env = KrapsIterator::env.get();
        return env->CallBooleanMethod(iterator, nextRow, (jlong)(size_t)&l);
    }
    SparkRDD(JNIEnv* env, jobject _iterator) : iterator(_iterator)
    {
        jclass rowIteratorClass = (jclass)env->FindClass("RowIterator");
        nextRow = env->GetMethodID(rowIteratorClass, "next", "(J)Z");
    } 
    
    jmethodID nextRow;
};
