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
    bool next(T& row)
    {
        JNIEnv* env = KrapsIterator::env.get();
        return env->CallBooleanMethod(iterator, nextRow, (jlong)(size_t)&row);
    }
    SparkRDD(JNIEnv* env, jobject iter) : iterator(iter)
    {
        jclass rowIteratorClass = (jclass)env->FindClass("RowIterator");
        nextRow = env->GetMethodID(rowIteratorClass, "next", "(J)Z");
    } 
    
  private:
    jmethodID nextRow;
    jobject iterator;
};
