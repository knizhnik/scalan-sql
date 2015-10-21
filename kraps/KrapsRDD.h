#include "rdd.h"
#include "pack.h"

class KrapsIterator
{
  public:
    virtual void* next() = 0;
    virtual ~KrapsIterator() {}
};


struct JavaContext 
{
    JNIEnv* env;
    jobjectArray inputs;

    JavaContext(JNIEnv* e, jobjectArray i) : env(e), inputs(i) {}
};

template<class T>
class SparkRDD : public RDD<T>
{
  public:
    bool next(T& row)
    {
        JavaContext* ctx = (JavaContext*)Cluster::instance->userData;
        jobject input = ctx->env->GetObjectArrayElement(ctx->inputs, inputNo);
        bool rc = ctx->env->CallBooleanMethod(input, nextRow, (jlong)(size_t)&row);
        ctx->env->DeleteLocalRef(input);
        return rc;
    }
    SparkRDD(JNIEnv* env, jint no) : inputNo(no)
    {
        jclass rowIteratorClass = (jclass)env->FindClass("kraps/RowIterator");
        nextRow = env->GetMethodID(rowIteratorClass, "next", "(J)Z");
    } 
    
  private:
    jmethodID nextRow;
    jint inputNo;
};
