#include "rdd.h"
#include "pack.h"

/**
 * Interface for accessing result of Kraps query exectiion from Spark
 */
class KrapsIterator
{
  public:
    /**
     * Get next record
     * @return pointer to next record or NULL if there are no more records
     */
    virtual void* next() = 0;
    virtual ~KrapsIterator() {}
};


/**
 * Spark context for execution of Kraps query
 */
struct JavaContext 
{
    JNIEnv* env;
    jobjectArray inputs;

    JavaContext(JNIEnv* e, jobjectArray i) : env(e), inputs(i) {}
};

/**
 * Proxy class for accessing Spark RDD from Kraps
 */
template<class T>
class SparkRDD : public RDD<T>
{
  public:
    /**
     * Get next row
     * @param row [out] placeholder for next row
     * @return true if there is next record, false otherwise
     */
    bool next(T& row)
    {
        JavaContext* ctx = (JavaContext*)Cluster::instance->userData;
        jobject input = ctx->env->GetObjectArrayElement(ctx->inputs, inputNo);
        bool rc = ctx->env->CallBooleanMethod(input, nextRow, (jlong)(size_t)&row);
        ctx->env->DeleteLocalRef(input);
        return rc;
    }
    
    /**
     * SparkRDD constructor
     * @param env JNI environment
     * @param no index of RDD in input array
     */ 
    SparkRDD(JNIEnv* env, jint no) : inputNo(no)
    {
        jclass rowIteratorClass = (jclass)env->FindClass("kraps/RowIterator");
        nextRow = env->GetMethodID(rowIteratorClass, "next", "(J)Z");
    } 
    
  private:
    jmethodID nextRow;
    jint inputNo;
};
