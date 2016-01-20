#include "rdd.h"
#include "pack.h"
#include "time.h"
#include "sys/time.h"

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

inline time_t getCurrentTime()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec*1000 + tv.tv_usec/1000;
}

#define TILE_SIZE 128

/**
 * Proxy class for accessing Spark RDD from Kraps
 */
template<class T>
class SparkRDD : public RDD<T>
{
    T tile[TILE_SIZE];
    int size;
    int used;
 public:
    /**
     * Get next row
     * @param row [out] placeholder for next row
     * @return true if there is next record, false otherwise
     */
    bool next(T& row)
    {
        if (used == size) {
            #ifdef MEASURE_SPARK_TIME
            time_t start = getCurrentTime();
            #endif
            JavaContext* ctx = (JavaContext*)Cluster::instance->userData;
            jobject input = ctx->env->GetObjectArrayElement(ctx->inputs, inputNo);
            size = ctx->env->CallIntMethod(input, nextTile, (jlong)(size_t)tile, TILE_SIZE);
            used = 0;
            ctx->env->DeleteLocalRef(input);
            #ifdef MEASURE_SPARK_TIME
            elapsed += getCurrentTime() - start;
            calls += 1;
            #endif
            if (size == 0) {
                return false;
            }
        }
        row = tile[used++];
        return true;
    }
    
    /**
     * SparkRDD constructor
     * @param env JNI environment
     * @param no index of RDD in input array
     */ 
    SparkRDD(JNIEnv* env, jint no) : size(0), used(0), inputNo(no), elapsed(0), calls(0)
    {
        jclass rowIteratorClass = (jclass)env->FindClass("kraps/RowIterator");
        nextTile = env->GetMethodID(rowIteratorClass, "nextTile", "(JI)I");
    } 
    ~SparkRDD() {
        #ifdef MEASURE_SPARK_TIME
        FILE* log = fopen("SparkRDD.log", "w");
        fprintf(log, "Elapsed time: %ld, total calls=%ld\n", elapsed, calls);
        fclose(log);
        #endif
    }
    
  private:
    jmethodID nextTile;
    jint inputNo;
    time_t elapsed;
    jlong calls;
};
