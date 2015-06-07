#define _JNI_IMPLEMENTATION_ 1
#include <jni.h>
#include "tpch.h"

class Q1_RDD : public RDD<Lineitem>
{
public:
    bool next(Lineitem& l) { 
        if (env->CallBooleanMethod(iterator, hasNext)) {
            jobject row = env->CallObjectMethod(iterator, getNext);
            l.l_orderkey = env->CallLongMethod(row, getLong, 0);
            l.l_partkey = env->CallIntMethod(row, getInt, 1);
            l.l_suppkey = env->CallIntMethod(row, getInt, 2);
            l.l_linenumber = env->CallIntMethod(row, getInt, 3);
            l.l_quantity = env->CallDoubleMethod(row, getDouble, 4);
            l.l_extendedprice = env->CallDoubleMethod(row, getDouble, 5);
            l.l_discount = env->CallDoubleMethod(row, getDouble, 6);
            l.l_tax = env->CallDoubleMethod(row, getDouble, 7);
            l.l_returnflag = env->CallByteMethod(row, getByte, 8);
            l.l_linestatus = env->CallByteMethod(row, getByte, 9);
            l.l_shipdate = env->CallIntMethod(row, getInt, 10);
            l.l_commitdate = env->CallIntMethod(row, getInt, 11);
            l.l_receiptdate = env->CallIntMethod(row, getInt, 12);
            env->DeleteLocalRef(row);
            return true;
        }
        delete this;
        return false;
    }

    Q1_RDD(JNIEnv* _env, jobject _iterator) : env(_env), iterator(_iterator) 
    {
        jclass rowClass = (jclass)env->FindClass("org/apache/spark/sql/Row");
        jclass iteratorClass = (jclass)env->FindClass("scala/collection/Iterator");
        hasNext = env->GetMethodID(iteratorClass, "hasNext", "()Z");
        getNext = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");
        getInt = env->GetMethodID(rowClass, "getInt", "(I)I");
        getDouble = env->GetMethodID(rowClass, "getDouble", "(I)D");
        getByte = env->GetMethodID(rowClass, "geByte", "(I)B");
        getLong = env->GetMethodID(rowClass, "getLong", "(I)J");
    }
        
private:    
    JNIEnv *env;
    jobject iterator; 
    jmethodID getInt;
    jmethodID getLong;
    jmethodID getByte;
    jmethodID getDouble;
    jmethodID hasNext;
    jmethodID getNext;
};

extern "C" {

JNIEXPORT jlong Java_Q1_rdd(JNIEnv *env, jobject self, jobject iterator)
{
    return (jlong)(size_t)new Q1_RDD(env, iterator);
}

JNIEXPORT jlong Java_Q1_nextRow(JNIEnv *env, jobject self, jlong rdd)
{
    RDD<Q1::Projection>* iter = (RDD<Q1::Projection>*)(size_t)rdd;
    Q1::Projection* projection = (Q1::Projection*)malloc(sizeof(Q1::Projection));
    if (iter->next(*projection)) { 
        return (jlong)(size_t)projection;
    } 
    free(projection);
    return 0;
}

JNIEXPORT void Java_Q1_freeRow(JNIEnv *env, jobject self, jlong addr)
{
    free((void*)(size_t)addr);
}

JNIEXPORT jint Java_RowDecoder_getInt(JNIEnv *env, jobject self, jlong addr, jint offs)
{
    char* record = (char*)(size_t)addr;
    return *(jint*)(record + offs);
}

JNIEXPORT jbyte Java_RowDecoder_getByte(JNIEnv *env, jobject self, jlong addr, jint offs)
{
    char* record = (char*)(size_t)addr;
    return *(jbyte*)(record + offs);
}

JNIEXPORT jlong Java_RowDecoder_getLong(JNIEnv *env, jobject self, jlong addr, jint offs)
{
    char* record = (char*)(size_t)addr;
    return *(jlong*)(record + offs);
}

JNIEXPORT jdouble Java_RowDecoder_getDouble(JNIEnv *env, jobject self, jlong addr, jint offs)
{
    char* record = (char*)(size_t)addr;
    return *(jdouble*)(record + offs);
}

}
