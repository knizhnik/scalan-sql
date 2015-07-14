#define _JNI_IMPLEMENTATION_ 1
#include <jni.h>
#include <assert.h>

typedef unsigned date_t;
struct Lineitem
{
    long   l_orderkey;
    int    l_partkey;
    int    l_suppkey;
    int    l_linenumber;
    double l_quantity;
    double l_extendedprice;
    double l_discount;
    double l_tax;
    char   l_returnflag;
    char   l_linestatus;
    date_t l_shipdate;
    date_t l_commitdate;
    date_t l_receiptdate;
};


extern "C" {

JNIEXPORT jint Java_ExternalRDD_nativeSumPushArr(JNIEnv *env, jobject self, jarray arr)
{
    jint len = env->GetArrayLength(arr);
    jint sum = 0;
    jboolean isCopy;
    jint* body = (jint*)env->GetPrimitiveArrayCritical(arr, &isCopy);
    assert(!isCopy);
    for (jint i = 0; i < len; i++) {
        sum += body[i];
    }
    env->ReleasePrimitiveArrayCritical(arr, body, 0);
    return sum;
}

JNIEXPORT jint Java_ExternalRDD_nativeSumPush(JNIEnv *env, jobject self, jobjectArray arr)
{
    jclass rowClass = (jclass)env->FindClass("org/apache/spark/sql/Row");
    jmethodID get = env->GetMethodID(rowClass, "getInt", "(I)I");
    jint len = env->GetArrayLength(arr);
    jint sum = 0;
    for (jint i = 0; i < len; i++) { 
        jobject row =  env->GetObjectArrayElement(arr, i);
        sum += env->CallIntMethod(row, get, 0);
	env->DeleteLocalRef(row);
    }
    return sum;
}
Lineitem l;
JNIEXPORT jint Java_ExternalRDD_nativeSumPull(JNIEnv *env, jobject self, jobject iterator)
{
    jclass rowClass = (jclass)env->FindClass("org/apache/spark/sql/Row");
    jclass iteratorClass = (jclass)env->FindClass("scala/collection/Iterator");
    jmethodID getInt = env->GetMethodID(rowClass, "getInt", "(I)I");
    jmethodID getDouble = env->GetMethodID(rowClass, "getDouble", "(I)D");
    jmethodID getByte = env->GetMethodID(rowClass, "getByte", "(I)B");
    jmethodID getLong = env->GetMethodID(rowClass, "getLong", "(I)J");

    jmethodID hasNext = env->GetMethodID(iteratorClass, "hasNext", "()Z");
    jmethodID next = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");
    jint sum = 0;
    size_t frameSize = 1024*1024;
    //env->PushLocalFrame(frameSize);
    while (env->CallBooleanMethod(iterator, hasNext)) {
        jobject row = env->CallObjectMethod(iterator, next);
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
	    sum += l.l_partkey;	
	//env->PopLocalFrame(NULL);
	//env->PushLocalFrame(frameSize);	
        env->DeleteLocalRef(row);
    }
	//env->PopLocalFrame(NULL);

    return sum;
}

}
