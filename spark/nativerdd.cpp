#define _JNI_IMPLEMENTATION_ 1
#include <jni.h>

const int FIELD_NO = 3; // l_linenumber

extern "C" {

JNIEXPORT jint Java_ExternalRDD_nativeSumPush(JNIEnv *env, jobject self, jobjectArray arr)
{
    jclass rowClass = (jclass)env->FindClass("org/apache/spark/sql/catalyst/expressions/Row");
    jmethodID get = env->GetMethodID(rowClass, "getInt", "(I)I");
    jint len = env->GetArrayLength(arr);
    jint sum = 0;
    for (jint i = 0; i < len; i++) { 
        jobject row =  env->GetObjectArrayElement(arr, i);
        sum += env->CallIntMethod(row, get, FIELD_NO);
    }
    return sum;
}

JNIEXPORT jint Java_ExternalRDD_nativeSumPull(JNIEnv *env, jobject self, jobject iterator)
{
    jclass rowClass = (jclass)env->FindClass("org/apache/spark/sql/catalyst/expressions/Row");
    jclass iteratorClass = (jclass)env->FindClass("scala/collection/Iterator");
    jmethodID get = env->GetMethodID(rowClass, "getInt", "(I)I");
    jmethodID hasNext = env->GetMethodID(iteratorClass, "hasNext", "()Z");
    jmethodID next = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");
    jint sum = 0;
    while (env->CallBooleanMethod(iterator, hasNext)) {
        jobject row = env->CallObjectMethod(iterator, next);
        sum += env->CallIntMethod(row, get, FIELD_NO);
    }
    return sum;
}

}
