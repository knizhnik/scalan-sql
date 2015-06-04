#define _JNI_IMPLEMENTATION_ 1
#include <jni.h>

const int FIELD_NO = 3; // l_linenumber

jint Java_com_huawei_hispark_ExternalRDD_nativeSumPush(JNIEnv *env, jobject self, jobjectArray arr)
{
    jclass rowClass = (jclass)env->FindClass("org/apache/spark/sql/catalyst/expressions/Row");
    jmethodID get = env->GetMethodID(rowClass, "getInt", "(I)I");
    jint len = env->GetArrayLength(arr);
    jint sum = 0;
    for (jint i = 0; i < len; i++) { 
        iobject row =  env->GetObjectArrayElement(arr, i);
        sum += end->CallIntMethod(row, get, FIELD_NO);
    }
    return sum;
}

jint Java_com_huawei_hispark_ExternalRDD_nativeSumPull(JNIEnv *env, jobject self, jobject iterator)
{
    jclass rowClass = (jclass)env->FindClass("org/apache/spark/sql/catalyst/expressions/Row");
    jclass iteratorClass = (jclass)env->FindClass("scala/collection/Iterator");
    jmethodID get = env->GetMethodID(rowClass, "getInt", "(I)I");
    jmethodID hasNext = env->GetMethodID(iteratorClass, "hasNext", "()Z");
    jmethodID next = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");
    jint len = env->GetArrayLength(arr);
    jint sum = 0;
    while (env->CallBooleanMethod(itertor, hasNext)) {
    for (jint i = 0; i < len; i++) { 
        jobject row = env->CallObjectMethod(itertor, next);
        sum += end->CallIntMethod(row, get, FIELD_NO);
    }
    return sum;
}
