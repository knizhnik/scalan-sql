#define _JNI_IMPLEMENTATION_ 1
#include <jni.h>
#include <assert.h>
#include <pthread.h>

extern "C" {

    struct Context {
        JNIEnv* env;
        jobject target;
        jmethodID method;
    };

    JavaVM* jvm;
    
    JNIEXPORT jint JNICALL
    JNI_OnLoad(JavaVM *vm, void *reserved)
    {
        jvm = vm;
        return JNI_VERSION_1_2;
    }

    static void* thread_proc(void* arg)
    {
        Context* ctx = (Context*)arg;
        JNIEnv* env;
        jvm->AttachCurrentThread((void**)&env, NULL);
        long result = env->CallIntMethod(ctx->target, ctx->method);
        jvm->DetachCurrentThread();
        return (void*)result;
    }

    JNIEXPORT jint Java_NativeClass_nativeMethod(JNIEnv *env, jobject self, jobject target)
    {
        pthread_t thread;
        Context ctx;
        void* result;
        jclass javaClass = (jclass)env->FindClass("JavaClass");
        ctx.env = env;
        ctx.target = target;
        ctx.method = env->GetMethodID(javaClass, "javaMethod", "()I");
        pthread_create(&thread, NULL, thread_proc, &ctx);
        pthread_join(thread, &result);
        return  (long)result;
    }
}
