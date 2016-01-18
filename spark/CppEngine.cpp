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

JNIEXPORT jdouble Java_CppEngine_compute(JNIEnv *env, jobject self, jobject iterator)
{
    jclass rowClass = (jclass)env->FindClass("JavaDataSource");
    jmethodID next = env->GetMethodID(rowClass, "next", "(J)Z");
    Lineitem lineitem;
    double sum = 0;
    while (env->CallBooleanMethod(iterator, next, (jlong)(size_t)&lineitem))
    {
      if (lineitem.l_shipdate <= 19981201) {
	sum += lineitem.l_extendedprice*(1-lineitem.l_discount)*(1+lineitem.l_tax)*lineitem.l_quantity;
      }
    }
    return sum;
}


const int TILE_SIZE = 128;
  
JNIEXPORT jdouble Java_CppEngine_computeTile(JNIEnv *env, jobject self, jobject iterator)
{
    jclass rowClass = (jclass)env->FindClass("JavaDataSource");
    jmethodID nextTile = env->GetMethodID(rowClass, "nextTile", "(JI)I");
    Lineitem lineitems[TILE_SIZE];
    double sum = 0;
    int n;
    while ((n = env->CallIntMethod(iterator, nextTile, (jlong)(size_t)lineitems, TILE_SIZE)) != 0)
    {
      for (int i = 0; i < n; i++) { 
	Lineitem const& lineitem = lineitems[i];
	if (lineitem.l_shipdate <= 19981201) {
	  sum += lineitem.l_extendedprice*(1-lineitem.l_discount)*(1+lineitem.l_tax)*lineitem.l_quantity;
	}
      }
    }
    return sum;
}
}
	
