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
        return false;
    }

    Q1_RDD(JNIEnv* env, jobject _iterator, jint nNodes) : iterator(_iterator) 
    {
        jclass rowClass = (jclass)env->FindClass("org/apache/spark/sql/Row");
        jclass iteratorClass = (jclass)env->FindClass("scala/collection/Iterator");
        hasNext = env->GetMethodID(iteratorClass, "hasNext", "()Z");
        getNext = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");
        getInt = env->GetMethodID(rowClass, "getInt", "(I)I");
        getDouble = env->GetMethodID(rowClass, "getDouble", "(I)D");
        getByte = env->GetMethodID(rowClass, "getByte", "(I)B");
        getLong = env->GetMethodID(rowClass, "getLong", "(I)J");
        hosts = new char*[nNodes];
        int nodeId = -1;
        for (int i = 0; i < nNodes; i++) { 
            hosts[i] = new char[16];
            sprintf(hosts[i], "lite0%d:5001", i+1); 
            if (Socket::isLocalHost(hosts[i])) {
                nodeId = i;
            }
        }
	FILE* log = fopen("/srv/remote/all-common/tpch/data/q1.log", "a");
	fprintf(log, "Start executor on node %d\n", nodeId);
	fclose(log);
        assert(nodeId >= 0);
        cluster = new Cluster(nodeId, nNodes, hosts);
	//sleep(60);
    }

    ~Q1_RDD() {
        for (size_t i = 0; i < cluster->nNodes; i++) { 
            delete hosts[i];
        }
        delete[] hosts;
    }
    
    static JNIEnv *env;
    jobject iterator; 
    jmethodID getInt;
    jmethodID getLong;
    jmethodID getByte;
    jmethodID getDouble;
    jmethodID hasNext;
    jmethodID getNext;
    Cluster* cluster;
    char** hosts;
};

JNIEnv* Q1_RDD::env;


namespace Q1
{
    struct Projection
    {
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        size_t count_order;
        char   l_returnflag;
        char   l_linestatus;

        friend void print(Projection const& p, FILE* out) { 
            fprintf(out, "%c, %c, %f, %f, %f, %f, %f, %f, %f, %lu", 
                    p.l_returnflag, p.l_linestatus, p.sum_qty, p.sum_base_price, p.sum_disc_price, p.sum_charge, p.avg_qty, p.avg_price, p.avg_disc, p.count_order);
        }
    };

    struct GroupBy
    {
        char   l_returnflag;
        char   l_linestatus;

        bool operator == (GroupBy const& other) const
        { 
            return l_returnflag == other.l_returnflag && l_linestatus == other.l_linestatus;
        }

        friend size_t hashCode(GroupBy const& gby) {
            return (gby.l_returnflag << 8) ^ gby.l_linestatus;
        }
    };
    
    struct Aggregate
    {
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double sum_disc;
        size_t count_order;
    };
    
    bool predicate(Lineitem const& lineitem) 
    {
        return lineitem.l_shipdate <= 19981201;
    }

    void map(Pair<GroupBy,Aggregate>& pair, Lineitem const& lineitem)
    {
        pair.key.l_returnflag = lineitem.l_returnflag;
        pair.key.l_linestatus = lineitem.l_linestatus;
        pair.value.sum_qty = lineitem.l_quantity;
        pair.value.sum_base_price = lineitem.l_extendedprice;
        pair.value.sum_disc_price = lineitem.l_extendedprice*(1-lineitem.l_discount);
        pair.value.sum_charge = lineitem.l_extendedprice*(1-lineitem.l_discount)*(1+lineitem.l_tax);
        pair.value.sum_disc = lineitem.l_discount;
        pair.value.count_order = 1;
    }

    void reduce(Aggregate& dst, Aggregate const& src)
    {
        dst.sum_qty += src.sum_qty;
        dst.sum_base_price += src.sum_base_price;
        dst.sum_disc_price += src.sum_disc_price;
        dst.sum_charge += src.sum_charge;
        dst.sum_disc  += src.sum_disc;
        dst.count_order += src.count_order;
    }


    void projection(Projection& out, Pair<GroupBy,Aggregate> const& in)
    {
        out.l_returnflag = in.key.l_returnflag;
        out.l_linestatus = in.key.l_linestatus;
        out.sum_qty = in.value.sum_qty;
        out.sum_base_price = in.value.sum_base_price;
        out.sum_disc_price = in.value.sum_disc_price;
        out.sum_charge = in.value.sum_charge;
        out.avg_qty = in.value.sum_qty / in.value.count_order;
        out.avg_price = in.value.sum_base_price / in.value.count_order;
        out.avg_disc = in.value.sum_disc / in.value.count_order;
        out.count_order = in.value.count_order;
    }

    int compare(Projection const* a, Projection const* b)
    {
        int diff = a->l_returnflag - b->l_returnflag;
        return diff != 0 ? diff : a->l_linestatus - b->l_linestatus;
    }

    RDD<Projection>* query(Q1_RDD* lineitem) 
    { 
        return
            lineitem->
            filter<predicate>()->
            mapReduce<GroupBy,Aggregate,map,reduce>(10000)->
            project<Projection, projection>()->
            sort<compare>(100);
    }
}

extern "C" {

JNIEXPORT jlong Java_Q1_runQuery(JNIEnv *env, jobject self, jobject iterator, jint nNodes)
{
    Q1_RDD::env = env;
    return (jlong)(size_t)Q1::query(new Q1_RDD(env, iterator, nNodes));
}

JNIEXPORT jlong Java_Q1_nextRow(JNIEnv *env, jobject self, jlong rdd)
{
    Q1_RDD::env = env;
    RDD<Q1::Projection>* iter = (RDD<Q1::Projection>*)(size_t)rdd;
    Q1::Projection* projection = (Q1::Projection*)malloc(sizeof(Q1::Projection));
    if (iter->next(*projection)) { 
        return (jlong)(size_t)projection;
    } 
    free(projection);
    Cluster* cluster = Cluster::instance;
    cluster->barrier();
    delete iter;
    delete cluster;
    return 0;
}

JNIEXPORT void Java_Q1_freeRow(JNIEnv *env, jobject self, jlong row)
{
    free((Q1::Projection*)(size_t)projection);
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
