#ifndef __TPCH_H__
#define __TPCH_H__

#include "rdd.h"
#include "pack.h"

//
// TPC-H classes (tables) defintion
//


typedef unsigned date_t;
typedef Char<25> name_t;
typedef Char<15> priority_t;
typedef Char<10> shipmode_t;

#define LineitemFields(FIELD) \
    FIELD(l_orderkey,long)        \
    FIELD(l_partkey,int)             \
    FIELD(l_suppkey,int)                \
    FIELD(l_linenumber,int)               \
    FIELD(l_quantity,double)                     \
    FIELD(l_extendedprice,double)                      \
    FIELD(l_discount,double)                                 \
    FIELD(l_tax,double)                                            \
    FIELD(l_returnflag,char)                                           \
    FIELD(l_linestatus,char)                                               \
    FIELD(l_shipdate,date_t)                                                  \
    FIELD(l_commitdate,date_t)                                                \
    FIELD(l_receiptdate,date_t)                                               \
    FIELD(l_shipinstruct,Char<25>)                                              \
    FIELD(l_shipmode,shipmode_t)                                                  \
    FIELD(l_comment,Char<44>) 


#define OrdersFields(FIELD) \
    FIELD(o_orderkey,long)      \
    FIELD(o_custkey,int)           \
    FIELD(o_orderstatus,char)          \
    FIELD(o_totalprice,double)               \
    FIELD(o_orderdate,date_t)                      \
    FIELD(o_orderpriority,priority_t)                        \
    FIELD(o_clerk,Char<15>)                                          \
    FIELD(o_shippriority,int)                                           \
    FIELD(o_comment,Char<79>)

#define CustomerFields(FIELD) \
    FIELD(c_custkey,int)         \
    FIELD(c_name,name_t)               \
    FIELD(c_address,Char<40>)                  \
    FIELD(c_nationkey,int)                        \
    FIELD(c_phone,Char<15>)                               \
    FIELD(c_acctball,double)                                    \
    FIELD(c_mktsegment,Char<10>)                                        \
    FIELD(c_comment,Char<117>) 


#define SupplierFields(FIELD) \
    FIELD(s_suppkey,int)         \
    FIELD(s_name,name_t)               \
    FIELD(s_address,Char<40>)                  \
    FIELD(s_nationkey,int)                     \
    FIELD(s_phone,Char<15>)                            \
    FIELD(s_acctbal,double)                                  \
    FIELD(s_comment,Char<101>)


#define PartsuppFields(FIELD) \
    FIELD(ps_partkey,int)        \
    FIELD(ps_suppkey,int)           \
    FIELD(ps_availqty,int)             \
    FIELD(ps_supplycost,double)              \
    FIELD(ps_comment,Char<199>)

#define RegionFields(FIELD) \
    FIELD(r_regionkey,int)     \
    FIELD(r_name,name_t)             \
    FIELD(r_comment,Char<152>)

#define NationFields(FIELD) \
    FIELD(n_nationkey,int)     \
    FIELD(n_name,name_t)             \
    FIELD(n_regionkey,int)              \
    FIELD(n_comment,Char<152>)


#define PartFields(FIELD)    \
    FIELD(p_partkey,int)     \
    FIELD(p_name,Char<55>)           \
    FIELD(p_mfgr,Char<25>)                   \
    FIELD(p_brand,Char<10>)                          \
    FIELD(p_type,Char<25>)                                   \
    FIELD(p_size,int)                                           \
    FIELD(p_container,Char<10>)                                         \
    FIELD(p_retailprice,double)                                               \
    FIELD(p_comment,Char<23>) 


#define HSTRUCT_FIELD(NAME,TYPE) TYPE NAME;


#ifdef COLUMNAR_STORE

#define VSTRUCT_FIELD(NAME,TYPE) TYPE* NAME;
#define VSTRUCT_CONS(NAME,TYPE) NAME = new TYPE[size];
#define VSTRUCT_ASSIGN(NAME,TYPE) NAME[used] = other.NAME;
#define VSTRUCT_COPY(NAME,TYPE) NAME = other.NAME;
#define VSTRUCT_DELETE(NAME,TYPE) delete[] NAME;
#define VSTRUCT_EXTEND(NAME,TYPE) {             \
    TYPE* newBuf = new TYPE[size];              \
    for (size_t i = 0; i < used; i++) {         \
        newBuf[i] = NAME[i];                    \
    }                                           \
    delete[] NAME;                              \
    NAME = newBuf;                              \
}

#define STRUCT_GETTER(NAME,TYPE) struct {       \
    operator TYPE() const {                                         \
        Self* self = (Self*)((char*)this - (char*)&((Self*)0)->NAME);   \
        return self->data->NAME[self->pos];     \
    }                                           \
} NAME;

#define SCHEMA(Class)                           \
struct H##Class {                               \
    Class##Fields(HSTRUCT_FIELD)                \
};                                              \
struct V##Class {                               \
    Class##Fields(VSTRUCT_FIELD)                \
    size_t used, size;                          \
    V##Class(size_t estimation) : used(0), size(estimation) { \
        Class##Fields(VSTRUCT_CONS);            \
    }                                           \
    V##Class(V##Class const& other) : used(other.used), size(0) {   \
        Class##Fields(VSTRUCT_COPY);            \
    }                                           \
    ~V##Class() {                               \
        if (size != 0) {                        \
            Class##Fields(VSTRUCT_DELETE);      \
        }                                       \
    }                                           \
    void append(H##Class const& other) {        \
        if (used == size) {                     \
            size *= 2;                          \
            Class##Fields(VSTRUCT_EXTEND);      \
        }                                       \
        Class##Fields(VSTRUCT_ASSIGN);          \
        used += 1;                              \
    }                                           \
};                                              \
struct Class {                                  \
    V##Class* data;                             \
    size_t pos;                                 \
    typedef Class Self;                         \
    Class##Fields(STRUCT_GETTER)                \
};                                              \
PACK(Class)                                     \
UNPACK(Class)                                   \
PARQUET_UNPACK(Class)                       

#else

#define SCHEMA(Class)                           \
struct Class {                                  \
    Class##Fields(HSTRUCT_FIELD)                \
};                                              \
PACK(Class)                                     \
UNPACK(Class)                                   \
PARQUET_UNPACK(Class)                       


#endif

SCHEMA(Lineitem)
SCHEMA(Part)
SCHEMA(Partsupp)
SCHEMA(Orders)
SCHEMA(Supplier)
SCHEMA(Customer)
SCHEMA(Nation)
SCHEMA(Region)
        
#endif
