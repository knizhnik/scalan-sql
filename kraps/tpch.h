#ifndef __TPCH_H__
#define __TPCH_H__

#include "rdd.h"
#include "pack.h"

//
// TPC-H classes (tables) defintion
//


typedef unsigned date_t;
typedef char name_t[25];
typedef char priority_t[15];
typedef char shipmode_t[10];


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
    char   l_shipinstruct[25];
    shipmode_t l_shipmode;
    char   l_comment[44];
};

#define LineitemFields(FIELD) \
    FIELD(l_orderkey) \
    FIELD(l_partkey) \
    FIELD(l_suppkey) \
    FIELD(l_linenumber) \
    FIELD(l_quantity) \
    FIELD(l_extendedprice) \
    FIELD(l_discount) \
    FIELD(l_tax) \
    FIELD(l_returnflag) \
    FIELD(l_linestatus) \
    FIELD(l_shipdate) \
    FIELD(l_commitdate) \
    FIELD(l_receiptdate) \
    FIELD(l_shipinstruct) \
    FIELD(l_shipmode) \
    FIELD(l_comment) 


   
struct Orders
{
    long o_orderkey;
    int o_custkey;
    char o_orderstatus;
    double o_totalprice;
    date_t o_orderdate;
    priority_t o_orderpriority;
    char o_clerk[15];
    int o_shippriority;
    char o_comment[79];
};

#define OrdersFields(FIELD) \
    FIELD(o_orderkey) \
    FIELD(o_custkey) \
    FIELD(o_orderstatus) \
    FIELD(o_totalprice) \
    FIELD(o_orderdate) \
    FIELD_STR(o_orderpriority) \
    FIELD_STR(o_clerk) \
    FIELD(o_shippriority) \
    FIELD_STR(o_comment)

struct Customer
{
    int c_custkey;
    name_t c_name;
    char c_address[40];
    int c_nationkey;
    char c_phone[15];
    double c_acctball;
    char c_mktsegment[10];
    char c_comment[117];
};

#define CustomerFields(FIELD) \
    FIELD(c_custkey) \
    FIELD(c_name) \
    FIELD_STR(c_address) \
    FIELD(c_nationkey) \
    FIELD_STR(c_phone) \
    FIELD(c_acctball) \
    FIELD_STR(c_mktsegment) \
    FIELD_STR(c_comment) 

struct Supplier
{
    int s_suppkey;
    name_t s_name;
    char s_address[40];
    int s_nationkey;
    char s_phone[15];
    double s_acctbal;
    char s_comment[101];
};

#define SupplierFields(FIELD) \
    FIELD(s_suppkey) \
    FIELD(s_name) \
    FIELD_STR(s_address) \
    FIELD(s_nationkey) \
    FIELD_STR(s_phone) \
    FIELD(s_acctbal) \
    FIELD_STR(s_comment)

struct Partsupp
{
    int ps_partkey;
    int ps_suppkey;
    int ps_availqty;
    double ps_supplycost;
    char ps_comment[199];
};

#define PartsuppFields(FIELD) \
    FIELD(ps_partkey) \
    FIELD(ps_suppkey) \
    FIELD(ps_availqty) \
    FIELD(ps_supplycost) \
    FIELD_STR(ps_comment)

struct Region
{
    int r_regionkey;
    name_t r_name;
    char r_comment[152];
};

#define RegionFields(FIELD) \
    FIELD(r_regionkey) \
    FIELD(r_name) \
    FIELD_STR(r_comment)

struct Nation
{
    int n_nationkey;
    name_t n_name;
    int n_regionkey;
    char n_comment[152];
};


#define NationFields(FIELD) \
    FIELD(n_nationkey) \
    FIELD(n_name) \
    FIELD(n_regionkey) \
    FIELD_STR(n_comment)


struct Part
{
    int p_partkey;
    char p_name[55];
    char p_mfgr[25];
    char p_brand[10];
    char p_type[25];
    int p_size;
    char p_container[10];
    double p_retailprice;
    char p_comment[23];
};

define PartFields(FIELD) \
    FIELD(p_partkey) \
    FIELD_STR(p_name) \
    FIELD_STR(p_mfgr) \
    FIELD_STR(p_brand) \
    FIELD_STR(p_type) \
    FIELD(p_size) \
    FIELD_STR(p_container) \
    FIELD(p_retailprice) \
    FIELD_STR(p_comment) 

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

    RDD<Projection>* query();
}


PACK(Lineitem)
PACK(Orders)
PACK(Customer)
PACK(Supplier)
PACK(Partsupp)
PACK(Region)
PACK(Nation)
PACK(Part)

UNPACK(Lineitem)
UNPACK(Orders)
UNPACK(Customer)
UNPACK(Supplier)
UNPACK(Partsupp)
UNPACK(Region)
UNPACK(Nation)
UNPACK(Part)

#if USE_PARQUET
PARQUET_UNPACK(Lineitem)
PARQUET_UNPACK(Orders)
PARQUET_UNPACK(Customer)
PARQUET_UNPACK(Supplier)
PARQUET_UNPACK(Partsupp)
PARQUET_UNPACK(Region)
PARQUET_UNPACK(Nation)
PARQUET_UNPACK(Part)
#endif

#endif
