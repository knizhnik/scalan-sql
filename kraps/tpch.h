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

    friend size_t pack(Lineitem const& src, char* dst) {
        size_t size = 0;       
        PACK(l_orderkey);
        PACK(l_partkey);
        PACK(l_suppkey);
        PACK(l_linenumber);
        PACK(l_quantity);
        PACK(l_extendedprice);
        PACK(l_discount);
        PACK(l_tax);
        PACK(l_returnflag);
        PACK(l_linestatus);
        PACK(l_shipdate);
        PACK(l_commitdate);
        PACK(l_receiptdate);
        PACK_STR(l_shipinstruct);
        PACK_STR(l_shipmode);
        PACK_STR(l_comment);
        return size;
    }
    friend size_t unpack(Lineitem& dst, char const* src) {
        size_t size = 0;       
        UNPACK(l_orderkey);
        UNPACK(l_partkey);
        UNPACK(l_suppkey);
        UNPACK(l_linenumber);
        UNPACK(l_quantity);
        UNPACK(l_extendedprice);
        UNPACK(l_discount);
        UNPACK(l_tax);
        UNPACK(l_returnflag);
        UNPACK(l_linestatus);
        UNPACK(l_shipdate);
        UNPACK(l_commitdate);
        UNPACK(l_receiptdate);
        UNPACK_STR(l_shipinstruct);
        UNPACK_STR(l_shipmode);
        UNPACK_STR(l_comment);
        return size;
    }

};

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

    friend size_t pack(Orders const& src, char* dst) {
        size_t size = 0;       
        PACK(o_orderkey);
        PACK(o_custkey);
        PACK(o_orderstatus);
        PACK(o_totalprice);
        PACK(o_orderdate);
        PACK_STR(o_orderpriority);
        PACK_STR(o_clerk);
        PACK(o_shippriority);
        PACK_STR(o_comment);
        return size;
    }
    friend size_t unpack(Orders& dst, char const* src) {
        size_t size = 0;       
        UNPACK(o_orderkey);
        UNPACK(o_custkey);
        UNPACK(o_orderstatus);
        UNPACK(o_totalprice);
        UNPACK(o_orderdate);
        UNPACK_STR(o_orderpriority);
        UNPACK_STR(o_clerk);
        UNPACK(o_shippriority);
        UNPACK_STR(o_comment);
        return size;
    }
};

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

    friend size_t pack(Customer const& src, char* dst) {
        size_t size = 0;       
        PACK(c_custkey);
        PACK(c_name);
        PACK_STR(c_address);
        PACK(c_nationkey);
        PACK_STR(c_phone);
        PACK(c_acctball);
        PACK_STR(c_mktsegment);
        PACK_STR(c_comment);
        return size;
    }
    friend size_t unpack(Customer& dst, char const* src) {
        size_t size = 0;       
        UNPACK(c_custkey);
        UNPACK(c_name);
        UNPACK_STR(c_address);
        UNPACK(c_nationkey);
        UNPACK_STR(c_phone);
        UNPACK(c_acctball);
        UNPACK_STR(c_mktsegment);
        UNPACK_STR(c_comment);
        return size;
    }
};


struct Supplier
{
    int s_suppkey;
    name_t s_name;
    char s_address[40];
    int s_nationkey;
    char s_phone[15];
    double s_acctbal;
    char s_comment[101];

    friend size_t pack(Supplier const& src, char* dst) {
        size_t size = 0;       
        PACK(s_suppkey);
        PACK(s_name);
        PACK_STR(s_address);
        PACK(s_nationkey);
        PACK_STR(s_phone);
        PACK(s_acctbal);
        PACK_STR(s_comment);
        return size;
    }
    friend size_t unpack(Supplier& dst, char const* src) {
        size_t size = 0;       
        UNPACK(s_suppkey);
        UNPACK(s_name);
        UNPACK_STR(s_address);
        UNPACK(s_nationkey);
        UNPACK_STR(s_phone);
        UNPACK(s_acctbal);
        UNPACK_STR(s_comment);
        return size;
    }
};

struct Partsupp
{
    int ps_partkey;
    int ps_suppkey;
    int ps_availqty;
    double ps_supplycost;
    char ps_comment[199];

    friend size_t pack(Partsupp const& src, char* dst) {
        size_t size = 0;       
        PACK(ps_partkey);
        PACK(ps_suppkey);
        PACK(ps_availqty);
        PACK(ps_supplycost);
        PACK_STR(ps_comment);
        return size;
    }
    friend size_t unpack(Partsupp& dst, char const* src) {
        size_t size = 0;       
        UNPACK(ps_partkey);
        UNPACK(ps_suppkey);
        UNPACK(ps_availqty);
        UNPACK(ps_supplycost);
        UNPACK_STR(ps_comment);
        return size;
    }
};


struct Region
{
    int r_regionkey;
    name_t r_name;
    char r_comment[152];

    friend size_t pack(Region const& src, char* dst) {
        size_t size = 0;       
        PACK(r_regionkey);
        PACK(r_name);
        PACK_STR(r_comment);
        return size;
    }
    friend size_t unpack(Region& dst, char const* src) {
        size_t size = 0;       
        UNPACK(r_regionkey);
        UNPACK(r_name);
        UNPACK_STR(r_comment);
        return size;
    }
};

struct Nation
{
    int n_nationkey;
    name_t n_name;
    int n_regionkey;
    char n_comment[152];

    friend size_t pack(Nation const& src, char* dst) {
        size_t size = 0;       
        PACK(n_nationkey);
        PACK(n_name);
        PACK(n_regionkey);
        PACK_STR(n_comment);
        return size;
    }
    friend size_t unpack(Nation& dst, char const* src) {
        size_t size = 0;       
        UNPACK(n_nationkey);
        UNPACK(n_name);
        UNPACK(n_regionkey);
        UNPACK_STR(n_comment);
        return size;
    }
};

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

    friend size_t pack(Part const& src, char* dst) {
        size_t size = 0;       
        PACK(p_partkey);
        PACK_STR(p_name);
        PACK_STR(p_mfgr);
        PACK_STR(p_brand);
        PACK_STR(p_type);
        PACK(p_size);
        PACK_STR(p_container);
        PACK(p_retailprice);
        PACK_STR(p_comment);
        return size;
    }
    friend size_t unpack(Part& dst, char const* src) {
        size_t size = 0;       
UNPACK(p_partkey);
        UNPACK_STR(p_name);
        UNPACK_STR(p_mfgr);
        UNPACK_STR(p_brand);
        UNPACK_STR(p_type);
        UNPACK(p_size);
        UNPACK_STR(p_container);
        UNPACK(p_retailprice);
        UNPACK_STR(p_comment);
        return size;
    }
};

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

#endif
