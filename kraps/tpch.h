typedef unsigned date_t;

struct Lineitem
{
    int    l_orderkey;
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
    char   l_shipmode[10];
    char   l_comment[44];
};
