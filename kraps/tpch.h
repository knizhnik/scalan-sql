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

struct Orders
{
    int o_orderkey;
    int o_custkey;
    char o_orderstatus;
    double o_totalprice;
    date_t o_orderdate;
    char o_orderpriority[15];
    char o_clerk[15];
    int o_shippriority;
    char o_comment[79];
};

struct Customer
{
    int c_custkey;
    char c_name[25];
    char c_address[40];
    int c_nationkey;
    char c_phone[15];
    double c_acctball;
    char c_mktsegment[10];
    char c_comment[117];
};


struct Supplier
{
    int s_suppkey;
    char s_name[25];
    char s_address[40];
    int s_nationkey;
    char s_phone[15];
    double s_acctbal;
    char s_comment[101];
};

struct PartSupp
{
    int ps_partkey;
    int ps_suppkey;
    int ps_availqty;
    double ps_supplycost;
    char ps_comment[199];
};


struct Region
{
    int r_regionkey;
    char r_name[25];
    char r_comment[152];
};

struct Nation
{
    int n_nationkey;
    char n_name[25];
    int n_regionkey;
    char n_comment[152];
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
    dobule p_retailprice;
    char p_comment[23];
};

