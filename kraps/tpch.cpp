#include <time.h>
#include <ctype.h>
#include "rdd.h"
#include "tpch.h"

const size_t SF = 100; // scale factor

#define STRCMP(s,p) strncmp(s, p, sizeof(s))
#define STREQ(s,p)  (STRCMP(s, p) == 0)
#define STRCPY(d,s) strncpy(d,s,sizeof(d))
#define SCALE(x)    ((x + Cluster::instance->nNodes - 1)*SF/(Cluster::instance->nNodes) + (x / 100)) // take in accoutn data skews

#define TABLE(x) (Cluster::instance->userData ? (RDD<x>*)((CachedData*)Cluster::instance->userData)->_##x->clone() : (RDD<x>*)FileManager::load<x>(filePath(#x)))

char const* dataDir;
char const* dataFormat;

static char* filePath(char const* fileName)
{ 
    char path[MAX_PATH_LEN];
    int n = (dataDir == NULL) ? sprintf(path, "%s", fileName) : sprintf(path, "%s/%s", dataDir, fileName);
    if (dataFormat != NULL) { 
        sprintf(path + n, ".%s", dataFormat);
    }
    return strdup(path);
}

void sum(double& dst, double const& src)
{
    dst += src;
}

void count(int& dst, int const& src)
{
    dst += src;
}

void nationKey(int& key, Nation const& nation)
{
    key = nation.n_nationkey;
}

void regionKey(int& key, Region const& region)
{
    key = region.r_regionkey;
}

void ordersKey(long& key, Orders const& record)
{
    key = record.o_orderkey;
}

void supplierKey(int& key, Supplier const& record)
{
    key = record.s_suppkey;
}

void customerKey(int& key, Customer const& record)
{
    key = record.c_custkey;
}

void partKey(int& key, Part const& record)
{
    key = record.p_partkey;
}

struct PartsuppKey
{
    int ps_partkey;
    int ps_suppkey;
    
    bool operator==(PartsuppKey const& other) const
    {
        return ps_partkey == other.ps_partkey
            && ps_suppkey == other.ps_suppkey;
    }
};

void partsuppKey(PartsuppKey& key, Partsupp const& ps)
{
    key.ps_partkey = ps.ps_partkey;
    key.ps_suppkey = ps.ps_suppkey;
}

    
class CachedData
{
  public:
    RDD<Lineitem>* _Lineitem;
    RDD<Orders>* _Orders;
    RDD<Supplier>* _Supplier;
    RDD<Customer>* _Customer;
    RDD<Part>* _Part;
    RDD<Partsupp>* _Partsupp;
    RDD<Nation>* _Nation;
    RDD<Region>* _Region;

    CachedData(bool sharded) 
    { 
        _Lineitem = new CachedRDD<Lineitem>(FileManager::load<Lineitem>(filePath("Lineitem")), SCALE(6000000));
        _Nation = new CachedRDD<Nation>(FileManager::load<Nation>(filePath("Nation")),       25);
        _Region = new CachedRDD<Region>(FileManager::load<Region>(filePath("Region")),       5);
        if (sharded) { 
            _Orders = FileManager::load<Orders>(filePath("Orders"))->scatter<long,ordersKey>();
            _Supplier = FileManager::load<Supplier>(filePath("Supplier"))->scatter<int,supplierKey>();
            _Customer = FileManager::load<Customer>(filePath("Customer"))->scatter<int,customerKey>();
            _Part = FileManager::load<Part>(filePath("Part"))->scatter<int,partKey>();
            _Partsupp = FileManager::load<Partsupp>(filePath("Partsupp"))->scatter<PartsuppKey,partsuppKey>();
        } else {
            _Orders = new CachedRDD<Orders>(FileManager::load<Orders>(filePath("Orders")),         SCALE(1500000));
            _Supplier = new CachedRDD<Supplier>(FileManager::load<Supplier>(filePath("Supplier")), SCALE(10000));
            _Customer = new CachedRDD<Customer>(FileManager::load<Customer>(filePath("Customer")), SCALE(150000));
            _Part = new CachedRDD<Part>(FileManager::load<Part>(filePath("Part")),                 SCALE(200000));
            _Partsupp = new CachedRDD<Partsupp>(FileManager::load<Partsupp>(filePath("Partsupp")), SCALE(800000));
        }
    }

    ~CachedData()
    {
        delete _Lineitem;
        delete _Orders;
        delete _Supplier;
        delete _Customer;
        delete _Part;
        delete _Partsupp;
        delete _Nation;
        delete _Region;
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

    RDD<Projection>* query() 
    { 
        return
            TABLE(Lineitem)->
            filter<predicate>()->
            mapReduce<GroupBy,Aggregate,map,reduce>(10000)->
            project<Projection, projection>()->
            sort<compare>(100);
    }
}

namespace Q3
{
    struct LineitemProjection 
    {
        long   l_orderkey;
        date_t l_shipdate;
        double l_extendedprice;
        double l_discount;
    };

    void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey;
        out.l_extendedprice = in.l_extendedprice;
        out.l_discount = in.l_discount;
        out.l_shipdate = in.l_shipdate;
    }

    void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        long o_orderkey;
        int o_custkey;
        date_t o_orderdate;
        int o_shippriority;
    };

    void projectOrders(OrdersProjection& out, Orders const& in)
    { 
        out.o_orderkey = in.o_orderkey;
        out.o_custkey = in.o_custkey;
        out.o_orderdate = in.o_orderdate;
        out.o_shippriority = in.o_shippriority;
    }            

    void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }

    struct CustomerProjection
    {
        int c_custkey;
    };
    
    void projectCustomer(CustomerProjection& out, Customer const& in)
    {
        out.c_custkey = in.c_custkey;
    }
    
    void customerKey(int& key, CustomerProjection const& customer)
    {
        key = customer.c_custkey;
    }

    bool lineitemFilter(Lineitem const& l)
    {
        return l.l_shipdate > 19950304;
    }
    
    bool orderFilter(Orders const& o)
    {
        return o.o_orderdate < 19950304;
    }
    
    bool customerFilter(Customer const& c)
    {
        return STREQ(c.c_mktsegment, "HOUSEHOLD");
    }
    
    void orderCustomerKey(int& key, Join<LineitemProjection,OrdersProjection> const& r)
    {
        key = r.o_custkey;
    }

    struct GroupBy
    { 
        long l_orderkey;
        date_t o_orderdate;
        int o_shippriority;

        bool operator == (GroupBy const& other) const
        { 
            return l_orderkey == other.l_orderkey && o_orderdate == other.o_orderdate && o_shippriority == other.o_shippriority;
        }
    };

    void map(Pair<GroupBy,double>& pair, Join<Join<LineitemProjection,OrdersProjection>,CustomerProjection> const& r)
    {
        pair.key.l_orderkey = r.l_orderkey;
        pair.key.o_orderdate = r.o_orderdate;
        pair.key.o_shippriority = r.o_shippriority;
        pair.value = r.l_extendedprice * (1 - r.l_discount);
    }

    struct Revenue : GroupBy
    {
        double revenue;

        friend void print(Revenue const& r, FILE* out) { 
            fprintf(out, "%ld, %f, %d, %d", r.l_orderkey, r.revenue, r.o_orderdate, r.o_shippriority);
        }
    };

    void revenue(Revenue& out, Pair<GroupBy, double> const& in)
    {
        (GroupBy&)out = in.key;
        out.revenue = in.value;
    }


    int byRevenueAndOrderDate(Revenue const* a, Revenue const* b) 
    {
        return a->revenue > b->revenue ? -1 : a->revenue == b->revenue ? (a->o_orderdate < b->o_orderdate ? -1 : a->o_orderdate == b->o_orderdate ? 0 : 1) : 1;
    }

    RDD<Revenue>* query() 
    { 
        return
            TABLE(Lineitem)->
            filter<lineitemFilter>()->
            project<LineitemProjection, projectLineitem>()->
            join<OrdersProjection,long,lineitemOrderKey,orderKey>(TABLE(Orders)->
                                                                  filter<orderFilter>()->
                                                                  project<OrdersProjection, projectOrders>(), 
                                                                  SCALE(1500000))->
            join<CustomerProjection,int,orderCustomerKey,customerKey>(TABLE(Customer)->
                                                                      filter<customerFilter>()->
                                                                      project<CustomerProjection,projectCustomer>(), 
                                                                      SCALE(150000))->
            mapReduce<GroupBy, double, map, sum>(1000000)->
            project<Revenue, revenue>()->
            top<byRevenueAndOrderDate>(10);
    }    
}
namespace Q4
{
    struct LineitemProjection 
    {
        long   l_orderkey;
        date_t l_commitdate;
        date_t l_receiptdate;
    };

    void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey;
        out.l_commitdate = in.l_commitdate;
        out.l_receiptdate = in.l_receiptdate;
    }

    void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        long o_orderkey;
        date_t o_orderdate;
        priority_t o_orderpriority;
    };

    void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey;
        out.o_orderdate = in.o_orderdate;
        STRCPY(out.o_orderpriority, in.o_orderpriority);
    }            

    void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }

    bool lineitemFilter(Lineitem const& l)
    {
        return l.l_commitdate < l.l_receiptdate;
    }
    
    bool orderFilter(Orders const& o)
    {
        return o.o_orderdate >= 19930801 && o.o_orderdate < 19931101;
    }
    
    void map(Pair<Key<priority_t>,int>& pair, Join<LineitemProjection,OrdersProjection> const& r)
    {
        STRCPY(pair.key.val, r.o_orderpriority);
        pair.value = 1;
    }

    int byPriority(Pair<Key<priority_t>,int> const* a, Pair<Key<priority_t>,int> const* b)  { 
        return STRCMP(a->key.val, b->key.val);
    }

    RDD< Pair<Key<priority_t>,int> >* query() 
    { 
        return
            TABLE(Lineitem)->
            filter<lineitemFilter>()->
            project<LineitemProjection, projectLineitem>()->
            join<OrdersProjection,long,lineitemOrderKey,orderKey>(TABLE(Orders)->
                                                                  filter<orderFilter>()->
                                                                  project<OrdersProjection,projectOrders>(), 
                                                                  SCALE(1500000))->
            mapReduce<Key<priority_t>, int, map, count>(25)->
            sort<byPriority>(25);
    }    
}   
namespace Q5
{
    struct LineitemProjection 
    {
        long   l_orderkey;
        int    l_suppkey;
        double l_extendedprice;
        double l_discount;
    };

    void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey;
        out.l_suppkey = in.l_suppkey;
        out.l_extendedprice = in.l_extendedprice;
        out.l_discount = in.l_discount;
    }

    void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        int o_orderkey;
        int o_custkey;
    };

    void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey;
        out.o_custkey = in.o_custkey;
    }            

    void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }
    
    struct SupplierProjection
    {
        int s_suppkey;
        int s_nationkey;
    };

    void projectSupplier(SupplierProjection& out, Supplier const& in)
    {
        out.s_suppkey = in.s_suppkey;
        out.s_nationkey = in.s_nationkey;
    }            

    void supplierKey(int& key, SupplierProjection const& in)
    {
        key = in.s_suppkey;
    }
    
    struct CustomerProjection
    {
        int c_custkey;
        int c_nationkey;
    };

    void projectCustomer(CustomerProjection& out, Customer const& in)
    {
        out.c_custkey = in.c_custkey;
        out.c_nationkey = in.c_nationkey;
    }            

    void customerKey(int& key, CustomerProjection const& in)
    {
        key = in.c_custkey;
    }

    bool orderRange(Orders const& order) 
    {
        return order.o_orderdate >= 19960101 && order.o_orderdate < 19970101;
    }

    bool sameNation(Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection> const& r)
    {
        return r.c_nationkey == r.s_nationkey;
    }

    void customerNationKey(int& key, Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection> const& r)
    {
        key = r.c_nationkey;
    }
    
    void lineitemSupplierKey(int& key, Join<LineitemProjection,OrdersProjection> const& r)
    {
        key = r.l_suppkey;
    }
    
    void orderCustomerKey(int& key, Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection> const& r)
    {
        key = r.o_custkey;
    }

    void nationRegionKey(int& key, Join<Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection>,Nation> const& r)
    {
        key = r.n_regionkey;
    }
    
    bool asiaRegion(Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection>,Nation>,Region> const& r) 
    { 
        return STREQ(r.r_name, "ASIA");

    }

    void map(Pair<Key<name_t>,double>& pair, Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection>,Nation>,Region> const& r)
    {
        STRCPY(pair.key.val, r.n_name);
        pair.value = r.l_extendedprice * (1 - r.l_discount);
    }

    struct Revenue 
    {
        name_t n_name;
        double revenue;

        friend void print(Revenue const& r, FILE* out) { 
            printf("%s, %f", r.n_name, r.revenue);
        }
    };

    void revenue(Revenue& out, Pair<Key<name_t>,double> const& in)
    {
        STRCPY(out.n_name, in.key.val);
        out.revenue = in.value;
    }

    int byRevenue(Revenue const* a, Revenue const* b) 
    {
        return a->revenue > b->revenue ? -1 : a->revenue == b->revenue ? 0 : 1;
    }

    RDD<Revenue>* query() 
    { 
        return
            TABLE(Lineitem)->
            project<LineitemProjection,projectLineitem>()->            
            join<OrdersProjection,long,lineitemOrderKey,orderKey>(TABLE(Orders)->
                                                                  filter<orderRange>()->
                                                                  project<OrdersProjection,projectOrders>(),
                                                                  SCALE(1500000))->
            join<SupplierProjection,int,lineitemSupplierKey,supplierKey>(TABLE(Supplier)->
                                                                         project<SupplierProjection,projectSupplier>(),
                                                                         SCALE(10000))->
            join<CustomerProjection,int,orderCustomerKey,customerKey>(TABLE(Customer)->
                                                                      project<CustomerProjection,projectCustomer>(),
                                                                      SCALE(150000))->
            filter<sameNation>()->
            join<Nation,int,customerNationKey,nationKey>(TABLE(Nation),25)->
            join<Region,int,nationRegionKey,regionKey>(TABLE(Region),5)->
            filter<asiaRegion>()->
            mapReduce<Key<name_t>,double,map,sum>(25)->
            project<Revenue,revenue>()->
            sort<byRevenue>(25);
    }    
}
namespace Q6
{
    bool lineitemFilter(Lineitem const& l)
    {
        return l.l_shipdate >= 19960101 && l.l_shipdate <= 19970101
            && l.l_discount >= 0.08 && l.l_discount <= 0.1
            && l.l_quantity < 24;
    }
    void revenue(double& result, Lineitem const& l)
    {
        result += l.l_extendedprice*l.l_discount;
    }
    
    RDD<double>* query() 
    { 
        return
            TABLE(Lineitem)->
            filter<lineitemFilter>()->
            reduce<double,revenue,sum>(0);
    }
}
namespace Q7
{
    struct LineitemProjection 
    {
        long   l_orderkey;
        int    l_suppkey;
        date_t l_shipdate;
        double l_extendedprice;
        double l_discount;
    };

    void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey;
        out.l_suppkey = in.l_suppkey;
        out.l_shipdate = in.l_shipdate;
        out.l_extendedprice = in.l_extendedprice;
        out.l_discount = in.l_discount;
    }

    void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        int o_orderkey;
        int o_custkey;
    };

    void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey;
        out.o_custkey = in.o_custkey;
    }            

    void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }
    
    struct CustomerProjection
    {
        int c_custkey;
        int c_nationkey;
    };

    void projectCustomer(CustomerProjection& out, Customer const& in)
    {
        out.c_custkey = in.c_custkey;
        out.c_nationkey = in.c_nationkey;
    }            

    void customerKey(int& key, CustomerProjection const& in)
    {
        key = in.c_custkey;
    }

    struct SupplierProjection
    {
        int s_suppkey;
        int s_nationkey;
    };

    void projectSupplier(SupplierProjection& out, Supplier const& in)
    {
        out.s_suppkey = in.s_suppkey;
        out.s_nationkey = in.s_nationkey;
    }            

    void supplierKey(int& key, SupplierProjection const& in)
    {
        key = in.s_suppkey;
    }

    struct Nation1 { Nation n1; };
    struct Nation2 { Nation n2; };

    void lineitemSupplierKey(int& key, Join<LineitemProjection,OrdersProjection> const& r)
    {
        key = r.l_suppkey;
    }

    void orderCustomerKey(int& key, Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection> const& r)
    {
        key = r.o_custkey;
    }

    void supplierNationKey(int& key, Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection> const& r)
    {
        key = r.s_nationkey;
    }
    
    void customerNationKey(int& key, Join<Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection>,Nation1> const& r)
    {
        key = r.c_nationkey;
    }

    void nation1Key(int& key, Nation1 const& nation)
    {
        key = nation.n1.n_nationkey;
    }

    void nation2Key(int& key, Nation2 const& nation)
    {
        key = nation.n2.n_nationkey;
    }

    bool filterNation(Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection>,Nation1>,Nation2> const& r) 
    {
        return (STREQ(r.n1.n_name, "UNITED STATES") && STREQ(r.n2.n_name, "INDONESIA"))
            || (STREQ(r.n2.n_name, "UNITED STATES") && STREQ(r.n1.n_name, "INDONESIA"));
    }

    bool filterLineitem(Lineitem const& l)
    {
        return l.l_shipdate >= 19950101 && l.l_shipdate <= 19961231;
    }

    struct Shipping
    {
        name_t supp_nation;
        name_t cust_nation;
        int    l_year;

        bool operator == (Shipping const& other) const
        {
            return STREQ(supp_nation, other.supp_nation)
                && STREQ(cust_nation, other.cust_nation) 
                && l_year == other.l_year;
        }

        friend size_t hashCode(Shipping const& s)
        {
            return ::hashCode(s.supp_nation) + ::hashCode(s.cust_nation) + s.l_year;
        }

        friend void print(Shipping const& s, FILE* out) 
        {
            fprintf(out, "%s, %s, %d", s.supp_nation, s.cust_nation, s.l_year);
        }
    };

    #define ShippingFields(FIELD) \
        FIELD(supp_nation)        \
        FIELD(cust_nation)        \
        FIELD(l_year)

    PACK(Shipping)
    UNPACK(Shipping)

    void map(Pair<Shipping,double>& pair, Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection>,Nation1>,Nation2> const& r)
    {
        STRCPY(pair.key.supp_nation, r.n1.n_name);
        STRCPY(pair.key.cust_nation, r.n2.n_name);
        pair.key.l_year = r.l_shipdate/10000;
        pair.value = r.l_extendedprice * (1-r.l_discount);
    }

    int byShipping(Pair<Shipping,double> const* a, Pair<Shipping,double> const* b)
    {
        int diff;
        diff = STRCMP(a->key.supp_nation, b->key.supp_nation);
        if (diff != 0) return diff;
        diff = STRCMP(a->key.cust_nation, b->key.cust_nation);
        return (diff != 0) ? diff : a->key.l_year - b->key.l_year;
    }

    RDD< Pair<Shipping,double> > * query() 
    { 
        return
            TABLE(Lineitem)->
            filter<filterLineitem>()->           
            project<LineitemProjection, projectLineitem>()->            
            join<OrdersProjection,long,lineitemOrderKey,orderKey>(TABLE(Orders)->
                                                                  project<OrdersProjection,projectOrders>(), 
                                                                  SCALE(1500000))->
            join<SupplierProjection,int,lineitemSupplierKey,supplierKey>(TABLE(Supplier)->
                                                                         project<SupplierProjection,projectSupplier>(),
                                                                         SCALE(10000))->
            join<CustomerProjection,int,orderCustomerKey,customerKey>(TABLE(Customer)->
                                                                      project<CustomerProjection,projectCustomer>(),
                                                                      SCALE(150000))-> 
            join<Nation1,int,supplierNationKey,nation1Key>((RDD<Nation1>*)TABLE(Nation), 25)->
            join<Nation2,int,customerNationKey,nation2Key>((RDD<Nation2>*)TABLE(Nation), 25)->
            filter<filterNation>()->
            mapReduce<Shipping,double,map,sum>(25*25*100)->
            sort<byShipping>(25*25*100);
    }    
}
namespace Q8
{
    struct LineitemProjection 
    {
        long   l_orderkey;
        int    l_suppkey;
        int    l_partkey;
        double l_extendedprice;
        double l_discount;
    };

    void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey;
        out.l_suppkey = in.l_suppkey;
        out.l_partkey = in.l_partkey;
        out.l_extendedprice = in.l_extendedprice;
        out.l_discount = in.l_discount;
    }

    void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        int o_orderkey;
        int o_custkey;
        date_t o_orderdate;
    };

    void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey;
        out.o_custkey = in.o_custkey;
        out.o_orderdate = in.o_orderdate;
    }            

    void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }
    
    struct CustomerProjection
    {
        int c_custkey;
        int c_nationkey;
    };
    
    void projectCustomer(CustomerProjection& out, Customer const& in)
    {
        out.c_custkey = in.c_custkey;
        out.c_nationkey = in.c_nationkey;
    }
    
    void customerKey(int& key, CustomerProjection const& customer)
    {
        key = customer.c_custkey;
    }

    struct PartProjection
    {
        int p_partkey;
        char p_type[25];
     };

    void projectPart(PartProjection& out, Part const& in)
    {
        out.p_partkey = in.p_partkey;
        STRCPY(out.p_type, in.p_type);
    }            

    void partKey(int& key, PartProjection const& in)
    {
        key = in.p_partkey;
    }

    struct SupplierProjection
    {
        int s_suppkey;
        int s_nationkey;
    };

    void projectSupplier(SupplierProjection& out, Supplier const& in)
    {
        out.s_suppkey = in.s_suppkey;
        out.s_nationkey = in.s_nationkey;
    }            

    void supplierKey(int& key, SupplierProjection const& in)
    {
        key = in.s_suppkey;
    }

    struct Nation1 { Nation n1; };
    struct Nation2 { Nation n2; };

    bool orderRange(Orders const& orders) 
    {
        return orders.o_orderdate >= 19950101 && orders.o_orderdate < 19961231;
    }

    bool partType(Part const& part)
    {
        return STREQ(part.p_type, "MEDIUM ANODIZED NICKEL");
    }

    bool regionName(Region const& region)
    {
        return STREQ(region.r_name, "ASIA");
    }

    void lineitemSupplierKey(int& key, Join<Join<LineitemProjection,OrdersProjection>,PartProjection> const& r)
    {
        key = r.l_suppkey;
    }

    void lineitemPartKey(int& key, Join<LineitemProjection,OrdersProjection> const& r)
    {
        key = r.l_partkey;
    }

    void orderCustomerKey(int& key, Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,SupplierProjection> const& r)
    {
        key = r.o_custkey;
    }

    void supplierNationKey(int& key, Join<Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,SupplierProjection>,CustomerProjection> const& r)
    {
        key = r.s_nationkey;
    }
    
    void customerNationKey(int& key, Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,SupplierProjection>,CustomerProjection>,Nation1> const& r)
    {
        key = r.c_nationkey;
    }

    void nation1Key(int& key, Nation1 const& nation)
    {
        key = nation.n1.n_nationkey;
    }

    void nation2Key(int& key, Nation2 const& nation)
    {
        key = nation.n2.n_nationkey;
    }

    void nationRegionKey(int& key, Join<Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,SupplierProjection>,CustomerProjection>,Nation1>,Nation2> const& r)
    {
        key = r.n1.n_regionkey;
    }

    struct Volume
    {
        double total;
        double nation;
        
        bool operator==(Volume const& other) { 
            return total == other.total && nation == other.nation;
        }
    };

    void map(Pair<int,Volume>& pair, Join<Join<Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,SupplierProjection>,CustomerProjection>,Nation1>,Nation2>,Region> const& r)
    {
        double volume = r.l_extendedprice * (1-r.l_discount);
        pair.value.nation = STREQ(r.n2.n_name, "INDONESIA") ? volume : 0;
        pair.value.total = volume;
        pair.key = r.o_orderdate/10000;
    }

    void reduce(Volume& dst, Volume const& src)
    {
        dst.total += src.total;
        dst.nation += src.nation;
    }

    struct Share 
    {
        int o_year;
        double mkt_share;

        friend void print(Share const& s, FILE* out) { 
            fprintf(out, "%d, %f", s.o_year, s.mkt_share);
        }
    };

    void mkt(Share& out, Pair<int,Volume> const& in)
    {
        out.o_year = in.key;
        out.mkt_share = in.value.nation/in.value.total;
    }

    int byYear(Share const* a, Share const* b)
    {
        return a->o_year - b->o_year;
    }
    
    RDD<Share>* query() 
    { 
        return
            TABLE(Lineitem)->
            project<LineitemProjection,projectLineitem>()->            
            join<OrdersProjection,long,lineitemOrderKey,orderKey>(TABLE(Orders)->
                                                                  filter<orderRange>()->
                                                                  project<OrdersProjection,projectOrders>(),
                                                                  SCALE(1500000))->
            join<PartProjection,int,lineitemPartKey,partKey>(TABLE(Part)->
                                                             filter<partType>()->
                                                             project<PartProjection,projectPart>(),
                                                             SCALE(200000))->
            join<SupplierProjection,int,lineitemSupplierKey,supplierKey>(TABLE(Supplier)->
                                                                         project<SupplierProjection,projectSupplier>(),
                                                                         SCALE(10000))->
            join<CustomerProjection,int,orderCustomerKey,customerKey>(TABLE(Customer)->
                                                                      project<CustomerProjection,projectCustomer>(),
                                                                      SCALE(150000))-> 
            join<Nation1,int,supplierNationKey,nation1Key>((RDD<Nation1>*)TABLE(Nation),25)->
            join<Nation2,int,customerNationKey,nation2Key>((RDD<Nation2>*)TABLE(Nation),25)->
            join<Region,int,nationRegionKey, regionKey>(TABLE(Region)->filter<regionName>(), 5)->
            mapReduce<int,Volume,map,reduce>(100)->
            project<Share, mkt>()->
            sort<byYear>(100);
    }    
}
namespace Q9
{
    struct LineitemProjection 
    {
        long   l_orderkey;
        int    l_suppkey;
        int    l_partkey;
        double l_extendedprice;
        double l_discount;
        double l_quantity;
    };

    void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey;
        out.l_suppkey = in.l_suppkey;
        out.l_partkey = in.l_partkey;
        out.l_extendedprice = in.l_extendedprice;
        out.l_discount = in.l_discount;
        out.l_quantity = in.l_quantity;
    }

    void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        int o_orderkey;
        int o_orderdate;
    };

    void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey;
        out.o_orderdate = in.o_orderdate;
    }            

    void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }
    
    struct SupplierProjection
    {
        int s_suppkey;
        int s_nationkey;
    };

    void projectSupplier(SupplierProjection& out, Supplier const& in)
    {
        out.s_suppkey = in.s_suppkey;
        out.s_nationkey = in.s_nationkey;
    }            

    void supplierKey(int& key, SupplierProjection const& in)
    {
        key = in.s_suppkey;
    }

    struct PartProjection
    {
        int p_partkey;
        name_t p_name;
     };

    void projectPart(PartProjection& out, Part const& in)
    {
        out.p_partkey = in.p_partkey;
        STRCPY(out.p_name, in.p_name);
    }            

    void partKey(int& key, PartProjection const& in)
    {
        key = in.p_partkey;
    }

    struct PartsuppProjection
    {
        int ps_partkey;
        int ps_suppkey;
        double ps_supplycost;
     };

    void projectPartsupp(PartsuppProjection& out, Partsupp const& in)
    {
        out.ps_partkey = in.ps_partkey;
        out.ps_suppkey = in.ps_suppkey;
        out.ps_supplycost = in.ps_supplycost;
    }            

    void partsuppKey(PartsuppKey& key, PartsuppProjection const& in)
    {
        key.ps_partkey = in.ps_partkey;
        key.ps_suppkey = in.ps_suppkey;
    }

    bool partName(Part const& part)
    {
        return strstr(part.p_name, "ghost") != NULL;
    }

    void lineitemSupplierKey(int& key, Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,PartsuppProjection> const& r)
    {
        key = r.l_suppkey;
    }

    void lineitemPartKey(int& key, Join<LineitemProjection,OrdersProjection> const& r)
    {
        key = r.l_partkey;
    }


    void supplierNationKey(int& key, Join<Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,PartsuppProjection>,SupplierProjection> const& r)
    {
        key = r.s_nationkey;
    }
    
    void lineitemPartsuppKey(PartsuppKey& ps, Join<Join<LineitemProjection,OrdersProjection>,PartProjection> const& r)
    {
        ps.ps_partkey = r.l_partkey;
        ps.ps_suppkey = r.l_suppkey;
    }
    
    struct Profit
    {
        name_t nation;
        int    o_year;
        
        bool operator==(Profit const& other) const
        { 
            return STREQ(nation, other.nation) && o_year == other.o_year;
        }

        friend size_t hashCode(Profit const& p) { 
            return ::hashCode(p.nation) + ::hashCode(p.o_year);
        }

        friend void print(Profit const& p, FILE* out) {
            fprintf(out, "%s, %d", p.nation, p.o_year);
        } 
    };

    #define ProfitFields(FIELD) \
        FIELD(nation)           \
        FIELD(o_year)

    PACK(Profit)
    UNPACK(Profit)

    void map(Pair<Profit,double>& pair, Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,PartsuppProjection>,SupplierProjection>,Nation> const& r)
    {
        pair.value = r.l_extendedprice * (1-r.l_discount)-r.ps_supplycost * r.l_quantity;
        STRCPY(pair.key.nation, r.n_name);
        pair.key.o_year = r.o_orderdate/10000;
    }
    
    int byNationYear(Pair<Profit,double> const* a, Pair<Profit,double> const* b)
    {
        int diff = STRCMP(a->key.nation, b->key.nation);
        return (diff != 0) ? diff : b->key.o_year - a->key.o_year;
    }
        
    RDD< Pair<Profit,double> >* query() 
    { 
        return
            TABLE(Lineitem)->
            project<LineitemProjection,projectLineitem>()->            
            join<OrdersProjection,long,lineitemOrderKey,orderKey>(TABLE(Orders)->
                                                                  project<OrdersProjection,projectOrders>(),
                                                                  SCALE(1500000))->
            join<PartProjection,int,lineitemPartKey,partKey>(TABLE(Part)->
                                                             filter<partName>()->
                                                             project<PartProjection,projectPart>(),
                                                             SCALE(200000))->
            join<PartsuppProjection,PartsuppKey,lineitemPartsuppKey,partsuppKey>(TABLE(Partsupp)->
                                                                                 project<PartsuppProjection,projectPartsupp>(),
                                                                                 SCALE(800000))->
            join<SupplierProjection,int,lineitemSupplierKey,supplierKey>(TABLE(Supplier)->
                                                                         project<SupplierProjection,projectSupplier>(),
                                                                         SCALE(10000))->
            join<Nation,int,supplierNationKey,nationKey>(TABLE(Nation), 25)->
            mapReduce<Profit,double,map,sum>(25*100)->
            sort<byNationYear>(100);
    }    
}
namespace Q10
{
    struct LineitemProjection 
    {
        long   l_orderkey;
        double l_extendedprice;
        double l_discount;
    };

    void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey;
        out.l_extendedprice = in.l_extendedprice;
        out.l_discount = in.l_discount;
    }

    void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        int o_orderkey;
        int o_custkey;
        date_t o_orderdate;
    };

    void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey;
        out.o_custkey = in.o_custkey;
        out.o_orderdate = in.o_orderdate;
    }            

    void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }
    
    bool orderRange(Orders const& orders) 
    {
        return orders.o_orderdate >= 19941101 && orders.o_orderdate < 19950201;
    }
    
    bool lineitemFilter(Lineitem const& l)
    {
        return l.l_returnflag == 'R';
    }

    void orderCustomerKey(int& key, Join<LineitemProjection,OrdersProjection> const& r)
    {
        key = r.o_custkey;
    }

    void customerNationKey(int& key, Join<Join<LineitemProjection,OrdersProjection>,Customer> const& r)
    {
        key = r.c_nationkey;
    }
    
    struct GroupBy
    {
        int c_custkey;
        name_t c_name;
        double c_acctball;
        name_t n_name;
        char c_address[40];
        char c_phone[15];
        char c_comment[117];
        
        bool operator == (GroupBy const& other) const
        {
            return c_custkey == other.c_custkey
                && STREQ(c_name, other.c_name)
                && c_acctball == other.c_acctball
                && STREQ(n_name, other.n_name)
                && STREQ(c_address, other.c_address)
                && STREQ(c_phone,  other.c_phone)
                && STREQ(c_comment, other.c_comment);
        }

        friend size_t hashCode(GroupBy const& g)
        {
            return g.c_custkey + ::hashCode(g.c_name) + ::hashCode(g.n_name) + ::hashCode(g.c_address) + ::hashCode(g.c_phone) + ::hashCode(g.c_comment) + ::hashCode(g.c_acctball);
        }

        friend void print(GroupBy const& g, FILE* out) {
            fprintf(out, "%d, %s, %f, %s, %s, %s, %s", g.c_custkey, g.c_name, g.c_acctball, g.n_name, g.c_address, g.c_phone, g.c_comment);
        } 
    };

    #define GroupByFields(FIELD) \
        FIELD(c_custkey) \
        FIELD(c_name) \
        FIELD(c_acctball) \
        FIELD(n_name) \
        FIELD(c_address) \
        FIELD(c_phone) \
        FIELD(c_comment)

    PACK(GroupBy)
    UNPACK(GroupBy)
    
    void map(Pair<GroupBy,double>& pair, Join<Join<Join<LineitemProjection,OrdersProjection>,Customer>,Nation> const& r)
    {
        pair.key.c_custkey = r.c_custkey;
        STRCPY(pair.key.c_name, r.c_name);
        STRCPY(pair.key.n_name, r.n_name);
        pair.key.c_acctball = r.c_acctball;
        STRCPY(pair.key.c_address, r.c_address);
        STRCPY(pair.key.c_phone, r.c_phone);
        STRCPY(pair.key.c_comment, r.c_comment);
        pair.value = r.l_extendedprice * (1 - r.l_discount);
    }
    
    int byRevenue(Pair<GroupBy,double> const* a, Pair<GroupBy,double> const* b)
    {
        return a->value < b->value ? 1 : a->value == b->value ? 0 : -1;
    }
        
    RDD< Pair<GroupBy,double> >* query() 
    { 
        return
            TABLE(Lineitem)->
            filter<lineitemFilter>()->
            project<LineitemProjection,projectLineitem>()->            
            join<OrdersProjection,long,lineitemOrderKey,orderKey>(TABLE(Orders)->
                                                                  filter<orderRange>()->
                                                                  project<OrdersProjection,projectOrders>(),
                                                                  SCALE(1500000))->
            join<Customer,int,orderCustomerKey,customerKey>(TABLE(Customer),
                                                            SCALE(150000))-> 
            join<Nation,int,customerNationKey,nationKey>(TABLE(Nation), 25)->
            mapReduce<GroupBy,double,map,sum>(1000)->
            top<byRevenue>(20);
    }    
}
namespace Q12
{
    struct LineitemProjection 
    {
        long   l_orderkey;
        date_t l_shipdate;
        date_t l_commitdate;
        date_t l_receiptdate;
        shipmode_t l_shipmode;
    };

    void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey;
        out.l_shipdate = in.l_shipdate;
        out.l_commitdate = in.l_commitdate;
        out.l_receiptdate = in.l_receiptdate;
        STRCPY(out.l_shipmode, in.l_shipmode);
    }

    void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        int o_orderkey;
        priority_t o_orderpriority;
    };

    void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey;
        STRCPY(out.o_orderpriority, in.o_orderpriority);
    }            

    void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }
    
    bool lineitemFilter(Lineitem const& l)
    {
        return l.l_commitdate < l.l_receiptdate
            && l.l_shipdate < l.l_commitdate
            && l.l_receiptdate >= 19940101
            && l.l_receiptdate < 19950101
            && (STREQ(l.l_shipmode, "MAIL") || STREQ(l.l_shipmode, "SHIP"));
    }

    struct LineCount
    {
        int high;
        int low;

        friend void print(LineCount const& l, FILE* out) {
            fprintf(out, "%d, %d", l.high, l.low);
        }
    };

    void map(Pair<Key<shipmode_t>,LineCount>& pair, Join<LineitemProjection,OrdersProjection> const& r)
    {
        STRCPY(pair.key.val, r.l_shipmode);
        pair.value.high = STREQ(r.o_orderpriority, "1-URGENT") || STREQ(r.o_orderpriority, "2-HIGH");
        pair.value.low = !pair.value.high;
    }
    
    void reduce(LineCount& dst, LineCount const& src)      
    {
        dst.high += src.high;
        dst.low += src.low;
    }

    int byShipmode(Pair<Key<shipmode_t>,LineCount> const* a, Pair<Key<shipmode_t>,LineCount> const* b)
    {
        return STRCMP(a->key.val, b->key.val);
    }
        
    RDD< Pair<Key<shipmode_t>,LineCount> >* query() 
    { 
        return
            TABLE(Lineitem)->
            filter<lineitemFilter>()->
            project<LineitemProjection,projectLineitem>()->                        
            join<OrdersProjection,long,lineitemOrderKey,orderKey>(TABLE(Orders)->
                                                                  project<OrdersProjection,projectOrders>(), 
                                                                  SCALE(1500000))->
            mapReduce<Key<shipmode_t>,LineCount,map,reduce>(100)->
            sort<byShipmode>(100);
    }    
}
namespace Q13
{
    struct OrdersProjection
    {
        int o_custkey;
    };
    
    void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_custkey = in.o_custkey;
    }

    void orderCustomerKey(int& key, OrdersProjection const& r)
    {
        key = r.o_custkey;
    }
    
    struct CustomerProjection
    {
        int c_custkey;
    };
    
    void projectCustomer(CustomerProjection& out, Customer const& in)
    {
        out.c_custkey = in.c_custkey;
    }
    
    void customerKey(int& key, CustomerProjection const& customer)
    {
        key = customer.c_custkey;
    }

    bool orderFilter(Orders const& o)
    {
        char const* occ = strstr(o.o_comment, "unusual");
        return occ == NULL || strstr(occ + 7, "packages") == NULL;
    }

    void map1(Pair<int,int>& pair, Join<OrdersProjection,CustomerProjection> const& r)
    {
        pair.key = r.c_custkey;
        pair.value = 1;
    }
    
    void map2(Pair<int,int>& out, Pair<int,int> const& in)
    {
        out.key = in.value;
        out.value = 1;
    }
    
    int byCustDistCount(Pair<int,int> const* a, Pair<int,int> const* b)
    {
        int diff = b->value - a->value;
        return (diff != 0) ? diff : b->key - a->key;
    }
        
    RDD< Pair<int,int> >* query() 
    { 
        return
            TABLE(Orders)->
            filter<orderFilter>()->
            project<OrdersProjection, projectOrders>()->
            join<CustomerProjection,int,orderCustomerKey,customerKey>(TABLE(Customer)->
                                                                      project<CustomerProjection,projectCustomer>(),
                                                                      SCALE(150000), OuterJoin)->
            mapReduce<int,int,map1,count>(1000000)->
            mapReduce<int,int,map2,count>(10000)->
            sort<byCustDistCount>(10000);
    }    
}
namespace Q14
{                
    struct LineitemProjection 
    {
        int    l_partkey;
        date_t l_shipdate;
        double l_extendedprice;
        double l_discount;
    };

    void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_partkey = in.l_partkey;
        out.l_shipdate = in.l_shipdate;
        out.l_extendedprice = in.l_extendedprice;
        out.l_discount = in.l_discount;
    }

    void lineitemPartKey(int& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_partkey;
    }

    struct PartProjection
    {
        int p_partkey;
        char p_type[25];
    };

    void projectPart(PartProjection& out, Part const& in)
    {
        out.p_partkey = in.p_partkey;
        STRCPY(out.p_type, in.p_type);
    }            

    void partKey(int& key, PartProjection const& in)
    {
        key = in.p_partkey;
    }

    bool lineitemFilter(Lineitem const& l)
    {
        return l.l_shipdate >= 19940301 && l.l_shipdate < 19940401;
    }

    struct PromoRevenue
    {
        double promo;
        double revenue;

        PromoRevenue(double p = 0.0, double r = 0.0) : promo(p), revenue(r) {}
    };
      
    void promoRevenue(PromoRevenue& acc, Join<LineitemProjection,PartProjection> const& r) 
    { 
        acc.promo += strncmp(r.p_type, "PROMO", 5) == 0 ? r.l_extendedprice*(1-r.l_discount) : 0.0;
        acc.revenue += r.l_extendedprice * (1 - r.l_discount);
    }

    void combineRevenue(PromoRevenue& acc, PromoRevenue const& partial)
    {
        acc.promo += partial.promo;
        acc.revenue += partial.revenue;
    }
    
    void relation(double& result, PromoRevenue const& pr)
    {
        result = 100*pr.promo/pr.revenue;
    }

    RDD<double>* query() 
    { 
        return
            TABLE(Lineitem)->
            filter<lineitemFilter>()->
            project<LineitemProjection,projectLineitem>()->                        
            join<PartProjection,int,lineitemPartKey,partKey>(TABLE(Part)->
                                                             project<PartProjection,projectPart>(),
                                                             SCALE(200000))->
            reduce<PromoRevenue,promoRevenue,combineRevenue>(PromoRevenue(0,0))->
            project<double,relation>();
    }    
}
namespace Q19
{   
    struct LineitemProjection 
    {
        int    l_partkey;
        double l_extendedprice;
        double l_discount;
        double l_quantity;
        char   l_shipinstruct[25];
        shipmode_t l_shipmode;
    };

    void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_partkey = in.l_partkey;
        out.l_extendedprice = in.l_extendedprice;
        out.l_discount = in.l_discount;
        out.l_quantity = in.l_quantity;
        STRCPY(out.l_shipinstruct, in.l_shipinstruct);
        STRCPY(out.l_shipmode, in.l_shipmode);
    }

    void lineitemPartKey(int& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_partkey;
    }

    struct PartProjection
    {
        int p_partkey;
        int p_size; 
        char p_brand[10];
        char p_container[10];
    };

    void projectPart(PartProjection& out, Part const& in)
    {
        out.p_partkey = in.p_partkey;
        out.p_size = in.p_size;
        STRCPY(out.p_brand, in.p_brand);
        STRCPY(out.p_container, in.p_container);
    }            

    void partKey(int& key, PartProjection const& in)
    {
        key = in.p_partkey;
    }

    bool brandFilter(Join<LineitemProjection,PartProjection> const& r)
    {
        return 
            (STREQ(r.p_brand, "Brand#31")
             && (STREQ(r.p_container, "SM CASE") || STREQ(r.p_container, "SM BOX") || STREQ(r.p_container, "SM PACK") || STREQ(r.p_container, "SM PKG"))
             && r.l_quantity >= 26 && r.l_quantity <= 36
             && r.p_size >= 1 && r.p_size <= 5
             && (STREQ(r.l_shipmode, "AIR") || STREQ(r.l_shipmode, "AIR REG"))
             && STREQ(r.l_shipinstruct, "DELIVER IN PERSON"))
            || 
            (STREQ(r.p_brand, "Brand#43")
             && (STREQ(r.p_container, "MED BAG") || STREQ(r.p_container, "MED BOX") || STREQ(r.p_container, "MED PKG") || STREQ(r.p_container, "MED PACK"))
             && r.l_quantity >= 15 && r.l_quantity <= 25
             && r.p_size >= 1 && r.p_size <= 10
             && (STREQ(r.l_shipmode, "AIR") || STREQ(r.l_shipmode, "AIR REG"))
             && STREQ(r.l_shipinstruct, "DELIVER IN PERSON"))
            ||
            (STREQ(r.p_brand, "Brand#43")
             && (STREQ(r.p_container, "LG CASE") || STREQ(r.p_container, "LG BOX") || STREQ(r.p_container, "LG PACK") || STREQ(r.p_container, "LG PKG"))
             && r.l_quantity >= 15 && r.l_quantity <= 14
             && r.p_size >= 1 && r.p_size <= 15
             && (STREQ(r.l_shipmode, "AIR") || STREQ(r.l_shipmode, "AIR REG"))
             && STREQ(r.l_shipinstruct, "DELIVER IN PERSON"));
    }

    void revenue(double& acc, Join<LineitemProjection,PartProjection> const& r)
    {
        acc += r.l_extendedprice * (1 - r.l_discount);
    }

    RDD<double>* query() 
    { 
        return
            TABLE(Lineitem)->
            project<LineitemProjection,projectLineitem>()->                        
            join<PartProjection,int,lineitemPartKey,partKey>(TABLE(Part)->
                                                             project<PartProjection,projectPart>(), SCALE(200000))->
            filter<brandFilter>()->
            reduce<double,revenue,sum>(0);
    }    
}
    


template<class T>
void execute(char const* name, RDD<T>* (*query)()) 
{
    time_t start = time(NULL);
    RDD<T>* result = query();
    result->output(stdout);
    delete result;

    if (Cluster::instance->nodeId == 0) {
        FILE* results = fopen("results.csv", "a");
        fprintf(results, "%s,%d\n", name, (int)(time(NULL) - start));
        fclose(results);
    }
       
    printf("Elapsed time for %s: %d seconds\n", name, (int)(time(NULL) - start));
    fflush(stdout);
}

class TPCHJob : public Job
{
    Cluster cluster;
    bool useCache;
    bool doSharding;
    
  public:
    TPCHJob(size_t nodeId, size_t nHosts, char** hosts = NULL, size_t nQueues = 64, size_t bufferSize = 4*64*1024, size_t recvQueueSize = 4*64*1024*1024,  size_t sendQueueSize = 4*4*1024*1024, size_t syncInterval = 64*1024*1024, size_t broadcastJoinThreshold = 10000, size_t inmemJoinThreshold = 10000000, char const* tmp = "/tmp", bool sharedNothing = false, size_t split = 1, bool _useCache = false, bool _doSharding = false)
    : cluster(nodeId, nHosts, hosts, nQueues, bufferSize, recvQueueSize, sendQueueSize, syncInterval, broadcastJoinThreshold, inmemJoinThreshold, tmp, sharedNothing, split),
      useCache(_useCache),
      doSharding(_doSharding)
    {}
    
  public:
    void run() {
        Cluster::instance.set(&cluster);
        printf("Node %d started...\n", (int)cluster.nodeId);

        time_t start = time(NULL);
        if (useCache) { 
            cluster.userData = new CachedData(doSharding);
            printf("Elapsed time for loading all data in memory: %d seconds\n", (int)(time(NULL) - start));
            cluster.barrier(); 
        }
    
        execute("Q1",  Q1::query);
        execute("Q3",  Q3::query);
        execute("Q4",  Q4::query);
        execute("Q5",  Q5::query);
        execute("Q6",  Q6::query);
        execute("Q7",  Q7::query);
        execute("Q8",  Q8::query);
        execute("Q9",  Q9::query);
        execute("Q10", Q10::query);
        execute("Q12", Q12::query);
        execute("Q13", Q13::query);
        execute("Q14", Q14::query);
        execute("Q19", Q19::query);
        
        delete (CachedData*)cluster.userData;

        printf("Node %d finished.\n", (int)cluster.nodeId);
    }
};

int main(int argc, char* argv[])
{
    int i;
    bool useCache = false;
    bool doSharding = false;
    size_t nQueues = 64;
    size_t bufferSize = 4*64*1024;
    size_t recvQueueSize = 4*64*1024*1024;
    size_t sendQueueSize = 4*4*1024*1024;
    size_t syncInterval = 64*1024*1024;
    size_t broadcastJoinThreshold = 10000;
    size_t inmemJoinThreshold = 10000000;
    size_t split = 1;
    bool   sharedNothing = false;
    char const* option;
    char const* tmp = "/tmp";
    
    fclose(fopen("tpch.start", "w")); // needed to innitialize stdio in single threaded environment

    for (i = 1; i < argc; i++) { 
        if (*argv[i] == '-') { 
            option = argv[i]+1;
            if (strcmp(option, "cache") == 0) { 
                useCache = true;
            } else if (strcmp(option, "sharding") == 0) { 
                doSharding = true;
            } else if (strcmp(option, "dir") == 0) { 
                dataDir = argv[++i];
            } else if (strcmp(option, "format") == 0) { 
                dataFormat = argv[++i];
            } else if (strcmp(option, "queues") == 0) { 
                nQueues = atol(argv[++i]);
            } else if (strcmp(option, "buffer") == 0) { 
                bufferSize = atol(argv[++i]);
            } else if (strcmp(option, "recv-queue") == 0) { 
                recvQueueSize = atol(argv[++i]);
            } else if (strcmp(option, "send-queue") == 0) { 
                sendQueueSize = atol(argv[++i]);
            } else if (strcmp(option, "sync") == 0) { 
                syncInterval = atol(argv[++i]);
            } else if (strcmp(option, "broadcast-threshold") == 0) { 
                broadcastJoinThreshold = atol(argv[++i]);
            } else if (strcmp(option, "inmem-threshold") == 0) { 
                inmemJoinThreshold = atol(argv[++i]);
            } else if (strcmp(option, "tmp") == 0) { 
                tmp = argv[++i];
            } else if (strcmp(option, "shared-nothing") == 0) { 
                sharedNothing = atoi(argv[++i]) != 0;
             } else if (strcmp(option, "split") == 0) { 
                split = atoi(argv[++i]);
            } else { 
                fprintf(stderr, "Unrecognized option %s\n", option);
              Usage:
                fputs("Usage: kraps [Options] NODE_ID N_NODES address1:port1  address2:port2...\n"
                      "Options:\n"
                      "-dir\tdata directory (.)\n"
                      "-format\tdata format: parquet, plain-file,... ()\n"
                      "-cache\tCache all data in memory\n"
                      "-tmp DIR\ttemporary files location (/tmp)\n"
                      "-shared-nothing 0/1\tdata is located at executor nodes (1)\n"
                      "-queues N\tnumber of queues (64)\n"
                      "-buffer SIZE\tbuffer size (256Kb)\n"
                      "-recv-queue SIZE\treceive queue size (256Mb)\n"
                      "-send-queue SIZE\tsend  queue size (16Mb)\n"
                      "-sync SIZE\tsycnhronization interval (64Mb)\n"
                      "-broadcast-threshold SIZE\tbroadcast join threshold (10000)\n"
                      "-inmem-threshold SIZE\tinmemory join threshold (10000000)\n"
                      "-split N\tsplit file into N parts (1)\n", stderr);
                return 1;                
            }
        } else { 
            break;
        }
    }
    if (i+2 > argc) {  
        goto Usage;
    }
    int nodeId = atoi(argv[i++]);
    int nNodes = atoi(argv[i++]);
    if (nodeId < 0 || nodeId >= nNodes)  { 
        fprintf(stderr, "Invalid node ID %d\n", nodeId);
        return 1;
    }
    if (argc == i) {
        // multhithreaded cluster
        Cluster::nodes = new Cluster*[nNodes];
        Thread** clusterThreads = new Thread*[nNodes];
        for (nodeId = 0; nodeId < nNodes; nodeId++) {
            clusterThreads[nodeId] = new Thread(new TPCHJob(nodeId, nNodes, NULL, nQueues, bufferSize, recvQueueSize, sendQueueSize, syncInterval, broadcastJoinThreshold, inmemJoinThreshold, tmp, sharedNothing, split, useCache, doSharding), nodeId);
        }
        for (nodeId = 0; nodeId < nNodes; nodeId++) {
            delete clusterThreads[nodeId];
        }
        delete[] clusterThreads;
        delete[] Cluster::nodes;
    } else if (argc == i + nNodes) {
        TPCHJob test(nodeId, nNodes, &argv[i], nQueues, bufferSize, recvQueueSize, sendQueueSize, syncInterval, broadcastJoinThreshold, inmemJoinThreshold, tmp, sharedNothing, split, useCache, doSharding);
        test.run();
    } else {      
        fprintf(stderr, "At least one node has to be specified\n");
        goto Usage;
    }
    return 0;
}
