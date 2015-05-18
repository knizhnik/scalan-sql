#include <time.h>
#include "rdd.h"
#include "tpch.h"

const size_t SF = 100; // scale factor

#define STRCMP(s,p) strncmp(s, p, sizeof(s))
#define STREQ(s,p)  (STRCMP(s, p) == 0)
#define STRCPY(d,s) strncpy(d,s,sizeof(d))
#define SCALE(x)    ((x + Cluster::instance->nNodes - 1)*SF/(Cluster::instance->nNodes))


class CachedData
{
  public:
    CachedRDD<Lineitem> lineitem;
    CachedRDD<Orders> orders;
    CachedRDD<Supplier> supplier;
    CachedRDD<Customer> customer;
    CachedRDD<Part> part;
    CachedRDD<Partsupp> partsupp;
    CachedRDD<Nation> nation;
    CachedRDD<Region> region;

    CachedData() : 
    lineitem(FileManager::load<Lineitem>("lineitem"), SCALE(6000000)),
    orders(FileManager::load<Orders>("orders"),       SCALE(1500000)),
    supplier(FileManager::load<Supplier>("supplier"), SCALE(10000)),
    customer(FileManager::load<Customer>("customer"), SCALE(150000)),
    part(FileManager::load<Part>("part"),             SCALE(200000)),
    partsupp(FileManager::load<Partsupp>("partsupp"), SCALE(800000)),
    nation(FileManager::load<Nation>("nation"),       25),
    region(FileManager::load<Region>("region"),       5) {}

};

CachedData* cache;

void sum(double& dst, double const& src)
{
    dst += src;
}

void count(int& dst, int const&)
{
    dst += 1;
}

void orderKey(long& key, Orders const& order)
{
    key = order.o_orderkey;
}

void lineitemOrderKey(long& key, Lineitem const& lineitem)
{
    key = lineitem.l_orderkey;
}

void lineitemPartKey(int& key, Lineitem const&  lineitem)
{
    key = lineitem.l_partkey;
}

 void supplierKey(int& key, Supplier const& supplier)
{
    key = supplier.s_suppkey;
}

void customerKey(int& key, Customer const& customer)
{
    key = customer.c_custkey;
}

void nationKey(int& key, Nation const& nation)
{
    key = nation.n_nationkey;
}

void regionKey(int& key, Region const& region)
{
    key = region.r_regionkey;
}

void partKey(int& key, Part const& part)
{
    key = part.p_partkey;
}
    
struct PartSuppKey
{
    int partkey;
    int suppkey;
    
    bool operator==(PartSuppKey const& other) const
    {
        return partkey == other.partkey
            && suppkey == other.suppkey;
    }
    
    friend size_t hashCode(PartSuppKey const& ps)
    {
        return ps.partkey ^ ps.suppkey;
    }
};

void partsuppKey(PartSuppKey& key, Partsupp const& ps)
{
    key.partkey = ps.ps_partkey;
    key.suppkey = ps.ps_suppkey;
}

    
namespace Q1
{
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
    
    struct Projection
    {
        char   l_returnflag;
        char   l_linestatus;
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        size_t count_order;

        friend void print(Projection const& p, FILE* out) { 
            fprintf(out, "%c, %c, %f, %f, %f, %f, %f, %f, %f, %lu", 
                    p.l_returnflag, p.l_linestatus, p.sum_qty, p.sum_base_price, p.sum_disc_price, p.sum_charge, p.avg_qty, p.avg_price, p.avg_disc, p.count_order);
        }
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
            FileManager::load<Lineitem>("lineitem")->
            filter<predicate>()->
            mapReduce<GroupBy,Aggregate,map,reduce>(100)->
            project<Projection, projection>()->
            sort<compare>(100);
    }

    RDD<Projection>* cachedQuery() 
    { 
        return
            cache->lineitem.get()->
            filter<predicate>()->
            mapReduce<GroupBy,Aggregate,map,reduce>(10000)->
            project<Projection, projection>()->
            sort<compare>(100);
    }
}

namespace Q3
{
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
    
    void orderCustomerKey(int& key, Join<Lineitem,Orders> const& r)
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

    void map(Pair<GroupBy,double>& pair, Join<Join<Lineitem,Orders>,Customer> const& r)
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

    RDD<Revenue>* cachedQuery() 
    { 
        return
            cache->lineitem.get()->
            filter<lineitemFilter>()->
            join<Orders, long, lineitemOrderKey, orderKey>(cache->orders.get()->filter<orderFilter>(), SCALE(1500000))->
            join<Customer, int, orderCustomerKey, customerKey>(cache->customer.get()->filter<customerFilter>(), SCALE(150000))->
            mapReduce<GroupBy, double, map, sum>(1000)->
            project<Revenue, revenue>()->
            sort<byRevenueAndOrderDate>(1000);
    }    
}
namespace Q4
{
    bool lineitemFilter(Lineitem const& l)
    {
        return l.l_commitdate < l.l_receiptdate;
    }
    
    bool orderFilter(Orders const& o)
    {
        return o.o_orderdate >= 19930801 && o.o_orderdate < 19931101;
    }
    
    void map(Pair<Key<priority_t>,int>& pair, Join<Lineitem,Orders> const& r)
    {
        STRCPY(pair.key.val, r.o_orderpriority);
        pair.value = 1;
    }

    int byPriority(Pair<Key<priority_t>,int> const* a, Pair<Key<priority_t>,int> const* b)  { 
        return STRCMP(a->key.val, b->key.val);
    }

    RDD< Pair<Key<priority_t>,int> >* cachedQuery() 
    { 
        return
            cache->lineitem.get()->
            filter<lineitemFilter>()->
            join<Orders, long, lineitemOrderKey, orderKey>(cache->orders.get()->filter<orderFilter>(), SCALE(1500000))->
            mapReduce<Key<priority_t>, int, map, count>(25)->
            sort<byPriority>(25);
    }    
}   
namespace Q5
{
    bool orderRange(Orders const& order) 
    {
        return order.o_orderdate >= 19960101 && order.o_orderdate < 19970101;
    }

    bool sameNation(Join<Join<Join<Lineitem,Orders>,Supplier>,Customer> const& r)
    {
        return r.c_nationkey == r.s_nationkey;
    }

    void customerNationKey(int& key, Join<Join<Join<Lineitem,Orders>,Supplier>,Customer> const& r)
    {
        key = r.c_nationkey;
    }
    
    void lineitemSupplierKey(int& key, Join<Lineitem,Orders> const& r)
    {
        key = r.l_suppkey;
    }
    
    void orderCustomerKey(int& key, Join<Join<Lineitem,Orders>,Supplier> const& r)
    {
        key = r.o_custkey;
    }

    void nationRegionKey(int& key, Join<Join<Join<Join<Lineitem,Orders>,Supplier>,Customer>,Nation> const& r)
    {
        key = r.n_regionkey;
    }
    
    bool asiaRegion(Join<Join<Join<Join<Join<Lineitem,Orders>,Supplier>,Customer>,Nation>,Region> const& r) 
    { 
        return STREQ(r.r_name, "ASIA");

    }

    void map(Pair<Key<name_t>,double>& pair, Join<Join<Join<Join<Join<Lineitem,Orders>,Supplier>,Customer>,Nation>,Region> const& r)
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
            FileManager::load<Lineitem>("lineitem")->            
            join<Orders, long, lineitemOrderKey, orderKey>(FileManager::load<Orders>("orders")->filter<orderRange>(), SCALE(1500000))->
            join<Supplier, int, lineitemSupplierKey, supplierKey>(FileManager::load<Supplier>("supplier"), SCALE(10000))->
            join<Customer, int, orderCustomerKey, customerKey>(FileManager::load<Customer>("customer"), SCALE(150000))->
            filter<sameNation>()->
            join<Nation, int, customerNationKey, nationKey>(FileManager::load<Nation>("nation"), 25)->
            join<Region, int, nationRegionKey, regionKey>(FileManager::load<Region>("region"), 5)->
            filter<asiaRegion>()->
            mapReduce<Key<name_t>, double, map, sum>(25)->
            project<Revenue, revenue>()->
            sort<byRevenue>(25);
    }    
    RDD<Revenue>* cachedQuery() 
    { 
        return
            cache->lineitem.get()->            
            join<Orders, long, lineitemOrderKey, orderKey>(cache->orders.get()->filter<orderRange>(), SCALE(1500000))->
            join<Supplier, int, lineitemSupplierKey, supplierKey>(cache->supplier.get(), SCALE(10000))->
            join<Customer, int, orderCustomerKey, customerKey>(cache->customer.get(), SCALE(150000))->
            filter<sameNation>()->
            join<Nation, int, customerNationKey, nationKey>(cache->nation.get(), 25)->
            join<Region, int, nationRegionKey, regionKey>(cache->region.get(), 5)->
            filter<asiaRegion>()->
            mapReduce<Key<name_t>, double, map, sum>(25)->
            project<Revenue, revenue>()->
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
    
    RDD<double>* cachedQuery() 
    { 
        return
            cache->lineitem.get()->
            filter<lineitemFilter>()->
            reduce<double,revenue>(0);
    }
}
namespace Q7
{
    struct Nation1 { Nation n1; };
    struct Nation2 { Nation n2; };

    void lineitemSupplierKey(int& key, Join<Lineitem,Orders> const& r)
    {
        key = r.l_suppkey;
    }

    void orderCustomerKey(int& key, Join<Join<Lineitem,Orders>,Supplier> const& r)
    {
        key = r.o_custkey;
    }

    void supplierNationKey(int& key, Join<Join<Join<Lineitem,Orders>,Supplier>,Customer> const& r)
    {
        key = r.s_nationkey;
    }
    
    void customerNationKey(int& key, Join<Join<Join<Join<Lineitem,Orders>,Supplier>,Customer>,Nation1> const& r)
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

    bool filterNation(Join<Join<Join<Join<Join<Lineitem,Orders>,Supplier>,Customer>,Nation1>,Nation2> const& r) 
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

    void map(Pair<Shipping,double>& pair, Join<Join<Join<Join<Join<Lineitem,Orders>,Supplier>,Customer>,Nation1>,Nation2> const& r)
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

    RDD< Pair<Shipping,double> > * cachedQuery() 
    { 
        return
            cache->lineitem.get()->filter<filterLineitem>()->           
            join<Orders, long, lineitemOrderKey, orderKey>(cache->orders.get(), SCALE(1500000))->
            join<Supplier, int, lineitemSupplierKey, supplierKey>(cache->supplier.get(), SCALE(10000))->
            join<Customer, int, orderCustomerKey, customerKey>(cache->customer.get(), SCALE(150000))-> 
            join<Nation1, int, supplierNationKey, nation1Key>((RDD<Nation1>*)cache->nation.get(), 25)->
            join<Nation2, int, customerNationKey, nation2Key>((RDD<Nation2>*)cache->nation.get(), 25)->
            filter<filterNation>()->
            mapReduce<Shipping,double,map,sum>(25*25*100)->
            sort<byShipping>(25*25*100);
    }    
}
namespace Q8
{
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

    void lineitemSupplierKey(int& key, Join<Join<Lineitem,Orders>,Part> const& r)
    {
        key = r.l_suppkey;
    }

    void lineitemPartKey(int& key, Join<Lineitem,Orders> const& r)
    {
        key = r.l_partkey;
    }

    void orderCustomerKey(int& key, Join<Join<Join<Lineitem,Orders>,Part>,Supplier> const& r)
    {
        key = r.o_custkey;
    }

    void supplierNationKey(int& key, Join<Join<Join<Join<Lineitem,Orders>,Part>,Supplier>,Customer> const& r)
    {
        key = r.s_nationkey;
    }
    
    void customerNationKey(int& key, Join<Join<Join<Join<Join<Lineitem,Orders>,Part>,Supplier>,Customer>,Nation1> const& r)
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

    void nationRegionKey(int& key, Join<Join<Join<Join<Join<Join<Lineitem,Orders>,Part>,Supplier>,Customer>,Nation1>,Nation2> const& r)
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

    void map(Pair<int,Volume>& pair, Join<Join<Join<Join<Join<Join<Join<Lineitem,Orders>,Part>,Supplier>,Customer>,Nation1>,Nation2>,Region> const& r)
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
    
    RDD<Share>* cachedQuery() 
    { 
        return
            cache->lineitem.get()->
            join<Orders, long, lineitemOrderKey, orderKey>(cache->orders.get()->filter<orderRange>(), SCALE(1500000))->
            join<Part, int, lineitemPartKey, partKey>(cache->part.get()->filter<partType>(), SCALE(200000))->
            join<Supplier, int, lineitemSupplierKey, supplierKey>(cache->supplier.get(), SCALE(10000))->
            join<Customer, int, orderCustomerKey, customerKey>(cache->customer.get(), SCALE(150000))-> 
            join<Nation1, int, supplierNationKey, nation1Key>((RDD<Nation1>*)cache->nation.get(), 25)->
            join<Nation2, int, customerNationKey, nation2Key>((RDD<Nation2>*)cache->nation.get(), 25)->
            join<Region, int, nationRegionKey, regionKey>(cache->region.get()->filter<regionName>(), 5)->
            mapReduce<int,Volume,map,reduce>(100)->
            project<Share, mkt>()->
            sort<byYear>(100);
    }    
}
namespace Q9
{
    bool partName(Part const& part)
    {
        return strstr(part.p_name, "ghost") != NULL;
    }

    void lineitemSupplierKey(int& key, Join<Join<Join<Lineitem,Orders>,Part>,Partsupp> const& r)
    {
        key = r.l_suppkey;
    }

    void lineitemPartKey(int& key, Join<Lineitem,Orders> const& r)
    {
        key = r.l_partkey;
    }


    void supplierNationKey(int& key, Join<Join<Join<Join<Lineitem,Orders>,Part>,Partsupp>,Supplier> const& r)
    {
        key = r.s_nationkey;
    }
    
    void lineitemPartSuppKey(PartSuppKey& ps, Join<Join<Lineitem,Orders>,Part> const& r)
    {
        ps.partkey = r.l_partkey;
        ps.suppkey = r.l_suppkey;
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

    void map(Pair<Profit,double>& pair, Join<Join<Join<Join<Join<Lineitem,Orders>,Part>,Partsupp>,Supplier>,Nation> const& r)
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
        
    RDD< Pair<Profit,double> >* cachedQuery() 
    { 
        return
            cache->lineitem.get()->
            join<Orders, long, lineitemOrderKey, orderKey>(cache->orders.get(), SCALE(1500000))->
            join<Part, int, lineitemPartKey, partKey>(cache->part.get()->filter<partName>(), SCALE(200000))->
            join<Partsupp, PartSuppKey, lineitemPartSuppKey, partsuppKey>(cache->partsupp.get(), SCALE(800000))->
            join<Supplier, int, lineitemSupplierKey, supplierKey>(cache->supplier.get(), SCALE(10000))->
            join<Nation, int, supplierNationKey, nationKey>(cache->nation.get(), 25)->
            mapReduce<Profit,double,map,sum>(25*100)->
            sort<byNationYear>(100);
    }    
}
namespace Q10
{
    bool orderRange(Orders const& orders) 
    {
        return orders.o_orderdate >= 19941101 && orders.o_orderdate < 19950201;
    }
    
    bool lineitemFilter(Lineitem const& l)
    {
        return l.l_returnflag == 'R';
    }

    void orderCustomerKey(int& key, Join<Lineitem,Orders> const& r)
    {
        key = r.o_custkey;
    }

    void customerNationKey(int& key, Join<Join<Lineitem,Orders>,Customer> const& r)
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

    void map(Pair<GroupBy,double>& pair, Join<Join<Join<Lineitem,Orders>,Customer>,Nation> const& r)
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
        
    RDD< Pair<GroupBy,double> >* cachedQuery() 
    { 
        return
            cache->lineitem.get()->filter<lineitemFilter>()->
            join<Orders, long, lineitemOrderKey, orderKey>(cache->orders.get()->filter<orderRange>(), SCALE(1500000))->
            join<Customer, int, orderCustomerKey, customerKey>(cache->customer.get(), SCALE(150000))-> 
            join<Nation, int, customerNationKey, nationKey>(cache->nation.get(), 25)->
            mapReduce<GroupBy,double,map,sum>(1000)->
            sort<byRevenue>(1000);
    }    
}
namespace Q12
{
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

    void map(Pair<Key<shipmode_t>,LineCount>& pair, Join<Lineitem,Orders> const& r)
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
        
    RDD< Pair<Key<shipmode_t>,LineCount> >* cachedQuery() 
    { 
        return
            cache->lineitem.get()->filter<lineitemFilter>()->
            join<Orders, long, lineitemOrderKey, orderKey>(cache->orders.get(), SCALE(1500000))->
            mapReduce<Key<shipmode_t>,LineCount,map,reduce>(100)->
            sort<byShipmode>(100);
    }    
}
namespace Q13
{
    bool orderFilter(Orders const& o)
    {
        char const* occ = strstr(o.o_comment, "unusual");
        return occ == NULL || strstr(occ + 7, "packages") == NULL;
    }

    void orderCustomerKey(int& key, Orders const& r)
    {
        key = r.o_custkey;
    }

    void map1(Pair<int,int>& pair, Join<Orders,Customer> const& r)
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
        
    RDD< Pair<int,int> >* cachedQuery() 
    { 
        return
            cache->orders.get()->filter<orderFilter>()->
            join<Customer, int, orderCustomerKey, customerKey>(cache->customer.get(), SCALE(150000), true)->
            mapReduce<int,int,map1,count>(10000)->
            mapReduce<int,int,map2,count>(10000)->
            sort<byCustDistCount>(10000);
    }    
}
namespace Q14
{                
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

      
    void promoRevenue(PromoRevenue& acc, Join<Lineitem,Part> const& r) 
    { 
        acc.promo += strncmp(r.p_type, "PROMO", 5) == 0 ? r.l_extendedprice*(1-r.l_discount) : 0.0;
        acc.revenue += r.l_extendedprice * (1 - r.l_discount);
    }

    void relation(double& result, PromoRevenue const& pr)
    {
        result = 100*pr.promo/pr.revenue;
    }

    RDD<double>* cachedQuery() 
    { 
        return
            cache->lineitem.get()->filter<lineitemFilter>()->
            join<Part, int, lineitemPartKey, partKey>(cache->part.get(), SCALE(200000))->
            reduce<PromoRevenue, promoRevenue>(PromoRevenue(0,0))->
            project<double, relation>();
    }    
}
namespace Q19
{   
    bool brandFilter(Join<Lineitem,Part> const& r)
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

    void revenue(double& acc, Join<Lineitem,Part> const& r)
    {
        acc += r.l_extendedprice * (1 - r.l_discount);
    }

    RDD<double>* cachedQuery() 
    { 
        return
            cache->lineitem.get()->
            join<Part, int, lineitemPartKey, partKey>(cache->part.get(), SCALE(200000))->
            filter<brandFilter>()->
            reduce<double, revenue>(0);
    }    
}
    


template<class T>
void execute(char const* name, RDD<T>* (*query)()) 
{
    time_t start = time(NULL);
    RDD<T>* result = query();
    result->output(stdout);
    delete result;
    printf("Elapsed time for %s: %d seconds\n", name, (int)(time(NULL) - start));
}

int main(int argc, char* argv[])
{
    if (argc < 4) {
        fprintf(stderr, "Usage: kraps NODE_ID N_NODES address1:port1 address2:port2...\n");
        return 1;
    }
    int nodeId = atoi(argv[1]);
    int nNodes = atoi(argv[2]);
    if (nodeId < 0 || nodeId >= nNodes)  { 
        fprintf(stderr, "Invalid node ID %d\n", nodeId);
        return 1;
    }
    if (argc != 3 + nNodes) { 
        fprintf(stderr, "At least one node has to be specified\nUsage: kraps NODE_ID N_NODES address1:port1  address2:port2...\n");
        return 1;
    }
    printf("Node %d started...\n", nodeId);
    Cluster cluster(nodeId, nNodes, &argv[3]);

    // execute("Q1", Q1::query);    
    // execute("Q5", Q5::query);
    
    time_t start = time(NULL);
    cache = new CachedData();
    printf("Elapsed time for loading all data in memory: %d seconds\n", (int)(time(NULL) - start));
    cluster.barrier(); 
    
    execute("Q1",  Q1::cachedQuery);
    execute("Q3",  Q3::cachedQuery);
    execute("Q4",  Q4::cachedQuery);
    execute("Q5",  Q5::cachedQuery);
    execute("Q6",  Q6::cachedQuery);
    execute("Q7",  Q7::cachedQuery);
    execute("Q8",  Q8::cachedQuery);
    execute("Q9",  Q9::cachedQuery);
    execute("Q10", Q10::cachedQuery);
    execute("Q12", Q12::cachedQuery);
    execute("Q13", Q13::cachedQuery);
    execute("Q14", Q14::cachedQuery);
    execute("Q19", Q19::cachedQuery);

    delete cache;
    
    printf("Node %d finished.\n", nodeId);
    return 0;
}
