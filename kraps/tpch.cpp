#include <time.h>
#include <sys/time.h>
#include <ctype.h>
#include "rdd.h"
#include "tpch.h"

const size_t SF = 100; // scale factor

#define SCALE(x)    ((x + Cluster::instance->nNodes - 1)*SF/(Cluster::instance->nNodes) + (x / 100)) // take in account data skews

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

#ifdef COLUMNAR_STORE

#define TABLE(x) &((ColumnarStore*)Cluster::instance->userData)->_##x

class ColumnarStore
{
  public:
    ColumnarRDD<HLineitem,Lineitem,VLineitem> _Lineitem;
    ColumnarRDD<HOrders,Orders,VOrders> _Orders;
    ColumnarRDD<HSupplier,Supplier,VSupplier> _Supplier;
    ColumnarRDD<HCustomer,Customer,VCustomer> _Customer;
    ColumnarRDD<HPart,Part,VPart> _Part;
    ColumnarRDD<HPartsupp,Partsupp,VPartsupp> _Partsupp;
    ColumnarRDD<HNation,Nation,VNation> _Nation;
    ColumnarRDD<HRegion,Region,VRegion> _Region;

    ColumnarStore() : 
    _Lineitem(new DirRDD<HLineitem>(filePath("Lineitem")), SCALE(6000000)),
    _Orders(new DirRDD<HOrders>(filePath("Orders")),       SCALE(1500000)),
    _Supplier(new DirRDD<HSupplier>(filePath("Supplier")), SCALE(10000)),
    _Customer(new DirRDD<HCustomer>(filePath("Customer")), SCALE(150000)),
    _Part(new DirRDD<HPart>(filePath("Part")),             SCALE(200000)),
    _Partsupp(new DirRDD<HPartsupp>(filePath("Partsupp")), SCALE(800000)),
    _Nation(new DirRDD<HNation>(filePath("Nation")),       25),
    _Region(new DirRDD<HRegion>(filePath("Region")),       5) {}

};

#else

#define TABLE(x) &((CachedData*)Cluster::instance->userData)->_##x

class CachedData
{
  public:
    CachedRDD<Lineitem> _Lineitem;
    CachedRDD<Orders> _Orders;
    CachedRDD<Supplier> _Supplier;
    CachedRDD<Customer> _Customer;
    CachedRDD<Part> _Part;
    CachedRDD<Partsupp> _Partsupp;
    CachedRDD<Nation> _Nation;
    CachedRDD<Region> _Region;

    CachedData() : 
    _Lineitem(new DirRDD<Lineitem>(filePath("Lineitem")), SCALE(6000000)),
    _Orders(new DirRDD<Orders>(filePath("Orders")),       SCALE(1500000)),
    _Supplier(new DirRDD<Supplier>(filePath("Supplier")), SCALE(10000)),
    _Customer(new DirRDD<Customer>(filePath("Customer")), SCALE(150000)),
    _Part(new DirRDD<Part>(filePath("Part")),             SCALE(200000)),
    _Partsupp(new DirRDD<Partsupp>(filePath("Partsupp")), SCALE(800000)),
    _Nation(new DirRDD<Nation>(filePath("Nation")),       25),
    _Region(new DirRDD<Region>(filePath("Region")),       5) {}

};
#endif

inline void sum(double& dst, double const& src)
{
    dst += src;
}

inline void count(int& dst, int const& src)
{
    dst += src;
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

inline void partsuppKey(PartsuppKey& key, Partsupp const& ps)
{
    key.ps_partkey = ps.ps_partkey();
    key.ps_suppkey = ps.ps_suppkey();
}

struct NationProjection
{
    int n_nationkey;
    int n_regionkey;
    name_t n_name;
};

inline void projectNation(NationProjection& out, Nation const& in)
{
    out.n_nationkey = in.n_nationkey();
    out.n_regionkey = in.n_regionkey();
    out.n_name = in.n_name();
}

auto nationProjection() { 
    return project<Nation,NationProjection,projectNation>(TABLE(Nation));
}

struct Nation1 { 
    NationProjection n1;
};

struct Nation2 { 
    NationProjection n2;
};

inline void projectNation1(Nation1& out, Nation const& in)
{
    projectNation(out.n1, in);
}

auto nationProjection1() { 
    return project<Nation,Nation1,projectNation1>(TABLE(Nation));
}

inline void projectNation2(Nation2& out, Nation const& in)
{
    projectNation(out.n2, in);
}

auto nationProjection2() { 
    return project<Nation,Nation2,projectNation2>(TABLE(Nation));
}

struct RegionProjection
{
    int r_regionkey;
    name_t r_name;
};

inline void projectRegion(RegionProjection& out, Region const& in)
{
    out.r_regionkey = in.r_regionkey();
    out.r_name = in.r_name();
}

auto regionProjection() { 
    return project<Region,RegionProjection,projectRegion>(TABLE(Region));
}

inline void nationKey(int& key, NationProjection const& nation)
{
    key = nation.n_nationkey;
}

inline void regionKey(int& key, RegionProjection const& region)
{
    key = region.r_regionkey;
}


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
    
    inline bool predicate(Lineitem const& lineitem) 
    {
        return lineitem.l_shipdate() <= 19981201;
    }

    inline void map(Pair<GroupBy,Aggregate>& pair, Lineitem const& lineitem)
    {
        pair.key.l_returnflag = lineitem.l_returnflag();
        pair.key.l_linestatus = lineitem.l_linestatus();
        pair.value.sum_qty = lineitem.l_quantity();
        pair.value.sum_base_price = lineitem.l_extendedprice();
        pair.value.sum_disc_price = lineitem.l_extendedprice()*(1-lineitem.l_discount());
        pair.value.sum_charge = lineitem.l_extendedprice()*(1-lineitem.l_discount())*(1+lineitem.l_tax());
        pair.value.sum_disc = lineitem.l_discount();
        pair.value.count_order = 1;
    }

    inline void reduce(Aggregate& dst, Aggregate const& src)
    {
        dst.sum_qty += src.sum_qty;
        dst.sum_base_price += src.sum_base_price;
        dst.sum_disc_price += src.sum_disc_price;
        dst.sum_charge += src.sum_charge;
        dst.sum_disc  += src.sum_disc;
        dst.count_order += src.count_order;
    }


    inline void projection(Projection& out, Pair<GroupBy,Aggregate> const& in)
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

    inline int compare(Projection const* a, Projection const* b)
    {
        int diff = a->l_returnflag - b->l_returnflag;
        return diff != 0 ? diff : a->l_linestatus - b->l_linestatus;
    }

    auto query() 
    { 
        return
            (sort<Projection,compare>
             (project<Pair<GroupBy,Aggregate>,Projection, projection>
              (mapReduce<Lineitem,GroupBy,Aggregate,map,reduce>
               (filter<Lineitem,predicate>(TABLE(Lineitem)), 10000)), 100));
    }

    struct LineitemProjection 
    {
        date_t l_shipdate;
        double l_extendedprice;
        double l_discount;
        double l_quantity;
        double l_tax;
		char   l_returnflag;
		char   l_linestatus;
    };

    inline void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_extendedprice = in.l_extendedprice();
        out.l_discount = in.l_discount();
        out.l_tax = in.l_tax();
		out.l_quantity = in.l_quantity();
		out.l_shipdate = in.l_shipdate();
		out.l_returnflag = in.l_returnflag();
		out.l_linestatus = in.l_linestatus();
    }

    inline bool predicate(LineitemProjection const& lineitem) 
    {
        return lineitem.l_shipdate <= 19981201;
    }

    inline void map(Pair<GroupBy,Aggregate>& pair, LineitemProjection const& lineitem)
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

    auto streamConsumer() 
    { 
        return
              (continuousMapReduce<LineitemProjection,GroupBy,Aggregate,map,reduce>
               (filter<LineitemProjection,predicate>
				(project<Lineitem,LineitemProjection,projectLineitem>
				 (new DirRDD<Lineitem>(filePath("Lineitem")))), 10000));
    }

	template<class Rdd>
    auto continuousView(Rdd* stream) 
    { 
        return
            (sort<Projection,compare>
             (project<Pair<GroupBy,Aggregate>,Projection, projection>
			  (stream), 100));
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

    inline void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey();
        out.l_extendedprice = in.l_extendedprice();
        out.l_discount = in.l_discount();
        out.l_shipdate = in.l_shipdate();
    }

    inline void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
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

    inline void projectOrders(OrdersProjection& out, Orders const& in)
    { 
        out.o_orderkey = in.o_orderkey();
        out.o_custkey = in.o_custkey();
        out.o_orderdate = in.o_orderdate();
        out.o_shippriority = in.o_shippriority();
    }            

    inline void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }

    struct CustomerProjection
    {
        int c_custkey;
    };
    
    inline void projectCustomer(CustomerProjection& out, Customer const& in)
    {
        out.c_custkey = in.c_custkey();
    }
    
    inline void customerKey(int& key, CustomerProjection const& customer)
    {
        key = customer.c_custkey;
    }

    inline bool lineitemFilter(Lineitem const& l)
    {
        return l.l_shipdate() > 19950304;
    }
    
    inline bool orderFilter(Orders const& o)
    {
        return o.o_orderdate() < 19950304;
    }
    
    inline bool customerFilter(Customer const& c)
    {
        return c.c_mktsegment() == "HOUSEHOLD";
    }
    
    inline void orderCustomerKey(int& key, Join<LineitemProjection,OrdersProjection> const& r)
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

    inline void map(Pair<GroupBy,double>& pair, Join<Join<LineitemProjection,OrdersProjection>,CustomerProjection> const& r)
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

    inline void revenue(Revenue& out, Pair<GroupBy, double> const& in)
    {
        (GroupBy&)out = in.key;
        out.revenue = in.value;
    }


    inline int byRevenueAndOrderDate(Revenue const* a, Revenue const* b) 
    {
        return a->revenue > b->revenue ? -1 : a->revenue == b->revenue ? (a->o_orderdate < b->o_orderdate ? -1 : a->o_orderdate == b->o_orderdate ? 0 : 1) : 1;
    }

    auto query() 
    { 
        auto s1 = filter<Lineitem,lineitemFilter>(TABLE(Lineitem));
        auto s2 = project<Lineitem,LineitemProjection, projectLineitem>(s1);
        auto s3 = filter<Orders,orderFilter>(TABLE(Orders));
        auto s4 = project<Orders,OrdersProjection,projectOrders>(s3);            
        auto s5 = join<LineitemProjection,OrdersProjection,long,lineitemOrderKey,orderKey>(s2, s4, SCALE(1500000));
        auto s6 = filter<Customer,customerFilter>(TABLE(Customer));
        auto s7 = project<Customer,CustomerProjection,projectCustomer>(s6);
        auto s8 = join<typeof(s5),CustomerProjection,int,orderCustomerKey,customerKey>(s5, s7, SCALE(150000));
        auto s9 = mapReduce<typeof(s8),GroupBy,double,map,sum>(s8, 1000000);
        auto s10 = project<Pair<GroupBy,double>,Revenue,revenue>(s9);
        return top<Revenue,byRevenueAndOrderDate>(s10, 10);
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

    inline void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey();
        out.l_commitdate = in.l_commitdate();
        out.l_receiptdate = in.l_receiptdate();
    }

    inline void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        long o_orderkey;
        date_t o_orderdate;
        priority_t o_orderpriority;
    };

    inline void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey();
        out.o_orderdate = in.o_orderdate();
        out.o_orderpriority = in.o_orderpriority();
    }            

    inline void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }

    inline bool lineitemFilter(Lineitem const& l)
    {
        return l.l_commitdate() < l.l_receiptdate();
    }
    
    inline bool orderFilter(Orders const& o)
    {
        return o.o_orderdate() >= 19930801 && o.o_orderdate() < 19931101;
    }
    
    inline void map(Pair<priority_t,int>& pair, Join<LineitemProjection,OrdersProjection> const& r)
    {
        pair.key = r.o_orderpriority;
        pair.value = 1;
    }

    inline int byPriority(Pair<priority_t,int> const* a, Pair<priority_t,int> const* b)  { 
        return a->key == b->key;
    }

    auto query() 
    { 
        auto s1 = filter<Lineitem,lineitemFilter>(TABLE(Lineitem));
        auto s2 = project<Lineitem,LineitemProjection,projectLineitem>(s1);
        auto s3 = filter<Orders,orderFilter>(TABLE(Orders));
        auto s4 = project<Orders,OrdersProjection,projectOrders>(s3);
        auto s5 = join<LineitemProjection,OrdersProjection,long,lineitemOrderKey,orderKey>(s2, s4, SCALE(1500000));
        auto s6 = mapReduce<typeof(s5),priority_t,int,map,count>(s5, 25);
        return sort<Pair<priority_t,int>,byPriority>(s6, 25);
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

    inline void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey();
        out.l_suppkey = in.l_suppkey();
        out.l_extendedprice = in.l_extendedprice();
        out.l_discount = in.l_discount();
    }

    inline void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        long o_orderkey;
        int  o_custkey;
    };

    inline void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey();
        out.o_custkey = in.o_custkey();
    }            

    inline void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }
    
    struct SupplierProjection
    {
        int s_suppkey;
        int s_nationkey;
    };

    inline void projectSupplier(SupplierProjection& out, Supplier const& in)
    {
        out.s_suppkey = in.s_suppkey();
        out.s_nationkey = in.s_nationkey();
    }            

    inline void supplierKey(int& key, SupplierProjection const& in)
    {
        key = in.s_suppkey;
    }
    
    struct CustomerProjection
    {
        int c_custkey;
        int c_nationkey;
    };

    inline void projectCustomer(CustomerProjection& out, Customer const& in)
    {
        out.c_custkey = in.c_custkey();
        out.c_nationkey = in.c_nationkey();
    }            

    inline void customerKey(int& key, CustomerProjection const& in)
    {
        key = in.c_custkey;
    }

    inline bool orderRange(Orders const& order) 
    {
        return order.o_orderdate() >= 19960101 && order.o_orderdate() < 19970101;
    }

    inline bool sameNation(Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection> const& r)
    {
        return r.c_nationkey == r.s_nationkey;
    }

    inline void customerNationKey(int& key, Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection> const& r)
    {
        key = r.c_nationkey;
    }
    
    inline void lineitemSupplierKey(int& key, Join<LineitemProjection,OrdersProjection> const& r)
    {
        key = r.l_suppkey;
    }
    
    inline void orderCustomerKey(int& key, Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection> const& r)
    {
        key = r.o_custkey;
    }

    inline void nationRegionKey(int& key, Join<Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection>,NationProjection> const& r)
    {
        key = r.n_regionkey;
    }
    
    inline bool asiaRegion(Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection>,NationProjection>,RegionProjection> const& r) 
    { 
        return r.r_name == "ASIA";

    }

    inline void map(Pair<name_t,double>& pair, Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection>,NationProjection>,RegionProjection> const& r)
    {
        pair.key = r.n_name;
        pair.value = r.l_extendedprice * (1 - r.l_discount);
    }

    struct Revenue 
    {
        name_t n_name;
        double revenue;

        friend void print(Revenue const& r, FILE* out) { 
            printf("%s, %f", r.n_name.cstr(), r.revenue);
        }
    };

    inline void revenue(Revenue& out, Pair<name_t,double> const& in)
    {
        out.n_name = in.key;
        out.revenue = in.value;
    }

    inline int byRevenue(Revenue const* a, Revenue const* b) 
    {
        return a->revenue > b->revenue ? -1 : a->revenue == b->revenue ? 0 : 1;
    }

    auto query() 
    { 
        auto s1 = project<Lineitem,LineitemProjection,projectLineitem>(TABLE(Lineitem));
        auto s2 = filter<Orders,orderRange>(TABLE(Orders));
        auto s3 = project<Orders,OrdersProjection,projectOrders>(s2);
        auto s4 = join<LineitemProjection,OrdersProjection,long,lineitemOrderKey,orderKey>(s1, s3, SCALE(1500000));
        auto s5 = project<Supplier,SupplierProjection,projectSupplier>(TABLE(Supplier));
        auto s6 = join<typeof(s4),SupplierProjection,int,lineitemSupplierKey,supplierKey>(s4, s5, SCALE(10000));
        auto s7 = project<Customer,CustomerProjection,projectCustomer>(TABLE(Customer));
        auto s8 = join<typeof(s6),CustomerProjection,int,orderCustomerKey,customerKey>(s6, s7, SCALE(150000));
        auto s9 = filter<typeof(s8),sameNation>(s8);        
        auto s10 = join<typeof(s9),NationProjection,int,customerNationKey,nationKey>(s9, nationProjection(), 25);
        auto s11 = join<typeof(s10),RegionProjection,int,nationRegionKey,regionKey>(s10, regionProjection(), 5);
        auto s12 = filter<typeof(s11),asiaRegion>(s11);
        auto s13 = mapReduce<typeof(s12),name_t,double,map,sum>(s12, 25);
        auto s14 = project<Pair<name_t,double>,Revenue,revenue>(s13);
        return sort<Revenue,byRevenue>(s14, 25);
    }    
}
namespace Q6
{
    inline bool lineitemFilter(Lineitem const& l)
    {
        return l.l_shipdate() >= 19960101 && l.l_shipdate() <= 19970101
            && l.l_discount() >= 0.08 && l.l_discount() <= 0.1
            && l.l_quantity() < 24;
    }
    inline void revenue(double& result, Lineitem const& l)
    {
        result += l.l_extendedprice()*l.l_discount();
    }
    
    auto query() 
    { 
        return
            (reduce<Lineitem,double,revenue,sum>
             (filter<Lineitem,lineitemFilter>(TABLE(Lineitem)), 0));
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
    
    inline void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey();
        out.l_suppkey = in.l_suppkey();
        out.l_shipdate = in.l_shipdate();
        out.l_extendedprice = in.l_extendedprice();
        out.l_discount = in.l_discount();
    }

    inline void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        long o_orderkey;
        int  o_custkey;
    };

    inline void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey();
        out.o_custkey = in.o_custkey();
    }            

    inline void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }
    
    struct CustomerProjection
    {
        int c_custkey;
        int c_nationkey;
    };

    inline void projectCustomer(CustomerProjection& out, Customer const& in)
    {
        out.c_custkey = in.c_custkey();
        out.c_nationkey = in.c_nationkey();
    }            

    inline void customerKey(int& key, CustomerProjection const& in)
    {
        key = in.c_custkey;
    }

    struct SupplierProjection
    {
        int s_suppkey;
        int s_nationkey;
    };

    inline void projectSupplier(SupplierProjection& out, Supplier const& in)
    {
        out.s_suppkey = in.s_suppkey();
        out.s_nationkey = in.s_nationkey();
    }            

    inline void supplierKey(int& key, SupplierProjection const& in)
    {
        key = in.s_suppkey;
    }

    inline void lineitemSupplierKey(int& key, Join<LineitemProjection,OrdersProjection> const& r)
    {
        key = r.l_suppkey;
    }

    inline void orderCustomerKey(int& key, Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection> const& r)
    {
        key = r.o_custkey;
    }

    inline void supplierNationKey(int& key, Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection> const& r)
    {
        key = r.s_nationkey;
    }
    
    inline void customerNationKey(int& key, Join<Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection>,Nation1> const& r)
    {
        key = r.c_nationkey;
    }

    inline void nation1Key(int& key, Nation1 const& nation)
    {
        key = nation.n1.n_nationkey;
    }

    inline void nation2Key(int& key, Nation2 const& nation)
    {
        key = nation.n2.n_nationkey;
    }

    inline bool filterNation(Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection>,Nation1>,Nation2> const& r) 
    {
        return (r.n1.n_name == "UNITED STATES" && r.n2.n_name == "INDONESIA")
            || (r.n2.n_name == "UNITED STATES" && r.n1.n_name == "INDONESIA");
    }

    inline bool filterLineitem(Lineitem const& l)
    {
        return l.l_shipdate() >= 19950101 && l.l_shipdate() <= 19961231;
    }

    struct Shipping
    {
        name_t supp_nation;
        name_t cust_nation;
        int    l_year;

        bool operator == (Shipping const& other) const
        {
            return supp_nation == other.supp_nation
                && cust_nation == other.cust_nation 
                && l_year == other.l_year;
        }

        friend size_t hashCode(Shipping const& s)
        {
            return ::hashCode(s.supp_nation) + ::hashCode(s.cust_nation) + s.l_year;
        }

        friend void print(Shipping const& s, FILE* out) 
        {
            fprintf(out, "%s, %s, %d", s.supp_nation.cstr(), s.cust_nation.cstr(), s.l_year);
        }
    };

#define ShippingFields(FIELD) \
    FIELD(supp_nation)        \
    FIELD(cust_nation)        \
    FIELD(l_year)

    PACK(Shipping)
    UNPACK(Shipping)
    
    inline void map(Pair<Shipping,double>& pair, Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,SupplierProjection>,CustomerProjection>,Nation1>,Nation2> const& r)
    {
        pair.key.supp_nation = r.n1.n_name;
        pair.key.cust_nation = r.n2.n_name;
        pair.key.l_year = r.l_shipdate/10000;
        pair.value = r.l_extendedprice * (1-r.l_discount);
    }

    inline int byShipping(Pair<Shipping,double> const* a, Pair<Shipping,double> const* b)
    {
        int diff;
        diff = a->key.supp_nation.compare(b->key.supp_nation);
        if (diff != 0) return diff;
        diff = a->key.cust_nation.compare(b->key.cust_nation);
        return (diff != 0) ? diff : a->key.l_year - b->key.l_year;
    }

    auto query() 
    { 
        auto s1 = filter<Lineitem,filterLineitem>(TABLE(Lineitem));
        auto s2 = project<Lineitem,LineitemProjection,projectLineitem>(s1);            
        auto s3 = project<Orders,OrdersProjection,projectOrders>(TABLE(Orders));
        auto s4 = join<LineitemProjection,OrdersProjection,long,lineitemOrderKey,orderKey>(s2, s3, SCALE(1500000));
        auto s5 = project<Supplier,SupplierProjection,projectSupplier>(TABLE(Supplier));
        auto s6 = join<typeof(s4),SupplierProjection,int,lineitemSupplierKey,supplierKey>(s4, s5, SCALE(10000));
        auto s7 = project<Customer,CustomerProjection,projectCustomer>(TABLE(Customer));
        auto s8 = join<typeof(s6),CustomerProjection,int,orderCustomerKey,customerKey>(s6, s7, SCALE(150000));
        auto s9 = join<typeof(s8),Nation1,int,supplierNationKey,nation1Key>(s8, nationProjection1(), 25);
        auto s10 = join<typeof(s9),Nation2,int,customerNationKey,nation2Key>(s9, nationProjection2(), 25);
        auto s11 = filter<typeof(s10),filterNation>(s10);
        auto s12 = mapReduce<typeof(s11),Shipping,double,map,sum>(s11, 25*25*100);
        return sort<Pair<Shipping,double>,byShipping>(s12, 25*25*100);
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

    inline void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey();
        out.l_suppkey = in.l_suppkey();
        out.l_partkey = in.l_partkey();
        out.l_extendedprice = in.l_extendedprice();
        out.l_discount = in.l_discount();
    }

    inline void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        long o_orderkey;
        int  o_custkey;
        date_t o_orderdate;
    };

    inline void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey();
        out.o_custkey = in.o_custkey();
        out.o_orderdate = in.o_orderdate();
    }            

    inline void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }
    
    struct CustomerProjection
    {
        int c_custkey;
        int c_nationkey;
    };
    
    inline void projectCustomer(CustomerProjection& out, Customer const& in)
    {
        out.c_custkey = in.c_custkey();
        out.c_nationkey = in.c_nationkey();
    }
    
    inline void customerKey(int& key, CustomerProjection const& customer)
    {
        key = customer.c_custkey;
    }

    struct PartProjection
    {
        int p_partkey;
        Char<25> p_type;
    };

    inline void projectPart(PartProjection& out, Part const& in)
    {
        out.p_partkey = in.p_partkey();
        out.p_type = in.p_type();
    }            

    inline void partKey(int& key, PartProjection const& in)
    {
        key = in.p_partkey;
    }

    struct SupplierProjection
    {
        int s_suppkey;
        int s_nationkey;
    };

    inline void projectSupplier(SupplierProjection& out, Supplier const& in)
    {
        out.s_suppkey = in.s_suppkey();
        out.s_nationkey = in.s_nationkey();
    }            

    inline void supplierKey(int& key, SupplierProjection const& in)
    {
        key = in.s_suppkey;
    }

    inline bool orderRange(Orders const& orders) 
    {
        return orders.o_orderdate() >= 19950101 && orders.o_orderdate() < 19961231;
    }

    inline bool partType(Part const& part)
    {
        return part.p_type() == "MEDIUM ANODIZED NICKEL";
    }

    inline bool regionName(RegionProjection const& region)
    {
        return region.r_name == "ASIA";
    }

    inline void lineitemSupplierKey(int& key, Join<Join<LineitemProjection,OrdersProjection>,PartProjection> const& r)
    {
        key = r.l_suppkey;
    }

    inline void lineitemPartKey(int& key, Join<LineitemProjection,OrdersProjection> const& r)
    {
        key = r.l_partkey;
    }

    inline void orderCustomerKey(int& key, Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,SupplierProjection> const& r)
    {
        key = r.o_custkey;
    }

    inline void supplierNationKey(int& key, Join<Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,SupplierProjection>,CustomerProjection> const& r)
    {
        key = r.s_nationkey;
    }
    
    inline void customerNationKey(int& key, Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,SupplierProjection>,CustomerProjection>,Nation1> const& r)
    {
        key = r.c_nationkey;
    }

    inline void nation1Key(int& key, Nation1 const& nation)
    {
        key = nation.n1.n_nationkey;
    }

    inline void nation2Key(int& key, Nation2 const& nation)
    {
        key = nation.n2.n_nationkey;
    }

    inline void nationRegionKey(int& key, Join<Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,SupplierProjection>,CustomerProjection>,Nation1>,Nation2> const& r)
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

    inline void map(Pair<int,Volume>& pair, Join<Join<Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,SupplierProjection>,CustomerProjection>,Nation1>,Nation2>,RegionProjection> const& r)
    {
        double volume = r.l_extendedprice * (1-r.l_discount);
        pair.value.nation = r.n2.n_name == "INDONESIA" ? volume : 0;
        pair.value.total = volume;
        pair.key = r.o_orderdate/10000;
    }

    inline void reduce(Volume& dst, Volume const& src)
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

    inline void mkt(Share& out, Pair<int,Volume> const& in)
    {
        out.o_year = in.key;
        out.mkt_share = in.value.nation/in.value.total;
    }

    inline int byYear(Share const* a, Share const* b)
    {
        return a->o_year - b->o_year;
    }
    
    auto query() 
    { 
        auto s1 = project<Lineitem,LineitemProjection,projectLineitem>(TABLE(Lineitem));
        auto s2 = filter<Orders,orderRange>(TABLE(Orders));
        auto s3 = project<Orders,OrdersProjection,projectOrders>(s2);
        auto s4 = join<LineitemProjection,OrdersProjection,long,lineitemOrderKey,orderKey>(s1, s3, SCALE(1500000));
        auto s5 = filter<Part,partType>(TABLE(Part));
        auto s6 = project<Part,PartProjection,projectPart>(s5);
        auto s7 = join<typeof(s4),PartProjection,int,lineitemPartKey,partKey>(s4, s6, SCALE(200000));
        auto s8 = project<Supplier,SupplierProjection,projectSupplier>(TABLE(Supplier));
        auto s9 = join<typeof(s7),SupplierProjection,int,lineitemSupplierKey,supplierKey>(s7, s8, SCALE(10000));
        auto s10 = project<Customer,CustomerProjection,projectCustomer>(TABLE(Customer));
        auto s11 = join<typeof(s9),CustomerProjection,int,orderCustomerKey,customerKey>(s9, s10, SCALE(150000));
        auto s12 = join<typeof(s11),Nation1,int,supplierNationKey,nation1Key>(s11, nationProjection1(), 25);
        auto s13 = join<typeof(s12),Nation2,int,customerNationKey,nation2Key>(s12, nationProjection2(), 25);
        auto s14 = filter<RegionProjection,regionName>(regionProjection());
        auto s15 = join<typeof(s13),RegionProjection,int,nationRegionKey,regionKey>(s13, s14, 5);
        auto s16 = mapReduce<typeof(s15),int,Volume,map,reduce>(s15, 100);
        auto s17 = project<Pair<int,Volume>,Share,mkt>(s16);
        return sort<Share,byYear>(s17, 100);
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

    inline void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey();
        out.l_suppkey = in.l_suppkey();
        out.l_partkey = in.l_partkey();
        out.l_extendedprice = in.l_extendedprice();
        out.l_discount = in.l_discount();
        out.l_quantity = in.l_quantity();
    }

    inline void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        long o_orderkey;
        int  o_orderdate;
    };

    inline void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey();
        out.o_orderdate = in.o_orderdate();
    }            

    inline void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }
    
    struct SupplierProjection
    {
        int s_suppkey;
        int s_nationkey;
    };

    inline void projectSupplier(SupplierProjection& out, Supplier const& in)
    {
        out.s_suppkey = in.s_suppkey();
        out.s_nationkey = in.s_nationkey();
    }            

    inline void supplierKey(int& key, SupplierProjection const& in)
    {
        key = in.s_suppkey;
    }

    struct PartProjection
    {
        int p_partkey;
        Char<55> p_name;
     };

    inline void projectPart(PartProjection& out, Part const& in)
    {
        out.p_partkey = in.p_partkey();
        out.p_name = in.p_name();
    }            

    inline void partKey(int& key, PartProjection const& in)
    {
        key = in.p_partkey;
    }

    struct PartsuppProjection
    {
        int ps_partkey;
        int ps_suppkey;
        double ps_supplycost;
    };

    inline void projectPartsupp(PartsuppProjection& out, Partsupp const& in)
    {
        out.ps_partkey = in.ps_partkey();
        out.ps_suppkey = in.ps_suppkey();
        out.ps_supplycost = in.ps_supplycost();
    }            

    inline void partsuppKey(PartsuppKey& key, PartsuppProjection const& in)
    {
        key.ps_partkey = in.ps_partkey;
        key.ps_suppkey = in.ps_suppkey;
    }

    inline bool partName(Part const& part)
    {
        return strstr(part.p_name(), "ghost") != NULL;
    }

    inline void lineitemSupplierKey(int& key, Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,PartsuppProjection> const& r)
    {
        key = r.l_suppkey;
    }

    inline void lineitemPartKey(int& key, Join<LineitemProjection,OrdersProjection> const& r)
    {
        key = r.l_partkey;
    }


    inline void supplierNationKey(int& key, Join<Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,PartsuppProjection>,SupplierProjection> const& r)
    {
        key = r.s_nationkey;
    }
    
    inline void lineitemPartsuppKey(PartsuppKey& ps, Join<Join<LineitemProjection,OrdersProjection>,PartProjection> const& r)
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
            return nation == other.nation && o_year == other.o_year;
        }

        friend size_t hashCode(Profit const& p) { 
            return ::hashCode(p.nation) + ::hashCode(p.o_year);
        }

        friend void print(Profit const& p, FILE* out) {
            fprintf(out, "%s, %d", p.nation.cstr(), p.o_year);
        } 
    };

#define ProfitFields(FIELD) \
    FIELD(nation)           \
    FIELD(o_year)

    PACK(Profit)
    UNPACK(Profit)

    inline void map(Pair<Profit,double>& pair, Join<Join<Join<Join<Join<LineitemProjection,OrdersProjection>,PartProjection>,PartsuppProjection>,SupplierProjection>,NationProjection> const& r)
    {
        pair.value = r.l_extendedprice * (1-r.l_discount)-r.ps_supplycost * r.l_quantity;
        pair.key.nation = r.n_name;
        pair.key.o_year = r.o_orderdate/10000;
    }
    
    inline int byNationYear(Pair<Profit,double> const* a, Pair<Profit,double> const* b)
    {
        int diff = a->key.nation.compare(b->key.nation);
        return (diff != 0) ? diff : b->key.o_year - a->key.o_year;
    }
        
    auto query() 
    { 
        auto s1 = project<Lineitem,LineitemProjection,projectLineitem>(TABLE(Lineitem));
        auto s2 = project<Orders,OrdersProjection,projectOrders>(TABLE(Orders));
        auto s3 = join<LineitemProjection,OrdersProjection,long,lineitemOrderKey,orderKey>(s1, s2, SCALE(1500000));
        auto s4 = filter<Part,partName>(TABLE(Part));
        auto s5 = project<Part,PartProjection,projectPart>(s4);
        auto s6 = join<typeof(s3),PartProjection,int,lineitemPartKey,partKey>(s3, s5, SCALE(200000));
        auto s7 = project<Partsupp,PartsuppProjection,projectPartsupp>(TABLE(Partsupp));
        auto s8 = join<typeof(s6),PartsuppProjection,PartsuppKey,lineitemPartsuppKey,partsuppKey>(s6, s7, SCALE(800000));
        auto s9 = project<Supplier,SupplierProjection,projectSupplier>(TABLE(Supplier));
        auto s10 = join<typeof(s8),SupplierProjection,int,lineitemSupplierKey,supplierKey>(s8, s9, SCALE(10000));
        auto s11 = join<typeof(s10),NationProjection,int,supplierNationKey,nationKey>(s10, nationProjection(), 25);
        auto s12 = mapReduce<typeof(s11),Profit,double,map,sum>(s11, 25*100);
        return sort<Pair<Profit,double>,byNationYear>(s12, 100);
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

    inline void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey();
        out.l_extendedprice = in.l_extendedprice();
        out.l_discount = in.l_discount();
    }

    inline void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        long   o_orderkey;
        int    o_custkey;
        date_t o_orderdate;
    };

    inline void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey();
        out.o_custkey = in.o_custkey();
        out.o_orderdate = in.o_orderdate();
    }            

    inline void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }
    
    inline bool orderRange(Orders const& orders) 
    {
        return orders.o_orderdate() >= 19941101 && orders.o_orderdate() < 19950201;
    }
    
    inline bool lineitemFilter(Lineitem const& l)
    {
        return l.l_returnflag() == 'R';
    }

    inline void orderCustomerKey(int& key, Join<LineitemProjection,OrdersProjection> const& r)
    {
        key = r.o_custkey;
    }

    struct CustomerProjection
    {
        int c_custkey;
        int c_nationkey;
        name_t c_name;
        double c_acctball;
        Char<40> c_address;
        Char<15> c_phone;
        Char<117> c_comment;
    };
    
    inline void projectCustomer(CustomerProjection& out, Customer const& in)
    {
        out.c_custkey = in.c_custkey();
        out.c_nationkey = in.c_nationkey();
        out.c_name = in.c_name();
        out.c_acctball = in.c_acctball();
        out.c_address = in.c_address();
        out.c_phone = in.c_phone();
        out.c_comment = in.c_comment();
    }

    inline void customerNationKey(int& key, Join<Join<LineitemProjection,OrdersProjection>,CustomerProjection> const& r)
    {
        key = r.c_nationkey;
    }
    
    inline void customerKey(int& key, CustomerProjection const& customer)
    {
        key = customer.c_custkey;
    }    

    struct GroupBy
    {
        int c_custkey;
        name_t c_name;
        double c_acctball;
        name_t n_name;
        Char<40> c_address;
        Char<15> c_phone;
        Char<117> c_comment;

        bool operator == (GroupBy const& other) const
        {
            return c_custkey == other.c_custkey
                && c_name == other.c_name
                && c_acctball == other.c_acctball
                && n_name == other.n_name
                && c_address == other.c_address
                && c_phone == other.c_phone
                && c_comment == other.c_comment;
        }

        friend size_t hashCode(GroupBy const& g)
        {
            return g.c_custkey + ::hashCode(g.c_name) + ::hashCode(g.n_name) + ::hashCode(g.c_address) + ::hashCode(g.c_phone) + ::hashCode(g.c_comment) + ::hashCode(g.c_acctball);
        }

        friend void print(GroupBy const& g, FILE* out) {
            fprintf(out, "%d, %s, %f, %s, %s, %.*s, %s", g.c_custkey,
                    g.c_name.cstr(), g.c_acctball, g.n_name.cstr(), g.c_address.cstr(),
                    (int)sizeof(g.c_phone), g.c_phone.cstr(), g.c_comment.cstr());
        } 
    };

#define GroupByFields(FIELD) \
    FIELD(c_custkey)         \
    FIELD(c_name)            \
    FIELD(c_acctball)        \
    FIELD(n_name)            \
    FIELD(c_address)         \
    FIELD(c_phone)           \
    FIELD(c_comment)

    PACK(GroupBy)
    UNPACK(GroupBy)
    
    inline void map(Pair<GroupBy,double>& pair, Join<Join<Join<LineitemProjection,OrdersProjection>,CustomerProjection>,NationProjection> const& r)
    {
        pair.key.c_custkey = r.c_custkey;
        pair.key.c_name = r.c_name;
        pair.key.n_name = r.n_name;
        pair.key.c_acctball = r.c_acctball;
        pair.key.c_address = r.c_address;
        pair.key.c_phone = r.c_phone;
        pair.key.c_comment = r.c_comment;
        pair.value = r.l_extendedprice * (1 - r.l_discount);
    }
    
    inline int byRevenue(Pair<GroupBy,double> const* a, Pair<GroupBy,double> const* b)
    {
        return a->value < b->value ? 1 : a->value == b->value ? 0 : -1;
    }
        
    auto query() 
    { 
        auto s1 = filter<Lineitem,lineitemFilter>(TABLE(Lineitem));
        auto s2 = project<Lineitem,LineitemProjection,projectLineitem>(s1);
        auto s3 = filter<Orders,orderRange>(TABLE(Orders));
        auto s4 = project<Orders,OrdersProjection,projectOrders>(s3);
        auto s5 = join<LineitemProjection,OrdersProjection,long,lineitemOrderKey,orderKey>(s2, s4, SCALE(1500000));
        auto s6 = project<Customer,CustomerProjection,projectCustomer>(TABLE(Customer));
        auto s7 = join<typeof(s5),CustomerProjection,int,orderCustomerKey,customerKey>(s5, s6, SCALE(150000));
        auto s8 = join<typeof(s7),NationProjection,int,customerNationKey,nationKey>(s7, nationProjection(), 25);
        auto s9 = mapReduce<typeof(s8),GroupBy,double,map,sum>(s8, 1000);
        return top<Pair<GroupBy,double>,byRevenue>(s9, 20);
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

    inline void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_orderkey = in.l_orderkey();
        out.l_shipdate = in.l_shipdate();
        out.l_commitdate = in.l_commitdate();
        out.l_receiptdate = in.l_receiptdate();
        out.l_shipmode = in.l_shipmode();
    }

    inline void lineitemOrderKey(long& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_orderkey;
    }

    struct OrdersProjection
    {
        long o_orderkey;
        priority_t o_orderpriority;
    };

    inline void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_orderkey = in.o_orderkey();
        out.o_orderpriority = in.o_orderpriority();
    }            

    inline void orderKey(long& key, OrdersProjection const& in)
    {
        key = in.o_orderkey;
    }
    
    inline bool lineitemFilter(Lineitem const& l)
    {
        return l.l_commitdate() < l.l_receiptdate()
            && l.l_shipdate() < l.l_commitdate()
            && l.l_receiptdate() >= 19940101
            && l.l_receiptdate() < 19950101
            && (l.l_shipmode() == "MAIL" || l.l_shipmode() == "SHIP");
    }

    struct LineCount
    {
        int high;
        int low;

        friend void print(LineCount const& l, FILE* out) {
            fprintf(out, "%d, %d", l.high, l.low);
        }
    };

    inline void map(Pair<shipmode_t,LineCount>& pair, Join<LineitemProjection,OrdersProjection> const& r)
    {
        pair.key = r.l_shipmode;
        pair.value.high = r.o_orderpriority == "1-URGENT" || r.o_orderpriority == "2-HIGH";
        pair.value.low = !pair.value.high;
    }
    
    inline void reduce(LineCount& dst, LineCount const& src)      
    {
        dst.high += src.high;
        dst.low += src.low;
    }

    inline int byShipmode(Pair<shipmode_t,LineCount> const* a, Pair<shipmode_t,LineCount> const* b)
    {
        return a->key.compare(b->key);
    }
        
    auto query() 
    { 
        auto s1 = filter<Lineitem,lineitemFilter>(TABLE(Lineitem));
        auto s2 = project<Lineitem,LineitemProjection,projectLineitem>(s1);
        auto s3 = project<Orders,OrdersProjection,projectOrders>(TABLE(Orders));
        auto s4 = join<LineitemProjection,OrdersProjection,long,lineitemOrderKey,orderKey>(s2, s3, SCALE(1500000));
        auto s5 = mapReduce<typeof(s4),shipmode_t,LineCount,map,reduce>(s4, 100);
        return sort<Pair<shipmode_t,LineCount>,byShipmode>(s5, 100);
    }    
}
namespace Q13
{
    struct OrdersProjection
    {
        int o_custkey;
    };
    
    inline void projectOrders(OrdersProjection& out, Orders const& in)
    {
        out.o_custkey = in.o_custkey();
    }

    inline void orderCustomerKey(int& key, OrdersProjection const& r)
    {
        key = r.o_custkey;
    }
    
    struct CustomerProjection
    {
        int c_custkey;
    };
    
    inline void projectCustomer(CustomerProjection& out, Customer const& in)
    {
        out.c_custkey = in.c_custkey();
    }
    
    inline void customerKey(int& key, CustomerProjection const& customer)
    {
        key = customer.c_custkey;
    }

    inline bool orderFilter(Orders const& o)
    {
        char const* occ = strstr(o.o_comment(), "unusual");
        return occ == NULL || strstr(occ + 7, "packages") == NULL;
    }

    inline void map1(Pair<int,int>& pair, Join<OrdersProjection,CustomerProjection> const& r)
    {
        pair.key = r.c_custkey;
        pair.value = 1;
    }
    
    inline void map2(Pair<int,int>& out, Pair<int,int> const& in)
    {
        out.key = in.value;
        out.value = 1;
    }
    
    inline int byCustDistCount(Pair<int,int> const* a, Pair<int,int> const* b)
    {
        int diff = b->value - a->value;
        return (diff != 0) ? diff : b->key - a->key;
    }
        
    auto query() 
    { 
        auto s1 = filter<Orders,orderFilter>(TABLE(Orders));
        auto s2 = project<Orders,OrdersProjection,projectOrders>(s1);
        auto s3 = project<Customer,CustomerProjection,projectCustomer>(TABLE(Customer));
        auto s4 = join<OrdersProjection,CustomerProjection,int,orderCustomerKey,customerKey>(s2, s3, SCALE(150000), OuterJoin);
        auto s5 = mapReduce<typeof(s4),int,int,map1,count>(s4, 1000000);
        auto s6 = mapReduce<typeof(s5),int,int,map2,count>(s5, 10000);
        return sort<Pair<int,int>,byCustDistCount>(s6,10000);
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

    inline void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_partkey = in.l_partkey();
        out.l_shipdate = in.l_shipdate();
        out.l_extendedprice = in.l_extendedprice();
        out.l_discount = in.l_discount();
    }

    inline void lineitemPartKey(int& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_partkey;
    }

    struct PartProjection
    {
        int p_partkey;
        Char<25> p_type;
    };

    inline void projectPart(PartProjection& out, Part const& in)
    {
        out.p_partkey = in.p_partkey();
        out.p_type = in.p_type();
    }            

    inline void partKey(int& key, PartProjection const& in)
    {
        key = in.p_partkey;
    }

    inline bool lineitemFilter(Lineitem const& l)
    {
        return l.l_shipdate() >= 19940301 && l.l_shipdate() < 19940401;
    }

    struct PromoRevenue
    {
        double promo;
        double revenue;

        PromoRevenue(double p = 0.0, double r = 0.0) : promo(p), revenue(r) {}
    };
      
    inline void promoRevenue(PromoRevenue& acc, Join<LineitemProjection,PartProjection> const& r) 
    { 
        acc.promo += strncmp(r.p_type, "PROMO", 5) == 0 ? r.l_extendedprice*(1-r.l_discount) : 0.0;
        acc.revenue += r.l_extendedprice * (1 - r.l_discount);
    }

    inline void combineRevenue(PromoRevenue& acc, PromoRevenue const& partial)
    {
        acc.promo += partial.promo;
        acc.revenue += partial.revenue;
    }
    
    inline void relation(double& result, PromoRevenue const& pr)
    {
        result = 100*pr.promo/pr.revenue;
    }

    auto query() 
    { 
        auto s1 = filter<Lineitem,lineitemFilter>(TABLE(Lineitem));
        auto s2 = project<Lineitem,LineitemProjection,projectLineitem>(s1);
        auto s3 = project<Part,PartProjection,projectPart>(TABLE(Part));
        auto s4 = join<LineitemProjection,PartProjection,int,lineitemPartKey,partKey>(s2, s3, SCALE(200000));
        auto s5 = reduce<typeof(s4),PromoRevenue,promoRevenue,combineRevenue>(s4, PromoRevenue(0,0));
        return project<PromoRevenue,double,relation>(s5);
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
        Char<25>   l_shipinstruct;
        shipmode_t l_shipmode;
    };

    inline void projectLineitem(LineitemProjection& out, Lineitem const& in)
    {
        out.l_partkey = in.l_partkey();
        out.l_extendedprice = in.l_extendedprice();
        out.l_discount = in.l_discount();
        out.l_quantity = in.l_quantity();
        out.l_shipinstruct = in.l_shipinstruct();
        out.l_shipmode = in.l_shipmode();
    }

    inline void lineitemPartKey(int& key, LineitemProjection const& lineitem)
    {
        key = lineitem.l_partkey;
    }

    struct PartProjection
    {
        int p_partkey;
        int p_size; 
        Char<10> p_brand;
        Char<10> p_container;
    };

    inline void projectPart(PartProjection& out, Part const& in)
    {
        out.p_partkey = in.p_partkey();
        out.p_size = in.p_size();
        out.p_brand = in.p_brand();
        out.p_container = in.p_container();
    }            

    inline void partKey(int& key, PartProjection const& in)
    {
        key = in.p_partkey;
    }

    inline bool brandFilter(Join<LineitemProjection,PartProjection> const& r)
    {
        return 
            (r.p_brand == "Brand#31"
             && (r.p_container == "SM CASE" || r.p_container == "SM BOX" || r.p_container == "SM PACK" || r.p_container == "SM PKG")
             && r.l_quantity >= 26 && r.l_quantity <= 36
             && r.p_size >= 1 && r.p_size <= 5
             && (r.l_shipmode == "AIR" || r.l_shipmode == "AIR REG")
             && r.l_shipinstruct == "DELIVER IN PERSON")
            || 
            (r.p_brand == "Brand#43"
             && (r.p_container == "MED BAG" || r.p_container == "MED BOX" || r.p_container == "MED PKG" || r.p_container == "MED PACK")
             && r.l_quantity >= 15 && r.l_quantity <= 25
             && r.p_size >= 1 && r.p_size <= 10
             && (r.l_shipmode == "AIR" || r.l_shipmode == "AIR REG")
             && r.l_shipinstruct == "DELIVER IN PERSON")
            ||
            (r.p_brand == "Brand#43"
             && (r.p_container == "LG CASE" || r.p_container == "LG BOX" || r.p_container == "LG PACK" || r.p_container == "LG PKG")
             && r.l_quantity >= 4 && r.l_quantity <= 14
             && r.p_size >= 1 && r.p_size <= 15
             && (r.l_shipmode == "AIR" || r.l_shipmode == "AIR REG")
             && r.l_shipinstruct == "DELIVER IN PERSON");
    }

    inline void revenue(double& acc, Join<LineitemProjection,PartProjection> const& r)
    {
        acc += r.l_extendedprice * (1 - r.l_discount);
    }

    auto query() 
    { 
        auto s1 = project<Lineitem,LineitemProjection,projectLineitem>(TABLE(Lineitem));
        auto s2 = project<Part,PartProjection,projectPart>(TABLE(Part));
        auto s3 = join<LineitemProjection,PartProjection,int,lineitemPartKey,partKey>(s1, s2, SCALE(200000));
        auto s4 = filter<typeof(s3), brandFilter>(s3);
        return reduce<typeof(s4),double,revenue,sum>(s4, 0);
    }    
}
    
template<class Rdd>
void execute(char const* name, Rdd* (*query)()) 
{
    time_t start = getCurrentTime();
    Rdd* result = query();
	output<typeof(result),Rdd>(result, stdout);

    if (Cluster::instance->nodeId == 0) {
        FILE* results = fopen("results.csv", "a");
        fprintf(results, "%s,%d\n", name, (int)(getCurrentTime() - start));
        fclose(results);
    }
    printf("Elapsed time for %s: %d milliseconds\n", name, (int)(getCurrentTime() - start));
    fflush(stdout);
}

template<class I, class O>
void continuousExecute(char const* name, I* in, O* (*query)(I*)) 
{
    time_t start = getCurrentTime();
    O* result = query(in);
	output<typeof(result),O>(result, stdout);

    if (Cluster::instance->nodeId == 0) {
        FILE* results = fopen("results.csv", "a");
        fprintf(results, "%s,%d\n", name, (int)(getCurrentTime() - start));
        fclose(results);
    }
    printf("Elapsed time for %s: %d milliseconds\n", name, (int)(getCurrentTime() - start));
    fflush(stdout);
}

class TPCHJob : public Job
{
    Cluster cluster;

  public:
    TPCHJob(size_t nodeId, size_t nHosts, char** hosts = NULL, size_t nThreads = 8, size_t bufferSize = 64*1024, size_t socketBufferSize = 64*1024*1024, size_t broadcastJoinThreshold = 10000, bool sharedNothing = false, size_t split = 1, bool verbose = false)
    : cluster(nodeId, nHosts, hosts, nThreads, bufferSize, socketBufferSize, broadcastJoinThreshold, sharedNothing, verbose, split)
    {}
    
  public:
    void run() {
        Cluster::instance.set(&cluster);
        printf("Node %d started...\n", (int)cluster.nodeId);

#ifdef STREAM_DB
		auto stream = Q1::streamConsumer();
        bool exhausted;
		do {
            exhausted = stream->exhausted();
			auto result = Q1::continuousView(stream);
			append(result, stdout);
		} while (!exhausted);

		delete stream;
#else
		
        time_t start = getCurrentTime();

#ifdef COLUMNAR_STORE
        cluster.userData = (void*)new ColumnarStore();
#else
        cluster.userData = (void*)new CachedData();
#endif
        printf("Elapsed time for loading all data in memory: %d milliseconds\n", (int)(getCurrentTime() - start));
 
        execute("Q1",  Q1::query);
execute("Q1",  Q1::query);
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
#ifdef COLUMNAR_STORE
       delete (ColumnarStore*)cluster.userData;
#else
       delete (CachedData*)cluster.userData;
#endif
#endif
       sleep(1);
       printf("Node %d finished.\n", (int)cluster.nodeId);
    }
};

int main(int argc, char* argv[])
{
    int i;
    size_t nThreads = 8;
    size_t bufferSize = 64*1024;
	size_t socketBufferSize = 64*1024*1024;
    size_t broadcastJoinThreshold = 10000;
    size_t split = 1;
    bool   sharedNothing = false;
	bool verbose = false;
    char const* option;
    
    fclose(fopen("tpch.start", "w")); // needed to innitialize stdio in single threaded environment

    for (i = 1; i < argc; i++) { 
        if (*argv[i] == '-') { 
            option = argv[i]+1;
            if (strcmp(option, "dir") == 0) { 
                dataDir = argv[++i];
            } else if (strcmp(option, "format") == 0) { 
                dataFormat = argv[++i];
            } else if (strcmp(option, "threads") == 0) { 
                nThreads = atol(argv[++i]);
            } else if (strcmp(option, "buffer") == 0) { 
                bufferSize = atol(argv[++i]);
            } else if (strcmp(option, "send-buffer") == 0) { 
                socketBufferSize = atol(argv[++i]);
            } else if (strcmp(option, "broadcast-threshold") == 0) { 
                broadcastJoinThreshold = atol(argv[++i]);
            } else if (strcmp(option, "shared-nothing") == 0) { 
                sharedNothing = atoi(argv[++i]) != 0;
             } else if (strcmp(option, "split") == 0) { 
                split = atoi(argv[++i]);
             } else if (strcmp(option, "verbose") == 0) { 
                verbose = atoi(argv[++i]) != 0;
            } else { 
                fprintf(stderr, "Unrecognized option %s\n", option);
              Usage:
                fputs("Usage: kraps [Options] NODE_ID N_NODES address1:port1  address2:port2...\n"
                      "Options:\n"
                      "-dir\tdata directory (.)\n"
                      "-format\tdata format: parquet, plain-file,... ()\n"
                      "-shared-nothing 0/1\tdata is located at executor nodes (1)\n"
                      "-verbose 0/1\tprint stage times (0)\n"
                      "-threads N\tnumber of concurrent threads (8)\n"
                      "-channels N\tnumber of channels (64)\n"
                      "-buffer SIZE\tbuffer size (64Kb)\n"
                      "-send-buffer SIZE\tsocket send buffer size (64Mb)\n"
                      "-broadcast-threshold SIZE\tbroadcast join threshold (10000)\n"
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
	if (argc == i + nNodes) {
        TPCHJob test(nodeId, nNodes, &argv[i], nThreads, bufferSize, socketBufferSize, broadcastJoinThreshold, sharedNothing, verbose, split);
        test.run();
    } else {      
        fprintf(stderr, "%d nodes expected, %d given\n", nNodes, i - argc);
        goto Usage;
    }
    return 0;
}
