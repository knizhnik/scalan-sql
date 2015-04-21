#include <time.h>
#include "rdd.h"
#include "tpch.h"

const size_t SF = 1000;

namespace Q1
{
    struct GroupBy
    {
        char   l_returnflag;
        char   l_linestatus;

        bool operator == (GroupBy const& other) { 
            return l_returnflag == other.l_returnflag && l_linestatus == other.l_linestatus;
        }
    };

    size_t hashCode(GroupBy const& gby) {
        return (gby.l_returnflag << 8) ^ gby.l_linestatus;
    }
    
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

        void print(FILE* out) { 
            fprintf(out, "%c, %c, %f, %f, %f, %f, %f, %f, %f, %lu\n", 
                    l_returnflag, l_linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order);
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
            mapReduce<GroupBy,Aggregate,map,reduce>(10000)->
            project<Projection, projection>()->
            sort<compare>(100);
    }
}


namespace Q5
{
    bool orderRange(Orders const& orders) 
    {
        return orders.o_orderdate >= 19960101 && orders.o_orderdate < 19970101;
    }

    void orderKey(long& key, Orders const& order)
    {
        key = order.o_orderkey;
    }
    
    void lineitemOrderKey(long& key, Lineitem const& lineitem)
    {
        key = lineitem.l_orderkey;
    }
    
    void lineitemSupplierKey(int& key, Join<Lineitem,Orders> const& r)
    {
        key = r.l_suppkey;
    }

    
    void supplierKey(int& key, Supplier const& supplier)
    {
        key = supplier.s_suppkey;
    }

    void orderCustomerKey(int& key, Join<Join<Lineitem,Orders>,Supplier> const& r)
    {
        key = r.o_custkey;
    }

    void customerKey(int& key, Customer const& customer)
    {
        key = customer.c_custkey;
    }

    bool sameNation(Join<Join<Join<Lineitem,Orders>,Supplier>,Customer> const& r)
    {
        return r.c_nationkey == r.s_nationkey;
    }

    void customerNationKey(int& key, Join<Join<Join<Lineitem,Orders>,Supplier>,Customer> const& r)
    {
        key = r.c_nationkey;
    }
    
    void nationKey(int& key, Nation const& nation)
    {
        key = nation.n_nationkey;
    }
    
    void nationRegionKey(int& key, Join<Join<Join<Join<Lineitem,Orders>,Supplier>,Customer>, Nation> const& r)
    {
        key = r.n_regionkey;
    }
    
    void regionKey(int& key, Region const& region)
    {
        key = region.r_regionkey;
    }
    
    bool asiaRegion(Join<Join<Join<Join<Join<Lineitem,Orders>,Supplier>,Customer>,Nation>,Region> const& r) 
    { 
        return strcmp(r.r_name, "ASIA") == 0;

    }

    struct Name 
    { 
        name_t name;
        
        bool operator==(Name const& other) { 
            return strcmp(name, other.name) == 0;
        }
    };
 
    size_t hashCode(Name const& key)
    {
        return ::hashCode(key.name);
    }

    void map(Pair<Name,double>& pair, Join<Join<Join<Join<Join<Lineitem,Orders>,Supplier>,Customer>,Nation>,Region> const& r)
    {
        strcpy(pair.key.name, r.n_name);
        pair.value = r.l_extendedprice * (1 - r.l_discount);
    }

    void reduce(double& dst, double const& src) 
    {
        dst += src;
    }

    struct Revenue 
    {
        name_t n_name;
        double revenue;

        void print(FILE* out) { 
            printf("%s, %f\n", n_name, revenue);
        }
    };

    void revenue(Revenue& out, Pair<Name,double> const& in)
    {
        strcpy(out.n_name, in.key.name);
        out.revenue = in.value;
    }

    int byRevenue(Revenue const* a, Revenue const* b) 
    {
        return a->revenue < b->revenue ? -1 : a->revenue == b->revenue ? 0 : 1;
    }

    RDD<Revenue>* query() 
    { 
        return
            FileManager::load<Lineitem>("lineitem")->            
            join<Orders, long, lineitemOrderKey, orderKey>(FileManager::load<Orders>("orders")->filter<orderRange>(), 1500000*SF)->
            join<Supplier, int, lineitemSupplierKey, supplierKey>(FileManager::load<Supplier>("supplier"), 10000*SF)->
            join<Customer, int, orderCustomerKey, customerKey>(FileManager::load<Customer>("customer"), 150000*SF)->
            filter<sameNation>()->
            join<Nation, int, customerNationKey, nationKey>(FileManager::load<Nation>("nation"), 25)->
            join<Region, int, nationRegionKey, regionKey>(FileManager::load<Region>("region"), 5)->
            filter<asiaRegion>()->
            mapReduce<Name, double, map, reduce>(25)->
            project<Revenue, revenue>()->
            sort<byRevenue>(25);
    }    
}

template<class T>
void execute(char const* name, RDD<T>* (*query)()) 
{
    time_t start = time(NULL);
    Cluster::instance->reset();
    RDD<T>* result = query();
    result->print(stdout);
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
    execute("Q1", Q1::query);
    execute("Q5", Q5::query);
    printf("Node %d finished.\n", nodeId);
    return 0;
}
