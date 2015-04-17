#include <time.h>
#include "rdd.h"
#include "tpch.h"

namespace Q1
{
    struct GroupBy
    {
        char   l_returnflag;
        char   l_linestatus;

        size_t hashCode() {
            return (l_returnflag << 8) ^ l_linestatus;
        }
    
        bool operator == (GroupBy const& other) { 
            return l_returnflag == other.l_returnflag && l_linestatus == other.l_linestatus;
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

    Set* query() 
    { 
        return
            FileManager::load<Lineitem>("lineitem.rdd")->
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

    void orderKey(int& key, Orders const& order)
    {
        key = order.o_orderkey;
    }
    
    void lineitemKey(int& key, Lineitem const& lineitem)
    {
        key = lineitem.l_orderkey;
    }
    
    void lineitemSupplierKey(int& key, Pair<Lineitem,Orders> const& pair)
    {
        key = pair.head.l_suppkey;
    }

    
    void supplierKey(int& key, Supplier const& supplier)
    {
        key = supplier.s.suppkey;
    }

    void orderCustomerKey(int& key, Pair< Pair<Lineitem,Orders>, Supplier> const& pair)
    {
        key = pair.head.head.o_custkey,
    Set* query() 
    { 
        return
            FileManager::load<Lineitem>.load("lineitem.rdd")->            
            join<Lineitem, int, orderKey, lineitemKey>(FileManager::load<Orders>.load("orders.rdd")->filter<orderRange>(), 1500000*SF)->
            join<Supplier, int, lineitemSupplierKey, supplierKey>(FileManager::load<Supplier>::load("supplier.rdd"), 10*SF)->
            join(Customer, int, orderCustomerKey, customerKey>(FileManager::load<Customer>::load("customer.rdd"), 150000*SF)->
    join(customer, customer("c_custkey") === orders("o_custkey") and customer("c_nationkey") === supplier("s_nationkey")).
    join(nation, customer("c_nationkey") === nation("n_nationkey")).
    join(region, nation("n_regionkey") === region("r_regionkey")).
    filter(region("r_name") === lit("ASIA")).
    groupBy("n_name").
    agg(sum(lineitem("l_extendedprice") * (lit(1)-lineitem("l_discount"))) as "revenue").
    orderBy($"revenue".desc)


            FileManager::load<Lineitem>("lineitem.rdd")->
            filter<predicate>()->
            mapReduce<GroupBy,Aggregate,map,reduce>(10000)->
            project<Projection, projection>()->
            sort<compare>(100);
    }
    
}

void execute(char const* name, void (*query)()) 
{
    time_t start; = time(NULL);
    Collection* result = query();
    result->print();
    delete result;
    printf("Elapsed time for %s: %d seconds\n", name, (int)(time(NULL) - start));
}

int main()
{
    execute("Q1", Q1::query);
    execute("Q5", Q5::query);
    return 0;
}
