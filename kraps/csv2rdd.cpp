#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "tpch.h"


int split(char* columns[], char* buf, char sep)
{
    char *p = buf, *q;
    int i;
    for (i = 0; (q = strchr(p, sep)) != NULL; p = q + 1, i++) {
        *q = '\0';
        columns[i] = p;
    }
    return i;
}

date_t parseDate(char const* str) 
{ 
    int y, m, d;
    int rc = sscanf(str, "%d-%d-%d", &y, &m, &d);
    assert(rc == 3);
    return y*10000 + m*100 + d;
}

void openShards(FILE** files, size_t nShards, char const* name)
{
    char path[256];
    if (nShards == 1) { 
        sprintf(path, "%s.rdd", name);
        files[0] = fopen(path, "wb");
        assert(files[0] != NULL);
    } else { 
        mkdir(name, 0777);
        for (size_t i = 0; i < nShards; i++) { 
            sprintf(path, "%s/%ld.rdd", name, i);
            files[i] = fopen(path, "wb");
            assert(files[i] != NULL);
        }
    }
}

void closeShards(FILE** files, size_t nShards)
{
    for (size_t i = 0; i < nShards; i++) { 
        fclose(files[i]);
    }
}
int main(int argc, char* argv[]) 
{ 
    char buf[1024];
    char* columns[64];
    FILE* in;
    size_t nShards = argc < 2 ? 1 : atoi(argv[1]);
    FILE** out = new FILE*[nShards];
    size_t i;

    in = fopen("lineitem.tbl", "r");
    if (in != NULL) { 
        openShards(out, nShards, "lineitem");
        Lineitem lineitem;
        
        for (i = 0; fgets(buf, sizeof buf, in); i++) { 
            int nColumns = split(columns, buf, '|');
            assert(nColumns == 16);
            lineitem.l_orderkey = atol(columns[0]);
            lineitem.l_partkey = atoi(columns[1]);
            lineitem.l_suppkey = atoi(columns[2]);
            lineitem.l_linenumber = atoi(columns[3]);
            lineitem.l_quantity = atof(columns[4]);
            lineitem.l_extendedprice = atof(columns[5]);
            lineitem.l_discount = atof(columns[6]);
            lineitem.l_tax = atof(columns[7]);
            lineitem.l_returnflag = *columns[8];
            lineitem.l_linestatus = *columns[9];
            lineitem.l_shipdate = parseDate(columns[10]);
            lineitem.l_commitdate = parseDate(columns[11]);
            lineitem.l_receiptdate = parseDate(columns[12]);
            strncpy(lineitem.l_shipinstruct, columns[13], sizeof(lineitem.l_shipinstruct));
            strncpy(lineitem.l_shipmode, columns[14], sizeof(lineitem.l_shipmode));
            strncpy(lineitem.l_comment, columns[15], sizeof(lineitem.l_comment));
            size_t rc = fwrite(&lineitem, sizeof(lineitem), 1, out[i % nShards]);
            assert(rc == 1);
        }
        printf("Load %ld lineitems\n", i);
        fclose(in);
        closeShards(out, nShards);
    }

    in = fopen("orders.tbl", "r");
    if (in != NULL) { 
        openShards(out, nShards, "orders");
        Orders orders;
        
        for (i = 0; fgets(buf, sizeof buf, in); i++) { 
            int nColumns = split(columns, buf, '|');
            assert(nColumns == 9);
            orders.o_orderkey = atol(columns[0]);
            orders.o_custkey = atoi(columns[1]);
            orders.o_orderstatus = *columns[2];
            orders.o_totalprice = atof(columns[3]);
            orders.o_orderdate = parseDate(columns[4]);
            strncpy(orders.o_orderpriority, columns[5], sizeof orders.o_orderpriority);
            strncpy(orders.o_clerk, columns[6], sizeof orders.o_clerk);
            orders.o_shippriority = atoi(columns[7]);
            strncpy(orders.o_comment, columns[8], sizeof(orders.o_comment));
            size_t rc = fwrite(&orders, sizeof(orders), 1, out[i % nShards]);
            assert(rc == 1);
        }
        printf("Load %ld orders\n", i);
        fclose(in);
        closeShards(out, nShards);
    }

    in = fopen("customer.tbl", "r");
    if (in != NULL) { 
        openShards(out, nShards, "customer");
        Customer customer;
        
        for (i = 0; fgets(buf, sizeof buf, in); i++) { 
            int nColumns = split(columns, buf, '|');
            assert(nColumns == 8);
            customer.c_custkey = atoi(columns[0]);
            strncpy(customer.c_name, columns[1], sizeof customer.c_name);
            strncpy(customer.c_address, columns[2], sizeof customer.c_address);
            customer.c_nationkey = atoi(columns[3]);
            strncpy(customer.c_phone, columns[4], sizeof customer.c_phone);
            customer.c_acctball = atof(columns[5]);
            strncpy(customer.c_mktsegment, columns[6], sizeof customer.c_mktsegment);
            strncpy(customer.c_comment, columns[7], sizeof customer.c_comment);
            size_t rc = fwrite(&customer, sizeof(customer), 1, out[i % nShards]);
            assert(rc == 1);
        }
        printf("Load %ld customers\n", i);
        fclose(in);
        closeShards(out, nShards);
    }

    in = fopen("supplier.tbl", "r");
    if (in != NULL) { 
        openShards(out, nShards, "supplier");
        Supplier supplier;
        
        for (i = 0; fgets(buf, sizeof buf, in); i++) { 
            int nColumns = split(columns, buf, '|');
            assert(nColumns == 7);
            supplier.s_suppkey = atoi(columns[0]);
            strncpy(supplier.s_name, columns[1], sizeof supplier.s_name);
            strncpy(supplier.s_address, columns[2], sizeof supplier.s_address);
            supplier.s_nationkey = atoi(columns[3]);
            strncpy(supplier.s_phone, columns[4], sizeof supplier.s_phone);
            supplier.s_acctbal = atof(columns[5]);
            strncpy(supplier.s_comment, columns[6], sizeof supplier.s_comment);
            size_t rc = fwrite(&supplier, sizeof(supplier), 1, out[i % nShards]);
            assert(rc == 1);
        }
        printf("Load %ld suppliers\n", i);
        fclose(in);
        closeShards(out, nShards);
    }

    in = fopen("partsupp.tbl", "r");
    if (in != NULL) { 
        openShards(out, nShards, "partsupp");
        Partsupp partsupp;
        
        for (i = 0; fgets(buf, sizeof buf, in); i++) { 
            int nColumns = split(columns, buf, '|');
            assert(nColumns == 5);
            partsupp.ps_partkey = atoi(columns[0]);
            partsupp.ps_suppkey = atoi(columns[1]);
            partsupp.ps_availqty = atoi(columns[2]);
            partsupp.ps_supplycost = atoi(columns[3]);
            strncpy(partsupp.ps_comment, columns[4], sizeof partsupp.ps_comment);
            size_t rc = fwrite(&partsupp, sizeof(partsupp), 1, out[i % nShards]);
            assert(rc == 1);
        }
        printf("Load %ld partsupps\n", i);
        fclose(in);
        closeShards(out, nShards);
    }

    in = fopen("region.tbl", "r");
    if (in != NULL) { 
        openShards(out, nShards, "region");
        Region region;
        
        for (i = 0; fgets(buf, sizeof buf, in); i++) { 
            int nColumns = split(columns, buf, '|');
            assert(nColumns == 3);
            region.r_regionkey = atoi(columns[0]);
            strncpy(region.r_name, columns[1], sizeof region.r_name);
            strncpy(region.r_comment, columns[2], sizeof region.r_comment);
            size_t rc = fwrite(&region, sizeof(region), 1, out[i % nShards]);
            assert(rc == 1);
        }
        printf("Load %ld regions\n", i);
        fclose(in);
        closeShards(out, nShards);
    }

    in = fopen("nation.tbl", "r");
    if (in != NULL) { 
        openShards(out, nShards, "nation");
        Nation nation;
        
        for (i = 0; fgets(buf, sizeof buf, in); i++) { 
            int nColumns = split(columns, buf, '|');
            assert(nColumns == 4);
            nation.n_nationkey = atoi(columns[0]);
            strncpy(nation.n_name, columns[1], sizeof nation.n_name);
            nation.n_regionkey = atoi(columns[2]);
            strncpy(nation.n_comment, columns[3], sizeof nation.n_comment);
            size_t rc = fwrite(&nation, sizeof(nation), 1, out[i % nShards]);
            assert(rc == 1);
        }
        printf("Load %ld nations\n", i);
        fclose(in);
        closeShards(out, nShards);
    }

    in = fopen("part.tbl", "r");
    if (in != NULL) { 
        openShards(out, nShards, "part");
        Part part;
        
        for (i = 0; fgets(buf, sizeof buf, in); i++) { 
            int nColumns = split(columns, buf, '|');
            assert(nColumns == 9);
            part.p_partkey = atoi(columns[0]);
            strncpy(part.p_name, columns[1], sizeof part.p_name);
            strncpy(part.p_mfgr, columns[2], sizeof part.p_mfgr);
            strncpy(part.p_brand, columns[3], sizeof part.p_brand);
            strncpy(part.p_type, columns[4], sizeof part.p_type);
            part.p_size = atoi(columns[5]);
            strncpy(part.p_container, columns[6], sizeof part.p_container);
            part.p_retailprice = atof(columns[7]);
            strncpy(part.p_comment, columns[8], sizeof part.p_comment);
            size_t rc = fwrite(&part, sizeof(part), 1, out[i % nShards]);
            assert(rc == 1);
        }
        printf("Load %ld parts\n", i);
        fclose(in);
        closeShards(out, nShards);
    }

    return 0;
}

