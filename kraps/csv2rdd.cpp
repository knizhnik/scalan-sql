#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
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

int main() 
{ 
    FILE* in = fopen("lineitem.tab", "r");
    FILE* out = fopen("lineitem.rdd", "wb");
    char buf[1024];
    char* columns[64];
    Lineitem lineitem;

    while (fgets(buf, sizeof buf, in)) { 
        int nColumns = split(columns, buf, '|');
        assert(nColumns == 16);
        lineitem.l_orderkey = atoi(columns[0]);
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
        int rc = fwrite(&lineitem, 1, sizeof(lineitem), out);
        assert(rc == 1);
    }
    fclose(in);
    fclose(out);
    return 0;
}

