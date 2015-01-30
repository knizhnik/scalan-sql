#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <sqlite3.h>


#define CHECK(op) do { int error_ = (op); if (error_ != SQLITE_OK) { fprintf(stderr, "%s:%d: %s failed with code %d\n", __FILE__, __LINE__, #op, error_); exit(1); } } while (0)

int main(int arc, char* argv[]) 
{
    sqlite3 *conn;
    sqlite3_stmt* stmt;
    int n_records = arc > 1 ? atoi(argv[1]) : 50000000;
    int i, j, sum = 0;
    time_t start;
    CHECK(sqlite3_open(":memory:", &conn));
    CHECK(sqlite3_exec(conn, "create table key_value (key integer, value varchar(20))", 0, 0, 0));
    CHECK(sqlite3_prepare_v2(conn, "insert into key_value (key,value) values (?,?)", -1, &stmt, NULL));

    start = time(NULL);
    for (i = 0; i < n_records; i++) {         
        char buf[16];
        int len = sprintf(buf, "%d", i);
        CHECK(sqlite3_bind_int(stmt, 1, i+1));
        CHECK(sqlite3_bind_text(stmt, 2, buf, len, NULL));
        while (sqlite3_step(stmt) == SQLITE_ROW) {}
        CHECK(sqlite3_reset(stmt));
    }
    sqlite3_finalize(stmt); 
    printf("Elapsed time for inserting %d records: %d seconds\n", n_records, (int)(time(NULL) - start));
    
    start = time(NULL);    
    CHECK(sqlite3_prepare_v2(conn, "select count(*) from key_value where value like '%123%' and key > 0", -1, &stmt, NULL));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int count = sqlite3_column_int(stmt, 0);
        printf("count = %d\n", count);
    }
    sqlite3_finalize(stmt);
    printf("Elapsed time for count query: %d seconds\n", (int)(time(NULL) - start));
    
    start = time(NULL);    
    CHECK(sqlite3_prepare_v2(conn, "select sum(key) from key_value", -1, &stmt, NULL));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int sum = sqlite3_column_int(stmt, 0);
        printf("sum = %d\n", sum);
    }
    sqlite3_finalize(stmt);
    printf("Elapsed time for aggregate query: %d seconds\n", (int)(time(NULL) - start));
    
    start = time(NULL);    
    CHECK(sqlite3_prepare_v2(conn, "select sum(key) from key_value where value like '1%'", -1, &stmt, NULL));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int sum = sqlite3_column_int(stmt, 0);
        printf("sum = %d\n", sum);
    }
    sqlite3_finalize(stmt);
    printf("Elapsed time for aggregate query with filter: %d seconds\n", (int)(time(NULL) - start));
    
    sqlite3_close(conn);
    return 0;
}
