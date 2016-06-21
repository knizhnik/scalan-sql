#pragma once

struct MySQLCursor;
struct Query;

extern "C" { 

#include "helpers.h"
#include "lua_bridge.h"

int  mysqlNextRecord(MySQLCursor* cursor, int* eof);
void mysqlGetString(MySQLCursor* cursor, int columnNo, flexistring *fstr); 
void mysqlGetChunk(MySQLCursor* cursor, int columnNo, str_chunk* chunk); 
void mysqlGetInt(MySQLCursor* cursor, int columnNo, int* dst); 
void mysqlGetDouble(MySQLCursor* cursor, int columnNo, double* dst); 
void mysqlGetDate(MySQLCursor* cursor, int columnNo, date_t* dst); 
MySQLCursor* mysqlGetCursor(Query* query, int k);
MySQLResult* mysqlResultCreate(Query* query);
void mysqlResetSend(MySQLResultSet* result);
void mysqlResultWriteInt(MySQLResultSet* result, int val);
void mysqlResultWriteDate(MySQLResultSet* result, date_t val);
void mysqlResultWriteDouble(MySQLResultSet* result, double val);
void mysqlResultWriteString(MySQLResultSet* result, const char *str);
void mysqlResultEnd(MySQLResultSet* result);

int mysqlGetTables(lua_State *L);
int mysqlGetTableColumns(lua_State *L);
int mysqlGetKernelParameters(lua_State *L);

};
