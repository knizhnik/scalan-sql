#pragma once

#include "helpers.h"
#include "lua_bridge.h"

class MySQLCursor;
class Engine;

typedef unsigned date_t;

extern "C" { 

int  mysqlNextRecord(MySQLCursor* cursor, int* eof);
void mysqlGetString(MySQLCursor* cursor, int columnNo, flexistring *fstr); 
void mysqlGetInt(MySQLCursor* cursor, int columnNo, int* dst); 
void mysqlGetDouble(MySQLCursor* cursor, int columnNo, double* dst); 
void mysqlGetDate(MySQLCursor* cursor, int columnNo, date_t* dst); 
MySQLCursor* mysqlGetCursor(Engine* engine, int k);
MySQLResult* mysqlResultCreate(Engine* engine);
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
