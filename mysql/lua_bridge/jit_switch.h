#pragma once

#include "sqliteInt.h"

#include "helpers.h"
#include "lua_bridge.h"

bool tryLearnSelectSignature(const Select *p);
bool tryJitSelect(Parse *pParse, Select *p);

void runQueryLearner(sqlite3 *db);
void readActiveJitKernels(sqlite3 *db);

// helpers for lua
int luaGetSqlTables(lua_State *L);
int luaGetSqlTableColumns(lua_State *L);

int luaGetKernelParameters(lua_State *L);
// Switches
void SetJitEnabled(bool s);
void SetPrintSelectTree(bool s);
