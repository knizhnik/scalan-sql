#pragma once

#include "helpers.h"
#include "lua_bridge.h"

int luaGetSqlTables(lua_State *L);
int luaGetSqlTableColumns(lua_State *L);
int luaGetKernelParameters(lua_State *L);

