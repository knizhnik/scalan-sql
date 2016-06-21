#pragma once

#include <luajit-2.1/lua.h>
#include <luajit-2.1/luajit.h>

#include "helpers.h"

lua_State* LuaInitialize();

int LuaKernelCall(lua_State *L, const char *fun, void *query, char const* code);

