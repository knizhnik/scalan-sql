extern "C" { 

#include <luajit-2.1/lua.h>
#include <luajit-2.1/luajit.h>

#include "helpers.h"

lua_State *InitLua();

int LuaKernelCall(lua_State *L, const char *fun,
                  void *query, int p1, int p2, int p3);

}
