#include <luajit-2.1/lua.h>
#include <luajit-2.1/luajit.h>

#include "helpers.h"

lua_State *InitLua();

int LuaKernelCall(lua_State *L, const char *fun,
                  void *pVdbe, int p1, int p2, int p3);

bool LuaCallCommonVdbeBuilder(lua_State *L, Parse *pParse, Select *pSel, const char *kernel_file, int kernel_id);
