#include <stdio.h>
#include "lua_bridge.h"

static inline void Die(const char *msg) {
    fprintf(stderr, "Error: %s\n", msg);
//    exit(2);
}

#ifdef COMPILED_LUA
#include "kernel_runner_lua.h"
#include "iter_lua.h"
#include "oop_lua.h"
#endif

#define MOD_SYMBOL(M) luaJIT_BC_ ## M
#define MOD_SIZE(M) luaJIT_BC_ ## M ## _SIZE
#define MOD_FILE(M) "src/lua/" #M ".lua"
#define MOD_NAME(M) #M "_bytecode"

#ifdef COMPILED_LUA
#define LOAD_BYTECODE(L,M) luaL_loadbuffer(L, MOD_SYMBOL(M), MOD_SIZE(M), MOD_NAME(M))
#else
#define LOAD_BYTECODE(L,M) luaL_loadfile(L, MOD_FILE(M))
#endif

static int lua_initializer(lua_State *L) {
    luaopen_debug(L);
    luaopen_base(L);
    luaopen_io(L);
    luaopen_bit(L);
    luaopen_jit(L);
    luaopen_os(L);
    luaopen_string(L);
    luaopen_math(L);
    luaopen_ffi(L);
    luaopen_package(L);
    luaopen_table(L);

    int res = luaJIT_setmode(L, 0, LUAJIT_MODE_ON);

    luaL_findtable(L, LUA_REGISTRYINDEX, "_PRELOAD",
                   2);

    if (LOAD_BYTECODE(L, iter)) {
        fprintf(stderr, "cannot load: %s",
                lua_tostring(L, -1));
    }
    lua_setfield(L, -2, "iter");

    if (LOAD_BYTECODE(L, oop)) {
        fprintf(stderr, "cannot load: %s",
                lua_tostring(L, -1));
    }
    lua_setfield(L, -2, "oop");

    lua_pop(L, 1);

    // TODO: two loads should probably be separated
    if (LOAD_BYTECODE(L, kernel_runner) || lua_pcall(L, 0, 0, 0)) {
        fprintf(stderr, "cannot load: %s",
                lua_tostring(L, -1));
        Die("init failed");
    }

    if (LOAD_BYTECODE(L, vdbe_builder) || lua_pcall(L, 0, 0, 0)) {
        fprintf(stderr, "cannot load: %s",
                 lua_tostring(L, -1));
        Die("init failed");
    }

    return 0;
}

// initialize single global LUAJIT instance
lua_State *LuaInitialize() {
    static lua_State *L;
    if (!L) {
        L = lua_open();
        int status = lua_cpcall(L, lua_initializer, NULL);
    }
    return L;
}

// Universal means to call lua in OP
int LuaKernelCall(lua_State *L, const char *fun, void *query, char const *code) {
    lua_getglobal(L, fun);  /* function to be called */

    const int n_args = 1;
    lua_pushlightuserdata(L, param);
    lua_pushstring(L, code);
    
    if (lua_pcall(L, n_args, 1, 0) != 0) {
        fprintf(stderr,"error running function '%s': %s\n",
                fun,
                lua_tostring(L, -1));
        return -1;
    }

    int z = -1;
    /* retrieve result */
    if (!lua_isnumber(L, -1)) {
        fprintf(stderr, "function '%s' must return a number\n", fun);
    } else {
        z = lua_tonumber(L, -1);
    }
    lua_pop(L, 1);

    return z;
}
