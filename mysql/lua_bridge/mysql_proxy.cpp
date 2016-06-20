#include "jit_switch.h"

#include <select_printer.h>
#include <select_hasher.h>
#include <select_comparator.h>
#include <select_duplicator.h>

#include "tst_queries.h" // TODO: just sample, get rid of

typedef struct {
    int kernel_id;
    u32 hash;

    char *kernel_name;
    char *query_text;
    char *kernel_code;

    Select *tree;
} SqlKernel;

#define MAX_KERNELS 1000

static int g_num_kernels = 0;
static SqlKernel g_kernels[MAX_KERNELS] = {};
static ParamArray *s_params;

static bool s_jit_enabled = 1;
static bool s_print_select_tree = 0;

void SetJitEnabled(bool s) {
    s_jit_enabled = s;
}

void SetPrintSelectTree(bool s) {
    s_print_select_tree = s;
}

void registerKernel(int k_id, const char *kernel_name, const char *query_text, const char *kernel_code) {
    if (g_num_kernels >= MAX_KERNELS) {
        JERR(READER, "Can't register kernel, no free slots");
        return;
    }

    SqlKernel *kern = &g_kernels[g_num_kernels];
    memset(kern, 0, sizeof(*kern));

    kern->kernel_id = k_id;
    kern->kernel_name = strdup(kernel_name);
    kern->query_text = strdup(query_text); // TODO: currently kernels are not deleted and these strings
    kern->kernel_code = strdup(kernel_code); // are not freed

    if (!kern->query_text || !kern->kernel_code) {
        free(kern->kernel_name);
        free(kern->query_text);
        free(kern->kernel_code);
        JERR(READER, "Can't register kernel, no memory");
    }

    g_num_kernels++;
}

// Helpers for lua kernel
// ones prefixed with 'lua' are standard Lua API calls(needed to pass tables to lua side)
// ones prefixed with 'lj' are regular C functions designed to be called through LUAJIT FFI

//returns a lua table describing sql tables
int luaGetSqlTables(lua_State *L) {
    assert(lua_islightuserdata(L,1));
    JOIN* join = (JOIN*)lua_touserdata(L, 1);

    lua_newtable(L);
    int tbl = lua_gettop(L);

    for (int i=0; i < join->table_count; i++) {
        TABLE *t = join->table[i];
        lua_pushstring(L, t->s->table_name.str);
        lua_newtable(L);
        int dtab = lua_gettop(L);
        lua_pushstring(L, "id");
        lua_pushinteger(L, i);
        lua_settable(L, dtab);
        lua_pushstring(L, "ptr");
        lua_pushlightuserdata(L, t);
        lua_settable(L, dtab);
        lua_settable(L, tbl);
    }

    return 1;
}

//returns a lua table describing sql table columns
int luaGetSqlTableColumns(lua_State *L) {
    assert(lua_islightuserdata(L,1));
    Select *pSel = lua_touserdata(L, 1);

    lua_newtable(L);
    int tbl = lua_gettop(L);

    for (int i=0; i < join->table_count; i++) {
        const TABLE *t = join->table[i];
        lua_pushstring(L, t->s->table_name.str);
        lua_newtable(L);
        int col_tbl = lua_gettop(L);
        Field** field = t->field;
        for (int j = 0; t->field[j]; j++) {
            lua_pushstring(L, t->field[j]->field_name);
            lua_pushinteger(L, j);
            lua_settable(L, col_tbl);
        }
        lua_settable(L, tbl);
    }

    return 1;
}

// Reads Parameter table from s_params static structure
// FIXME, better explicitly pass them, but no spare slots in vdbe fit
int luaGetKernelParameters(lua_State *L) {
    int i;
    lua_newtable(L);
    int tbl = lua_gettop(L);
    if (!s_params) return;
    for (i = 0; i < s_params->num; i++) {
        lua_pushinteger(L, i+1);
        switch(s_params->arr[i].kind) {
        case PAR_INT:
            lua_pushinteger(L, s_params->arr[i].u.i);
            break;
        case PAR_STR:
            lua_pushstring(L, s_params->arr[i].u.str);
            break;
        default:
            assert(false); // unknown paramerer
        };
        lua_settable(L, tbl);
    }
    return 1;
}
