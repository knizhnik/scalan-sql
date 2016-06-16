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

static SqlKernel *s_learned_kernel = NULL;

void learnModeOn(SqlKernel *kern) {
    s_learned_kernel = kern;
}

void learnModeOff() {
    s_learned_kernel = NULL;
}

// read kernels from db table
void readActiveJitKernels(sqlite3 *db) {
    assert(db);
    JLOG(READER, "hello from readActiveJitKernels");
    const char *zSql = "SELECT kernel_id, kernel_name, query_text, kernel_code FROM jit_kernels WHERE active=1 order by gen_date desc;";
    sqlite3_stmt *pStmt = NULL;
    int res = sqlite3_prepare_v2(db, zSql, -1, &pStmt, NULL);
    if (res != SQLITE_OK) {
        sqlite3Error(db, SQLITE_OK);
        JERR(READER, "got error preparing kernel reading query");
        return;
    }
    assert(pStmt);

    while(true) {
        res = sqlite3_step(pStmt);
        if (res != SQLITE_ROW) break;

        int kernel_id = sqlite3_column_int(pStmt, 0);
        const char *kernel_name = sqlite3_column_text(pStmt, 1);
        const char *query_text = sqlite3_column_text(pStmt, 2);
        const char *kernel_code = sqlite3_column_text(pStmt, 3);
        if (query_text && kernel_code) {
            registerKernel(kernel_id, kernel_name, query_text, kernel_code);
            JLOG(READER, "Registered kernel: id %d, name %s", kernel_id, kernel_name);
        } else {
            JLOG(READER, "Skipped semi-filled kernel: id %d, name %s", kernel_id, kernel_name);
        }
    }

    sqlite3_finalize(pStmt);
}

// Detects if it's an auxiliary select originating from depths of engine
// these should be skipped
static bool isSystemSelect(const Select *p) {
    return !p->pSrc || p->pSrc->nSrc < 1 || !p->pSrc->a[0].zName || strcmp(p->pSrc->a[0].zName, MASTER_NAME) == 0 || strcmp(p->pSrc->a[0].zName, TEMP_MASTER_NAME) == 0;
}

// learns query if learning mode is active
bool tryLearnSelectSignature(const Select *p) {
    if (isSystemSelect(p)) {
        JLOG(LEARNER, "skipping name resolving query");
        return false;
    }

    if (s_print_select_tree) {
        fprintf(stderr, "select = ");
        PrintSelect(p);
    }

    if (s_learned_kernel) {
        MEASURE(dt);
        s_learned_kernel->tree = duplicate_Select(p, NULL);
        s_learned_kernel->hash = HashSelect(p);
        REPORT(dt, "signature learned in ");
        return true;
    }
}

// Injects a sample kernel for 'select avg(x) from y;'
static bool runVdbeInjector(Parse *pParse, Select *pSel, unsigned int k_idx) {
    assert(k_idx < g_num_kernels);

    MEASURE(li);
    lua_State *L = InitLua();
    REPORT(li, "lua init");

    MEASURE(inj);
    LuaCallCommonVdbeBuilder(L, pParse, pSel, g_kernels[k_idx].kernel_code, g_kernels[k_idx].kernel_id);
    REPORT(inj, "vdbe_builder run time");
    return true;
}

// detects jittable queries and injects the kernel
bool tryJitSelect(Parse *pParse, Select *p) {
    if (s_learned_kernel) goto skip; // dont even try to jit in learning mode
    if (!s_jit_enabled) goto skip;
    if (g_num_kernels == 0) goto skip;

    if (isSystemSelect(p)) {
        JLOG(PATCHER, "skipping name resolving query");
        goto skip;
    }

    MEASURE(ht);
    u32 hsum = HashSelect(p);
    REPORT(ht, "hash time");
    JLOG(PATCHER, "select_hash = 0x%08x\n", hsum);

    int i;
    for (i=0; i<g_num_kernels; i++) {
        assert(g_kernels[i].tree);
        if (hsum == g_kernels[i].hash) {
            JLOG(PATCHER, "hash hit for %d", i);
            MEASURE(ct);
            int cmp_res = CompareSelect(p, g_kernels[i].tree, &s_params);

            REPORT(ct, "compare time");
            if (cmp_res) {
                if (g_kernels[i].kernel_id > 0) {
                    return runVdbeInjector(pParse, p, i);
                } else {
                    JLOG(PATCHER, "null injector, skipping");
                    goto skip;
                }
            }
        }
    }

skip:
    return false;
}

// Parses SQL queries interrupting generated ASTs and store them, called on db open
void runQueryLearner(sqlite3 *db) {
    int i;
    for (i=0; i<g_num_kernels; i++) {
        SqlKernel *K = &g_kernels[i];
        if (K->tree == NULL) {
            learnModeOn(K);
            // do this to grab parsed tree
            sqlite3_stmt *pStmt;    /* OUT: A pointer to the prepared statement */
            const char *zTail;       /* OUT: End of parsed string */

            int res = sqlite3_prepare_v2(db, K->query_text, strlen(K->query_text), &pStmt, &zTail);
            if (res == SQLITE_ERROR) {
                sqlite3Error(db, SQLITE_OK);
                JERR(LEARNER, "can't learn the kernel id %d", K->kernel_id);
            } else {
                JLOG(LEARNER, "learned kernel id %d, name %s, hash 0x%x", K->kernel_id, K->kernel_name, K->hash);
            }

        }
    }
    learnModeOff();
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

// Requests to generate code to lock table
void ljGenReadLock(Parse *pParse, Table *pTab) {
    const int iDb = sqlite3SchemaToIndex(pParse->db, pTab->pSchema);
    JLOG(PATCHER, "idb=%d", iDb);

    sqlite3CodeVerifySchema(pParse, iDb);
    sqlite3TableLock(pParse, iDb, pTab->tnum, 0, pTab->zName);
}

// Registers a table cursor in VM
int ljGenCursorOpen(Parse *pParse, Table *pTab, int maxCol) {
    const int iDb = sqlite3SchemaToIndex(pParse->db, pTab->pSchema);
    const int iCsr = pParse->nTab++;
    Vdbe *v = sqlite3GetVdbe(pParse);
    JLOG(PATCHER, "iCsr=%d", iCsr);

    sqlite3VdbeAddOp4Int(v, OP_OpenRead, iCsr, pTab->tnum, iDb, maxCol);
    return iCsr;
}

// Calls a lua kernel passing it three integer arguments(through VdbeOp args P1,P2,P3)
void ljGenKernelCall(Parse *pParse, const char *kernel_name, int arg1, int arg2, int arg3) {
    Vdbe *v = sqlite3GetVdbe(pParse);
    sqlite3VdbeAddOp4(v, OP_RunLuaKernel, arg1, arg2, arg3, kernel_name, P4_STATIC);
}

// Reserve some Mem cells
int ljReserveMem(Parse *pParse, int n_cells) {
    assert(n_cells >= 1);
    Vdbe *v = sqlite3GetVdbe(pParse);
    const int iMem = pParse->nMem+1;
    pParse->nMem += n_cells;
    return iMem;
}

// Generates a single result row
void ljGenResultRow(Parse *pParse, int place, int n_results) {
    Vdbe *v = sqlite3GetVdbe(pParse);
    sqlite3VdbeAddOp2(v, OP_ResultRow, place, n_results);
    sqlite3VdbeSetNumCols(v, n_results);
}

// Closes cursor
void ljGenCursorClose(Parse *pParse, int iCsr) {
    Vdbe *v = sqlite3GetVdbe(pParse);
    sqlite3VdbeAddOp1(v, OP_Close, iCsr);
}

// Make fresh label
int ljMakeLabel(Parse *pParse) {
    Vdbe *v = sqlite3GetVdbe(pParse);
    return sqlite3VdbeMakeLabel(v);
}

// Resolve label to point to next to be encoded instruction
void ljResolveLabel(Parse *pParse, int x) {
    Vdbe *v = sqlite3GetVdbe(pParse);
    sqlite3VdbeResolveLabel(v, x);
}

// loads val into cell
void ljGenInteger(Parse *pParse, int cell, int val) {
    Vdbe *v = sqlite3GetVdbe(pParse);
    sqlite3VdbeAddOp2(v, OP_Integer, val, cell);
}

// Generates unconditional goto
void ljGenGoto(Parse *pParse, int dest) {
    Vdbe *v = sqlite3GetVdbe(pParse);
    sqlite3VdbeGoto(v, dest);
}
    // Generates conditional goto to label if cell if positive, optionally decrements by decr
void ljGenGotoIfPos(Parse *pParse, int cell, int label, int decr) {
    Vdbe *v = sqlite3GetVdbe(pParse);
    sqlite3VdbeAddOp3(v, OP_IfPos, cell, label, decr);
}

