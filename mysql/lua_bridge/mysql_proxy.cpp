#include "mysql_proxy.h"
#include "engine.h"

// Helpers for lua kernel
// ones prefixed with 'lua' are standard Lua API calls(needed to pass tables to lua side)
// ones prefixed with 'lj' are regular C functions designed to be called through LUAJIT FFI

//returns a lua table describing sql tables
int mysqlGetTables(lua_State *L) {
    assert(lua_islightuserdata(L,1));
    JOIN* join = ((Query*)lua_touserdata(L, 1))->join;

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
int mysqlGetTableColumns(lua_State *L) 
{
    assert(lua_islightuserdata(L,1));
    JOIN* join = ((Query*)lua_touserdata(L, 1))->join;

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

int mysqlGetKernelParameters(lua_State *L) 
{
    int i;
    lua_newtable(L);
    int tbl = lua_gettop(L);
    Query* query = (Query*)lua_touserdata(L, 1);
    for (i = 0; i < query->params.size(); i++) {
        lua_pushinteger(L, i+1);
        switch(query->params[i].type) {
        case QueryParam::PARAM_INT:
            lua_pushinteger(L, (int)query->params[i].ival);
            //lua_pushlong(L, query->params[i].ival);
            break;
        case QueryParam::PARAM_REAL:
            lua_pushdouble(L, query->params[i].ival);
            break;
        case QueryParam::PARAM_STRING:
            lua_pushstring(L, query->params[i].sval.c_str());
            break;
        default:
            assert(false); // unknown paramerer
        };
        lua_settable(L, tbl);
    }
    return 1;
}


int mysqlNextRecord(MySQLCursor* cursor, int* eof)
{
	while (true) { 
		int rc = table->file->ha_rnd_next(table->record);
		if (rc == HA_ERR_END_OF_FILE) { 
			*eof = true;
			return 0;
		} else if (rc == HA_ERR_RECORD_DELETED) { 
			continue;
		} else { 
			*eof = false;
			return rc;
		}
	}
}

void mysqlGetString(MySQLCursor* cursor, int columnNo, flexistring *fstr) 
{ 
	cursor->fields[columnNo]->val_str(&cursor->strings[columnsNo].str);
	fstr->materialized = true;
	fstr->mem.buf.ptr = &cursor->strings[columnsNo].str[0];
}

void mysqlGetInt(MySQLCursor* cursor, int columnNo, int* dst) 
{
	*dst = (int)cursor->fields[columnNo]->val_int();
}

void mysqlGetDouble(MySQLCursor* cursor, int columnNo, double* dst) 
{
	*dst = cursor->fields[columnNo]->val_real();
}

void mysqlGetDate(MySQLCursor* cursor, int columnNo, xdate_t* dst) 
{
	MYSQL_TIME t;
	cursor->fields[columnNo]->get_date(&t, TIME_NO_ZERO_DATE);
	*dst = (date_t)TIME_to_ulonglong(&t);
}

MySQLCursor* mysqlGetCursor(JOIN *join, int k)
{
	return new MySQLCursor(join->table[k]);
} 

MySQLResult* mysqlResultCreate(JOIN* join)
{
	return new MySQLResult(join);
}

void mysqlResetSend(MySQLResultSet* result)
{
	result->join->result->send_data(result->list);
	result->list.empty();
}

void mysqlResultWriteInt(MySQLResultSet* result, int val)
{
	result->list.push_back(new (result->join->thd->mem_root) Item_int(result->join->thd, val), join->thd->mem_root);}

void mysqlResultWriteDate(MySQLResultSet* result, date_t val)
{
	MYSQL_TIME t;
	unpack_time(&t, val);
	result->list.push_back(new (result->join->thd->mem_root) Item_date_literal(result->join->thd, &t), join->thd->mem_root);
}

void mysqlResultWriteDouble(MySQLResultSet* result, double val)
{
	result->list.push_back(new (result->join->thd->mem_root) Item_float(result->join->thd, val), join->thd->mem_root);
}

void mysqlResultWriteString(MySQLResultSet* result, const char *str)
{
	result->list.push_back(new (result->join->thd->mem_root) Item_string(result->join->thd, str, strlen(str), system_charset_info), join->thd->mem_root);}
}

void mysqlResultEnd(MySQLResultSet* result)
{
	result->join->send_eof();
	delete result;
}
