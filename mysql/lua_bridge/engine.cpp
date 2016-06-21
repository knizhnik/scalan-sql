#include "engine.h"

bool Engine::run(JOIN* join)
{												
	LEX_STRING* sql = thd_query_string(join->thd);
	if (sql != NULL && sql->str != NULL) { 
		Query query(join);
		string canonical_sql = generalize_query(sql->str, query.params);
		string* code = cache.find(canonical_sql);
		if (code != NULL) { 
			lua_State* L = LuaInitialize();
			LuaKernelCall(L, "kernel_entry_point", &query, code->c_str());
			return true;
		}
	}
	return false;
}
