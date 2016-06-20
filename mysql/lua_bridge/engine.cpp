#include "engine.h"

bool Engine::run(JOIN* join)
{												
	LEX_STRING* query = thd_query_string(join->thd);
	if (query != NULL && query->str != NULL) { 
		Query query(join);
		string canonical_sql = generalize_query(query->str, query.params);
		string* kernel = cache.find(canonical_sql);
		if (kernel != NULL) { 
			lua_State* L = InitLua();
			LuaKernelCall(L, "kernel_entry_point", &query, 0, 0, 0);
			return true;
		}
	}
	return false;
}
