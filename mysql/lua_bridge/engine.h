#include "sql_select.h"
#include "query_cache.h"

using namespace std;

struct StringBuf { 
	char buf[64];
	String str;

	StringBuf() : str(buf, sizeof buf, system_charset_info) { }
};
		

struct MySQLCursor 
{ 
	vector<Field*> fields;
	vector<StringBuf> strings;
	TABLE* table;

	MySQLCursor(TABLE* tab) : fields(nFields), strings(nFields), tab(tab) {}
};

struct MySQLResultSet
{
	JOIN* join;
	List<Item> list;

	MySQLResultSet(JOIN* join)
	{
		join->result->prepare2();
		join->result->send_result_set_metadata(join->fields_list,
											   Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF);
	}
};

struct Query
{
	vector<QueryParam> params;
	JOIN* join;

	Query(JOIN* select) : join(select) {}
};
	
class Engine 
{ 
	QueryCache cache;

public:
	Engine(string const& kernel_dir) : cache(kernel_dir) {}
	bool run(JOIN* join);
};
