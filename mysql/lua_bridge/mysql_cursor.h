#include "sql_select.h"
#include <vector>

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

extern "C" {

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

void mysqlGetDate(MySQLCursor* cursor, int columnNo, date_t* dst) 
{
	MYSQL_TIME t;
	cursor->fields[columnNo]->get_date(&t, TIME_NO_ZERO_DATE);
	*dst = TIME_to_ulonglong(&t);
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

}
	
