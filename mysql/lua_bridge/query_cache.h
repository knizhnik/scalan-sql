#pragma once 

#include <map>
#include <vector>
#include <string>

using namespace std;

struct QueryParam 
{
	enum ParamType {
		PARAM_INT,
		PARAM_REAL,
		PARAM_STRING
	};
	ParamType type;
	long   ival;
	double rval;
	string sval;
};

extern string generalize_query(string const& sql, vector<QueryParam>& params)

class QueryCache {
	map<string,string> hash;
  public:
	QueryCache(string const& kernel_dir);

	string* find(string const& canonic_query);
};

	
	
