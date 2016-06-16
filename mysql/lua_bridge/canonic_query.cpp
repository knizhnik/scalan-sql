#include <string>
#include <vector>
#include <assert.h>
#include <ctype.h>

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

string generalize_query(string const& sql, vector<QueryParam>& params)
{
	string buf;
	QueryParam param;
	for (size_t i = 0; i < sql.size(); i++) {
		char ch = sql[i];
		if (isspace(ch)) { 
			while (i+1 < sql.size() && isspace(sql[i+1])) { 
				i += 1;
			}
			buf << ' ';
		} else if (ch == '\'') { 
			param.sval.clear();
			param.type = QueryParam::PARAM_STRING;
			while (true) { 
				i += 1;
				assert (i < sql.size()); 
				if (sql[i] == '\'') { 
					if (i+1 == sql.size() || sql[i+1] != '\'') { 
						break;
					} else {
						param.sval += '\'';
						i += 1;
					}
				} else { 
					param.sval += sql[i];
				}
			}
			params.push_back(param);
			buf += '?';
		} else if (ch == '-' || isdigit(ch)) { 
			int n1, n2;
			if (sscanf(&sql[i], "%lf%n", &param.rval, &n1) == 1) { 
				if (sscanf(&sql[i], "%ld%n", &param.ival, &n2) == 1 && n1 == n2) { 
					param.type = QueryParam::PARAM_INT;
				} else { 
					param.type = QueryParam::PARAM_REAL;
				}
				i += n1;
				params.push_back(param);
				buf += '?';
			} else { 
				buf += ch;
			}
		} else if (isalpha(ch) || ch == '_') {
            do { 
                buf += ch;
            } while (++i < sql.size() && ((ch = buf[i]) == '_' || isalnum(ch)));
            i -= 1;
		} else { 
			buf += ch;
		}
	}											
	return buf;
}
