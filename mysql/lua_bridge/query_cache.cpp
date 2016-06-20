#include <stdio.h>
#include <assert.h>
#include <ctype.h>
#include <dirent.h>

#include "query_cache.h"


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

QueryCache::QueryCache(char const* kernel_dir)
{
	DIR* dir = diropen(kernel_dir.c_str());
	if (dir == NULL) { 
		perror("diropen");
		return;
	}
	dirent* entry;
	while ((entry = readdir(dir)) != NULL) { 
		if (strlen(entry->d_name) > 4 && strcmp(entry->d_name + strlen(entry->d_name) - 4, ".sql") == 0) {
			string path = kernet_dir + "/" + entry->d_name;
			FILE* in = fopen(path.c_str(), "r");
			int ch;
			string sql;
			while ((ch = getc(in)) != EOF) { 
				if (isspace(ch)) { 
					do { 
						ch = getc(in);
					} while (ch != EOF && isspace(ch));
					ch = ' ';
				}
				sql += (char)ch;
			}
			hash[sql] = path.replace(path.size() - 3,  3, "lua");
		}
	}
	dirclose(dir);

}

string* QueryCache::find(string const& query)
{
	hash.find(query);
	map<string,string>::iterator it = hash.find(query);
	return it != hash.end() ? &it->second : NULL;
}
