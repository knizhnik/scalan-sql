#include "sql_select.h"

class Engine 
{ 
	QueryCache cache;
	
	bool run(JOIN* join);
};
