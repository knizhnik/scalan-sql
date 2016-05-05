#include <stdio.h>
#include <string.h>

int main(int argc, char* argv[])
{
	char buf[1024];
	FILE* in = fopen(argv[1], "r");
	FILE* out = fopen(argv[2], "w");
	fprintf(out, "<meta http-equiv=\"refresh\" content=\"1\">\
<html>\
<table border>\
<tr><th>Country</th><th>Revenue</th></tr>\n");
	while (fgets(buf, sizeof buf, in)) { 
		char* start = buf;
		char* sep;
		fputs("<tr><td>", out);
		while ((sep = strchr(start, ',')) != NULL) { 
			fprintf(out, "%.*s</td><td>", (int)(sep - start), start);
			start = sep + 1;
		}
		fputs("</td></tr>\n", out);
	}
	fputs("</html>\n", out);
	return 0;
}

	
