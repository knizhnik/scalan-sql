#include <stdlib.h>

//
// Abstract compression functions (particular implementation may vary)
//
extern size_t compress(char* dst, char const* src, size_t length);
extern void decompress(char* dst, char const* src, size_t length, size_t compressedLength);
