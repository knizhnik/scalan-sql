#include <string.h>

inline size_t strcopy(char* dst, char const* src, size_t len)
{
    char* beg = dst;
    while (len-- != 0 && (*dst++ = *src++) != 0);
    return dst - beg;
}

#define PACK(x) memcpy(dst + size, &src.x, sizeof(src.x)), size += sizeof(src.x)
#define UNPACK(x) memcpy(&dst.x, src + size, sizeof(dst.x)), size += sizeof(dst.x)
#define PACK_STR(x) size += strcopy(dst + size, src.x, sizeof(src.x))
#define UNPACK_STR(x) size += strcopy(dst.x, src + size, sizeof(dst.x))
