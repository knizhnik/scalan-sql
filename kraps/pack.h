#ifndef __PACK_H__
#define __PACK_H__

#include <string.h>

//
// Default pack/unpack functions
//
template<class T>
inline size_t pack(T const& src, char* dst) { 
    memcpy(dst, &src, sizeof(T));
    return sizeof(T);
}
template<class T>
inline size_t unpack(T& dst, char const* src) { 
    memcpy(&dst, src, sizeof(T));
    return sizeof(T);
}

//
// Copy fixed size string (may be not zero zerminated)
//
inline size_t strcopy(char* dst, char const* src, size_t len)
{
    char* beg = dst;
    while (len-- != 0 && (*dst++ = *src++) != 0);
    return dst - beg;
}

//
// Macros for packing/unpacking class components
//
#define PACK(x) memcpy(dst + size, &src.x, sizeof(src.x)), size += sizeof(src.x)
#define UNPACK(x) memcpy(&dst.x, src + size, sizeof(dst.x)), size += sizeof(dst.x)
#define PACK_STR(x) size += strcopy(dst + size, src.x, sizeof(src.x))
#define UNPACK_STR(x) size += strcopy(dst.x, src + size, sizeof(dst.x))

#endif
