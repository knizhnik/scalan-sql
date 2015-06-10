#ifndef __PACK_H__
#define __PACK_H__

#include <string.h>

//
// Default pack/unpack functions for scalar types
//
template<class T>
inline size_t pack(T const& src, char* dst, size_t size = 0) 
{ 
    memcpy(dst, &src, sizeof(T));
    return sizeof(T);
}
template<class T>
inline size_t unpack(T& dst, char const* src, size_t size = 0) 
{ 
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
// Pack/unpack functions for fixed size strings
//
inline size_t pack(char const* src, char* dst, size_t size)
{
    return strcopy(dst, src, size);
}

inline size_t unpack(char* dst, char const* src, size_t size)
{
    return strcopy(dst, src, size);
}

#define PACK_FIELD(x) size += ::pack(src.x, dst + size, sizeof(src.x));

#define PACK(Class)                                 \
inline size_t pack(Class const& src, char* dst, size_t size = 0)    \
{                                                   \
    size = 0;                                       \
    Class##Fields(PACK_FIELD);                      \
    return size;                                    \
}

#define UNPACK_FIELD(x) size += ::unpack(dst.x, src + size, sizeof(dst.x));

#define UNPACK(Class)                               \
inline size_t unpack(Class& dst, char const* src, size_t size = 0)  \
{                                                   \
    size = 0;                                       \
    Class##Fields(UNPACK_FIELD);                    \
    return size;                                    \
}

#define PARQUET_FIELD(x) unpackParquet(dst.x, reader.columns[i++].reader, sizeof(dst.x)) && 
#define PARQUET_UNPACK(Class)                       \
inline bool unpackParquet(Class& dst, ParquetReader& reader)   \
{                                                   \
    size_t i = 0;                                   \
    return Class##Fields(PARQUET_FIELD) true;       \
}

#endif
