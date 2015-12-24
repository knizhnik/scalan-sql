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

#if 0
#define PACK_FIELD(name,typr) size += ::pack(src.name, dst + size, sizeof(src.name));

#define PACK(Class)                                 \
inline size_t pack(Class const& src, char* dst, size_t size = 0)    \
{                                                   \
    size = 0;                                       \
    Class##Fields(PACK_FIELD);                      \
    return size;                                    \
}

#define UNPACK_FIELD(name,type) size += ::unpack(dst.name, src + size, sizeof(dst.name));

#define UNPACK(Class)                               \
inline size_t unpack(Class& dst, char const* src, size_t size = 0)  \
{                                                   \
    size = 0;                                       \
    Class##Fields(UNPACK_FIELD);                    \
    return size;                                    \
}
#else
#define PACK(Class)                                 \
inline size_t pack(Class const& src, char* dst, size_t size = 0)    \
{                                                   \
    *(Class*)dst = src;                             \
    return sizeof(Class);                           \
}
#define UNPACK(Class)                               \
inline size_t unpack(Class& dst, char const* src, size_t size = 0)  \
{                                                   \
    dst = *(Class*)src;                            \
    return sizeof(Class);                          \
}
#endif

#if USE_PARQUET
#define PARQUET_FIELD(name,type) unpackParquet(dst.name, reader.columns[i++].reader, sizeof(dst.name)) && 
#define PARQUET_UNPACK(Class)                       \
inline bool unpackParquet(Class& dst, ParquetReader& reader)   \
{                                                   \
    size_t i = 0;                                   \
    return Class##Fields(PARQUET_FIELD) true;       \
}
#define ENUM_FIELD(NAME,TYPE) __field_##NAME,
#define PARQUET_GETTER(NAME, TYPE) TYPE NAME() const { TYPE dst; reader->unpack(dst,__field_##NAME); return dst; }
#define PARQUET_LAZY_UNPACK(Class)                  \
struct P##Class {				    \
    ParquetReader* reader;			    \
    enum {					    \
        Class##Fields(ENUM_FIELD)		    \
        __end_of_fields				    \
    };						    \
    Class##Fields(PARQUET_GETTER)		    \
};
#else
#define PARQUET_UNPACK(Class)  
#define PARQUET_LAZY_UNPACK(Class)                 
#endif

#endif
