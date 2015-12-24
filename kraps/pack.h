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
    dst = *(Class*)src;                             \
    return sizeof(Class);                           \
}
#endif

#if USE_PARQUET
#define PARQUET_FIELD(NAME,TYPE) unpackParquet(_dst.NAME, _reader.columns[_i++].reader, sizeof(_dst.NAME)) && 
#ifdef COLUMNAR_STORE
#define PARQUET_UNPACK(Class)                       \
inline bool unpackParquet(H##Class& _dst, ParquetReader& _reader)   \
{                                                   \
    size_t _i = 0;                                  \
    return Class##Fields(PARQUET_FIELD) true;       \
}
#else
#define PARQUET_UNPACK(Class)                       \
inline bool unpackParquet(Class& _dst, ParquetReader& _reader)   \
{                                                   \
    size_t _i = 0;                                  \
    return Class##Fields(PARQUET_FIELD) true;       \
}
#endif

#define ENUM_FIELD(NAME,TYPE) _field_##NAME,
#define FIELD_CACHE(NAME,TYPE) TYPE _##NAME;
#define PARQUET_GETTER(NAME, TYPE)                  \
TYPE NAME() {                                       \
    if (!(_mask & (int64_t(1) << _field_##NAME))) { \
        _mask |= int64_t(1) << _field_##NAME;       \
        _reader->unpack(_##NAME,_field_##NAME);     \
    }                                               \
    return _##NAME;                                 \
}

#define PARQUET_LAZY_UNPACK(Class)                  \
struct P##Class {                                   \
    ParquetReader* _reader;                         \
    int64_t _mask;                                  \
    enum {                                          \
        Class##Fields(ENUM_FIELD)                   \
        _end_of_fields                              \
    };                                              \
    Class##Fields(FIELD_CACHE)                      \
    Class##Fields(PARQUET_GETTER)                   \
};
#else
#define PARQUET_UNPACK(Class)  
#define PARQUET_LAZY_UNPACK(Class)                 
#endif

#endif
