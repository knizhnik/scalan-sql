#ifndef __CONVERT_H__
    #define __CONVERT_H__

    #include "stdtp.h"

    namespace McoSql
    {

        inline char* packShort(char* dst, short val)
        {
            *dst++ = char(val >> 8);
            *dst++ = char(val);
            return dst;
        }

        inline char* packInt(char* dst, int val)
        {
            *dst++ = char(val >> 24);
            *dst++ = char(val >> 16);
            *dst++ = char(val >> 8);
            *dst++ = char(val);
            return dst;
        }


        inline char* packLong(char* dst, int64_t val)
        {
            return packInt(packInt(dst, (int)(val >> 32)), (int)val);
        }

        #ifndef NO_FLOATING_POINT
            inline char* packFloat(char* dst, float val)
            {
                union { 
                    int i;
                    float  d;
                } u;
                u.d = val;
                return packInt(dst, u.i);
            }
            inline char* packDouble(char* dst, double val)
            {
                union { 
                    int64_t i;
                    double  d;
                } u;
                u.d = val;
                return packLong(dst, u.i);
            }
        #endif 

        inline short unpackShort(char* src)
        {
            unsigned char* s = (unsigned char*)src;
            return (s[0] << 8) + s[1];
        }

        inline int unpackInt(char* src)
        {
            unsigned char* s = (unsigned char*)src;
            return (((((s[0] << 8) + s[1]) << 8) + s[2]) << 8) + s[3];
        }

        inline int64_t unpackLong(char* src)
        {
            return ((int64_t)unpackInt(src) << 32) | (unsigned)unpackInt(src + 4);
        }

        #ifndef NO_FLOATING_POINT
            inline float unpackFloat(char* src)
            {
                union { 
                    int i;
                    float f;
                } u;
                u.i = unpackInt(src);
                return u.f;
            }
            inline double unpackDouble(char* src)
            {
                union { 
                    int64_t i;
                    double  d;
                } u;
                u.i = unpackLong(src);
                return u.d;
            }
        #endif 

    }

#endif
