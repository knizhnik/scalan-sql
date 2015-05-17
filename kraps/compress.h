#include <stdio.h>
#include <time.h>
#include <stdlib.h>

#ifdef USE_ZLIB

#include <zlib.h>

size_t compress(char* dst, char const* src, size_t length) 
{
    uLongf dstLen = length;    
    int rc = compress2((Bytef*)dst, &dstLen, (Bytef*)src, length, 1);
    return rc == Z_OK ? dstLen : length;
}

void decompress(char* dst, char const* src, size_t length, size_t compressedLength) 
{
    uLongf decompressedBufferSize = length;
    int rc = uncompress((Bytef*)dst, &decompressedBufferSize, (Bytef*)src, compressedLength);
    assert(rc == Z_OK);
}

#else
    
#include "simdcomp.h"

size_t compress(char* dst, char const* src, size_t length) 
{
    uint32_t* datain = (uint32_t*)src;
    uint8_t* buffer = (uint8_t*)dst;
	size_t n = (length + SIMDBlockSize*sizeof(uint32_t) - 1)/(SIMDBlockSize*sizeof(uint32_t));
                       
    memset((char*)datain + length, 0, n*SIMDBlockSize*sizeof(uint32_t) - length);
    
	for(size_t k = 0; k < n; k++) {
        uint32_t b = maxbits(datain + k * SIMDBlockSize);
		*buffer++ = b;
		simdpackwithoutmask(datain + k * SIMDBlockSize, (__m128i *) buffer, b);
        buffer += b * sizeof(__m128i);
	}
    return buffer - (uint8_t*)dst;
}

void decompress(char* dst, char const* src, size_t length, size_t compressedLength) 
{
    uint32_t* dataout = (uint32_t*)dst;
    uint8_t* buffer = (uint8_t*)src;
	size_t n = (length + SIMDBlockSize*sizeof(uint32_t) - 1)/(SIMDBlockSize*sizeof(uint32_t));
    
    for (size_t k = 0; k < n; k++) {
        uint8_t b = *buffer++;
        simdunpack((__m128i*)buffer, dataout, b);
        buffer += b * sizeof(__m128i);
        dataout += SIMDBlockSize;
    }
}

#endif
