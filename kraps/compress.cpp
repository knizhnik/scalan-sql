#include <string.h>
#include <assert.h>

#include "compress.h"

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

#elif defined(USE_SIMDCOMP)
    
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

#else


/* ----------
 * Local definitions
 * ----------
 */
#define LZ_HISTORY_LISTS	8192	/* must be power of 2 */
#define LZ_HISTORY_MASK		(LZ_HISTORY_LISTS - 1)
#define LZ_HISTORY_SIZE		4096
#define LZ_MAX_MATCH		273

#define INT_MAX             0x7FFFFFFF

/* ----------
 * PGLZ_Strategy -
 *
 *		Some values that control the compression algorithm.
 *
 *		min_input_size		Minimum input data size to consider compression.
 *
 *		max_input_size		Maximum input data size to consider compression.
 *
 *		min_comp_rate		Minimum compression rate (0-99%) to require.
 *							Regardless of min_comp_rate, the output must be
 *							smaller than the input, else we don't store
 *							compressed.
 *
 *		first_success_by	Abandon compression if we find no compressible
 *							data within the first this-many bytes.
 *
 *		match_size_good		The initial GOOD match size when starting history
 *							lookup. When looking up the history to find a
 *							match that could be expressed as a tag, the
 *							algorithm does not always walk back entirely.
 *							A good match fast is usually better than the
 *							best possible one very late. For each iteration
 *							in the lookup, this value is lowered so the
 *							longer the lookup takes, the smaller matches
 *							are considered good.
 *
 *		match_size_drop		The percentage by which match_size_good is lowered
 *							after each history check. Allowed values are
 *							0 (no change until end) to 100 (only check
 *							latest history entry at all).
 * ----------
 */
typedef struct LZ_Strategy
{
    int		min_input_size;
    int		max_input_size;
    int		min_comp_rate;
    int		first_success_by;
    int		match_size_good;
    int		match_size_drop;
} LZ_Strategy;


/* ----------
 * The standard strategies
 *
 *		strategy_default		    Recommended default strategy for TOAST.
 *
 *		strategy_always		        Try to compress inputs of any length.
 *									Fallback to uncompressed storage only if
 *									output would be larger than input.
 * ----------
 */

/* ----------
 * LZ_HistEntry -
 *
 *		Linked list for the backward history lookup
 *
 * All the entries sharing a hash key are linked in a doubly linked list.
 * This makes it easy to remove an entry when it's time to recycle it
 * (because it's more than 4K positions old).
 * ----------
 */
typedef struct LZ_HistEntry
{
    struct LZ_HistEntry *next;	/* links for my hash key's list */
    struct LZ_HistEntry *prev;
    int			hindex;			/* my current hash key */
    const char *pos;			/* my input position */
} LZ_HistEntry;


typedef struct LZ_Header {
    int		rawsize;
} LZ_Header;


/* ----------
 * The provided standard strategies
 * ----------
 */
static const LZ_Strategy strategy_default_data = {
    32,							/* Data chunks less than 32 bytes are not
                                 * compressed */
    INT_MAX,					/* No upper limit on what we'll try to
                                 * compress */
    25,							/* Require 25% compression rate, or not worth
                                 * it */
    1024,						/* Give up if no compression in the first 1KB */
    128,						/* Stop history lookup if a match of 128 bytes
                                 * is found */
    10							/* Lower good match size by 10% at every loop
                                 * iteration */
};

static const LZ_Strategy strategy_always_data = {
    0,							/* Chunks of any size are compressed */
    INT_MAX,
    0,							/* It's enough to save one single byte */
    INT_MAX,					/* Never give up early */
    128,						/* Stop history lookup if a match of 128 bytes
                                 * is found */
    6							/* Look harder for a good match */
};


/* ----------
 * lz_hist_idx -
 *
 *		Computes the history table slot for the lookup by the next 4
 *		characters in the input.
 *
 * NB: because we use the next 4 characters, we are not guaranteed to
 * find 3-character matches; they very possibly will be in the wrong
 * hash list.  This seems an acceptable tradeoff for spreading out the
 * hash keys more.
 * ----------
 */
#define lz_hist_idx(_s,_e) (												\
            ((((_e) - (_s)) < 4) ? (int) (_s)[0] :							\
             (((_s)[0] << 9) ^ ((_s)[1] << 6) ^								\
              ((_s)[2] << 3) ^ (_s)[3])) & (LZ_HISTORY_MASK)				\
        )


/* ----------
 * lz_hist_add -
 *
 *		Adds a new entry to the history table.
 *
 * If _recycle is true, then we are recycling a previously used entry,
 * and must first delink it from its old hashcode's linked list.
 *
 * NOTE: beware of multiple evaluations of macro's arguments, and note that
 * _hn and _recycle are modified in the macro.
 * ----------
 */
#define lz_hist_add(_hs,_he,_hn,_recycle,_s,_e) \
do {									\
            int __hindex = lz_hist_idx((_s),(_e));						\
            LZ_HistEntry **__myhsp = &(_hs)[__hindex];					\
            LZ_HistEntry *__myhe = &(_he)[_hn];							\
            if (_recycle) {													\
                if (__myhe->prev == NULL)									\
                    (_hs)[__myhe->hindex] = __myhe->next;					\
                else														\
                    __myhe->prev->next = __myhe->next;						\
                if (__myhe->next != NULL)									\
                    __myhe->next->prev = __myhe->prev;						\
            }																\
            __myhe->next = *__myhsp;										\
            __myhe->prev = NULL;											\
            __myhe->hindex = __hindex;										\
            __myhe->pos  = (_s);											\
            if (*__myhsp != NULL)											\
                (*__myhsp)->prev = __myhe;									\
            *__myhsp = __myhe;												\
            if (++(_hn) >= LZ_HISTORY_SIZE) {								\
                (_hn) = 0;													\
                (_recycle) = 1;											    \
            }																\
} while (0)


/* ----------
 * lz_out_ctrl -
 *
 *		Outputs the last and allocates a new control byte if needed.
 * ----------
 */
#define lz_out_ctrl(__ctrlp,__ctrlb,__ctrl,__buf) \
do { \
    if ((__ctrl & 0xff) == 0)												\
    {																		\
        *(__ctrlp) = __ctrlb;												\
        __ctrlp = (__buf)++;												\
        __ctrlb = 0;														\
        __ctrl = 1;															\
    }																		\
} while (0)


/* ----------
 * lz_out_literal -
 *
 *		Outputs a literal byte to the destination buffer including the
 *		appropriate control bit.
 * ----------
 */
#define lz_out_literal(_ctrlp,_ctrlb,_ctrl,_buf,_byte) \
do { \
    lz_out_ctrl(_ctrlp,_ctrlb,_ctrl,_buf);								\
    *(_buf)++ = (unsigned char)(_byte);										\
    _ctrl <<= 1;															\
} while (0)


/* ----------
 * lz_out_tag -
 *
 *		Outputs a backward reference tag of 2-4 bytes (depending on
 *		offset and length) to the destination buffer including the
 *		appropriate control bit.
 * ----------
 */
#define lz_out_tag(_ctrlp,_ctrlb,_ctrl,_buf,_len,_off) \
do { \
    lz_out_ctrl(_ctrlp,_ctrlb,_ctrl,_buf);								\
    _ctrlb |= _ctrl;														\
    _ctrl <<= 1;															\
    if (_len > 17)															\
    {																		\
        (_buf)[0] = (unsigned char)((((_off) & 0xf00) >> 4) | 0x0f);		\
        (_buf)[1] = (unsigned char)(((_off) & 0xff));						\
        (_buf)[2] = (unsigned char)((_len) - 18);							\
        (_buf) += 3;														\
    } else {																\
        (_buf)[0] = (unsigned char)((((_off) & 0xf00) >> 4) | ((_len) - 3)); \
        (_buf)[1] = (unsigned char)((_off) & 0xff);							\
        (_buf) += 2;														\
    }																		\
} while (0)


/* ----------
 * lz_find_match -
 *
 *		Lookup the history table if the actual input stream matches
 *		another sequence of characters, starting somewhere earlier
 *		in the input buffer.
 * ----------
 */
#if defined(_WIN32) || defined(_VXWORKS)
static int
#else
static inline int
#endif
lz_find_match(LZ_HistEntry **hstart, const char *input, const char *end,
                int *lenp, int *offp, int good_match, int good_drop)
{
    LZ_HistEntry *hent;
    int		len = 0;
    int		off = 0;

    /*
     * Traverse the linked history list until a good enough match is found.
     */
    hent = hstart[lz_hist_idx(input, end)];
    while (hent)
    {
        const char *ip = input;
        const char *hp = hent->pos;
        int		thisoff;
        int		thislen;

        /*
         * Stop if the offset does not fit into our tag anymore.
         */
        thisoff = (int)(ip - hp);
        if (thisoff >= 0x0fff)
            break;

        /*
         * Determine length of match. A better match must be larger than the
         * best so far. And if we already have a match of 16 or more bytes,
         * it's worth the call overhead to use memcmp() to check if this match
         * is equal for the same size. After that we must fallback to
         * character by character comparison to know the exact position where
         * the diff occurred.
         */
        thislen = 0;
        if (len >= 16)
        {
            if (memcmp(ip, hp, len) == 0)
            {
                thislen = len;
                ip += len;
                hp += len;
                while (ip < end && *ip == *hp && thislen < LZ_MAX_MATCH)
                {
                    thislen++;
                    ip++;
                    hp++;
                }
            }
        }
        else
        {
            while (ip < end && *ip == *hp && thislen < LZ_MAX_MATCH)
            {
                thislen++;
                ip++;
                hp++;
            }
        }

        /*
         * Remember this match as the best (if it is)
         */
        if (thislen > len)
        {
            len = thislen;
            off = thisoff;
        }

        /*
         * Advance to the next history entry
         */
        hent = hent->next;

        /*
         * Be happy with lesser good matches the more entries we visited. But
         * no point in doing calculation if we're at end of list.
         */
        if (hent)
        {
            if (len >= good_match)
                break;
            good_match -= (good_match * good_drop) / 100;
        }
    }

    /*
     * Return match information only if it results at least in one byte
     * reduction.
     */
    if (len > 2)
    {
        *lenp = len;
        *offp = off;
        return 1;
    }

    return 0;
}


/* ----------
 * lz_compress -
 *
 *		Compresses source into dest using strategy.
 * ----------
 */
int lz_compress(void* dest, const void *source, int slen, LZ_Strategy const* strategy)
{
    unsigned char *bp = (unsigned char *)dest + sizeof(LZ_Header);
    unsigned char *bstart = bp;
    int			hist_next = 0;
    int 	    hist_recycle = 0;
    const char *dp = (const char*)source;
    const char *dend = dp + slen;
    unsigned char ctrl_dummy = 0;
    unsigned char *ctrlp = &ctrl_dummy;
    unsigned char ctrlb = 0;
    unsigned char ctrl = 0;
    int	        found_match = 0;
    int		match_len;
    int		match_off;
    int		good_match;
    int		good_drop;
    int		result_size;
    int		result_max;
    int		need_rate;

    /* ----------
     * Allocated work arrays for history
     * ----------
     */
    LZ_HistEntry *hist_start[LZ_HISTORY_LISTS];
    LZ_HistEntry hist_entries[LZ_HISTORY_SIZE];

    /*
     * Save the original source size in the header.
     */
    ((LZ_Header*)dest)->rawsize = slen;

    /*
     * Limit the match parameters to the supported range.
     */
    good_match = strategy->match_size_good;
    if (good_match > LZ_MAX_MATCH)
        good_match = LZ_MAX_MATCH;
    else if (good_match < 17)
        good_match = 17;

    good_drop = strategy->match_size_drop;
    if (good_drop < 0)
        good_drop = 0;
    else if (good_drop > 100)
        good_drop = 100;

    need_rate = strategy->min_comp_rate;
    if (need_rate < 0)
        need_rate = 0;
    else if (need_rate > 99)
        need_rate = 99;

    /*
     * Compute the maximum result size allowed by the strategy, namely the
     * input size minus the minimum wanted compression rate.  This had better
     * be <= slen, else we might overrun the provided output buffer.
     */
    if (slen > (INT_MAX / 100))
    {
        /* Approximate to avoid overflow */
        result_max = (slen / 100) * (100 - need_rate);
    }
    else
        result_max = (slen * (100 - need_rate)) / 100;

    /*
     * Initialize the hi9story lists to empty.  We do not need to zero the
     * hist_entries[] array; its entries are initialized as they are used.
     */
    memset(hist_start, 0, sizeof(hist_start));

    /*
     * Compress the source directly into the output buffer.
     */
    while (dp < dend)
    {
        /*
         * If we already exceeded the maximum result size, fail.
         *
         * We check once per loop; since the loop body could emit as many as 4
         * bytes (a control byte and 3-byte tag), LZ_MAX_OUTPUT() had better
         * allow 4 slop bytes.
         */
        if (bp - bstart >= result_max)
            return 0;

        /*
         * If we've emitted more than first_success_by bytes without finding
         * anything compressible at all, fail.	This lets us fall out
         * reasonably quickly when looking at incompressible input (such as
         * pre-compressed data).
         */
        if (!found_match && bp - bstart >= strategy->first_success_by)
            return 0;

        /*
         * Try to find a match in the history
         */
        if (lz_find_match(hist_start, dp, dend, &match_len,
                            &match_off, good_match, good_drop))
        {
            /*
             * Create the tag and add history entries for all matched
             * characters.
             */
            lz_out_tag(ctrlp, ctrlb, ctrl, bp, match_len, match_off);
            while (match_len--)
            {
                lz_hist_add(hist_start, hist_entries,
                              hist_next, hist_recycle,
                              dp, dend);
                dp++;			/* Do not do this ++ in the line above! */
                /* The macro would do it four times - Jan.	*/
            }
            found_match = 1;
        }
        else
        {
            /*
             * No match found. Copy one literal byte.
             */
            lz_out_literal(ctrlp, ctrlb, ctrl, bp, *dp);
            lz_hist_add(hist_start, hist_entries,
                          hist_next, hist_recycle,
                          dp, dend);
            dp++;				/* Do not do this ++ in the line above! */
            /* The macro would do it four times - Jan.	*/
        }
    }

    /*
     * Write out the last control byte and check that we haven't overrun the
     * output size allowed by the strategy.
     */
    *ctrlp = ctrlb;
    result_size = (int)(bp - bstart);
    if (result_size >= result_max)
        return 0;

    return result_size + sizeof(LZ_Header);
}


/* ----------
 * lz_decompress -
 *
 *		Decompresses source into dest.
 * ----------
 */
int lz_decompress(void* dest, const void* source, int compressed_size)
{
    const unsigned char *sp;
    const unsigned char *srcend;
    unsigned char *dp;
    unsigned char *destend;
    int rawsize = ((LZ_Header*)source)->rawsize;

    sp = (const unsigned char *)source + sizeof(LZ_Header);
    srcend = (const unsigned char *) source + compressed_size;
    dp = (unsigned char *) dest;
    destend = dp + rawsize;

    while (sp < srcend && dp < destend)
    {
        /*
         * Read one control byte and process the next 8 items (or as many as
         * remain in the compressed input).
         */
        unsigned char ctrl = *sp++;
        int			ctrlc;

        for (ctrlc = 0; ctrlc < 8 && sp < srcend; ctrlc++)
        {
            if (ctrl & 1)
            {
                /*
                 * Otherwise it contains the match length minus 3 and the
                 * upper 4 bits of the offset. The next following byte
                 * contains the lower 8 bits of the offset. If the length is
                 * coded as 18, another extension tag byte tells how much
                 * longer the match really was (0-255).
                 */
                int		len;
                int		off;

                len = (sp[0] & 0x0f) + 3;
                off = ((sp[0] & 0xf0) << 4) | sp[1];
                sp += 2;
                if (len == 18)
                    len += *sp++;

                /*
                 * Check for output buffer overrun, to ensure we don't clobber
                 * memory in case of corrupt input.
                 */
                if (dp + len > destend)
                {
                    return 0;
                }

                /*
                 * Now we copy the bytes specified by the tag from OUTPUT to
                 * OUTPUT. It is dangerous and platform dependent to use
                 * memcpy() here, because the copied areas could overlap
                 * extremely!
                 */
                while (len--)
                {
                    *dp = dp[-off];
                    dp++;
                }
            }
            else
            {
                /*
                 * An unset control bit means LITERAL BYTE. So we just copy
                 * one from INPUT to OUTPUT.
                 */
                if (dp >= destend)		/* check for buffer overrun */
                    break;		/* do not clobber memory */

                *dp++ = *sp++;
            }

            /*
             * Advance the control bit
             */
            ctrl >>= 1;
        }
    }

    /*
     * Check we decompressed the right amount.
     */
    return dp == destend && sp == srcend ? rawsize : 0; /* false if compressed data is corrupt */
}

size_t compress(char* dst, char const* src, size_t length)
{
    size_t compressedLength = lz_compress(dst, src, length, &strategy_default_data);
    return compressedLength == 0 ? length : compressedLength;
}

void decompress(char* dst, char const* src, size_t length, size_t compressedLength)
{
    size_t rawSize = lz_decompress(dst, src, compressedLength);
    assert(rawSize == length);
}

#endif
