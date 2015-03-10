/*******************************************************************
 *                                                                 *
 *  util.cpp                                                      *
 *                                                                 *
 *  This file is a part of the eXtremeDB source code               *
 *  Copyright (c) 2001-2005 McObject LLC                           *
 *  All Rights Reserved                                            *
 *                                                                 *
 *******************************************************************/
/*
 * ++
 *
 * PROJECT:   eXtremeDB(tm) (c) McObject LLC
 *
 * SUBSYSTEM: SQL support
 *
 * MODULE:    util.cpp
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#include <apidef.h>
#include "util.h"
#include "cursor.h"

namespace McoSql
{

    char* int64ToString(int64_t val, char* buf, size_t bufSize)
    {
        bool negative = false;
        if (val < 0)
        {
            val =  - val;
            negative = true;
            if (val < 0)
            {
                return (char*)"-9223372036854775808";
            }
        }
        int i = (int)bufSize - 1;
        buf[i] = '\0';
        do
        {
            assert(i > 0);
            buf[--i] = (char)((int)(val % 10) + '0');
            val /= 10;
        }
        while (val != 0);

        if (negative)
        {
            assert(i > 0);
            buf[--i] = '-';
        }
        assert(i >= 0);
        return  &buf[i];
    }

    bool stringToInt64(char* str, int64_t &val)
    {
        int sign = 1;
        if (*str == '-')
        {
            sign =  - 1;
            str += 1;
        }
        int64_t v = 0;
        while (*str >= '0' && * str <= '9')
        {
            v = v * 10+* str++ - '0';
        }
        val = v*sign;
        return * str == '\0';
    }

    bool stringToNumeric(char* str, int64_t &val, int precision)
    {
        int sign = 1;
        if (*str == '-')
        {
            sign =  - 1;
            str += 1;
        }
        int64_t v = 0;
        while (*str >= '0' && *str <= '9')
        {
            v = v * 10 + *str++ - '0';
        }
        if (*str == '.') {
            str += 1;
            while (precision > 0 && *str >= '0' && *str <= '9')
            {
                v = v * 10 + *str++ - '0';
                precision -= 1;
            }
        }
        while (--precision >= 0) {
            v *= 10;
        }
        val = v*sign;
        return *str == '\0';
    }

    bool stringToTime(char* str, time_t &val)
    {
        if (*str == '\0')
        {
            val = 0;
            return true;
        }
        int D, M, Y, m = 0, h = 0, s = 0, n;
        n = sscanf(str, "%d/%d/%d %d:%d:%d", &M, &D, &Y, &h, &m, &s);
        if (n < 3)
        {
            n = sscanf(str, "%d-%d-%d %d:%d:%d", &Y, &M, &D, &h, &m, &s);
            if (n < 3)
            {
                n = sscanf(str, "%d.%d.%d %d:%d:%d", &D, &M, &Y, &h, &m, &s);
            }
        } else if (M > 1900) { // replace MM/DD/YY with YY/MM/DD
            int Y1 = M, M1 = D, D1 = Y;
            Y = Y1, M = M1, D = D1;
        }                        
        if (n == 3 || n >= 5)
        {
            struct tm t;
            t.tm_isdst =  - 1;

            t.tm_mday = D;
            t.tm_mon = M - 1;
            t.tm_year = Y > 1900 ? Y - 1900: Y < 50 ? Y + 100: Y;

            t.tm_hour = h;
            t.tm_min = m;
            t.tm_sec = s;
            val = mktime(&t);
            return val != (time_t) - 1;
        }
        else
        {
            switch (sscanf(str, "%d:%d:%d", &h, &m, &s))
            {
                case 1:
                    val = h; // time in seconds
                    break;
                case 2:
                    val = (h* 60+m)* 60;
                    break;
                case 3:
                    val = (h* 60+m)* 60+s;
                    break;
                default:
                    return false;
            }
        }
        return true;
    }

    int timeToString(time_t val, char* buf, size_t bufSize)
    {
        #ifdef HAVE_LOCALTIME_R
            struct tm t;
            return strftime(buf, bufSize, DateTime::format, localtime_r(&val, &t));
        #else
            return (int)strftime(buf, bufSize, DateTime::format, localtime(&val));
        #endif
    }


    HashTable::HashTable(int size)
    {
        table = Vector < Entry > ::create(size);
        memset(table->items, 0, size* sizeof(Entry*));
    }

    HashTable::Iterator::Iterator(HashTable* hash)
    {
        this->hash = hash;
        entry = NULL;
        for (i = 0; i < hash->table->length; i++)
        {
            if (hash->table->items[i] != NULL)
            {
                entry = hash->table->items[i];
                break;
            }
        }
    }

    bool HashTable::Iterator::hasNext()
    {
        return entry != NULL;
    }

    HashTable::Entry* HashTable::Iterator::next()
    {
        Entry* curr = entry;
        if (curr != NULL)
        {
            if ((entry = curr->next) == NULL)
            {
                while (++i < hash->table->length)
                {
                    if (hash->table->items[i] != NULL)
                    {
                        entry = hash->table->items[i];
                        break;
                    }
                }
            }
        }
        return curr;
    }

    HashTable::Iterator HashTable::iterator()
    {
        return Iterator(this);
    }



    inline int hashcode(char const* str)
    {
        int h = 0;

        while (*str != 0)
        {
            h = 31 * h + *(unsigned char*)str;
            str += 1;
        }
        return h;
    }

    void* HashTable::get(char const* key)
    {
        int h = hashcode(key);
        int i = (unsigned)h % table->length;
        for (Entry* e = table->items[i]; e != NULL; e = e->next)
        {
            if (e->hashcode == h && STRCMP(e->key, key) == 0)
            {
                return e->value;
            }
        }
        return NULL;
    }

    void HashTable::put(char const* key, void* value)
    {
        int h = hashcode(key);
        int i = (unsigned)h % table->length;
        table->items[i] = new Entry(key, value, h, table->items[i]);
    }

    void* HashTable::remove(char const* key)
    {
        int h = hashcode(key);
        int i = (unsigned)h % table->length;
        Entry **epp, *ep;
        for (epp = &table->items[i]; (ep = *epp) != NULL; epp = &ep->next) {
            if (STRCMP(ep->key, key) == 0) {
                *epp = ep->next;
                return ep->value;
            }
        }
        return NULL;
    }

    static const size_t primeNumbers[] =
    {
        1361,           /* 6 */
        2729,           /* 7 */
        5471,           /* 8 */
        10949,          /* 9 */
        21911,          /* 10 */
        43853,          /* 11 */
        87719,          /* 12 */
        175447,         /* 13 */
        350899,         /* 14 */
        701819,         /* 15 */
        1403641,        /* 16 */
        2807303,        /* 17 */
        5614657,        /* 18 */
        11229331,       /* 19 */
        22458671,       /* 20 */
        44917381,       /* 21 */
        89834777,       /* 22 */
        179669557,      /* 23 */
        359339171,      /* 24 */
        718678369,      /* 25 */
        1437356741,     /* 26 */
        2147483647,     /* 27 */
        4294967291U     /* 28 */
    };

    DynamicHashTable::DynamicHashTable()
    {
        tableSizeLog = 0;
        used = 0;
        table = Vector<Entry>::create(primeNumbers[tableSizeLog], &alloc);
        memset(table->items, 0, table->length*sizeof(Entry*));
    }

    DynamicHashTable::Iterator::Iterator(DynamicHashTable* hash)
    {
        this->hash = hash;
        entry = NULL;
        for (i = 0; i < hash->table->length; i++)
        {
            if (hash->table->items[i] != NULL)
            {
                entry = hash->table->items[i];
                break;
            }
        }
    }

    bool DynamicHashTable::Iterator::hasNext()
    {
        return entry != NULL;
    }

    DynamicHashTable::Entry* DynamicHashTable::Iterator::next()
    {
        Entry* curr = entry;
        if (curr != NULL)
        {
            if ((entry = curr->next) == NULL)
            {
                while (++i < hash->table->length)
                {
                    if (hash->table->items[i] != NULL)
                    {
                        entry = hash->table->items[i];
                        break;
                    }
                }
            }
        }
        return curr;
    }

    DynamicHashTable::Iterator DynamicHashTable::iterator()
    {
        return Iterator(this);
    }

    void DynamicHashTable::put(Value* key, void* value)
    {
        size_t h;
        Entry *entry, *next;
        if (++used > table->length) { 
            size_t newHashTableSize = primeNumbers[++tableSizeLog];
            Vector<Entry>* newTable = Vector<Entry>::create(newHashTableSize, &alloc);
            memset(newTable->items, 0, newHashTableSize*sizeof(Entry*));
            for (int i = 0, n = table->length; i < n; i++) {
                for (entry = table->items[i]; entry != NULL; entry = next) {
                    next = entry->next;
                    h = entry->hashcode % newHashTableSize;
                    entry->next = newTable->items[h];
                    newTable->items[h] = entry;
                }
            }
            table = newTable;
        }
        uint64_t hash = key->hashCode();
        h = hash % table->length;
        table->items[h] = new (&alloc) Entry(key->clone(&alloc), value, hash, table->items[h]);
    }

    void* DynamicHashTable::get(Value* key)
    {
        uint64_t hash = key->hashCode();
        size_t h = hash % table->length;
        for (Entry* entry = table->items[h]; entry != NULL; entry = entry->next) {
            if (entry->hashcode == hash && key->equals(entry->key)) { 
                return entry->value;
            }
        }
        return NULL;
    }

    int findLongestSubstring(String* pattern, int& pos, char esc)
    {
        int maxLen = 0;
        int maxPos = pos;
        int curLen = 0;
        int i, n;
        char* p = pattern->body();

        for (i = pos, n = pattern->size(); i < n; i++) {
            if (p[i] == '%' || p[i] == '_' || p[i] == esc)
            {
                if (curLen > maxLen) {
                    maxPos = i - curLen;
                    maxLen = curLen;
                }
                curLen = 0;
                if (p[i] == esc) {
                    i += 1;
                }
            } else {
                curLen += 1;
            }
        }
        if (curLen > maxLen) {
            maxLen = curLen;
            maxPos = i - curLen;
        }
        pos = maxPos;
        return maxLen;
    }

    int findWildcard(String* pattern, int pos, char esc)
    {
        char* p = pattern->body();
        for (int i = pos, n = pattern->size(); i < n; i++)
        {
            if (p[i] == '%' || p[i] == '_' || p[i] == esc)
            {
                return i;
            }
        }
        return  - 1;
    }

    bool matchString(String* str, String* pat, char esc, Runtime* runtime)
    {
        int pi, si, pn = pat->size(), sn = str->size();
        char* ps = pat->body();
        char* ss = str->body();
        int wildcard =  - 1, strpos =  - 1;
        BMSearchContext* ctx = &runtime->bmCtx;
        if (ctx->pattern != ps) {
            int wc = 0;
            for (pi = 0; pi < pn; pi++) {
                char ch = ps[pi];
                if (ch == esc) {
                    pi += 1;
                } else if (ch == '%' || ch == '_') {
                    wc += 1;
                }
            }
            ctx->nWildcards = wc;
            ctx->pattern = ps;
            if (wc == 2 && ps[0] == '%' && ps[pn-1] == '%') {
                int len = pn-3;
                ctx->kind = BMSearchContext::SK_SUBSTRING;
                for (pi = 0; pi < 0xFF; pi++) {
                    ctx->shift[pi] = len+1;
                }
                for (pi = 0; pi < len; pi++) {
                    ctx->shift[ps[pi+1] & 0xFF] = len-pi;
                }
            } else if (wc == 0) {
                ctx->kind = BMSearchContext::SK_EQ;
            } else {
                ctx->kind = BMSearchContext::SK_MATCH;
            }
        }
        if (ctx->kind == BMSearchContext::SK_EQ) {
            return pn == sn && memcmp(ps, ss, pn) == 0;
        } else if (pn - ctx->nWildcards > sn) {
            return false;
        } else if (ctx->kind == BMSearchContext::SK_SUBSTRING) {
            int len = pn-3;
            for (si = len; si < sn; si += ctx->shift[ss[si] & 0xFF]) {
                int k = si;
                pi = len;
                while (ps[pi+1] == ss[k]) {
                    k -= 1;
                    if (--pi < 0) {
                        return true;
                    }
                }
            }
            return false;
        }
        pi = si = 0;
        while (true)
        {
            if (pi < pn && ps[pi] == '%')
            {
                wildcard = ++pi;
                strpos = si;
            }
            else if (si == sn || ss[si] == '\0')
            {
                return pi == pn || ps[pi] == '\0';
            }
            else if (pi + 1 < pn && ps[pi] == esc && ps[pi + 1] == ss[si])
            {
                si += 1;
                pi += 2;
            }
            else if (pi < pn && ((ps[pi] != esc && (ss[si] == ps[pi] || ps[pi] == '_'))))
            {
                si += 1;
                pi += 1;
            }
            else if (wildcard >= 0)
            {
                si = ++strpos;
                pi = wildcard;
            }
            else if (pi == pn)
            {
                return ss[si] == '\0';
            }
            else
            {
                return false;
            }
        }
    }

    #ifdef UNICODE_SUPPORT
        int findLongestSubstring(UnicodeString* pattern, int& pos, char esc)
        {
            int maxLen = 0;
            int maxPos = pos;
            int curLen = 0;
            wchar_t* p = pattern->body();
            int i, n;

            for (i = pos, n = pattern->size(); i < n; i++) {
                if (p[i] == '%' || p[i] == '_' || p[i] == esc)
                {
                    if (curLen > maxLen) {
                        maxPos = i - curLen;
                        maxLen = curLen;
                    }
                    curLen = 0;
                    if (p[i] == esc) {
                        i += 1;
                    }
                } else {
                    curLen += 1;
                }
            }
            if (curLen > maxLen) {
                maxLen = curLen;
                maxPos = i - curLen;
            }
            pos = maxPos;
            return maxLen;
        }

        int findWildcard(UnicodeString* pattern, int pos, char esc)
        {
            wchar_t* p = pattern->body();
            for (int i = pos, n = pattern->size(); i < n; i++)
            {
                if (p[i] == '%' || p[i] == '_' || p[i] == esc)
                {
                    return i;
                }
            }
            return  - 1;
        }

        bool matchString(UnicodeString* str, UnicodeString* pat, char esc)
        {
            int pi = 0, si = 0, pn = pat->size(), sn = str->size();
            wchar_t* ps = pat->body();
            wchar_t* ss = str->body();
            int wildcard =  - 1, strpos =  - 1;
            while (true)
            {
                if (pi < pn && ps[pi] == '%')
                {
                    wildcard = ++pi;
                    strpos = si;
                }
                else if (si == sn || ss[si] == '\0')
                {
                    return pi == pn || ps[pi] == '\0';
                }
                else if (pi + 1 < pn && ps[pi] == esc && ps[pi + 1] == ss[si])
                {
                    si += 1;
                    pi += 2;
                }
                else if (pi < pn && ((ps[pi] != esc && (ss[si] == ps[pi] || ps[pi] == '_'))))
                {
                    si += 1;
                    pi += 1;
                }
                else if (wildcard >= 0)
                {
                    si = ++strpos;
                    pi = wildcard;
                }
                else if (pi == pn)
                {
                    return ps[si] == '\0';
                }
                else
                {
                    return false;
                }
            }
        }
    #endif

    //
    //  The insertion sort template is used for small partitions.
    //  Insertion sort is stable.
    //

    void insertion_sort(void** array, size_t nmemb, comparator_t compare, void* ctx)
    {
        void* temp, ** last, ** first, ** middle;
        if (nmemb > 1)
        {
            first = middle = 1+array;
            last = nmemb - 1+array;
            while (first != last)
            {
                ++first;
                if (compare(*middle, * first, ctx) > 0)
                {
                    middle = first;
                }
            }
            if (compare(*array, * middle, ctx) > 0)
            {
                temp = * array;
                *array = * middle;
                *middle = temp;
            }
            ++array;
            while (array != last)
            {
                first = array++;
                if (compare(*first, * array, ctx) > 0)
                {
                    middle = array;
                    temp = * middle;
                    do
                    {
                        *middle-- = * first--;
                    }
                    while (compare(*first, temp, ctx) > 0);
                    *middle = temp;
                }
            }
        }
    }

    //
    // The median estimate is used to choose pivots for the quicksort algorithm
    //

    static void median_estimate(void** array, size_t n, comparator_t compare, void* ctx)
    {
        void* temp;
        static long unsigned seed = 123456789LU;
        const size_t k = (seed = 69069 * seed + 362437) % --n;

        temp = * array;
        *array = *(array + k);
        *(array + k) = temp;

        if (compare(*(array + 1), * array, ctx) > 0)
        {
            temp = *(array + 1);
            if (compare(*(array + n), * array, ctx) > 0)
            {
                *(array + 1) = * array;
                if (compare(temp, *(array + n), ctx) > 0)
                {
                    *array = *(array + n);
                    *(array + n) = temp;
                }
                else
                {
                    *array = temp;
                }
            }
            else
            {
                *(array + 1) = *(array + n);
                *(array + n) = temp;
            }
        }
        else
        {
            if (compare(*array, *(array + n), ctx) > 0)
            {
                if (compare(*(array + 1), *(array + n), ctx) > 0)
                {
                    temp = *(array + 1);
                    *(array + 1) = *(array + n);
                    *(array + n) = * array;
                    *array = temp;
                }
                else
                {
                    temp = * array;
                    *array = *(array + n);
                    *(array + n) = temp;
                }
            }
        }
    }


    //
    // This heap sort is better than average because it uses Lamont's heap.
    //

    static void heapsort(void** array, size_t nmemb, comparator_t compare, void* ctx)
    {
        size_t i, child, parent;
        void* temp;

        if (nmemb > 1)
        {
            i = --nmemb / 2;
            do
            {
                parent = i;
                temp = array[parent];
                child = parent * 2;
                while (nmemb > child)
                {
                    if (compare(*(array + child + 1), *(array + child), ctx) > 0)
                    {
                        child += 1;
                    }
                    if (compare(*(array + child), temp, ctx) > 0)
                    {
                        array[parent] = array[child];
                        parent = child;
                        child *= 2;
                    }
                    else
                    {
                        child -= 1;
                        break;
                    }
                }
                if (nmemb == child && compare(*(array + child), temp, ctx) > 0)
                {
                    array[parent] = array[child];
                    parent = child;
                }
                array[parent] = temp;
            }
            while (i--)
                ;

            temp = * array;
            *array = *(array + nmemb);
            *(array + nmemb) = temp;

            for (--nmemb; nmemb; --nmemb)
            {
                parent = 0;
                temp = array[parent];
                child = parent * 2;
                while (nmemb > child)
                {
                    if (compare(*(array + child + 1), *(array + child), ctx) > 0)
                    {
                        ++child;
                    }
                    if (compare(*(array + child), temp, ctx) > 0)
                    {
                        array[parent] = array[child];
                        parent = child;
                        child *= 2;
                    }
                    else
                    {
                        --child;
                        break;
                    }
                }
                if (nmemb == child && compare(*(array + child), temp, ctx) > 0)
                {
                    array[parent] = array[child];
                    parent = child;
                }
                array[parent] = temp;

                temp = * array;
                *array = *(array + nmemb);
                *(array + nmemb) = temp;
            }
        }
    }

    //
    // We use this to check to see if a partition is already sorted.
    //
    inline int sorted(void** array, size_t nmemb, comparator_t compare, void* ctx)
    {
        for (--nmemb; nmemb; --nmemb)
        {
            if (compare(*array, *(array + 1), ctx) > 0)
            {
                return 0;
            }
            array += 1;
        }
        return 1;
    }

    //
    // We use this to check to see if a partition is already reverse-sorted.
    //
    inline int rev_sorted(void** array, size_t nmemb, comparator_t compare, void* ctx)
    {
        for (--nmemb; nmemb; --nmemb)
        {
            if (compare(*(array + 1), * array, ctx) > 0)
            {
                return 0;
            }
            array += 1;
        }
        return 1;
    }

    //
    // We use this to reverse a reverse-sorted partition.
    //

    inline void rev_array(void** array, size_t nmemb)
    {
        void* temp, ** end;
        for (end = array + nmemb - 1; end > array; ++array)
        {
            temp = * array;
            *array = * end;
            *end = temp;
            --end;
        }
    }

    //
    // This is the heart of the quick sort algorithm used here.
    // If the sort is going quadratic, we switch to heap sort.
    // If the partition is small, we switch to insertion sort.
    //

    static void qloop(void** array, size_t nmemb, size_t d, comparator_t compare, void* ctx)
    {
        void* temp, ** first, ** last;

        while (nmemb > 50)
        {
            if (sorted(array, nmemb, compare, ctx))
            {
                return ;
            }
            if (d-- == 0)
            {
                heapsort(array, nmemb, compare, ctx);
                return ;
            }
            median_estimate(array, nmemb, compare, ctx);
            first = 1+array;
            last = nmemb - 1+array;

            do
            {
                ++first;
            }
            while (compare(*array, * first, ctx) > 0);

            do
            {
                --last;
            }
            while (compare(*last, * array, ctx) > 0);

            while (last > first)
            {
                temp = * last;
                *last = * first;
                *first = temp;

                do
                {
                    ++first;
                }
                while (compare(*array, * first, ctx) > 0);

                do
                {
                    --last;
                }
                while (compare(*last, * array, ctx) > 0);
            }
            temp = * array;
            *array = * last;
            *last = temp;

            qloop(last + 1, nmemb - 1+array - last, d, compare, ctx);
            nmemb = last - array;
        }
        insertion_sort(array, nmemb, compare, ctx);
    }

    //
    // Introspective quick sort algorithm user entry point.
    // You do not need to directly call any other sorting template.
    // This sort will perform very well under all circumstances.
    //
    void iqsort(void* array, size_t nmemb, comparator_t compare, void* ctx)
    {
        size_t d, n;
        void** a = (void**)array;
        if (nmemb > 1 && !sorted(a, nmemb, compare, ctx))
        {
            if (!rev_sorted(a, nmemb, compare, ctx))
            {
                n = nmemb / 4;
                d = 2;
                while (n)
                {
                    ++d;
                    n /= 2;
                }
                qloop(a, nmemb, 2* d, compare, ctx);
            }
            else
            {
                rev_array(a, nmemb);
            }
        }
    }

    inline char const* skipSep(char const* p) {
        int ch;
        do {
            ch = *p++ & 0xFF;
        } while (ch == '(' || ch == ')' || ch == '{' || ch == '}' || ch == '[' || ch == ']' || ch == ',' || isspace(ch));
        return p - 1;
    }

    inline int countElements(char const* p) {
        int n = 1;
        while ((p = strchr(p, ',')) != NULL) {
            n += 1;
            p += 1;
        }
        return n;
    }

    Array* castArray(Type elemType, Value* arr)
    {
        Array& src = *(Array*)arr;
        int len = src.size();
        switch (elemType)
        {
          case tpBool:
          {
              ScalarArray<bool>& dst = *new ScalarArray<bool>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = src[i]->isTrue();
              }
              return &dst;
          }
          case tpInt1:
          {
              ScalarArray<int1>& dst = *new ScalarArray<int1>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = (int1)src[i]->intValue();
              }
              return &dst;
          }
          case tpUInt1:
          {
              ScalarArray<uint1>& dst = *new ScalarArray<uint1>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = (uint1)src[i]->intValue();
              }
              return &dst;
          }
          case tpInt2:
          {
              ScalarArray<uint2>& dst = *new ScalarArray<uint2>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = (int2)src[i]->intValue();
              }
              return &dst;
          }
          case tpUInt2:
          {
              ScalarArray<int2>& dst = *new ScalarArray<int2>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = (uint2)src[i]->intValue();
              }
              return &dst;
          }
          case tpInt4:
          {
              ScalarArray<int4>& dst = *new ScalarArray<int4>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = (int4)src[i]->intValue();
              }
              return &dst;
          }
          case tpUInt4:
          {
              ScalarArray<uint4>& dst = *new ScalarArray<uint4>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = (uint4)src[i]->intValue();
              }
              return &dst;
          }
          case tpDateTime:
          {
              ScalarArray<int64_t>& dst = *new ScalarArray<int64_t>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = src[i]->timeValue();
              }
              return &dst;
          }
          case tpInt8:
          {
              ScalarArray<int64_t>& dst = *new ScalarArray<int64_t>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = src[i]->intValue();
              }
              return &dst;
          }
          case tpUInt8:
          {
              ScalarArray<uint64_t>& dst = *new ScalarArray<uint64_t>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = (uint64_t)src[i]->intValue();
              }
              return &dst;
          }
          #ifndef NO_FLOATING_POINT
          case tpReal4:
          {
              ScalarArray<float>& dst = *new ScalarArray<float>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = (float)src[i]->realValue();
              }
              return &dst;
          }
          case tpReal8:
          {
              ScalarArray<double>& dst = *new ScalarArray<double>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = src[i]->realValue();
              }
              return &dst;
          }
          #endif
          case tpString:
          {
              ScalarArray<String*>& dst = *new ScalarArray<String*>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = src[i]->stringValue();
              }
              return &dst;
          }
#ifdef UNICODE_SUPPORT
          case tpUnicode:
          {
              ScalarArray<UnicodeString*>& dst = *new ScalarArray<UnicodeString*>(len, len);
              for (int i = 0; i < len; i++) {
                  dst[i] = src[i]->unicodeStringValue();
              }
              return &dst;
          }
#endif
          default:
            MCO_THROW RuntimeError("Unsupported array element type");
        }
    }

    Value* parseStringAsArray(Type elemType, char const* str, int& errPos)
    {
        char const* p = str;
        int n, val, len = countElements(p);
        switch (elemType) {
          case tpBool:
          {
              ScalarArray<bool>& arr = *new ScalarArray<bool>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (sscanf(p, "%d%n", &val, &n) != 1) {
                      errPos = p - str;
                      return NULL;
                  }
                  arr[i] = val != 0;
                  p += n;
              }
              return &arr;
          }
           case tpInt1:
          {
              ScalarArray<int1>& arr = *new ScalarArray<int1>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (sscanf(p, "%d%n", &val, &n) != 1) {
                      errPos = p - str;
                      return NULL;
                  }
                  arr[i] = (int1)val;
                  p += n;
              }
              return &arr;
          }
          case tpUInt1:
          {
              ScalarArray<uint1>& arr = *new ScalarArray<uint1>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (sscanf(p, "%d%n", &val, &n) != 1) {
                      errPos = p - str;
                      return NULL;
                  }
                  arr[i] = (uint1)val;
                  p += n;
              }
              return &arr;
          }
          case tpInt2:
          {
              ScalarArray<uint2>& arr = *new ScalarArray<uint2>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (sscanf(p, "%d%n", &val, &n) != 1) {
                      errPos = p - str;
                      return NULL;
                  }
                  arr[i] = (int2)val;
                  p += n;
              }
              return &arr;
          }
          case tpUInt2:
          {
              ScalarArray<int2>& arr = *new ScalarArray<int2>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (sscanf(p, "%d%n", &val, &n) != 1) {
                      errPos = p - str;
                      return NULL;
                  }
                  arr[i] = (uint2)val;
                  p += n;
              }
              return &arr;
          }
          case tpInt4:
          {
              ScalarArray<int4>& arr = *new ScalarArray<int4>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (sscanf(p, "%d%n", &arr[i], &n) != 1) {
                      errPos = p - str;
                      return NULL;
                  }
                  p += n;
              }
              return &arr;
          }
          case tpUInt4:
          {
              ScalarArray<uint4>& arr = *new ScalarArray<uint4>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (sscanf(p, "%u%n", &arr[i], &n) != 1) {
                      errPos = p - str;
                      return NULL;
                  }
                  p += n;
              }
              return &arr;
          }
          case tpInt8:
          {
              ScalarArray<int64_t>& arr = *new ScalarArray<int64_t>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (sscanf(p, "%lld%n", (long long*)&arr[i], &n) != 1) {
                      errPos = p - str;
                      return NULL;
                  }
                  p += n;
              }
              return &arr;
          }
          case tpUInt8:
          {
              ScalarArray<uint64_t>& arr = *new ScalarArray<uint64_t>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (sscanf(p, "%llu%n", (unsigned long long*)&arr[i], &n) != 1) {
                      errPos = p - str;
                      return NULL;
                  }
                  p += n;
              }
              return &arr;
          }
          #ifndef NO_FLOATING_POINT
          case tpReal4:
          {
              ScalarArray<float>& arr = *new ScalarArray<float>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (sscanf(p, "%f%n", &arr[i], &n) != 1) {
                      errPos = p - str;
                      return NULL;
                  }
                  p += n;
              }
              return &arr;
          }
          case tpReal8:
          {
              ScalarArray<double>& arr = *new ScalarArray<double>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (sscanf(p, "%lf%n", &arr[i], &n) != 1) {
                      errPos = p - str;
                      return NULL;
                  }
                  p += n;
              }
              return &arr;
          }
          #endif
          case tpString:
          {
              ScalarArray<String*>& arr = *new ScalarArray<String*>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (*p++ != '"') {
                      errPos = p - str - 1;
                      return NULL;
                  }
                  char const* start = p;
                  int n_escapes = 0;
                  char ch;
                  while ((ch = *p++) != '"') {
                      if (ch == '\0') {
                          errPos = p - str - 1;
                          return NULL;
                       } else if (ch == '\\') {
                          n_escapes += 1;
                          p += 1;
                      }
                  }
                  if (n_escapes == 0) {
                      arr[i] = String::create(start, p-start-1);
                  } else {
                      String* s = String::create(p-start-1-n_escapes);
                      char* dst = s->body();
                      p = start;
                      while ((ch = *p++) != '"') {
                          if (ch == '\0') {
                              errPos = p - str - 1;
                              return NULL;
                          } else if (ch == '\\') {
                              ch = *p++;
                          }
                          *dst++ = ch;
                      }
                      *dst = '\0';
                      arr[i] = s;
                  }
              }
              return &arr;
          }
          #ifdef UNICODE_SUPPORT
          case tpUnicode:
          {
              ScalarArray<UnicodeString*>& arr = *new ScalarArray<UnicodeString*>(len, len);
              for (int i = 0; i < len; i++) {
                  p = skipSep(p);
                  if (*p++ != '"') {
                      errPos = p - str - 1;
                      return NULL;
                  }
                  char const* start = p;
                  int n_escapes = 0;
                  char ch;
                  while ((ch = *p++) != '"') {
                      if (ch == '\0') {
                          errPos = p - str - 1;
                          return NULL;
                       } else if (ch == '\\') {
                          n_escapes += 1;
                          p += 1;
                      }
                  }
                  if (n_escapes == 0) {
                      arr[i] = String::create(start, p-start-1)->unicodeStringValue();
                  } else {
                      String* s = String::create(p-start-1-n_escapes);
                      char* dst = s->body();
                      p = start;
                      while ((ch = *p++) != '"') {
                          if (ch == '\0') {
                              errPos = p - str - 1;
                              return NULL;
                          } else if (ch == '\\') {
                              ch = *p++;
                          }
                          *dst++ = ch;
                      }
                      *dst = '\0';
                      arr[i] = s->unicodeStringValue();
                  }
              }
              return &arr;
          }
          #endif
          default:
            errPos = -1;
            return NULL;
        }
    }


    #ifdef DO_NOT_USE_STDLIB

        int strlen(const char* s)
        {
            const char* b = s;

            while (*s != '\0')
            {
                s += 1;
            }
            return s - b;
        }

        int strncmp(const char* a, const char* b, size_t n)
        {
            while (n-- != 0)
            {
                int diff = * a - * b;
                if (diff != 0)
                {
                    return diff;
                }
            }
            return 0;
        }

        int strcmp(const char* a, const char* b)
        {
            int diff;
            while ((diff = * a - * b) == 0 && * a != '\0')
                ;
            return diff;
        }

        char* strstr(const char* s, const char* p)
        {
            do
            {
                if (strcmp(s, p) == 0)
                {
                    return (char*)s;
                }
            }
            while (*s++ != '\0');

            return NULL;
        }

        void memcpy(void* dst, void const* src, size_t n)
        {
            char* d = (char*)dst;
            char const* s = (char*)src;

            while (n-- != 0)
            {
                *d++ = * s++;
            }
        }

    #endif

}
