/*******************************************************************
 *                                                                 *
 *  util.h                                                      *
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
 * MODULE:    util.h
 *
 * ABSTRACT:  
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#ifndef __UTIL_H__
    #define __UTIL_H__

    namespace McoSql
    {

        class String;
        class Array;
        class Runtime;

        #define MCO_SQL_TIME_BUF_SIZE 64
        
        extern char* int64ToString(int64_t val, char* buf, size_t bufSize);
        extern bool stringToInt64(char* str, int64_t &val);
        extern bool stringToNumeric(char* str, int64_t &val, int precision);
        extern bool stringToTime(char* str, time_t &val);
        extern int timeToString(time_t val, char* buf, size_t bufSiz);
        extern bool matchString(String* str, String* pat, char esc, Runtime* runtime);
        extern int findWildcard(String* pattern, int pos, char esc);
        extern int findLongestSubstring(String* pattern, int& pos, char esc);

        extern Value* parseStringAsArray(Type elemType, char const* str, int& errPos);
        extern Array* castArray(Type elemType, Value* arr);

        #ifdef UNICODE_SUPPORT
            class UnicodeString;
            extern bool matchString(UnicodeString* str, UnicodeString* pat, char esc);
            extern int findWildcard(UnicodeString* pattern, int pos, char esc);
            extern int findLongestSubstring(UnicodeString* pattern, int& pos, char esc);
        #endif 

        #define STRLEN(x)      strlen(x)
        #define STRNCMP(x,y,n) strncmp(x,y,n)
        #define STRCMP(x,y)    strcmp(x,y)
        #define STRSTR(x,y)    strstr(x,y)

        #define DOALIGN(x,a) (((x) + (a) - 1) & ~((a) - 1))

/*        #if !defined(_VXWORKS)*/
            #ifndef max
                inline int max(int x, int y)
                {
                        return x < y ? y : x;
                }
                inline int min(int x, int y)
                {
                    return x > y ? y : x;
                }
            #endif 
/*        #endif */

        extern size_t calculateStructAlignment(Iterator < Field > * fi);


        typedef int(*comparator_t)(void* a, void* b, void* ctx);
        extern void iqsort(void* array, size_t nmemb, comparator_t compare, void* ctx);
        extern void insertion_sort(void** array, size_t nmemb, comparator_t compare, void* ctx);

        class HashTable: public DynamicObject
        {
            public:
                class Entry: public DynamicObject
                {
                    public:
                        char const* key;
                        void* value;
                        int hashcode;
                        Entry* next;

                        Entry(char const* key, void* value, int hashcode, Entry* next)
                        {
                                this->key = key;
                                this->value = value;
                                this->hashcode = hashcode;
                                this->next = next;
                        }
                };

                Vector < Entry > * table;
                class Iterator
                {
                        int i;
                        HashTable* hash;
                        Entry* entry;

                    public:
                        Iterator(HashTable* hash);

                        Entry* next();
                        bool hasNext();
                };

                Iterator iterator();

                void put(char const* key, void* value);
                void* get(char const* key);
                void* remove(char const* key);

                HashTable(int size);
        };

        class DynamicHashTable 
        {
        public:
            class Entry: public DynamicObject
            {
            public:
                Value*   key;
                void*    value;
                uint64_t hashcode;
                Entry*   next;

                Entry(Value* key, void* value, uint64_t hashcode, Entry* next)
                {
                    this->key = key;
                    this->value = value;
                    this->hashcode = hashcode;
                    this->next = next;
                }
            };

            class Iterator
            {
                int i;
                DynamicHashTable* hash;
                Entry* entry;
                
            public:
                Iterator(DynamicHashTable* hash);
                
                Entry* next();
                bool hasNext();
            };

            DynamicAllocator alloc;
            void  put(Value* key, void* value);
            void* get(Value* key);
            Iterator iterator();
            DynamicHashTable();
            
        private:
            int tableSizeLog;
            int used;
            Vector<Entry>* table;

        };

    }

#endif
