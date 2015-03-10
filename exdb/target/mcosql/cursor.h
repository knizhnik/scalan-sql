/*******************************************************************
 *                                                                 *
 *  cursor.h                                                      *
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
 * MODULE:    cursor.h
 *
 * ABSTRACT:  
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#ifndef __CURSOR_H__
#define __CURSOR_H__

#include "scheme.h"
#include "mcoapi.h"
#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
#include "mcoapiseq.h"
#endif
#include "compiler.h"

namespace McoSql
{

    class ColumnNode;

    struct BMSearchContext 
    { 
        enum SearchKind { 
            SK_EQ,
            SK_SUBSTRING,
            SK_MATCH
        };
        char* pattern;
        SearchKind kind;
        int nWildcards;
        unsigned short shift[256];
            
        BMSearchContext() { 
            pattern = NULL;
        }
    };

    class Runtime: public DynamicBoundedObject
    {
      public:
        DatabaseDescriptor* db;
        SqlEngine* engine;
        int groupBegin; /* index of first tuple in the group*/
        int groupEnd; /* index after last tuple in the group*/
        ResultSet* result;
        size_t mark;
        size_t segmentId;
        size_t frameMark;
        bool traceEnabled;
        bool autoCommit;
        size_t limit;
        Transaction* trans;
        DataSource* dependentDataSources;
        int indexVar[MAX_INDEX_VARS];
        BMSearchContext bmCtx;

        static int memProfileTop;

        void release();
        void linkDependentDataSource(DataSource* ds);
        void unlinkDependentDataSource(DataSource* ds);
        
        Runtime(DatabaseDescriptor* db, SqlEngine* engine, size_t segmentId, size_t mark, bool traceEnabled, Transaction* trans,
                bool autoCommit);
    };

    class ProjectionDataSource;

    class ProjectionRecord: public Record
    {
        AbstractAllocator* allocator;
      public:
        virtual int nComponents();
        virtual Value* get(int index);
        virtual void set(int index, Value* value);
        virtual Value* update(int index);

        virtual void deleteRecord();
        virtual void updateRecord();
        virtual Struct* source();
        virtual void* pointer();

        ProjectionRecord(AbstractAllocator* allocator, Vector < ColumnNode > * columns, Record* rec, ProjectionDataSource* ds)
        {
            this->allocator = allocator;
            this->columns = columns;
            this->rec = rec;
            this->ds = ds;
            columnsInitialized = false;
        }

        Vector < ColumnNode > * columns;
        Record* rec;
        ProjectionDataSource* ds;
        bool columnsInitialized;
    };

    class Tuple: public Record
    {
      public:
        virtual Value* clone(AbstractAllocator* alloc);                
        virtual int nComponents();
        virtual Value* get(int index);
        virtual void set(int index, Value* value);
        virtual Value* update(int index);

        virtual void deleteRecord();
        virtual void updateRecord();
        virtual Struct* source();
        virtual void* pointer();

        int nValues;
        Value* values[1];

        static Tuple* create(int nValues)
        {
            return new((nValues - 1)* sizeof(Value*))Tuple(nValues);
        }

        static Tuple* create(int nValues, AbstractAllocator* allocator)
        {
            return new((nValues - 1)* sizeof(Value*), allocator) Tuple(nValues);
        }

      private:
        Tuple(int nValues)
        {
            this->nValues = nValues;
        }
    };

    class ResultSet: public DataSource
    {
      public:
        Record* first;
        Record* last;
        int size;
        Vector < Record > * recordArray;
        Runtime* runtime;
        OrderNode* orderBy;

        Cursor* records();
        Cursor* records(Transaction* trans);

        virtual bool add(Record* rec);
        void add(int pos, Record* rec);

        bool isArray()
        {
            return recordArray != NULL;
        }

        virtual bool isHash();
        virtual bool isResultSet();
        virtual void toArray();

        bool isRIDAvailable();
        int compareRID(Record* r1, Record* r2);
        Reference* getRID(Record* rec);

        Value* get(int row, int column);
        void set(int row, int column, Value* val);

        void release();

        bool isNumberOfRecordsKnown();
        size_t nRecords(Transaction* trans);

        Transaction* currentTransaction();

        void reset();
        void setMark(size_t mark);

        virtual ResultSet* clone() = 0;

        ResultSet(Runtime* runtime);
    };


    class TableResultSet: public ResultSet
    {
      public:
        TableDescriptor* table;
      public:
        Iterator < Field > * fields();
        int nFields();
        Field* findField(String* fieldName);

        bool isRIDAvailable();
        int compareRID(Record* r1, Record* r2);
        Reference* getRID(Record* rec);
        Table* sourceTable();
        void extract(Record* rec, void* dst, size_t size, bool nullIndicators[], ExtractMode mode);
        
        ResultSet* clone();

        TableResultSet(TableDescriptor* table, Runtime* runtime);
    };

    class ColumnIterator: public Iterator < Field > 
    {
      public:
        bool hasNext();
        Field* next();

        ColumnIterator(AbstractAllocator* allocator, Vector < ColumnNode > * columns)
        {
            this->allocator = allocator;
            this->columns = columns;
            i = 0;
        }

        int i;
        Vector < ColumnNode > * columns;
    };


    class TemporaryResultSet: public ResultSet
    {
        Vector < ColumnNode > * columnArray;

      public:
        Iterator < Field > * fields();
        int nFields();

        ResultSet* clone();

        TemporaryResultSet(Vector < ColumnNode > * columns, Runtime* runtime);
    };

    class HashResultSet: public TemporaryResultSet
    {
        Vector<Tuple>* hashTable;
        GroupNode* aggregates;
        OrderNode* groupBy;
        size_t sizeLog;
        size_t hashKeyShift;
        DynamicAllocator hashAllocator;

      public:
        virtual bool isHash();
        virtual void toArray();
        virtual bool add(Record* tuple);
        virtual void release();
        
        HashResultSet(Vector < ColumnNode > * columns, Runtime* runtime, GroupNode* agg, OrderNode* grp);
    };

    class FilterDataSource;

    class InternalCursor: public Cursor
    {
      public:
        bool hasNext();
        Record* next();
        void release();

        InternalCursor(FilterDataSource* source);

      protected:
        FilterDataSource* source;
        Cursor* iterator;
        Record* curr;
    };

    class FilterIterator: public InternalCursor
    {
      public:
        bool hasNext();

        FilterIterator(FilterDataSource* source);

      private:
        size_t mark;
        size_t segment;
    };

    class FilterDataSource: public DataSource
    {
      public:
        SelectNode* select;
        ExprNode* condition;
        DataSource* source;
        Runtime* runtime;
        int tableNo;
        TableDescriptor* table;

        virtual Iterator < Field > * fields();
        virtual int nFields();
        virtual Field* findField(String* fieldName);
        virtual size_t nRecords(Transaction* trans);
        virtual bool isNumberOfRecordsKnown();
        virtual bool isRIDAvailable();
        virtual int compareRID(Record* r1, Record* r2);
        virtual Reference* getRID(Record* rec);
        virtual Cursor* internalCursor(Transaction* trans);
        virtual Cursor* records();
        virtual Cursor* records(Transaction* trans);
        virtual void release();
        virtual Table* sourceTable();
        virtual Transaction* currentTransaction();
        virtual void setMark(size_t mark);
        virtual SortOrder sortOrder();
        virtual bool invert(Transaction* trans);
        virtual void extract(Record* rec, void* dst, size_t size, bool nullIndicators[], ExtractMode mode);

        FilterDataSource(SelectNode* select, DataSource* source, TableDescriptor* table, ExprNode* condition,
                         Runtime* runtime, int tableNo = 1)
        {
            this->select = select;
            this->source = source;
            this->condition = condition;
            this->runtime = runtime;
            this->tableNo = tableNo;
            this->table = table;
        }
    };

    class IndexMergeDataSource : public DataSource
    {
	public:
        TableDescriptor* table;
        Value* alternatives;
        IndexDescriptor* index;
        ExprNode* filter;
        Runtime* runtime;
        SelectNode* select;
        int tableNo;
        int nKeys;
        Range keys[MAX_KEYS];
        DynamicAllocator mergeAllocator;

        class IndexMergeCursor : public Cursor
        {
          public:
            bool hasNext();
            Record* next();
            
            IndexMergeCursor(Transaction* trans, IndexMergeDataSource* source);
            
          protected:
            IndexMergeDataSource* source;
            Iterator<Value>* keyIterator;
            Cursor* valueIterator;
            Record* curr;
        };
      public:
        virtual Iterator < Field > * fields();
        virtual int nFields();
        virtual Field* findField(String* fieldName);
        virtual size_t nRecords(Transaction* trans);
        virtual bool isNumberOfRecordsKnown();
        virtual bool isRIDAvailable();
        virtual int compareRID(Record* r1, Record* r2);
        virtual Reference* getRID(Record* rec);
        virtual Cursor* internalCursor(Transaction* trans);
        virtual Cursor* records();
        virtual Cursor* records(Transaction* trans);
        virtual void release();
        virtual Table* sourceTable();
        virtual Transaction* currentTransaction();
        virtual void setMark(size_t mark);

        IndexMergeDataSource(SelectNode* select, IndexDescriptor* index, int tableNo, int nKeys, Range* keys, Value* alternatives, ExprNode* filter, Runtime* runtime);
    };

    class MergeDataSource : public DataSource
    {
        public:
        DataSource* left;
        DataSource* right;
        Runtime* runtime;

        class MergeCursor : public Cursor
        {
          public:
            bool hasNext();
            Record* next();
            
            MergeCursor(MergeDataSource* source);
            
          protected:
            Cursor* currCursor;            
            Cursor* leftCursor;            
            Cursor* rightCursor;            
        };
      public:
        virtual Iterator < Field > * fields();
        virtual int nFields();
        virtual Field* findField(String* fieldName);
        virtual size_t nRecords(Transaction* trans);
        virtual bool isNumberOfRecordsKnown();
        virtual bool isRIDAvailable();
        virtual int compareRID(Record* r1, Record* r2);
        virtual Reference* getRID(Record* rec);
        virtual Cursor* internalCursor(Transaction* trans);
        virtual Cursor* records();
        virtual Cursor* records(Transaction* trans);
        virtual void release();
        virtual Table* sourceTable();
        virtual Transaction* currentTransaction();
        virtual void setMark(size_t mark);

        MergeDataSource(Runtime* runtime, DataSource* left, DataSource* right);                        
    };


    class ProjectionCursor: public Cursor
    {
      public:
        bool hasNext();
        Record* next();
        void release();

        void init(Vector < ColumnNode > * columns, Cursor* cursor, ProjectionDataSource* ds)
        {
            this->cursor = cursor;
            this->columns = columns;
            this->ds = ds;
            recordAllocator = MemoryManager::getAllocator() == allocator ? NULL : allocator;
        }

      private:
        Cursor* cursor;
        Vector < ColumnNode > * columns;
        ProjectionDataSource* ds;
        AbstractAllocator* recordAllocator;
    };

    class ProjectionDataSource: public DataSource
    {
      public:
        virtual Iterator < Field > * fields();
        virtual int nFields();
        virtual size_t nRecords(Transaction* trans);
        virtual bool isNumberOfRecordsKnown();
        virtual bool isRIDAvailable();
        virtual int compareRID(Record* r1, Record* r2);
        virtual Reference* getRID(Record* rec);
        virtual Cursor* internalCursor(Transaction* trans);
        virtual Cursor* records();
        virtual Cursor* records(Transaction* trans);
        virtual void release();
        virtual Table* sourceTable();
        virtual Transaction* currentTransaction();
        virtual void setMark(size_t mark);
        virtual SortOrder sortOrder();
        virtual bool invert(Transaction* trans);

        ProjectionDataSource(Vector < ColumnNode > * columns, DataSource* source, SelectNode* select, Runtime* runtime)
        {
            this->columns = columns;
            this->source = source;
            this->select = select;
            this->runtime = runtime;
        }

        DataSource* source;
        Runtime* runtime;
        SelectNode* select;
        Vector < ColumnNode > * columns;
    };

    class LimitCursor: public Cursor
    {
      public:
        bool hasNext();
        Record* next();
        void release();

        void init(Cursor* cursor, int start, int limit);

      private:
        Cursor* cursor;
        int limit;
    };


    class LimitDataSource: public DataSource
    {
      public:
        virtual Iterator < Field > * fields();
        virtual int nFields();
        virtual size_t nRecords(Transaction* trans);
        virtual bool isNumberOfRecordsKnown();
        virtual bool isRIDAvailable();
        virtual int compareRID(Record* r1, Record* r2);
        virtual Reference* getRID(Record* rec);
        virtual Cursor* internalCursor(Transaction* trans);
        virtual Cursor* records();
        virtual Cursor* records(Transaction* trans);
        virtual void release();
        virtual Table* sourceTable();
        virtual Transaction* currentTransaction();
        virtual void setMark(size_t mark);

        LimitDataSource(Runtime* runtime, DataSource* source, int start, int limit)
        {
            this->runtime = runtime;
            this->source = source;
            this->start = start;
            this->limit = limit;
        }

        Runtime* runtime;
        DataSource* source;
        size_t start;
        size_t limit;
    };


    class DistinctDataSource: public DataSource
    {
      public:
        SelectNode* select;
        IndexDescriptor* index;
        Runtime* runtime;
        
        virtual Iterator < Field > * fields();
        virtual int nFields();
        virtual Field* findField(String* fieldName);
        virtual size_t nRecords(Transaction* trans);
        virtual bool isNumberOfRecordsKnown();
        virtual bool isRIDAvailable();
        virtual int compareRID(Record* r1, Record* r2);
        virtual Reference* getRID(Record* rec);
        virtual Cursor* internalCursor(Transaction* trans);
        virtual Cursor* records();
        virtual Cursor* records(Transaction* trans);
        virtual void release();
        virtual Table* sourceTable();
        virtual Transaction* currentTransaction();
        virtual void setMark(size_t mark);

        DistinctDataSource(SelectNode* select, IndexDescriptor* index, Runtime* runtime)
        {
            this->select = select;
            this->index = index;
            this->runtime = runtime;
        }
    };

    class DistinctIterator: public Cursor
    {
      public:
        bool hasNext();
        Record* next();

        DistinctIterator(DistinctDataSource* source);

      private:
        size_t mark;
        size_t segment;
        DistinctDataSource* source;
        Record* prev;
        Record* curr;
    };

#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
    class FlattenedDataSource;

    class FlattenedRecord: public Record
    {
        AbstractAllocator* allocator;
      public:
        virtual int nComponents();
        virtual Value* get(int index);
        virtual void set(int index, Value* value);
        virtual Value* update(int index);

        virtual void deleteRecord();
        virtual void updateRecord();
        virtual Struct* source();
        virtual void* pointer();

        bool moveNext();

        FlattenedRecord(AbstractAllocator* allocator, Vector < ColumnNode > * columns, Record* rec);

        Record* rec;
        Vector<ColumnNode>* columns;
        Vector<McoGenericSequence>* sequences;
        Vector<Value>* nextSeqElems;
     };

    class FlattenedCursor: public Cursor
    {
      public:
        bool hasNext();
        Record* next();
        void release();

        void init(Vector < ColumnNode > * columns, Cursor* cursor);

      private:
        Cursor* cursor;
        Vector<ColumnNode>* columns;
        FlattenedRecord* currRecord;
        size_t mark;
        size_t segment;
        bool   hasNextElem;        
    };

    class FlattenedDataSource: public DataSource
    {
      public:
        virtual Iterator < Field > * fields();
        virtual int nFields();
        virtual size_t nRecords(Transaction* trans);
        virtual bool isNumberOfRecordsKnown();
        virtual bool isRIDAvailable();
        virtual int compareRID(Record* r1, Record* r2);
        virtual Reference* getRID(Record* rec);
        virtual Cursor* internalCursor(Transaction* trans);
        virtual Cursor* records();
        virtual Cursor* records(Transaction* trans);
        virtual void release();
        virtual Table* sourceTable();
        virtual Transaction* currentTransaction();
        virtual void setMark(size_t mark);

        FlattenedDataSource(Vector < ColumnNode > * columns, DataSource* source, SelectNode* select, Runtime* runtime)
        {
            this->columns = columns;
            this->source = source;
            this->select = select;
            this->runtime = runtime;
        }

        DataSource* source;
        Runtime* runtime;
        SelectNode* select;
        Vector < ColumnNode > * columns;
    };
#endif
}


#endif
