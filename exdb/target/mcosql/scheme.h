/*******************************************************************
 *                                                                 *
 *  scheme.h                                                      *
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
 * MODULE:    scheme.h
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#ifndef __SCHEME_H__
    #define __SCHEME_H__

    #include <dbapi.h>
    #include "util.h"

    namespace McoSql
    {

        class TableDescriptor;
        class FieldDescriptor;
        class ConstraintDescriptor;
        class IndexDescriptor;
        class QueryNode;
        class Runtime;

        const int MAX_KEYS = 16;
        const int MAX_UNIONS = 16;
        const int MAX_INDEX_VARS = 16;

        class FieldDescriptor;
        class TableDescriptor;

        class DatabaseDescriptor: public Database
        {
            public:
                virtual Iterator < Table > * tables();
                virtual Table* findTable(String* name);
                virtual Table* createTable(Transaction* trans, Table* table);
                virtual Index* createIndex(Transaction* trans, Index* index);
                virtual void close();
                virtual Transaction* beginTransaction(Transaction::Mode mode, int priority = 0, Transaction::IsolationLevel level = Transaction::DefaultIsolationLevel);
                virtual Reference* createReference(int64_t id);
                virtual int getSchemaVersion();

                DatabaseDescriptor(Database* db);
                DatabaseDescriptor(Database* db, DatabaseDescriptor* desc);

                void removeTable(TableDescriptor* table);
                void updateMetadata(TableDescriptor* table);
                bool checkSchema();
                void loadSchema();

                void renameOldTable(TableDescriptor* table);

                Database* impl;
                TableDescriptor* _tables;
                TableDescriptor** _tablesTail;                
                int _schemaVersion;
        };

        class KeyDescriptor: public Key
        {
            public:
                virtual SortOrder order();
                virtual Field* field();

                KeyDescriptor* next;
                FieldDescriptor* _field;
                SortOrder _order;

                KeyDescriptor(FieldDescriptor* field, SortOrder order)
                {
                        _field = field;
                        _order = order;
                }
        };


        class IndexDescriptor: public Index
        {
            private:
                FieldDescriptor* locateKey(FieldDescriptor* field, int &index);
            public:
                virtual String* name();
                virtual Iterator < Key > * keys();
                virtual int nKeys();
                virtual bool isUnique();
                virtual bool isOrdered();
                virtual bool isSpatial();
                virtual bool isTrigram();
                virtual bool isApplicable(SearchOperation cop, int nRanges, Range ranges[]);
                virtual DataSource* select(Transaction* trans, SearchOperation cop, int nRanges, Range ranges[]);
                virtual void drop(Transaction* trans);
                virtual Table* table();

                IndexDescriptor(TableDescriptor* table, String* name, KeyDescriptor* keys);
                IndexDescriptor(TableDescriptor* table, Index* index);

                FieldDescriptor* getKey(int index);

                String* _name;
                KeyDescriptor* _keys;
                TableDescriptor* _table;
                int _nKeys;
                bool _ordered;
                bool _spatial;
                bool _trigram;
                bool _unique;
                Index* impl;
                IndexDescriptor* next;
        };


        class ConstraintDescriptor: public Constraint
        {
            public:
                virtual ConstraintType type();
                virtual Iterator < Field > * fields();
                virtual Table* table();
                virtual String* name();

                virtual bool onDeleteCascade();
                virtual String* primaryKeyTable();
                virtual Iterator < String > * primaryKey();

                ConstraintDescriptor(TableDescriptor* table, String* name, ConstraintType type, Vector < Field > * fields,
                                     ConstraintDescriptor* chain, bool ondeleteCascade, String* primaryKeyTable = NULL,
                                     Vector < String > * primaryKey = NULL);

                ConstraintDescriptor(TableDescriptor* table, Constraint* constraint);

                ConstraintDescriptor* next;
                ConstraintType _type;
                TableDescriptor* _table;
                Vector < Field > * _fields;
                bool _onDeleteCascade;
                String* _primaryKeyTable;
                Vector < String > * _primaryKey;
                String* _name;
        };

        enum IndexOperationKind { 
            IDX_EXACT_MATCH,
            IDX_UNIQUE_MATCH,
            IDX_RANGE,
            IDX_SUBSTRING
        };

        class TableDescriptor: public Table
        {
            public:
                virtual String* name();

                virtual Iterator < Index > * indices();
                virtual void drop(Transaction* trans);

                virtual void deleteAllRecords(Transaction* trans);
                virtual void deleteRecord(Transaction* trans, Record* record);
                virtual void updateRecord(Transaction* trans, Record* record);
                virtual void checkpointRecord(Transaction* trans, Record* record);

                virtual Iterator < Constraint > * constraints();

                virtual Iterator < Field > * fields();
                virtual Cursor* records(Transaction* trans);
                virtual Cursor* internalCursor(Transaction* trans);

                virtual int nFields();

                IndexDescriptor* findIndex(FieldDescriptor* desc, SortOrder order, IndexOperationKind kind = IDX_RANGE);

                virtual Field* findField(String* name);
                virtual FieldDescriptor* findFieldDescriptor(Field* field);

                virtual void release();
                virtual bool isNumberOfRecordsKnown();
                virtual size_t nRecords(Transaction* trans);
                virtual bool isRIDAvailable();
                virtual int compareRID(Record* r1, Record* r2);
                virtual Reference* getRID(Record* rec);
                virtual Record* createRecord(Transaction* trans);
                virtual void extract(Record* rec, void* dst, size_t size, bool nullIndicators[], ExtractMode mode);
                virtual bool temporary();

                TableDescriptor(DatabaseDescriptor* db, Table* table);

                TableDescriptor(DatabaseDescriptor* db, String* name, bool temporary = false);

                String* _name;
                IndexDescriptor* _indices;
                ConstraintDescriptor* _constraints;
                Vector < FieldDescriptor > * columns;
                Table* impl;
                TableDescriptor* next;
                DatabaseDescriptor* db;
                bool _temporary;
        };

        class SyntheticTableDescriptor: public TableDescriptor
        {
            public:
                virtual void drop(Transaction* trans);

                virtual void deleteAllRecords(Transaction* trans);
                virtual void deleteRecord(Transaction* trans, Record* record);
                virtual void updateRecord(Transaction* trans, Record* record);
                virtual void checkpointRecord(Transaction* trans, Record* record);

                virtual void release();
                virtual Record* createRecord(Transaction* trans);

                SyntheticTableDescriptor(DatabaseDescriptor* db, String* name, Vector < FieldDescriptor > * columns,
                                         DataSource* source);
        };

        class ArrayDataSource : public SyntheticTableDescriptor
        {
          public:
            class ArrayCursor : public Cursor
            {
                Array* arr;
                int    curr;
                int    size;

              public:
                ArrayCursor(Array* a) : arr(a), curr(0), size(a->size()) {}

                virtual bool hasNext();
                virtual Record* next();
            };

            virtual bool isNumberOfRecordsKnown();
            virtual Cursor* internalCursor(Transaction* trans);
            virtual Cursor* records(Transaction* trans);
            virtual size_t nRecords(Transaction* trans);
            virtual bool isRIDAvailable();
            virtual Reference* getRID(Record* rec);
            virtual int compareRID(Record* r1, Record* r2);
            ArrayDataSource(DatabaseDescriptor* db, Array* arr, Type elemType, Type subarrayElemType = tpNull);

          private:
            Array* arr;
        };
            

        #if MCO_CFG_CSV_IMPORT_SUPPORT
        class CsvTableDescriptor : public SyntheticTableDescriptor
        {
          public:
            FILE* file;
            String* fileName;
            bool isFixedSizeRecord;
            String* delimiter;
            int skip;

            class CsvCursor : public  Cursor
            {
                CsvTableDescriptor* desc;
                Record* curr;
                int line;
                char sep;
                AbstractAllocator* allocator;

                bool moveNext();
                void reportError(char const* msg);

              public:
                CsvCursor(CsvTableDescriptor* csv);

                virtual bool hasNext();
                virtual Record* next();
                virtual void release();
            };

          public:
            virtual void release();
            virtual bool isNumberOfRecordsKnown();
            virtual Cursor* internalCursor(Transaction* trans);
            virtual Cursor* records(Transaction* trans);
            virtual size_t nRecords(Transaction* trans);
            virtual bool isRIDAvailable();
            virtual Reference* getRID(Record* rec);
            virtual int compareRID(Record* r1, Record* r2);
            CsvTableDescriptor(DatabaseDescriptor* db, String* csvFile, TableDescriptor* pattern, String* delimiter = NULL, int skip = 0);
        };
        #endif

        class StatisticTableDescriptor: public SyntheticTableDescriptor
        {
          public:
            virtual Cursor* records(Transaction* trans);
            virtual Cursor* internalCursor(Transaction* trans);

            StatisticTableDescriptor(DatabaseDescriptor* db, String* name, Vector < FieldDescriptor > * columns,
                                     DataSource* source);
        };

        class IndexStatisticTableDescriptor: public SyntheticTableDescriptor
        {
          public:
            virtual Cursor* records(Transaction* trans);
            virtual Cursor* internalCursor(Transaction* trans);

            IndexStatisticTableDescriptor(DatabaseDescriptor* db, String* name, Vector < FieldDescriptor > * columns,
                                          DataSource* source);
        };

        class DynamicTableDescriptor: public SyntheticTableDescriptor
        {
            public:
                QueryNode* stmt;
                DynamicTableDescriptor* nextTable;

                void evaluate(Runtime* runtime);

                DynamicTableDescriptor(DatabaseDescriptor* db, QueryNode* stmt);
        };

        class FieldDescriptor: public Field
        {
            public:
                virtual String* name();
                virtual Type type();
                virtual Table* table();
                virtual Field* scope();

                virtual Value* get(Struct* rec);
                virtual void set(Struct* rec, Value* val);
                virtual Value* update(Struct* rec);

                virtual String* referencedTableName();
                virtual Iterator < Field > * components();
                virtual Type elementType();
                virtual Field* element();
                virtual int elementSize();
                virtual int fixedSize();
                virtual int width();
                virtual int precision();
                virtual SortOrder order();
                virtual bool isAutoGenerated();
                virtual bool isNullable(); 
                virtual Field* findComponent(String* name);

                FieldDescriptor(TableDescriptor* table, String* name, Type type, Type elemType, int precision = -1, bool nullable = false);
                FieldDescriptor(TableDescriptor* table, Field* field, FieldDescriptor* scope);

                String* _name;
                Type _type;
                Type _elemType; /* for sequences or array */
                int  _elemSize;
                TableDescriptor* _table;
                FieldDescriptor* _scope;
                Field* impl;
                FieldDescriptor* next;

                String* _referencedTableName;
                FieldDescriptor* _components;
                int _fixedSize;
                int _precision;
                int _width;
                bool _nullable;
                SortOrder _order;

                TableDescriptor* referencedTable;
        };

        class SyntheticFieldDescriptor: public FieldDescriptor
        {
                int fieldNo;
            public:
                virtual Value* get(Struct* rec);
                SyntheticFieldDescriptor(TableDescriptor* table, String* name, Type type, Type elemType, int fieldNo, int precision = -1);
        };

    }

#endif
