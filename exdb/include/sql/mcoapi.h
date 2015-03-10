/*******************************************************************
 *                                                                 *
 *  mcoapi.h                                                       *
 *                                                                 *
 *  This file is a part of the eXtremeDB source code               *
 *  Copyright (c) 2001-2005 McObject LLC                           *
 *  All Rights Reserved                                            *
 *                                                                 *
 *******************************************************************/
#ifndef __MCOAPI_H__
    #define __MCOAPI_H__

    #include "dbapi.h"
    #include "mcowrap.h"

    #ifndef MCO_DB_H__
        typedef struct mco_objhandle_t_
        {
            MCO_Hf h;
        } mco_objhandle_t;
    #endif 

    /* Forward declaration of get_dictionary function (if you do not want to include generated header file)*/
    #define GET_DICTIONARY(db) extern "C" mco_dictionary_h db##_get_dictionary()

    extern const McoSql::Type mco2sql[];

#if !defined(_MSC_VER) || _MSC_VER >= 1300
    template<class T>
    class McoSequence;
#endif
    class McoTable;
    class McoIndex;
    class McoField;
    class McoTransaction;
    class McoCursor;
    class McoRecord;
    class McoStruct;
    class McoReference;
    class McoArray;
    class McoVector;
    class McoBlob;
    class McoKey;
    class McoIndexSelection;
    class McoBtreeCursor;
    class McoHashCursor;
    class McoSqlEngine;
    class McoGenericSequence;
    class McoParallelSequence;

    typedef struct mco_database_t_* mco_database_h;

    #define MCO_HA_NOT_MASTER_ERROR_MESSAGE "Node is not a HA master"

    class McoDatabase: public McoSql::Database
    {
            friend class McoTable;
            friend class McoIndex;
            friend class McoField;
            friend class McoTransaction;
            friend class McoCursor;
            friend class McoRecord;
            friend class McoStruct;
            friend class McoReference;
            friend class McoArray;
            friend class McoVector;
            friend class McoBlob;
            friend class McoKey;
            friend class McoIndexSelection;
            friend class McoBtreeCursor;
            friend class McoHashCursor;
            friend class McoSqlEngine;

         public:
            McoDatabase();
            void open(mco_db_h con);
            void connect(McoDatabase const &d, mco_db_h con);
            void close();

            McoSql::Iterator < McoSql::Table > * tables();
            McoSql::Transaction* beginTransaction(McoSql::Transaction::Mode mode, int priority, McoSql::Transaction::IsolationLevel level);
            McoSql::Table* createTable(McoSql::Transaction* trans, McoSql::Table* table);
            McoSql::Index* createIndex(McoSql::Transaction* trans, McoSql::Index* index);
            McoSql::Reference* createReference(int64_t ref);
            int getSchemaVersion();

            static void checkStatus(MCO_RET ret);

            void registerExternalTables(McoSql::Table** table,  size_t nTables);

            /**
             * Create connection pool needed for parallel query execution.
             */
            void createConnectionPool();

            /**
             * Destroy connection pool created by  createConnectionPool()
             */
            void destroyConnectionPool();

            mco_database_h db;
            mco_db_h con;
            McoSql::Vector < McoSql::Table > * _tables;
            mco_db_h* connectionPool;
            size_t nPooledConnections;
            size_t nExternalTables;
            McoSql::Table** externalTables;
            int schema_version;
    };

    class McoTransaction: public McoSql::Transaction
    {
        friend class McoDatabase;
        friend class McoTable;
        friend class McoCursor;
        friend class McoBtreeCursor;
        friend class McoHashCursor;
        friend class McoReference;
        friend class McoIndex;
        friend class McoIndexSelection;
        friend class McoGenericSequence;
        friend class McoParallelSequence;

#if !defined(_MSC_VER) || _MSC_VER >= 1300
        template<class T>
        friend class McoSequence;
#endif
      public:
        mco_trans_h handle();

        virtual bool upgrade();
        virtual bool commit(int phases);
        virtual void checkpoint();
        virtual void rollback();

        McoTransaction(McoDatabase* db, mco_db_h con, MCO_TRANS_TYPE mode, MCO_TRANS_PRIORITY priority, MCO_TRANS_ISOLATION_LEVEL level);

      protected:

        McoTransaction() {}

        void start();
        void restart();

        MCO_TRANS_TYPE mode;
        MCO_TRANS_PRIORITY priority;
        MCO_TRANS_ISOLATION_LEVEL level;
        mco_trans_h transaction;
        mco_db_h con;
        McoDatabase* db;

        struct DerefCache {
            int64_t id;
            mco_objhandle_t hnd;

            DerefCache() { id = 0; }
        };
        DerefCache lastDeref;
    };

    class McoCursor: public McoSql::Cursor
    {
            friend class McoTable;

#if !defined(_MSC_VER) || _MSC_VER >= 1300
            template<class T>
            friend class McoSequence;
#endif
        private:
            bool hasNext();
            McoSql::Record* next();
            void release();

            void moveNext(mco_objhandle_h hnd);
            void open();
            McoCursor(McoTable* table, McoSql::Transaction* trans, uint2 classCode, int2 indexCode);

            McoTable* table;
            McoTransaction* trans;
            mco_cursor_t cursor;
            uint2 classCode;
            int2 indexCode;
            bool hasCurrent;
    };

    class McoTable: public McoSql::Table
    {
            friend class McoDatabase;
            friend class McoCursor;
            friend class McoField;
            friend class McoReference;
            friend class McoIndex;
            friend class McoIndexSelection;
            friend class McoBtreeCursor;
            friend class McoHashCursor;

#if !defined(_MSC_VER) || _MSC_VER >= 1300
            template<class T>
            friend class McoSequence;
#endif
        private:
            virtual int nFields();
            virtual McoSql::Iterator < McoSql::Field > * fields();
            virtual McoSql::Cursor* records(McoSql::Transaction* trans);
            virtual size_t nRecords(McoSql::Transaction* trans);
            virtual bool isNumberOfRecordsKnown();
            virtual int compareRID(McoSql::Record* r1, McoSql::Record* r2);
            virtual McoSql::Reference* getRID(McoSql::Record* rec);
            virtual bool isRIDAvailable();
            virtual McoSql::String* name();
            virtual McoSql::Iterator < McoSql::Index > * indices();
            virtual void drop(McoSql::Transaction* trans);
            virtual void updateRecord(McoSql::Transaction* trans, McoSql::Record* rec);
            virtual void checkpointRecord(McoSql::Transaction* trans, McoSql::Record* rec);
            virtual void deleteRecord(McoSql::Transaction* trans, McoSql::Record* rec);
            virtual void deleteAllRecords(McoSql::Transaction* trans);
            virtual McoSql::Record* createRecord(McoSql::Transaction* trans);
            virtual McoSql::Iterator < McoSql::Constraint > * constraints();
            virtual void extract(McoSql::Record* rec, void* dst, size_t size, bool nullIndicators[], McoSql::ExtractMode mode);
            virtual void updateStatistic(McoSql::Transaction* trans, McoSql::Record* stat);
            virtual bool temporary();

            McoTable(McoDatabase* db);

            void load(int cid, mco_database_h db);
            McoField* findFieldByOffset(int offset);

            static McoField* findFieldByOffsetCompact(McoSql::Vector < McoField > * fields, int offset);
            static McoField* findFieldByOffsetNormal(McoSql::Vector < McoField > * fields, int offset);

            uint2 classCode;
            int2 indexCode;
            int2 autoId;
            uint2 autoidOffsetU;
            uint2 autoidOffsetC;
            size_t initSize;
            uint2 event_id_new;
            uint2 event_id_delete;
            uint2 event_id_delete_all;
            bool isCompact;
            bool isPlain;
            uint2 autoidFieldNo;
            uint1 alignment;
            int flags;

            McoSql::String* _name;
            McoSql::Vector < McoField > * _fields;
            McoSql::Vector < McoIndex > * _indices;
            McoDatabase* db;
    };

    class McoField: public McoSql::Field
    {
            friend class McoTable;
            friend class McoCursor;
            friend class McoBlob;
            friend class McoArray;
            friend class McoVector;
            friend class McoRecord;
            friend class McoIndex;

#if !defined(_MSC_VER) || _MSC_VER >= 1300
            template<class T>
            friend class McoSequence;
#endif
        public:
            virtual McoSql::String* name();
            virtual McoSql::Type type();
            virtual McoSql::Table* table();
            virtual McoSql::Field* scope();
            virtual McoSql::Value* get(McoSql::Struct* rec);
            virtual void set(McoSql::Struct* rec, McoSql::Value* val);
            virtual McoSql::Value* update(McoSql::Struct* rec);
            virtual McoSql::String* referencedTableName();
            virtual McoSql::Iterator < McoSql::Field > * components();
            virtual McoSql::Type elementType();
            virtual McoSql::Field* element();
            virtual int elementSize();
            virtual int fixedSize();
            virtual int precision();
            virtual int width();
            virtual McoSql::SortOrder order();
            virtual bool isAutoGenerated();
            virtual bool isNullable();

            McoField* convert(McoTable* table, mco_database_h db, mco_dict_field_t* field, mco_dict_field_t* indicator, McoField* scope);
            McoField(){}

            static McoSql::Vector < McoField > * structComponents(McoTable* table, mco_database_h db,
                mco_dict_struct_t* sp, McoField* scope);

            bool loadStructComponent(mco_objhandle_h obj);
            void storeStructComponent(mco_objhandle_h obj, McoSql::Struct* rec);

            McoSql::String* _name;
            McoSql::Type sqlType;
            McoSql::Type sqlElemType;
            int mcoType;
            McoTable* _table;
            int2 indexAware;
            uint1 flags;
            uint2 arraySize;
            uint2 fieldSize;
            uint2 structSizeU;
            uint2 structSizeC;
            uint2 structAlignU;
            uint2 structAlignC;
            uint2 event_id;
            int structNum;
            McoField* _scope;
            mco_datalayout_t layout;
            mco_datalayout_t indicator_layout;
            McoField* _element;
            uint2 fieldNo;
            uint2 indicatorNo;
            McoSql::Vector < McoField > * _components;
            McoTable* referencedTable;
            int1 _precision;
            int1 seq_order;
            int1 seq_elem_size;
    };


    class McoRecord: public McoSql::Record
    {
            friend class McoTable;
            friend class McoCursor;
            friend class McoReference;
            friend class McoField;
            friend class McoStruct;
            friend class McoBtreeCursor;
            friend class McoHashCursor;
        public:
            virtual int nComponents();
            virtual McoSql::Value* get(int index);
            virtual McoSql::Value* update(int index);
            virtual void set(int index, McoSql::Value* value);
            virtual McoSql::Struct* source();
            virtual void deleteRecord();
            virtual void updateRecord();
            virtual McoSql::Value* clone(McoSql::AbstractAllocator* allocator);
            
            mco_size_t extract(char* dst);
            mco_size_t extractCompact(char* dst);

            McoRecord(McoSql::Vector < McoField > * fields, McoSql::Transaction* trans, mco_objhandle_h hnd);

            McoSql::Vector < McoField > * fields;
            McoTransaction* trans;
            McoSql::AbstractAllocator* allocator;
            mco_objhandle_t obj;

        public:
            mco_objhandle_h getHandle()
            {
                    return  &obj;
            }
    };

    class McoStruct: public McoRecord
    {
            friend class McoTable;
            friend class McoField;
            friend class McoArray;
            friend class McoVector;
        public:
            McoStruct(McoSql::Vector < McoField > * fields, McoSql::Transaction* trans, mco_objhandle_h hnd, McoSql::Value* scope): McoRecord(fields, trans, hnd)
            {
                    _scope = scope;
            }

            virtual McoSql::Value* clone(McoSql::AbstractAllocator* allocator);
            virtual McoSql::Value* scope();

        private:
            McoSql::Value* _scope;
    };


    class McoReference: public McoSql::Reference
    {
            friend class McoTable;
            friend class McoField;
            friend class McoArray;
            friend class McoVector;
            friend class McoDatabase;
        private:
            virtual int compare(McoSql::Value* x);
            virtual size_t toString(char* buf, size_t bufSize);
            virtual int64_t intValue();
            virtual McoSql::String* stringValue();
            virtual McoSql::Record* dereference();
            virtual McoSql::Value* clone(McoSql::AbstractAllocator* allocator);

#ifndef NO_FLOATING_POINT
            virtual double realValue();
#endif
            McoReference(McoSql::Transaction* trans, McoTable* referencedTable, int64_t id);

            int64_t id;
            McoTable* referencedTable;
            McoTransaction* trans;
    };

    class McoArray: public McoSql::Array
    {
            friend class McoField;
        private:
            virtual McoSql::Value* getAt(int index);
            virtual void setAt(int index, McoSql::Value* value);
            virtual McoSql::Value* updateAt(int index);
            virtual int size();
            virtual void setSize(int newSize);
            virtual void getBody(void* dst, int offs, int len);
            virtual void setBody(void* src, int offs, int len);
            virtual int getElemSize() const;
            virtual McoSql::Type getElemType() const;
            virtual McoSql::Value* clone(McoSql::AbstractAllocator* allocator);

            McoArray(McoField* field, McoSql::Transaction* trans, mco_objhandle_h hnd);

            McoField* field;
            McoSql::Transaction* trans;
            McoSql::AbstractAllocator* allocator;
            mco_objhandle_h hnd;
    };

    class McoVector: public McoSql::Array
    {
            friend class McoField;
        private:
            virtual McoSql::Value* getAt(int index);
            virtual void setAt(int index, McoSql::Value* value);
            virtual McoSql::Value* updateAt(int index);
            virtual int size();
            virtual void setSize(int newSize);
            virtual void getBody(void* dst, int offs, int len);
            virtual void setBody(void* src, int offs, int len);
            virtual int getElemSize() const;
            virtual McoSql::Type getElemType() const;
            virtual McoSql::Value* clone(McoSql::AbstractAllocator* allocator);

            McoVector(McoField* field, McoSql::Transaction* trans, mco_objhandle_h hnd);

            McoField* field;
            McoSql::Transaction* trans;
            McoSql::AbstractAllocator* allocator;
            mco_objhandle_h hnd;
    };

    class McoBlob: public McoSql::Blob
    {
            friend class McoField;
        private:
            virtual int available();
            virtual int get(void* buffer, int size);
            virtual void append(void const* buffer, int size);
            virtual void reset();
            virtual void truncate();
            virtual McoSql::Value* clone(McoSql::AbstractAllocator* allocator);

            McoBlob(McoField* field, mco_objhandle_h hnd);

            McoField* field;
            McoSql::Transaction* trans;
            mco_objhandle_t obj;
            int pos;
    };

    class McoKey: public McoSql::Key
    {
            friend class McoIndex;
        private:
            virtual McoSql::SortOrder order();
            virtual McoSql::Field* field();

            McoKey(McoField* field, McoSql::SortOrder order);

            McoField* _field;
            McoSql::SortOrder _order;
    };

    enum IndexType
    {
        BTREE, PTREE, RTREE, HASH, TRIGRAM
    };

    class McoIndexSelection: public McoSql::DataSource
    {
            friend class McoIndex;
            friend class McoBtreeCursor;
            friend class McoHashCursor;
        private:
            virtual int nFields();
            virtual McoSql::Iterator < McoSql::Field > * fields();
            virtual McoSql::Cursor* records();
            virtual McoSql::Cursor* records(McoSql::Transaction* trans);
            virtual size_t nRecords(McoSql::Transaction* trans);
            virtual bool isNumberOfRecordsKnown();
            virtual int compareRID(McoSql::Record* r1, McoSql::Record* r2);
            virtual McoSql::Reference* getRID(McoSql::Record* rec);
            virtual bool isRIDAvailable();
            virtual McoSql::SortOrder sortOrder();
            virtual bool invert(McoSql::Transaction* trans);
            virtual void extract(McoSql::Record* rec, void* dst, size_t size, bool nullIndicators[], McoSql::ExtractMode mode);
            static McoIndexSelection* create(int nKeys, McoIndex* index);

            McoIndexSelection(McoIndex* index);

            mco_cursor_t cursor;
            mco_objhandle_t obj;
            McoIndex* index;
            bool isEmpty;
            bool compare;
            bool resetNeeded;
            bool inverse;
            bool invertable;
            MCO_OPCODE op;
            McoSql::SortOrder order;
            mco_external_field_t ek[1];
    };

    class McoBtreeCursor: public McoSql::Cursor
    {
            friend class McoIndexSelection;
        private:
            bool hasNext();
            McoSql::Record* next();
            void release();

            McoBtreeCursor(McoIndexSelection* selection, McoSql::Transaction* trans);

            McoIndexSelection* selection;
            McoTransaction* trans;
            bool hasCurrent;
    };

    class McoHashCursor: public McoSql::Cursor
    {
            friend class McoIndexSelection;
        private:
            bool hasNext();
            McoSql::Record* next();
            void release();

            McoHashCursor(McoIndexSelection* selection, McoSql::Transaction* trans);

            McoSql::Record* found;
            McoIndexSelection* selection;
             McoTransaction* trans;
    };

    class McoIndex: public McoSql::Index
    {
            friend class McoTable;
            friend class McoDatabase;
            friend class McoIndexSelection;
            friend class McoHashCursor;
            friend class McoBtreeCursor;
        private:
            virtual McoSql::String* name();
            virtual McoSql::Iterator < McoSql::Key > * keys();
            virtual int nKeys();
            virtual McoSql::DataSource* select(McoSql::Transaction* trans, SearchOperation cop, int nRanges, McoSql
                                               ::Range ranges[]);
            virtual bool isApplicable(SearchOperation cop, int nRanges, McoSql::Range ranges[]);
            virtual void drop(McoSql::Transaction* trans);
            virtual McoSql::Table* table();
            virtual bool isUnique();
            virtual bool isOrdered();
            virtual bool isSpatial();
            virtual bool isTrigram();
            virtual void updateStatistic(McoSql::Transaction* trans, McoSql::Record* stat);

            McoIndex(McoTable* table, int indexCode, mco_database_h db);

            void setKeys(bool low, mco_external_field_t ek[], int nRanges, McoSql::Range ranges[]);
            void setMinValue(mco_external_field_t ek[], int nRanges);
            void setMaxValue(mco_external_field_t ek[], int nRanges);

            int indexCode;
            McoTable* _table;
            McoSql::String* _name;
            McoSql::Vector < McoKey > * _keys;
            bool _unique;
            uint4 _flags;
            IndexType type;

    };

    class FixedString: public McoSql::StringLiteral
    {
        public:
            virtual int compare(McoSql::Value* x);
            virtual McoSql::String* toLowerCase();
            virtual McoSql::String* toUpperCase();
            virtual McoSql::Value* clone(McoSql::AbstractAllocator* allocator);
	    virtual uint64_t hashCode();

            static FixedString* create(int len)
            {
                return new(len)FixedString(len);
            }
            static FixedString* create(int len, McoSql::AbstractAllocator* allocator)
            {
                return new(len, allocator) FixedString(len);
            }

            FixedString(int l): McoSql::StringLiteral(l){}
    };
    #ifdef UNICODE_SUPPORT
        class UnicodeFixedString: public McoSql::UnicodeStringLiteral
        {
            public:
                virtual int compare(McoSql::Value* x);
                virtual McoSql::UnicodeString* toLowerCase();
                virtual McoSql::UnicodeString* toUpperCase();
                virtual McoSql::Value* clone(McoSql::AbstractAllocator* allocator);
		virtual uint64_t hashCode();

                static UnicodeFixedString* create(int len)
                {
                    return new(len*sizeof(wchar_t))UnicodeFixedString(len);
                }

                static UnicodeFixedString* create(int len, McoSql::AbstractAllocator* allocator)
                {
                    return new(len*sizeof(wchar_t), allocator) UnicodeFixedString(len);
                }

                UnicodeFixedString(int l): McoSql::UnicodeStringLiteral(l){}
        };
    #endif

#endif
