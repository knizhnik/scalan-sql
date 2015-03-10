/*******************************************************************
 *                                                                 *
 *  dbapi.h                                                      *
 *                                                                 *
 *  This file is a part of the eXtremeDB source code               *
 *  Copyright (c) 2001-2005 McObject LLC                           *
 *  All Rights Reserved                                            *
 *                                                                 *
 *******************************************************************/
#ifndef __DBAPI_H__
    #define __DBAPI_H__

    #include "apidef.h"

    namespace McoSql
    {

        /**/
        /* The classes below have to be implemented by data provider layer.*/
        /**/

        class Key;
        class Table;
        class Field;
        class Table;
        class Index;

        /**
         * Transaction
         */
        class Transaction: public DynamicBoundedObject
        {
            public:
                enum Mode
                {
                    ReadOnly, Update, ReadWrite, Exclusive
                };
                enum IsolationLevel
                {
                    DefaultIsolationLevel = 0,
                    ReadCommitted = 1,
                    RepeatableRead = 2,
                    Serializable = 4
                };

                /**
                 * Upgrade transaction mode to ReadWrite.
                 * If transaction is already in ReadWrite mode, this method does nothing.
                 * @return true of transaction is upgraded to read-write, false if upgrade is not possible
                 */
                virtual bool upgrade() = 0;

                /**
                 * Commit current transaction.
                 * @param phases bit mask of commit transaction phases
                 * @return true is transaction is successfully committed, false if conflict of MVCC transactions is encountered
                 */
                virtual bool commit(int phases = 1+2) = 0;

            
                /**
                 * Roll back current transaction.
                 */
                virtual void rollback() = 0;


                /*
                 * Checkpoint all changes of transaction
                 */
                virtual void checkpoint() = 0;

                /**
                 * Release transaction.
                 * This method is used to release transaction object and associated
                 * resources when not needed anymore.
                 * This method should be called for transactions explicitly created using Database.beginTransaction method.
                 */
                virtual void release();

                /** 
                 * Do not release memory hold by transaction.
                 * It is used to protect DDL data from destruction
                 */
                virtual void hold();

            protected:
                friend class DatabaseDescriptor;

                Transaction()
                {
                        mark = 0;
                        segmentId = 0;
                        ddlTrans = false;
                }
                size_t mark;
                size_t segmentId;
                bool ddlTrans;
        };


        /**
         * Database interface
         */
        class Database: public DynamicObject
        {
                friend class SqlEngine;
            public:
                /**
                 * Get iterator for all tables in the database.
                 * @return table iterator
                 */
                virtual Iterator < Table > * tables() = 0;

                /**
                 * Find table by name.
                 * @param name table name
                 * @return table or <code>NULL</code> if not found
                 */
                virtual Table* findTable(String* name);

                /**
                 * Find table by name.
                 * @param name table name
                 * @return table or <code>NULL</code> if not found
                 */
                Table* findTable(char const* name);

                /**
                 * Create new table in the database.
                 * @param trans current transaction
                 * @param table descriptor of the created table
                 * @return created table
                 */
                virtual Table* createTable(Transaction* trans, Table* table) = 0;

                /**
                 * Create new index in the database.
                 * @param trans current transaction
                 * @param index descriptor of the created index
                 * @return created index
                 */
                virtual Index* createIndex(Transaction* trans, Index* index) = 0;

                /**
                 * Close database.
                 */
                virtual void close() = 0;

                /**
                 * Start new transaction.
                 * @param mode transaction mode
                 * @return new transaction
                 */
                virtual Transaction* beginTransaction(Transaction::Mode mode, int priority = 0, Transaction::IsolationLevel level = Transaction::DefaultIsolationLevel) = 0;

                /**
                 * Create reference from its integer representation.
                 * @param ref integer representation of reference produced by <code>Reference.inValue()</code> method
                 * @return reference created from specified integer ID
                 * @throws InvalidOperation exception if database doesn't support conversion from integer to reference
                 */
                virtual Reference* createReference(int64_t ref) = 0;

                virtual int getSchemaVersion() = 0;

                /**
                 * Constructor of database class
                 * This constructor sets static instance variable to point to itself.
                 * If no database is explicitly specified in SqlEngine open method,
                 * then value of <code>Database::instance</code> will be used.
                 */
                Database()
                {
                        instance = this;
                }

            private:
                static Database* instance;
        };


        /**
         * Reference value
         */
        class Reference: public Value
        {
            public:
                virtual Type type();
                virtual size_t serialize(char* buf, size_t size);
                virtual uint64_t hashCode();

                /**
                 * Get referenced record.
                 * @return record referenced by this reference
                 */
                virtual Record* dereference() = 0;
        };

        /**
         * Compound (structure) value
         * This value is mostly needed for array elements (array of structs).
         * In this case, this value provides base address for the structure.
         * It can also be used to extract structure components by index and
         * perform component-by-component comparison of records.
         */
        class Struct: public IterableValue
        {
                int currComponent;
            public:
                virtual Type type();
                virtual size_t toString(char* buf, size_t bufSize);
                virtual int compare(Value* x);
                virtual size_t serialize(char* buf, size_t size);
                virtual void output(FormattedOutput *streams, size_t n_streams);


                /**
                 * Get number of components in the records.
                 * It is the same as number of fields in the corresponding type.
                 * @return number of records components
                 */
                virtual int nComponents() = 0;

                /**
                 * Get component of the record with specified index.
                 * @param index record component index
                 * @return value of the record with specified index
                 * @throws OutOfBounds exception if there is no field with such index
                 */
                virtual Value* get(int index) = 0;

                /**
                 * Set component of the record with specified index.
                 * @param index record component index
                 * @param value new value of the component
                 * @throws OutOfBounds exception if there is no field with such index
                 */
                virtual void set(int index, Value* value) = 0;

                /**
                 * Get struct or array component of the record with specified index for update.
                 * @param index record component index
                 * @return value which components or elements will be updated
                 * @throws OutOfBounds exception if there is no field with such index
                 */
                virtual Value* update(int index) = 0;

                /**
                 * Get pointer to the underlying database structure.
                 * This method may be used to distinguish tuples created by SELECT statements
                 * and structures and records corresponding to database types and records.
                 * @return <code>NULL</code> for generated tuple, <code>this</code> for database record or structure
                 * and pointer to the original record for record projections
                 */
                virtual Struct* source() = 0;

                /**
                 * Get value to which this value belongs.
                 * @return outer struct or array or <code>NULL</code> if none
                 */
                virtual Value* scope();

                /**
                 * Implementation of Iterable.next()
                 * @return next element value or NULL if there are no more elements
                 */
                virtual Value* next();

                virtual Value* clone(AbstractAllocator* alloc);

                Struct() : currComponent(0) {}
        };

        /**
         * Array value
         */
        class Array: public List
        {
            public:
                virtual Type type();
                virtual int compare(Value* x);
                virtual size_t toString(char* buf, size_t bufSize);
                virtual size_t serialize(char* buf, size_t size);
                virtual void output(FormattedOutput *streams, size_t n_streams);

                /**
                 * Set array element.
                 * @param index index of the element
                 * @param value new element value
                 * @throws IndexOutOfBounds exception if index is out of range
                 */
                virtual void setAt(int index, Value* value) = 0;

                /**
                 * Get structure or array element with specified index for update.
                 * @param index element index
                 * @return value of element with specified index which components or elements will be updated
                 * @throws OutOfBounds exception if index is out of bounds
                 */
                virtual Value* updateAt(int index) = 0;

                /**
                 * Change array size.
                 * @param newSize new size of the array (can be larger or smaller than original value)
                 */
                virtual void setSize(int newSize) = 0;

                /**
                 * Get array body: copy specified number of elements with offset to the buffer.
                 * This method can be used only for arrays of scalar types.
                 * @param dst buffer to receive array elements
                 * @param offs offset in array from which elements will be taken
                 * @param len how many elements will be copied to the buffer
                 * @throws OutOfBounds exception if offs and len don't specify valid segment within array
                 */
                virtual void getBody(void* dst, int offs, int len) = 0;

                /**
                 * Set array body: copy specified number of elements from buffer to the array with specified offset.
                 * This method can be used only for arrays of scalar types
                 * @param src buffer with elements to be copied
                 * @param offs offset in array from which elements will be stored
                 * @param len how much elements will be copied from the buffer
                 * @throws OutOfBounds exception if offs and len don't specify valid segment within array
                 */
                virtual void setBody(void* src, int offs, int len) = 0;

                /**
                 * Get size of array element
                 * @return size in bytes of array element
                 */
                virtual int getElemSize() const = 0;

                /**
                 * Get type of array element
                 * @return type of array element
                 */
                virtual Type getElemType() const = 0;

                /**
                 * Check if all array data is stored in single segment of memory
                 * @return true if all array data is located at memory segment at address returned by pointer()
                 */
                virtual bool isPlain();
        };

        /**
         * Class used to assign initial value to BLOB and array components.
         * Examples:<br>
         * <b>To insert BLOB:</b>
         * <pre>
         * char* content = new char[content_length];
         * fread(content, content_length, 1, f);
         * ScalarArray&lt;char&gt; file(content, content_length);
         * engine.executeStatement("insert into File values (%i,%s,%v)", id, name, &file);
         * </pre><br>
         * <b>To insert array of integers:</b>
         * <pre>
         * ScalarArray&lt;int&gt; weights(10);
         * for (Node n = nodes; n != NULL; n = n-&gt;next) {
         *     arr.append(n-&gt;weight);
         * }
         * engine.executeStatement("insert into Node values (%i,%s,%v)", id, name, &weights);
         * </pre>
         */
        template <class T> class ScalarArray: public Array
        {
                void storeValue(bool* dst, Value* src) { 
                    *dst = src->isTrue();
                }
                void storeValue(char* dst, Value* src) { 
                    *dst = (char)src->intValue();
                }
                void storeValue(signed char* dst, Value* src) { 
                    *dst = (signed char)src->intValue();
                }
                void storeValue(signed short* dst, Value* src) { 
                    *dst = (signed short)src->intValue();
                }
                void storeValue(signed int* dst, Value* src) { 
                    *dst = (signed int)src->intValue();
                }
                void storeValue(unsigned char* dst, Value* src) { 
                    *dst = (unsigned char)src->intValue();
                }
                void storeValue(unsigned short* dst, Value* src) { 
                    *dst = (unsigned short)src->intValue();
                }
                void storeValue(unsigned int* dst, Value* src) { 
                    *dst = (unsigned int)src->intValue();
                }
                void storeValue(int64_t* dst, Value* src) { 
                    *dst = (int64_t)src->intValue();
                }
                void storeValue(uint64_t* dst, Value* src) { 
                    *dst = (uint64_t)src->intValue();
                }
                void storeValue(float* dst, Value* src) { 
                    *dst = (float)src->realValue();
                }
                void storeValue(double* dst, Value* src) { 
                    *dst = (double)src->realValue();
                }
                void storeValue(String** dst, Value* src) { 
                    *dst = src->stringValue();
                }
                void storeValue(Array** dst, Value* src) { 
                    *dst = (Array*)src;
                }


                Value* createValue(bool val) {
                    return BoolValue::create(val);
                }
                Value* createValue(signed char val) {
                    return new IntValue(val);
                }
                Value* createValue(signed short val) {
                    return new IntValue(val);
                }
                Value* createValue(signed int val) {
                    return new IntValue(val);
                }
                Value* createValue(unsigned char val) {
                    return new IntValue(val);
                }
                Value* createValue(unsigned short val) {
                    return new IntValue(val);
                }
                Value* createValue(unsigned int val) {
                    return new IntValue(val);
                }
                Value* createValue(int64_t val) {
                    return new IntValue(val);
                }
                Value* createValue(uint64_t val) {
                    return new IntValue((int64_t)val);
                }
                Value* createValue(float val) {
                    return new RealValue(val);
                }
                Value* createValue(double val) {
                    return new RealValue(val);
                }
                Value* createValue(String* val) {
                    return val;
                }
                Value* createValue(Array* val) {
                    return val;
                }
                #ifdef UNICODE_SUPPORT
                Value* createValue(UnicodeString* val) {
                    return val;
                }
                void storeValue(UnicodeString** dst, Value* src) { 
                    *dst = src->unicodeStringValue();
                }
                #endif
            public:
                static ScalarArray* create(T* body, int len, bool copy = false);

                /**
                 * Constructor of array constant with specified body and length
                 * Body is not copied so passed pointer should remain valid until end of life of
                 * ArrayConstant object.
                 * @param body array body
                 * @param len array length
                 */
                ScalarArray(T* body, int len, bool copy = false)
                {
                    used = len;
                    if (copy) {
                        buf = (T*)mco_sql_allocate(sizeof(T)*len);
                        if (body != NULL) {
                            memcpy(buf, body, len*sizeof(T));
                        }
                        allocated = len;
                    } else {
                        buf = body;
                        allocated = 0;
                    }
                }

                /**
                 * Allocate array of specified initial size.
                 * @param allocated initial allocated size of the array
                 * @param used used size of the array
                 */
                ScalarArray(int allocated, int used = 0)
                {
                        this->allocated = allocated;
                        this->used = used;
                        buf = (T*)mco_sql_allocate(sizeof(T)*allocated);
                }

                /**
                 * Allocate array of specified initial size using provided allocator
                 * @param allocated initial allocated size of the array
                 * @param allocator allocator to be used for this array 
                 */
                ScalarArray(int allocated, AbstractAllocator* allocator)
                {
                        this->allocated = allocated;
                        this->used = 0;
                        buf = (T*)allocator->allocate(sizeof(T)*allocated);
                }

                /**
                 * Access array element.
                 * @param index index of array element
                 * @return rerefence to array element
                 * @throws IndexOutOfBounds exception if index is less than 0 or greater or equal than array size
                 */
                T &operator[](int index)
                {
                        if ((unsigned)index >= (unsigned)used)
                        {
                                MCO_THROW IndexOutOfBounds(index, used);
                        }
                        return buf[index];
                }

                /** 
                 * Faster version of elmenent access method: no index range check
                 */
                inline T& at(int index) 
                { 
                    return buf[index];
                }

                /**
                 * Append new element to array.
                 * @param elem element to be appended
                 */
                void append(T elem)
                {
                        int i = used;
                        if (i >= allocated)
                        {
                                setSize(i == 0 ? 32 : i* 2);
                        }
                        used = i + 1;
                        buf[i] = elem;
                }

                /**
                 * Get pointer to the array elements.
                 * @return pointer to array element
                 */
                virtual void* pointer()
                {
                        return buf;
                }


                /**
                 * Get size of array element
                 * @return size in bytes of array element
                 */
                virtual int getElemSize() const
                {
                        return sizeof(T);
                }

                /**
                 * Get type of array element
                 * @return type of array element
                 */
                virtual Type getElemType() const;

                virtual void setAt(int index, Value* value)
                {
                        storeValue(&buf[index], value);
                }

                virtual Value* updateAt(int index)
                {
                        MCO_THROW InvalidOperation("ScalarArray::updateAt");
                }

                virtual void setSize(int newSize)
                {
                        if (newSize > allocated)
                        {
                            T* newBuf =  (T*)mco_sql_allocate(sizeof(T)*newSize);
                                memcpy(newBuf, buf, used* sizeof(T));
                                allocated = newSize;
                                buf = newBuf;
                        }
                        used = newSize;
                }

                virtual void getBody(void* dst, int offs, int len)
                {
                        if (offs < 0 || len < 0 || offs + len > used)
                        {
                                MCO_THROW IndexOutOfBounds(offs + len, used);
                        }
                        memcpy(dst, buf + offs, len* sizeof(T));
                }

                virtual void setBody(void* src, int offs, int len)
                {
                        if (offs < 0 || len < 0 || offs + len > used)
                        {
                                MCO_THROW IndexOutOfBounds(offs + len, used);
                        }
                        memcpy(buf + offs, src, len* sizeof(T));
                }

                virtual Value* getAt(int index)
                {
                       return createValue(buf[index]);
                }

                virtual int size()
                {
                        return used;
                }

                Value* clone(AbstractAllocator* alloc)
                {
                    T* copy = (T*)alloc->allocate(sizeof(T)*used);
                    memcpy(copy, buf, sizeof(T)*used);
                    return new (alloc->allocate(sizeof(*this))) ScalarArray(copy, used, false);
                }
                
                virtual bool isPlain() 
                { 
                    return getElemType() <= tpNumeric;
                }

            protected:
                T* buf;
                int used;
                int allocated;
        };


        Array* createScalarArray(AbstractAllocator* alloc, Type elemType, int elemSize, int size);

        template<>
        inline Type ScalarArray<Array*>::getElemType() const { return tpArray; }

        template<>
        inline Type ScalarArray<String*>::getElemType() const { return tpString; }

        #ifdef UNICODE_SUPPORT
        template<>
        inline Type ScalarArray<UnicodeString*>::getElemType() const { return tpUnicode; }
        #endif


        template<>
        inline Type ScalarArray<bool>::getElemType() const { return tpBool; }

        template<>
        inline Type ScalarArray<char>::getElemType() const { return tpInt1; }

        template<>
        inline Type ScalarArray<signed char>::getElemType() const { return tpInt1; }

        template<>
        inline Type ScalarArray<short>::getElemType() const { return tpInt2; }

        template<>
        inline Type ScalarArray<int>::getElemType() const { return tpInt4; }

        template<>
        inline Type ScalarArray<int64_t>::getElemType() const { return tpInt8; }

        template<>
        inline Type ScalarArray<unsigned char>::getElemType() const { return tpUInt1; }

        template<>
        inline Type ScalarArray<unsigned short>::getElemType() const { return tpUInt2; }

        template<>
        inline Type ScalarArray<unsigned int>::getElemType() const { return tpUInt4; }

        template<>
        inline Type ScalarArray<uint64_t>::getElemType() const { return tpUInt8; }

        template<>
        inline Type ScalarArray<float>::getElemType() const { return tpReal4; }

        template<>
        inline Type ScalarArray<double>::getElemType() const { return tpReal8; }

        /**
         * Array with fixed size string elements
         */
        class StringArray: public Array
        {
           public:
            /**
             * Allocate array of specified initial size using provided allocator
             * @param allocated initial allocated size of the array
             * @param allocator allocator to be used for this array 
             */
            StringArray(int nElems, int elemSize, AbstractAllocator* allocator)
            {
                this->nElems = nElems;
                this->elemSize = elemSize;
                buf = (char*)allocator->allocate(elemSize*nElems);
            }             

            /**
             * Get pointer to the array elements.
             * @return pointer to array element
             */
            virtual void* pointer()
            {
                return buf;
            }
            

            /**
             * Get size of array element
             * @return size in bytes of array element
             */
            virtual int getElemSize() const
            {
                return elemSize;
            }

            /**
             * Get type of array element
             * @return type of array element
             */
            virtual Type getElemType() const 
            { 
                return tpString;
            }

            virtual void setAt(int index, Value* value)
            {
                String* str = value->stringValue();
                int len = str->size();
                if ((unsigned)index >= (unsigned)nElems || len > elemSize)
                {
                    MCO_THROW IndexOutOfBounds(index, nElems);
                }
                memcpy(buf + index*elemSize, str->cstr(), len);
                memset(buf + index*elemSize + len, 0, elemSize - len);
            }

            virtual Value* updateAt(int index)
            {
                MCO_THROW InvalidOperation("StringArray::updateAt");
            }

            virtual void setSize(int newSize)
            {
                nElems = newSize;
            }

            virtual void getBody(void* dst, int offs, int len)
            {
                if (offs < 0 || len < 0 || offs + len > nElems)
                {
                    MCO_THROW IndexOutOfBounds(offs + len, nElems);
                }
                memcpy(dst, buf + offs*elemSize, len*elemSize);
            }

            virtual void setBody(void* src, int offs, int len)
            {
                if (offs < 0 || len < 0 || offs + len > nElems)
                {
                    MCO_THROW IndexOutOfBounds(offs + len, nElems);
                }
                memcpy(buf + offs*elemSize, src, len*elemSize);
            }

            virtual Value* getAt(int index)
            {
                if ((unsigned)index >= (unsigned)nElems)
                {
                    MCO_THROW IndexOutOfBounds(index, nElems);
                }
                return String::create(buf + index*elemSize, elemSize);
            }
            
            virtual int size()
            {
                return nElems;
            }
            
            Value* clone(AbstractAllocator* alloc)
            {
                StringArray* copy = new (alloc->allocate(sizeof(*this))) StringArray(nElems, elemSize, alloc);
                memcpy(copy->buf, buf, elemSize*nElems);
                return copy;
            }
            
            virtual bool isPlain()
            {
                return true;
            }

        protected:
            char* buf;
            int nElems;
            int elemSize;
        };
            

        /**
         * Large binary object
         */
        class Blob: public Value
        {
            public:
                virtual Type type();
                virtual int compare(Value* x);
                virtual size_t toString(char* buf, size_t bufSize);
                virtual size_t serialize(char* buf, size_t size);


                /**
                 * Return number of bytes available to be extracted.
                 * It is not the total size of BLOB. It can be smaller than BLOB size. For
                 * example, if BLOB consists of several segments, it can be size of the segment.
                 * @return number of bytes which can be read using one operation
                 */
                virtual int available() = 0;

                /**
                 * Copy BLOB data to the buffer.
                 * This method copies up to <code>size</code> bytes
                 * from the current position in the BLOB to the specified buffer. Then, current position
                 * is moved forward to number of fetched bytes.
                 * @param buffer destination for BLOB data
                 * @param size buffer size
                 * @return actual number of bytes transferred
                 * It can be smaller than <code>size</code> if end of BLOB or BLOB segment is reached.
                 */
                virtual int get(void* buffer, int size) = 0;

                /**
                 * Append new data to the BLOB.
                 * Append always performed at the end of BLOB and doesn't change current position for GET method.
                 * @param buffer source of the data to be inserted in BLOB
                 * @param size number of bytes to be added to BLOB
                 */
                virtual void append(void const* buffer, int size) = 0;

                /**
                 * Reset current read position to the beginning of the BLOB.
                 */
                virtual void reset() = 0;

                /**
                 * Eliminate content of the BLOB.
                 */
                virtual void truncate() = 0;
        };

        /**
         * Table record
         */
        class Record: public Struct
        {
            public:
                /**
                 * Delete record.
                 * This method is not used by OpenQSL itself, so it may not be implemented by underlying storage.
                 */
                virtual void deleteRecord() = 0;

                /**
                 * Store updated records in the storage.
                 * This method is not used by OpenQSL itself, so it may not be implemented by underlying storage.
                 */
                virtual void updateRecord() = 0;

                /**
                 * Pointer to next records (used by ResultSet class)
                 */
                Record* next;
        };

        /**
         * Iterator through set of records with single column
         */
        class ScalarSetIterator : public Iterator<Value>
        {
            Cursor* cursor;
          public:
            ScalarSetIterator(Transaction* trans, DataSource* src) {
                cursor = src->internalCursor(trans);
            }
            bool hasNext() {
                return cursor->hasNext();
            }
            Value* next() {
                return cursor->next()->get(0);
            }
        };


        /**
         * Class used to specify index keys
         */
        class Key: public DynamicObject
        {
            public:
                /**
                 * Order in which records are in index by this field
                 * @return element of Order enum
                 */
                virtual SortOrder order() = 0;

                /**
                 * Field by which records will be sorted in index
                 */
                virtual Field* field() = 0;
        };

        /**
         * Class specifying table constraints (used for <code>Database::createTable</code> method
         * SQL engine doesn't enforce or check any constraints.
         */
        class Constraint: public DynamicObject
        {
            public:
                /**
                 * Constraint type
                 */
                enum ConstraintType
                {
                    UNIQUE, PRIMARY_KEY, USING_INDEX, USING_HASH_INDEX, USING_RTREE_INDEX, USING_TRIGRAM_INDEX, FOREIGN_KEY, NOT_NULL, MAY_BE_NULL
                };
                /**
                 * Get constraint type.
                 * @return type of this constraint
                 */
                virtual ConstraintType type() = 0;

                /**
                 * Get iterator of fields for which this constraint is defined.
                 * @return field iterator for constrained fields
                 */
                virtual Iterator < Field > * fields() = 0;

                /**
                 * Get table this constraint belongs to.
                 * @return table for which this constraint is defined
                 */
                virtual Table* table() = 0;

                /**
                 * Foreign key constraint only
                 * Determines if database should delete all members of the relation when owner of the relation is removed.
                 * @return <code>true</code> if delete operation of relation owner should cause cascade delete of all
                 * relation members
                 * @throws InvalidOperation exception is called not for foreign key constraint
                 */
                virtual bool onDeleteCascade() = 0;

                /**
                 * Foreign key constraint only
                 * Reference to the table containing primary key
                 * @return name of the database which is primary key for this foreign key
                 * @throws InvalidOperation exception is called not for foreign key constraint
                 */
                virtual String* primaryKeyTable() = 0;

                /**
                 * Foreign key constraint only
                 * Get information about primary key fields.
                 * @return iterator of names of primary key fields
                 * @throws InvalidOperation exception is called not for foreign key constraint
                 */
                virtual Iterator < String > * primaryKey() = 0;

                /**
                 * Constraint name
                 */
                virtual String* name() = 0;
        };

        /**
         * Table index
         */
        class Index: public DynamicObject
        {
            public:
                /**
                 * Get name of the index.
                 * @return index name
                 */
                virtual String* name() = 0;

                /**
                 * Get iterator for key fields used in this index.
                 * @return field iterator for index key fields
                 */
                virtual Iterator < Key > * keys() = 0;

                /**
                 * Get number of keys.
                 * @return number of keys in the index
                 */
                virtual int nKeys() = 0;

                enum SearchOperation
                {
                    opNop, opEQ, opGT, opGE, opLT, opLE, opBetween, opLike, opILike, opExactMatch, opPrefixMatch, opOverlaps, opContains, opBelongs, opNear, opContainsAll, opContainsAny
                };


                /**
                 * Select records matching search criteria.
                 * @param trans current transaction
                 * @param op search operation code
                 * @param nRanges number of conditions
                 * It is possible to specify ranges for one or more index keys.
                 * This parameter can be 0. In this case, all records in the index sorted by index key are retrieved.
                 * If the value of this parameter is smaller than number of key fields in the index,
                 * then only first <code>nRanges</code> will be used during search.
                 * @param ranges key value ranges (high, low boundary, or both can be present).
                 * Also, each boundary can be inclusive or exclusive.
                 * @return data source for selected records or <code>NULL</code> if search requested search operation
                 * is not implemented
                 */
                virtual DataSource* select(Transaction* trans, SearchOperation cop, int nRanges, Range ranges[]) = 0;

                /**
                 * Check if index is applicable for specified ranges.
                 * The real values of range boundaries are not important and used only to
                 * determine code of operation: EQ, LT, LE, GT, GE or BETWEEN.
                 * This method should just check which one of them are specified (not NULL)
                 * and whether low boundary is equal to high boundary.
                 * @param op search operation code
                 * @param nRanges number of ranges specified
                 * @param ranges key value ranges (high, low boundary, or both can be present).
                 * Also, each boundary can be inclusive or exclusive.
                 * @return <code>true</code> is index is applicable, <code>false</code> otherwise
                 */
                virtual bool isApplicable(SearchOperation cop, int nRanges, Range ranges[]) = 0;

                /**
                 * Drop index
                 * @param trans current transaction
                 */
                virtual void drop(Transaction* trans) = 0;

                /**
                 * Get table this index belongs to.
                 * @return table for which this index is defined
                 */
                virtual Table* table() = 0;

                /**
                 * If index is unique
                 * @return <code>true</code> if index is unique, <code>false</code> if it is not unique or is unknown
                 */
                virtual bool isUnique() = 0;

                /**
                 * If index performs sort
                 * @return <code>true</code> if index is ordered, <code>false</code> if it is not ordered or is unknown
                 */
                virtual bool isOrdered() = 0;

                /**
                 * If index is spatial index (R-Tree)
                 * @return <code>true</code> if index is spatial, <code>false</code> if it is not spatial or is unknown
                 */
                virtual bool isSpatial() = 0;

                /**
                 * If index is trigram index 
                 * @return <code>true</code> if index is trigram index, <code>false</code> if it is not spatial or is unknown
                 */
                virtual bool isTrigram() = 0;

                  /**
                 * Update index statistic
                 * @param trans current transaction
                 * @param stat statistic record which fields should be updated
                 */
                virtual void updateStatistic(Transaction* trans, Record* stat);
        };

        /**
         * Database table
         */
        class Table: public DataSource
        {
            public:
                virtual Table* sourceTable();
                virtual Cursor* records();
                virtual Cursor* records(Transaction* trans) = 0;
            
                /**
                 * Name of the table
                 * @return name of the table
                 */
                virtual String* name() = 0;

                /**
                 * Check if table is temporary (means not stored on the disk(
                 */
                virtual bool temporary();

                /**
                 * Get iterator through indices defined for this table.
                 * @return index iterator for this table
                 */
                virtual Iterator < Index > * indices() = 0;

                /**
                 * Drop table
                 * @param trans current transaction
                 */
                virtual void drop(Transaction* trans) = 0;

                /**
                 * Update specified record.
                 * @param trans current transaction
                 * @param rec records which columns were changed
                 */
                virtual void updateRecord(Transaction* trans, Record* rec) = 0;

                /**
                 * Insert updated records in indexes
                 * @param trans current transaction
                 * @param rec records which columns were changed
                 */
                virtual void checkpointRecord(Transaction* trans, Record* rec);

                /**
                 * Delete record.
                 * @param trans current transaction
                 * @param rec record to be deleted
                 */
                virtual void deleteRecord(Transaction* trans, Record* rec) = 0;

                /**
                 * Delete all records in the table (but not the table itself).
                 * @param trans current transaction
                 */
                virtual void deleteAllRecords(Transaction* trans) = 0;

                /**
                 * Create new record in the table.
                 * Record is created with default values of the fields.
                 * Update this value using <code>Field.set</code> method and
                 * store updated record in the database using <code>Record.updateRecord</code> or
                 * <code>Table.updateRecord</code> methods.
                 * @param trans current transaction
                 * @return created record with default values of columns
                 */
                virtual Record* createRecord(Transaction* trans) = 0;

                /**
                 * Get iterator through table constraints.
                 * @return constraint iterator
                 */
                virtual Iterator < Constraint > * constraints() = 0;

                /**
                 * Commit current transaction. This method is needed only for virtual (user defined) tables.
                 * @param phases bit mask of commit transaction phases
                 * @return true is transaction is successfully committed, false if conflict of MVCC transactions is encountered
                 */
                virtual bool commit(int phases = 1+2);

                /**
                 * Roll back current transaction. This method is needed only for virtual (user defined) tables.
                 */
                virtual void rollback();

                /**
                 * Update table statistic
                 * @param trans current transaction
                 * @param stat statistic record which fields should be updated
                 */
                virtual void updateStatistic(Transaction* trans, Record* stat);
        };

        /**
         * Table field (column)
         */
        class Field: public DynamicObject
        {
            public:
                /**
                 * Get name of the fields.
                 * @return field name
                 */
                virtual String* name() = 0;

                /**
                 * Get type of the field.
                 * @return field type
                 */
                virtual Type type() = 0;

                /**
                 * Get table this field belongs to.
                 * @return table containing this field
                 */
                virtual Table* table() = 0;

                /**
                 * Get scope field of this field.
                 * @return structure or array field containing this field as component or
                 * <code>NULL</code> if this field is not component of any structure or array
                 */
                virtual Field* scope() = 0;

                /**
                 * Get value of this field.
                 * @param rec record or stucture containing this field
                 * In most cases rec is just table record (even for components of columns of this record having structure
                 * type). But for component of array of struct element, <code>rec</code> should be
                 * <code>Struct</code> value returned for this array element
                 * @return value of this field
                 */
                virtual Value* get(Struct* rec) = 0;

                /**
                 * Set new value of this field.
                 * @param rec record or stucture containing this field
                 * @param val value assigned to the field
                 * @see Field.get
                 */
                virtual void set(Struct* rec, Value* val) = 0;

                /**
                 * Get structure or array value for update.
                 * @param rec record or stucture containing this field
                 * @return value which components or elements will be updated
                 * @see Field.get
                 */
                virtual Value* update(Struct* rec) = 0;

                /**
                 * Only for reference field
                 * Get name of referenced table.
                 * @return name of referenced table or <code>NULL</code> if it not statically known (generic reference)
                 * @throws InvalidOperation exception when applied to non-reference field
                 */
                virtual String* referencedTableName() = 0;

                /**
                 * Only for structure field
                 * Get iterator for fields of this structure.
                 * @return field iterator for structure fields
                 * @throws InvalidOperation exception when applied to non-structure field
                 */
                virtual Iterator < Field > * components() = 0;


                /**
                 * Only for structure field
                 * Find structure component by name.
                 * @return structure component with specified name
                 * @throws InvalidOperation exception when applied to non-structure field
                 */
                virtual Field* findComponent(String* componentName);

                /**
                 * Only for structure field
                 * Find structure component by name.
                 * @return structure component with specified name
                 * @throws InvalidOperation exception when applied to non-structure field
                 */
                Field* findComponent(char const* componentName);

                /**
                 * Get type of sequence or array element.
                 * Only for array or sequence fields.
                 * @return element type or tpNull if field has type other than sequence or array
                 */
                virtual Type elementType() = 0;

                /**
                 * Get size of sequence element.
                 * Only for sequence fields.
                 * @return element type or -1l if field has type other than sequence
                 */
                virtual int elementSize() = 0;

                /**
                 * Only for array field
                 * Get descriptor of array element.
                 * @return descriptor of array element
                 * @throws InvalidOperation exception when applied to non-array field
                 */
                virtual Field* element() = 0;

                /**
                 * Only for array field.
                 * Get size of fixed arrays.
                 * @return size of fixed array or 0 for arrays with varying size
                 * @throws InvalidOperation exception when applied to non-array field
                 */
                virtual int fixedSize() = 0;

                /**
                 * Only for numeric field.
                 * Get field precision
                 * @return precision of numeric field or -1 for other fields
                 */
                virtual int precision() = 0;

                /**
                 * Only for numeric field.
                 * Get field precision
                 * @return width of numeric field or -1 for other fields
                 */
                virtual int width() = 0;

                /**
                 * Field is nullable.
                 * @return true if field can contain NULL values
                 */
                virtual bool isNullable() = 0;

                /**
                 * Column is autogenerated by database and should not be considered in "natural" join.
                 */
                virtual bool isAutoGenerated() = 0;

                /**
                 * Sequence order
                 */
                virtual SortOrder order() = 0;

                /**
                 * Calculate struct alignment.
                 * @return structure alignment
                 */
                size_t calculateStructAlignment();


                size_t serialize(char* buf, size_t bufSize);
        };

    }

#endif
