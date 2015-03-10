/*******************************************************************
 *                                                                 *
 *  apidef.h                                                      *
 *                                                                 *
 *  This file is a part of the eXtremeDB source code               *
 *  Copyright (c) 2001-2005 McObject LLC                           *
 *  All Rights Reserved                                            *
 *                                                                 *
 *******************************************************************/
#ifndef __APIDEF_H__
    #define __APIDEF_H__

    #include "config.h"
    #include "exceptions.h"
    #include "memmgr.h"

    namespace McoSql
    {
        /**
         * Field types
         */
        enum Type
        {
            tpNull, tpBool, tpInt1, tpUInt1, tpInt2, tpUInt2, tpInt4, tpUInt4, tpInt8, tpUInt8, tpReal4, tpReal8,
            tpDateTime,  /* time_t*/
            tpNumeric,
            tpUnicode, tpString, tpRaw, tpReference, tpArray, tpStruct, tpBlob, tpDataSource, tpList, 
            tpSequence,
            tpAverage,
            tpLast,
            tpInt = tpInt8, tpReal = tpReal8,
        };

        /**
         * Mode of extracting structs
         */
        enum ExtractMode
        {
            emReferenceToBody, emCopyFixedSizeStrings, emCloneStrings
        };

        extern "C" void* mco_sql_allocate(size_t size);

        extern char const* const typeMnemonic[];
        extern int const typeSizeof[];
        extern int const typeAlignment[];
        extern Type const fieldValueType[];

        class Runtime;
        class Record;
        class Field;
        class Table;
        class Transaction;
        class Reference;
        class UnicodeString;
        class ScalarSetIterator;

        inline bool isComparableType(Type t)
        {
            /*return unsigned(t - tpBool) <= unsigned(tpReference - tpBool);*/
            return unsigned(t - tpBool) <= unsigned(tpArray - tpBool);
        }

        inline bool isScalarType(Type t)
        {
            return unsigned(t - tpBool) <= unsigned(tpNumeric - tpBool);
        }

        inline bool isStringType(Type t)
        {
            return unsigned(t - tpUnicode) <= unsigned(tpString - tpUnicode);
        }

        /**
         * Base class for all objects allocated from specified memory buffer
         */
        class DynamicObject
        {
            public:
                void* operator new(size_t size, void* ptr) {
                    return ptr;
                }

                void* operator new(size_t size);

                void* operator new(size_t size, size_t varPart)
                {
                        return operator new(size + varPart);
                }

                void* operator new(size_t size, AbstractAllocator* allocator);

                void* operator new(size_t size, size_t varPart, AbstractAllocator* allocator)
                {
                        return operator new(size + varPart, allocator);
                }

                void operator delete (void* ptr){}
                void operator delete (void* ptr, size_t){}
                void operator delete (void* ptr, void*){}
                void operator delete (void* ptr, AbstractAllocator*){}
                void operator delete (void* ptr, size_t, AbstractAllocator*){}

                virtual ~DynamicObject();
        };

        class DynamicBoundedObject: public DynamicObject
        {
            public:
                DynamicBoundedObject();

                AbstractAllocator* allocator;
        };


        /**
         * Iterator interface
         * This is a one-way iterator without rewind.
         * When iterator is created, current position is before first record.
         * Invoke <code>Iterator.next()</code> to get first record
         */
        template <class T> class Iterator: public DynamicBoundedObject
        {
            public:
                /**
                 * Check if more elements are available.
                 * @return <code>true</code> if more elements are available
                 */
                virtual bool hasNext() = 0;

                /**
                 * Move current position to the next element.
                 * When iterator is opened, current position is before first element, so when
                 * this method is invoked the first time, you get first element.
                 * @return current element
                 * @throws NoMoreElements exception if there are no more elements
                 */
                virtual T* next() = 0;

                /**
                 * Release iterator's resources
                 */
                virtual void release() {}
        };

        /**
         * List iterator
         * This iterator performs iteration in L2 list elements.
         * linked by <code>next</code> field
         */
        template <class T> class ListIterator: public Iterator<T>
        {
            private:
                T* curr;
            public:
                virtual bool hasNext()
                {
                        return curr != NULL;
                }

                virtual T* next()
                {
                        T* obj = curr;
                        if (obj == NULL)
                        {
                                MCO_THROW NoMoreElements();
                        }
                        curr = obj->next;
                        return obj;
                }

                ListIterator(T* head)
                {
                        curr = head;
                }
        };


        /**
         * Array of pointers iterator
         * This iterator is used to iterate through array elements.
         */
        template <class T> class ArrayPtrIterator: public Iterator<T>
        {
            private:
                T** v;
                int i;
                int n;

            public:
                virtual bool hasNext()
                {
                        return i<n;
                }

                virtual T* next()
                {
                        if (i >= n)
                        {
                                MCO_THROW NoMoreElements();
                        }
                        return v[i++];
                }

                ArrayPtrIterator(T** arr, int length)
                {
                        v = arr;
                        n = length;
                        i = 0;
                }
        };

        /**
         * Array of instances iterator
         * This iterator is used to iterate through array elements.
         */
        template <class T> class ArrayIterator: public Iterator<T>
        {
            private:
                T*  v;
                int i;
                int n;

            public:
                virtual bool hasNext()
                {
                        return i<n;
                }

                virtual T* next()
                {
                        if (i >= n)
                        {
                                MCO_THROW NoMoreElements();
                        }
                        return &v[i++];
                }

                ArrayIterator(T* arr, int length)
                {
                        v = arr;
                        n = length;
                        i = 0;
                }
        };



        /**
         * Fixed size vector
         * This class is used to keep references to instances of class specified by template parameter.
         */
        template <class T> class Vector: public DynamicObject
        {
            public:
                /**
                 * Number of elements in the vector. Cannot be changed.
                 */
                const int length;

                /**
                 * Array with references
                 */
                T* items[1];


                /**
                 * Get reference to array element.
                 * @param index element index
                 * @return reference to array index
                 * @throws IndexOutOfBounds exception if index is out of range
                 */
                T*  &getAt(int index)
                {
                        if ((unsigned)index >= (unsigned)length)
                        {
                                MCO_THROW IndexOutOfBounds(index, length);
                        }
                        return items[index];
                }

                /**
                 * Copy elements from other vector.
                 * @param src vector source vector (should have the length as destination vector)
                 */
                void copy(Vector < T > * src)
                {
                    memcpy(items, src->items, (src->length < length ? src->length : length)*sizeof(T*));
                }

                /**
                 * Get iterator for vector elements.
                 * @return vector elements iterator
                 */
                Iterator < T > * iterator()
                {
                        return new ArrayPtrIterator < T > (items, length);
                }

                /**
                 * Get iterator for first <i>size</i> elements of the vector.
                 * @param size number of elements of the vector to be iterated
                 * @return vector elements iterator
                 */
                Iterator < T > * iterator(int size)
                {
                        return new ArrayPtrIterator < T > (items, size);
                }

                /**
                 * Create vector with specified size.
                 * @param size number of elements in the created vector
                 * @return created vector with <code>size</code> elements
                 */
                static Vector* create(int size)
                {
                        return new((size - 1)* sizeof(T*))Vector(size);
                }

                /**
                 * Create vector with specified size using provided allocator.
                 * @param size number of elements in the created vector
                 * @return created vector with <code>size</code> elements
                 */
                static Vector* create(int size, AbstractAllocator* allocator)
                {
                    return new((size - 1)*sizeof(T*), allocator)Vector(size);
                }

                /**
                 * Create vector with specified size and copy elements from specified buffer.
                 * @param size number of elements in the created vector
                 * @param ptr pointer to buffer contains values of vector elements
                 * The buffer should at least <code>size</code> elements long.
                 * @return created vector vector with <code>size</code> elements and specified values of elements
                 */
                static Vector* create(int size, T** ptr)
                {
                        return new((size - 1)*sizeof(T*))Vector(size, ptr);
                }

                /**
                 * Construct vector with single element.
                 * @param elem value of the single element of the vector
                 */
                Vector(T* elem): length(1)
                {
                        items[0] = elem;
                }

            private:
                Vector(int size): length(size){}

                Vector(int size, T** ptr): length(size)
                {
                        memcpy(items, ptr, size* sizeof(T*));
                }
        };


        class FormattedOutput 
        {
            public:
                /* default constants */
                enum { 
                    DEFAULT_SEQ_FIRST   = 5,
                    DEFAULT_SEQ_LAST    = 5,
                    DEFAULT_ARRAY_FIRST = 5,
                    DEFAULT_ARRAY_LAST  = 5,
                    UNLIMITED = (size_t)(-1)
                };
            
                /* output parameters */
                size_t seq_first;
                size_t seq_last;
                bool   seq_show_indexes;
                size_t array_first;
                size_t array_last;
                bool   array_show_indexes;

                void initFormat(bool full = true);
                FormattedOutput(FILE *f = 0) : fd(f), disabled(0), htmlEncode(false) { initFormat(); };

                void disable()    { disabled++; }
                void enable()     { disabled--; }
                bool isActive()   { return (fd && disabled == 0); }
                void reset()      { disabled = 0; htmlEncode = false; };
                FILE* &getFd()    { return fd; };
                void setFd(FILE *f);
                void setHTMLEncode(bool enable) { htmlEncode = enable; }
                void print(const char *format, va_list *list);
                void print(const char *format, ...);
                void put(const char *data, size_t len);
                void flush()      { if (fd) fflush(fd); }
            private:
                FILE *fd;
                int disabled;
                bool htmlEncode;

                const static size_t ENCODE_BUF_SIZE = 1024;
                void encode(char c);
        };


        /**
         * Base class for all values
         */
        class Value: public DynamicObject
        {
            public:
                /**
                 * Get value type.
                 * @return type of the value
                 */
                virtual Type type() = 0;

                /**
                 * Compare values.
                 * @param x value to be compared with
                 * @return negative integer if this value is less than <code>val</code>, 0 if the same,
                 * and positive integer if this value is greater than <code>val</code>
                 */
                virtual int compare(Value* x) = 0;

                /**
                 * Check if true boolean constant.
                 * @return <code>true</code> if value is true boolean constant
                 */
                virtual bool isTrue();

                /**
                 * Check if value is null.
                 * @return  <code>true</code> if value is Null
                 */
                virtual bool isNull();

               /**
                 * Check if value is iterable compound value (array, vector, struct, sequence)
                 * @return  <code>true</code> if value is iterable compound value.
                 */
                virtual bool isIterable();

                /**
                 * Get raw pointer to the value data.
                 * For example, for integer value it points to 64 bit integer, for string value - points
                 * to zero string,...
                 * @return pointer to the value data
                 */
                virtual void* pointer();

                /**
                 * Convert value to string.
                 * This method prints values to the specified buffer.
                 * @param buf buffer to receive string representation of value
                 * @param bufSize size of buffer
                 * Not more than <code>bufSize</code> bytes of
                 * string representation of the value are copied. If there is no NULL
                 * byte among the first <code>bufSize</code> bytes, then result will not
                 * be null-terminated.
                 * @return number of bytes actually written (NULL byte is not included)
                 */
                virtual size_t toString(char* buf, size_t bufSize) = 0;

                /**
                 * Check if two values are equal.
                 * Note: this method will return <code>true</code>
                 * when comparing two Null values, but in SQL statement this comparison returns <code>Null</code>.
                 * @param val value to be compared with
                 * @return  <code>true</code> if values are equal
                 */
                bool equals(Value* val)
                {
                    return type() == val->type() && compare(val) == 0;
                }

                /**
                 * Get integer value.
                 * This method tries to cast value to integer and throw
                 * InvalidTypeCast exception if it is not possible.
                 * @return integer value
                 * @throws InvalidTypeCast exception if this value is not convertible to integer
                 */
                virtual int64_t intValue();

                /**
                 * Get integer values scaled to the specified precision
                 */
                virtual int64_t intValue(int precision);


                #ifndef NO_FLOATING_POINT
                    /**
                     * Get real value.
                     * This method tries to cast value to floating point value and throw
                     * InvalidTypeCast exception if it is not possible.
                     * @return floating point value
                     * @throws InvalidTypeCast exception if this value is not convertible to real
                     */
                    virtual double realValue();
                #endif

                /**
                 * Get string representation of value.
                 * Any scalar type can be converted to string.
                 * @return string representation of value
                 * @throws InvalidTypeCast exception if this value is array, struct or BLOB
                 */
                virtual String* stringValue();

                /**
                 * Get timestamp value.
                 * @return time_t value
                 * @throws InvalidTypeCast exception if this value is not of DateTime and String types.
                 */
                virtual time_t timeValue();

                #ifdef UNICODE_SUPPORT
                    /**
                     * Get Unicode string representation of value.
                     * Any scalar type can be converted to to string.
                     * @return unicode string representation of value
                     * @throws InvalidTypeCast exception if this value is array, struct or BLOB
                     */
                    virtual UnicodeString* unicodeStringValue();
                #endif

                /**
                 * Serialize value to the buffer.
                 * @param buf buffer where to place serialized value
                 * @param size size of the buffer.
                 * @return number of bytes written to the buffer or 0 if value doesn't fit in the buffer
                 */
                virtual size_t serialize(char* buf, size_t size) = 0;

                /**
                 * Deserialize value from the buffer.
                 * @param buf buffer with serialized data (here to place serialized value
                 * @param pos number of bytes read fetched from the buffer
                 * @return unpacked value
                 */
                static Value* deserialize(char* buf, size_t &pos);

                virtual void output(FormattedOutput *streams, size_t n_streams);

                /**
                 * Calculate value's hash code
                 */
                virtual uint64_t hashCode();

                /**
                 * Clone this value
                 */
                virtual Value* clone(AbstractAllocator* alloc);
            protected:
                /**
                 * This method is used to copy string representation of value into specified buffer.
                 * Up to <code>size</code> bytes will be copied to the destination buffer.
                 * @param dst destination buffer
                 * @param src zero terminated string to be copied to buffer
                 * @param size size of buffer
                 * @return number of bytes actually placed in buffer
                 */
                static size_t fillBuf(char* dst, char const* src, size_t size);
        };

        /**
         * Compound value which elements can be iterated
         */
        class IterableValue : public Value {
          public:
            /**
             * Check if value is iterable compound value (array, vector, struct, sequence)
             * @return  <code>true</code> if value is iterable compound value.
             */
            virtual bool isIterable();

            /**
             * Iterate through elements
             * @return next element value or NULL if there are no more elements
             */
            virtual Value* next() = 0;
        };

        /**
         * Key sort order in index
         */
        enum SortOrder
        {
            UNSPECIFIED_ORDER = 0, ASCENT_ORDER = 1, DESCENT_ORDER = 2
        };

        /**
         * Cursor is implemented as record iterator.
         */
        typedef Iterator < Record > Cursor;

        /**
         * Abstract data source
         * Data source is some set of tuples.
         * It can be either the primary database table or temporary table.
         * Data source is also treated as value to handle subqueries.
         */
        class DataSource: public Value
        {
                friend class QueryNode;
                friend class SelectNode;
                friend class TableOpNode;
                friend class ScanSetNode;
                friend class ProjectionDataSource;
                friend class FlattenedDataSource;
                friend class LimitDataSource;
                friend class ScalarSetIterator;
                friend class Runtime;

            public:
                virtual Type type();
                virtual int compare(Value* x);
                virtual size_t toString(char* buf, size_t bufSize);

                /**
                 * Release resources used by data source.
                 * This method should be called after end of work with data source (after iterating through all
                 * data source elements).
                 * It will free all memory allocated during execution of this query.
                 */
                virtual void release();

                /**
                 * Get number of fields (columns) in the data source.
                 * @return number of fields in data source. Field can be of structure or vector type.
                 * Despite number of subcomponents in the fields, it will be treated as one field.
                 */
                virtual int nFields() = 0;

                /**
                 * Get iterator for fields (columns) in the data source.
                 * @return field iterator
                 */
                virtual Iterator < Field > * fields() = 0;

                /**
                 * Get iterator for records (tuples) in the data source.
                 * This method is used only by SubSQL, use records() without parameters.
                 * @param trans current transaction
                 * @return record iterator
                 */
                virtual Cursor* records(Transaction* trans) = 0;

                /**
                 * Get iterator for records (tuples) in the data source.
                 * @return record iterator
                 */
                virtual Cursor* records() = 0;

                /**
                 * Check if number of records is known for the data source.
                 * Data source can retrieve record incrementally (on demand).
                 * In this case, total number of records is unknown - iterate through all records
                 * to know actual number of records in data source.
                 * @return <code>true</code> if number of records in data source is known
                 */
                virtual bool isNumberOfRecordsKnown() = 0;

                /**
                 * Get number of records in data source.
                 * This method can be used only
                 * if <code>isNumberOfRecordsKnown()</code> method returns <code>true</code>.
                 * @return number of records in data source
                 * @throws InvalidOperation exception if number of records is not known
                 */
                inline size_t nRecords()
                {
                        return nRecords(currentTransaction());
                }

                /**
                 * Get number of records in data source.
                 * This method can be used only
                 * if <code>isNumberOfRecordsKnown()</code> method returns <code>true</code>.
                 * @param trans current transaction
                 * @return number of records in data source
                 * @throws InvalidOperation exception if number of records is not known
                 */
                virtual size_t nRecords(Transaction* trans) = 0;

                /**
                 * Check if record identifier is available for records in this data source.
                 */
                virtual bool isRIDAvailable() = 0;

                /**
                 * Compare IDs of two records.
                 * This method should be used only if <code>DataSource::isRIDAvailable()</code> returns true.
                 * @param r1 first record
                 * @param r2 second record
                 * @return -1 if ID of first record is smaller than ID of second record,
                 * 0 if they are equal, 1 if ID of first record is greater than ID of second record
                 * @throws InvalidOperation exception if RID is not available for this data source
                 */
                virtual int compareRID(Record* r1, Record* r2) = 0;

                /**
                 * Get ID of the record.
                 * @return reference to the specified record
                 * @throws InvalidOperation exception if RID is not available for this data source
                 */
                virtual Reference* getRID(Record* rec) = 0;

                /**
                 * Get table from which this data source was produced.
                 * @return table from which data is selected or <code>null</code> if data source is not projection of
                 * some table
                 */
                virtual Table* sourceTable();

                /**
                 * Get transaction within which this data source was produced.
                 * @return current transaction
                 */
                virtual Transaction* currentTransaction();

                /**
                 * Extract record components to corresponding C struct.
                 * The rules below describe C struct to be used as destination of this method:
                 * <OL>
                 * <LI>C struct should have exactly the same components in the same order as
                 * database structure.
                 * <LI>OpenSQL assumes <I>default alignment</I> of all struct members - i.e.,
                 * alignment used by the compiler without some special align pragmas.
                 * <LI>Array components are represented by pointer to <code>Array</code> value.
                 * <LI>String components are stored as zero-termninated ANSI string.
                 * <LI>If component of the structure doesn't belong to any table and is result of some
                 * SELECT statement expression calculation, then its type is determined by the following rules:
                 * <UL>
                 * <LI>integer types (char, short, unsigned short, int,...)
                 * are represented by int64_t type
                 * <LI>floating point types (float, double) - by double type
                 * <LI>Other types are represented by themselves
                 * </UL>
                 * <LI>Nested structures should be represented by the same C structs.
                 * </OL>
                 * @param rec record to be extracted
                 * @param dst pointer to the C struct to receive components of this database structures
                 * @param size Size of C struct should match the size of database record and is used only
                 * for verification.
                 * @param nullIndicators array which elements will be set to <code>true</code> if
                 * value of correspondent field is Null. If this array is not specified (is NULL), then
                 * attempting to extract Null value of scalar field will cause RuntimeException.
                 * This array should be large enough to collect indicators of all fields. In case of nested
                 * structure, it should contain elements for each component of this substructure.
                 * @param mode struct component extraction mode.
                 * <UL>
                 * <LI><code>emReferenceToBody</code> set pointer to the string
                 * body inside the database.
                 * Structure should have the char* element. Cursor::next() methods
                 * release memory after each iteration, it means that field values
                 * returned by next() are valid only until next invocation of
                 * next() method.
                 * <LI><code>emCopyFixedSizeStrings</code> copy fixed-size strings
                 * Structure should have char[N] element where N is the size of
                 * the fixed-size string.
                 * <LI><code>emCloneStrings</code> clone string components.
                 * Structure should have the char* element. In this case it is
                 * responsibility of the programmer to deallocate string bodies.
                 * (using "delete[]" operator).
                 */
                virtual void extract(Record* rec, void* dst, size_t size, bool nullIndicators[] = NULL, ExtractMode mode =
                                     emReferenceToBody);

                /**
                 * This method is reverse to extract and store in database values from C struct
                 * Restrictions are the same as for extract
                 */
                virtual void store(Record* rec, void const* src, size_t size, bool const nullIndicators[] = NULL,
                                   ExtractMode mode = emReferenceToBody);

                /**
                 * Find field by name.
                 * @param name field name
                 * @return field or <code>NULL</code> if not found
                 */
                virtual Field* findField(String* name);

                /**
                 * Find field by name.
                 * @param name field name
                 * @return field or <code>NULL</code> if not found
                 */
                Field* findField(char const* name);

                virtual size_t serialize(char* buf, size_t size);

                /**
                 * Check if data source is OpenSQL result set.
                 * This method is used internally by OpenSQL and should not be redefined or used.
                 * @return <code>true</code> if data source is OpenSql::ResultSet class
                 */
                virtual bool isResultSet();

                /**
                 * Get internal iterator for records (tuples) in the data source.
                 * This method is used internally by OpenSQL and should not be re-defined or used.
                 * @return record iterator
                 */
                virtual Cursor* internalCursor(Transaction* trans);

                /**
                 * Get data source sort order
                 */
                virtual SortOrder sortOrder(); 
            
                /** 
                 * Try ti change selection sort order
                 * @return true if order can be changed, false otherwise
                 */
                virtual bool invert(Transaction* trans);

                /**
                 * Explicitely set memory allocation mark to which release method of this data source should unwind stack allocator
                 */
                virtual void setMark(size_t mark);

                DataSource* next;
            protected:
                AbstractAllocator* allocator;

                DataSource();
        };

        /**
         * Class used to control life area of DataSource object
         * DataSource returned by executeQuery method has to be released
         * when no longer needed. Forgetting to release data source can cause
         * application deadlock if transaction lock is not re-entered.
         * This class helps you remember to release data source -
         * release method is called in destructor. It also correctly releases
         * data source in case of thrown exceptions.<BR>
         * Intended usage:<BR>
         * <pre>
         * {
         *      QueryResult result(db-&gt;executeQuery("select * from T"));
         *      Cursor* cursor = result->records();
         *      while (cursor-&gt;hasNext()) {
         *          Record* rec = cursor->next();
         *      }
         *  }
         * </pre>
         */
        class QueryResult
        {
            public:
                QueryResult(DataSource* src)
                {
                        this->src = src;
                }

                DataSource* source()
                {
                        return src;
                }

                DataSource* operator->()
                {
                        return src;
                }

                ~QueryResult()
                {
                    if (src != NULL) {
                        src->release();
                    }
                }

            private:
                DataSource* src;
        };

        /**
         * Null (unknown) value from QSl point of view
         */
        class NullValue: public Value
        {
            public:
                virtual Type type();
                virtual bool isNull();
                virtual void* pointer();
                virtual int compare(Value* x);
                virtual size_t toString(char* buf, size_t bufSize);
                virtual String* stringValue();
                virtual int64_t intValue();
                virtual uint64_t hashCode();
                virtual Value* clone(AbstractAllocator* alloc);
                #ifndef NO_FLOATING_POINT
                    virtual double realValue();
                #endif
                virtual size_t serialize(char* buf, size_t size);
                static NullValue* create();
        };

        /**
         * Single instance representing NULL value
         */
        extern NullValue Null;

        /**
         * Boolean value
         */
        class BoolValue: public Value
        {
            public:
                /**
                 * <code>true</code> or <code>false</code>
                 */
                const bool val;

                virtual Type type();
                virtual bool isTrue();
                virtual void* pointer();
                virtual int compare(Value* x);
                virtual int64_t intValue();
                virtual uint64_t hashCode();
                virtual Value* clone(AbstractAllocator* alloc);
                #ifndef NO_FLOATING_POINT
                    virtual double realValue();
                #endif
                virtual String* stringValue();
                virtual size_t toString(char* buf, size_t bufSize);

                /**
                 * Construct boolean value
                 * @param val <code>true</code> or <code>false</code>
                 */
                static BoolValue* create(bool val);

                virtual size_t serialize(char* buf, size_t size);

                /**
                 * Instance of true boolean value
                 */
                static BoolValue True;

                /**
                 * Instance of false boolean value
                 */
                static BoolValue False;

            private:
                BoolValue(bool v): val(v){}
        };


        /**
         * Signed 64-bit integer value
         * SQL engine doesn't support manipulation with unsigned 64-bit integer values.
         */
        class IntValue: public Value
        {
            public:
                int64_t val;

                virtual Type type();
                virtual void* pointer();
                virtual int compare(Value* x);
                virtual int64_t intValue();
                virtual time_t timeValue();
                virtual uint64_t hashCode();
                virtual Value* clone(AbstractAllocator* alloc);
                #ifndef NO_FLOATING_POINT
                    virtual double realValue();
                #endif
                virtual bool isTrue();
                virtual String* stringValue();
                virtual size_t toString(char* buf, size_t bufSize);
                virtual size_t serialize(char* buf, size_t size);

                static IntValue* create(int64_t v);

                IntValue(int64_t v = 0): val(v) {}
        };

        /**
         * Numeric value with fixed precision
         */
        class NumericValue: public IntValue
        {
          public:
            int precision;

            int64_t scale() const {
                int64_t s = 1;
                for (int prec = precision; --prec >= 0; s *= 10);
                return s;
            }

            virtual Type type();
            virtual void* pointer();
            virtual int compare(Value* x);
            virtual int64_t intValue();
            virtual int64_t intValue(int precision);
            virtual time_t timeValue();
            virtual uint64_t hashCode();
            virtual Value* clone(AbstractAllocator* alloc);
            #ifndef NO_FLOATING_POINT
            virtual double realValue();
            #endif
            virtual bool isTrue();
            virtual String* stringValue();
            virtual size_t toString(char* buf, size_t bufSize);
            virtual size_t serialize(char* buf, size_t size);

            bool parse(char const* str);

            int64_t scale(int prec) const {
                int64_t v = val;
                while (prec > precision) {
                    v *= 10;
                    prec -= 1;
                }
                return v;
            }

            static NumericValue* create(int64_t scaledVal, int prec);
            NumericValue(int64_t scaledVal, int prec) : IntValue(scaledVal), precision(prec) {}
            #ifndef NO_FLOATING_POINT
            NumericValue(double realVal, int prec);
            #endif
            NumericValue(char const* strVal, int prec);
            NumericValue(char const* strVal);
            NumericValue(int64_t intPart, int64_t fracPart, int prec);
        };

        /**
         * Value if user defined type
         */
        class RawValue: public Value
        {
            public:
                /**
                 * Immutable pointer to the opaque value
                 */
                const void* ptr;
                /**
                 * Length of value
                 */
                const int length;

                virtual Type type();
                virtual void* pointer();
                virtual int compare(Value* x);
                virtual String* stringValue();
                virtual size_t toString(char* buf, size_t bufSize);
                virtual size_t serialize(char* buf, size_t size);

                RawValue(void* p, int n): ptr(p), length(n){}
        };


        /**
         * Value specifying date and time based on standard C time_t (seconds since 1970)
         */
        class DateTime: public IntValue
        {
            public:
                virtual Type type();
                virtual String* stringValue();
                virtual size_t toString(char* buf, size_t bufSize);
                virtual size_t serialize(char* buf, size_t size);

                static DateTime* create(int64_t v);
                virtual Value* clone(AbstractAllocator* alloc);

                DateTime(int64_t v): IntValue(v){}

                /**
                 * Format used to convert date to string by strftime function
                 */
                static char const* format;
        };

        #ifndef NO_FLOATING_POINT

            /**
             * 64-bit ANSI floating point value
             */
            class RealValue: public Value
            {
                public:
                    double val;

                    virtual Type type();
                    virtual void* pointer();
                    virtual int compare(Value* x);
                    virtual int64_t intValue();
                    virtual int64_t intValue(int precision);
                    virtual double realValue();
                    virtual uint64_t hashCode();
                    virtual Value* clone(AbstractAllocator* alloc);
                    virtual bool isTrue();
                    virtual String* stringValue();
                    virtual size_t toString(char* buf, size_t bufSize);
                    virtual size_t serialize(char* buf, size_t size);

                    static RealValue* create(double v);

                    /**
                     * Format used to convert real to string by sprintf function
                     */
                    static char const* format;

                    RealValue(double v): val(v){}
            };

            class AvgValue : public RealValue
            {
              public:
                int64_t count;
                AvgValue(double v, int64_t c) : RealValue(v), count(c) {}
                virtual size_t serialize(char* buf, size_t size);
            };

        #endif

        class IterableValueIterator : public Iterator<Value>
        {
           public:
             virtual bool hasNext();
             virtual Value* next();

             IterableValueIterator(IterableValue* iterable);

           private:
             Value* curr;
             IterableValue* iterator;
        };

        /**
         * Common interface for strings and arrays
         */
        class List: public IterableValue
        {
            int currPos;
          public:
            Value* operator[](int index) {
                return getAt(index);
            }

            /**
             * Get iterator through list elements
             */
            Iterator<Value>* iterator()
            {
                currPos = 0;
                return new IterableValueIterator(this);
            }

            /**
             * Get element with specified index.
             * @param index element index
             * @return value of element with specified index
             * @throws OutOfBounds exception if index is out of bounds
             */
            virtual Value* getAt(int index) = 0;

            /**
             * Get number of elements in the list.
             * @return number of elements in the list
             */
            virtual int size() = 0;

            /**
             * Implementation of Iterable.next()
             * @return next element value or NULL if there are no more elements
             */
            virtual Value* next();

            List() : currPos(0) {}
        };

        /**
         * String value
         */
        class String: public List
        {
            public:
                virtual Type type();
                virtual void* pointer();
                virtual int compare(Value* x);
                virtual Value* getAt(int index);
                virtual String* stringValue();
                virtual bool isTrue();
                virtual int64_t intValue();
                virtual int64_t intValue(int precision);
                virtual uint64_t hashCode();
                virtual double realValue();
                virtual time_t timeValue();
                virtual size_t toString(char* buf, size_t bufSize);
                virtual size_t serialize(char* buf, size_t size);
                virtual void output(FormattedOutput *streams, size_t n_streams);

                bool equals(String* str) {
                    return compare(str) == 0;
                }

                /**
                 * Compare with zero-terminated string.
                 * @param str zero terminate string to be compared with
                 * @return negative integer if this value is less than <code>val</code>, 0 if they are the same
                 * and positive integer if this value is greater than <code>val</code>.
                 */
                int compare(char const* str);

                /**
                 * Check if string has specified prefix
                 * @param prefix prefix string
                 * @return true if strings has specified prefix
                 */
                bool startsWith(String* prefix);

                /**
                 * Find position of specified substring.
                 * @param s substring to be located
                 * @return position of specified substring or -1 if not found
                 */
                int indexOf(String* s);

                /**
                 * Create string with specified content and length.
                 * @param str string body
                 * @param len string length
                 * @return created string
                 */
                static String* create(char const* str, int len);

                /**
                 * Create string with specified content.
                 * @param str zero terminated string body
                 * @return created string
                 */
                static String* create(char const* str);

                /**
                 * Create string with specified length.
                 * @param len string length
                 * @return created string
                 */
                static String* create(int len);

                /**
                 * Create string with specified length using provided allocator.
                 * @param len string length
                 * @param allocator memory allocator
                 * @return created string
                 */
                static String* create(int len, AbstractAllocator* allocator);

                /**
                 * Create string with specified content.
                 * @param str zero terminated string body
                 * @param allocator memory allocator
                 * @return created string
                 */
                static String* create(char const* str, AbstractAllocator* allocator);


                /**
                 * Create string with specified content and length.
                 * @param str string body
                 * @param len string length
                 * @param allocator memory allocator
                 * @return created string
                 */
                static String* create(char const* str, int len, AbstractAllocator* allocator);

                /**
                 * Convert string to upper case.
                 * @return uppercase string
                 */
                virtual String* toUpperCase();

                /**
                 * Convert string to lower case.
                 * @return lowercase string
                 */
                virtual String* toLowerCase();

                /**
                 * Concatenate two strings.
                 * @param head left string
                 * @param tail right string
                 * @return concatenation of two strings
                 */
                static String* concat(String* head, String* tail);

                /**
                 * Create string with print-like formatting.
                 * @param fmt print-like format string
                 * @return string with all placeholders substituted with values
                 */
                static String* format(char const* fmt, ...);

                /**
                 * Get string body.
                 * @return char array (may be not zero terminated)
                 */
                virtual char* body() = 0;

                /**
                 * Convert to zero-terminated string.
                 * @return C zero-terminated string
                 */
                virtual char* cstr() = 0;

                /**
                 * Get substring of this string.
                 * @param pos start position of substring
                 * @param len substring length
                 * @return substring started at specified position and length chars long
                 */
                String* substr(int pos, int len);
        };

        class StringLiteral: public String
        {
                friend class String;
            public:
                virtual int size();
                virtual char* body();
                virtual char* cstr();

            private:
                const int length;
                char chars[1];

            public:
                virtual Value* clone(AbstractAllocator* alloc);

                StringLiteral(char const* s, int l): length(l)
                {
                        memcpy(chars, s, l);
                        chars[l] = '\0';
                }
                StringLiteral(int l): length(l){}
        };

        class StringRef: public String
        {
                friend class String;
            public:
                virtual Value* clone(AbstractAllocator* alloc);
                virtual int size();
                virtual char* body();
                virtual char* cstr();


            private:
                const int length;
                bool zeroTerminated;
                char* chars;

            public:
                StringRef(char const* s, int l, bool z = false): length(l)
                {
                        chars = (char*)s;
                        zeroTerminated = z;
                }
        };

        class UserString: public String
        {
            public:
                virtual int size();
                virtual char* body();
                virtual char* cstr();

            private:
                const int length;
                char* chars;

            public:
                virtual Value* clone(AbstractAllocator* alloc);
                UserString(char const* s, int l);
                virtual ~UserString();
        };

        #ifdef UNICODE_SUPPORT
            /**
             * Unicode string value
             */
            class UnicodeString: public List
            {
                public:
                    virtual Type type();
                    virtual void* pointer();
                    virtual int compare(Value* x);
                    virtual Value* getAt(int index);
                    virtual String* stringValue();
                    virtual size_t toString(char* buf, size_t bufSize);
                    virtual uint64_t hashCode();

                    virtual UnicodeString* unicodeStringValue();
                    virtual size_t serialize(char* buf, size_t size);

                    /**
                     * Compare with zero-terminated string.
                     * @param str zero terminate string to be compared with
                     * @return negative integer if this value is less than <code>val</code>, 0 if they are the same
                     * and positive integer if this value is greater than <code>val</code>
                     */
                    int compare(wchar_t const* str);

                    /**
                     * Check if string has specified prefix
                     * @param prefix prefix string
                     * @return true if strings has specified prefix
                     */
                    bool startsWith(UnicodeString* prefix);

                    /**
                     * Find position of specified substring.
                     * @param s substring to be located
                     * @return position of specified substring or -1 if not found
                     */
                    int indexOf(UnicodeString* s);

                    /**
                     * Create string with specified content and length.
                     * @param str string body
                     * @param len string length
                     * @return created string
                     */
                    static UnicodeString* create(wchar_t const* str, int len);

                    /**
                     * Create string with specified content.
                     * @param str zero terminated string body
                     * @return created string
                     */
                    static UnicodeString* create(wchar_t const* str);

                    /**
                     * Create string with specified length.
                     * @param len string length
                     * @return created string
                     */
                    static UnicodeString* create(int len);

                    /**
                     * Create string with specified length using provided allocator.
                     * @param len string length
                     * @param allocator memory allocator
                     * @return created string
                     */
                    static UnicodeString* create(int len, AbstractAllocator* allocator);

                    /**
                     * Create unicode string from multibyte string.
                     * @param mbs multibyte string
                     * @return created unicode string
                     */
                    static UnicodeString* create(String* mbs);

                    /**
                     * Convert string to upper case.
                     * @return uppercase string
                     */
                    virtual UnicodeString* toUpperCase();

                    /**
                     * Convert string to lower case.
                     * @return lowercase string
                     */
                    virtual UnicodeString* toLowerCase();

                    /**
                     * Concatenate two strings.
                     * @param head left string
                     * @param tail right string
                     * @return concatenation of two strings
                     */
                    static UnicodeString* concat(UnicodeString* head, UnicodeString* tail);

                    /**
                     * Create string with print-like formatting.
                     * @param fmt print-like format string
                     * @return string with all placeholders substituted with values
                     */
                    static UnicodeString* format(wchar_t const* fmt, ...);

                    /**
                     * Get string body.
                     * @return char array (may be not zero-terminated)
                     */
                    virtual wchar_t* body() = 0;

                    /**
                     * Convert to zero-terminated string.
                     * @return C zero-terminated string
                     */
                    virtual wchar_t* wcstr() = 0;

                    /**
                     * Get substring of this string.
                     * @param pos start position of substring
                     * @param len substring length
                     * @return substring started at specified position and length chars long
                     */
                    UnicodeString* substr(int pos, int len);
            };


            class UnicodeStringLiteral: public UnicodeString
            {
                    friend class UnicodeString;
                public:
                    virtual int size();
                    virtual wchar_t* body();
                    virtual wchar_t* wcstr();
                    virtual Value* clone(AbstractAllocator* alloc);

                private:
                    const int length;
                    wchar_t chars[1];

                public:
                    UnicodeStringLiteral(wchar_t const* s, int l): length(l)
                    {
                            memcpy(chars, s, l* sizeof(wchar_t));
                            chars[l] = '\0';
                    }
                    UnicodeStringLiteral(int l): length(l){}
            };

            class UnicodeStringRef: public UnicodeString
            {
                    friend class UnicodeString;
                public:
                    virtual int size();
                    virtual wchar_t* body();
                    virtual wchar_t* wcstr();
                    virtual Value* clone(AbstractAllocator* alloc);

                private:
                    const int length;
                    bool zeroTerminated;
                    wchar_t* chars;

                public:
                    UnicodeStringRef(wchar_t const* s, int l, bool z = false): length(l)
                    {
                            chars = (wchar_t*)s;
                            zeroTerminated = z;
                    }
            };
        #endif


        class Range: public DynamicObject
        {
            public:
                bool isLowInclusive;
                Value* lowBound;
                bool isHighInclusive;
                Value* highBound;

                Range(bool isLowInclusive, Value* lowBound, bool isHighInclusive, Value* highBound)
                {
                        this->isLowInclusive = isLowInclusive;
                        this->lowBound = lowBound;
                        this->isHighInclusive = isHighInclusive;
                        this->highBound = highBound;
                }

                Range()
                {
                        isLowInclusive = true;
                        lowBound = NULL;
                        isHighInclusive = true;
                        highBound = NULL;
                }
        };

        extern Vector<Value>* createVectorOfValue(int size);

    }

#endif
