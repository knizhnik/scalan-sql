/*******************************************************************
 *                                                                 *
 *  apidef.cpp                                                      *
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
 * MODULE:    apidef.cpp
 *
 * ABSTRACT:  
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#include <ctype.h>
#include <dbapi.h>
#ifdef _WIN32
  #include <float.h>
  #ifdef __MINGW32__
    #include <math.h>
  #endif
  #define isnan _isnan
#else
  #include <math.h>
  #ifndef isnan
  #define isnan(x) ((x) != (x))
  #endif
#endif
#include <float.h>
#include <memmgr.h>
#include "util.h"
#include "stub.h"
#include "convert.h"

namespace McoSql
{

    NullValue Null;
    BoolValue BoolValue::True(true);
    BoolValue BoolValue::False(false);

    #define BUF_SIZE 1024

    Database* Database::instance;

    void* DynamicObject::operator new(size_t size)
    {
        return MemoryManager::allocator->allocate(size);
    }

    void* DynamicObject::operator new(size_t size, AbstractAllocator* allocator)
    {
        return allocator->allocate(size);
    }

    DynamicObject::~DynamicObject() {}

    DynamicBoundedObject::DynamicBoundedObject()
    {
        allocator = MemoryManager::getAllocator();
    }

    char const* const typeMnemonic[] = 
    {
        "Null", "Bool", "Int1", "UInt1", "Int2", "UInt2", "Int4", "UInt4", "Int8", "UInt8", "Real4", "Real8", 
        "DateTime",  // time_t
        "Numeric", 
        "Unicode", "String", "tpRaw", "Reference", "Array", "Struct", "Blob", "DataSource", "List",
        "Sequence",
        "Average"
    };

    int const typeSizeof[] = 
    {
        sizeof(void*),  // tpNull
        sizeof(bool),  // tpBool
        1,  // tpInt1
        1,  // tpUInt1
        2,  // tpInt2
        2,  // tpUInt2
        4,  // tpInt4,
        4,  // tpUInt4,
        8,  // tpInt8
        8,  // tpUInt8,
        4,  // tpReal4
        8,  // tpReal8
        sizeof(time_t),  // tpDateTime
        8,  // tpNumeric
        sizeof(char*),  // tpUnicode
        sizeof(char*),  // tpString,
        sizeof(RawValue*),  // tpRaw,
        8,  // tpReference
        sizeof(Array*),  // tpArray
        0,  // tpStruct
        0,  // tpBlob
        0,  // tpDataSource
        0,  // tpList
        0,  // tpSequence
        16  // tpAverage
    };


    union _aligns
    {
        struct
        {
            char n;
            bool v;
        } _abool;
        struct
        {
            char n;
            short v;
        } _aint2;
        struct
        {
            char n;
            int v;
        } _aint4;
        struct
        {
            char n;
            time_t v;
        } _atime_t;
        struct
        {
            char n;
            int64_t v;
        } _aint8;
    #ifndef NO_FLOATING_POINT
        struct
        {
            char n;
            float v;
        } _afloat;
        struct
        {
            char n;
            double v;
        } _adouble;
    #endif 
        struct
        {
            char n;
            void* v;
        } _aptr;
    };
    #define alignof(type) \
    (((char *)&(((union _aligns *)0)->_a##type.v)) - ((char *)&(((union _aligns *)0)->_a##type.n)))

    int const typeAlignment[] = 
    {
        alignof(ptr),  // tpNull
        alignof(bool),  // tpBool
        1,  // tpInt1
        1,  // tpUInt1
        alignof(int2),  // tpInt2
        alignof(int2),  // tpUInt2
        alignof(int4),  // tpInt4,
        alignof(int4),  // tpUInt4,
        alignof(int8),  // tpInt8
        alignof(int8),  // tpUInt8,
    #ifndef NO_FLOATING_POINT
        alignof(float),  // tpReal4
        alignof(double),  // tpReal8
    #else
        4,
        8,
    #endif 
        alignof(time_t),  // tpDateTime
        alignof(int8), // tpNumeric
        alignof(ptr),  // tpUnicode,
        alignof(ptr),  // tpString,
        alignof(ptr),  // tpRaw,
        alignof(int8),  // tpReference
        alignof(ptr),  // tpArray
        1,  // tpStruct
        1,  // tpBlob
        1,  // tpDataSource
        1,  // tpList
        alignof(ptr)   // tpSequence
    };




    Type const fieldValueType[] = 
    {
        tpNull, tpBool, tpInt, tpInt, tpInt, tpInt, tpInt, tpInt, tpInt, tpInt, tpReal, tpReal, tpDateTime, tpNumeric, tpUnicode,
        tpString, tpRaw, tpReference, tpArray, tpStruct, tpBlob, tpDataSource, tpList, tpSequence
    };

    Vector<Value>* createVectorOfValue(int size) { 
        return Vector<Value>::create(size);
    }

    void Transaction::hold()
    {
        ddlTrans = true;
    }

    void Transaction::release()
    {
        if (!ddlTrans) { 
            MemoryManager::allocator->deleteSegment(segmentId, mark);
        }
    }

    Table* Database::findTable(String*)
    {
        MCO_THROW InvalidOperation("Database::findTable");
    }

    Table* Database::findTable(char const* name)
    {
        return findTable(String::create(name));
    }

    bool Value::isIterable()
    {
        return false;
    }

    void* Value::pointer()
    {
        return this;
    }

    #ifndef NO_FLOATING_POINT
        double Value::realValue()
        {
            MCO_THROW InvalidTypeCast(typeMnemonic[type()], "real");
        }
    #endif 

    int64_t Value::intValue()
    {
        MCO_THROW InvalidTypeCast(typeMnemonic[type()], "int");
    }

    int64_t Value::intValue(int precision)
    {
        int64_t v = intValue();
        while (precision > 0) { 
            v *= 10;
            precision -= 1;
        }
        return v;
    }
    
    Value* Value::clone(AbstractAllocator* alloc)
    {
        MCO_THROW InvalidOperation("Value::clone");
    }

    uint64_t Value::hashCode()
    {
        MCO_THROW InvalidOperation("Value::hashCode");
    }
        

    time_t Value::timeValue()
    {
        MCO_THROW InvalidTypeCast(typeMnemonic[type()], "time_t");
    }

    String* Value::stringValue()
    {
        MCO_THROW InvalidTypeCast(typeMnemonic[type()], "string");
    }


    bool Value::isTrue()
    {
        return false;
    }

    bool Value::isNull()
    {
        return false;
    }

    Value* Value::deserialize(char* buf, size_t &pos)
    {
        switch (*buf++)
        {
            case tpBool:
                pos = 2;
                return BoolValue::create(*buf != 0);
            case tpDateTime:
                {
                    int64_t val = unpackLong(buf);
                    pos = 9;
                    return new DateTime(val);
                }
            case tpInt8:
                {
                    int64_t val = unpackLong(buf);
                    pos = 9;
                    return new IntValue(val);
                }
            case tpNumeric:
                {
                    int precision = *buf++ & 0xFF;
                    int64_t val = unpackLong(buf);
                    pos = 10;
                    return new NumericValue(val, precision);
                }
            case tpRaw:
                {
                    int length = *buf++ & 0xFF;
                    pos = 2+length;
                    return new RawValue(buf, length);
                }
                #ifndef NO_FLOATING_POINT
                case tpAverage:
                   { 
                       double val = unpackDouble(buf);
                       int64_t count = unpackLong(buf + 8);
                       pos = 17;
                       return new AvgValue(val, count);
                   }                    
                case tpReal8:
                    {
                        double val = unpackDouble(buf);
                        pos = 9;
                        return new RealValue(val);
                    }
                #endif 
            case tpString:
                {
                    int len = unpackInt(buf);
                    pos = len + 6;
                    return new StringRef(buf + 4, len, true);
                }
                #ifdef UNICODE_SUPPORT
                case tpUnicode:
                    {
                        int len = unpackInt(buf);
                        pos = len * 2+7;
                        UnicodeString* str = UnicodeString::create(len);
                        wchar_t* chars = str->body();
                        buf += 4;
                        for (int i = 0; i < len; i++)
                        {
                            chars[i] = unpackShort(buf);
                            buf += 2;
                        }
                        chars[len] = 0;
                        return str;
                    }
                #endif 

            case tpArray:
                {
                    Type elemType = (Type)*buf++;
                    int len = unpackInt(buf);
                    buf += 4;
                    char* start = buf;
                    Array* arr;
                    switch (elemType) { 
                      case tpBool:
                          arr = new ScalarArray<bool>(len, len);
                          memcpy(arr->pointer(), buf, len);
                          buf += len;
                          break;
                      case tpInt1:
                          arr = new ScalarArray<char>(len, len);
                          memcpy(arr->pointer(), buf, len);
                          buf += len;
                          break;
                      case tpUInt1:
                          arr = new ScalarArray<unsigned char>(len, len);
                          memcpy(arr->pointer(), buf, len);
                          buf += len;
                          break;
                      case tpInt2:
                          arr = new ScalarArray<short>(len, len);
                          {
                              short* dst = (short*)arr->pointer();
                              for (int i = 0; i < len; i++) { 
                                  dst[i] = unpackShort(buf);
                                  buf += 2;
                              }
                          }
                          break;
                      case tpUInt2:
                          arr = new ScalarArray<unsigned short>(len, len);
                          {
                              unsigned short* dst = (unsigned short*)arr->pointer();
                              for (int i = 0; i < len; i++) { 
                                  dst[i] = (unsigned short)unpackShort(buf);
                                  buf += 2;
                              }
                          }
                          break;
                      case tpInt4:
                          arr = new ScalarArray<int>(len, len);
                          {
                              int* dst = (int*)arr->pointer();
                              for (int i = 0; i < len; i++) { 
                                  dst[i] = unpackInt(buf);
                                  buf += 4;
                              }
                          }
                          break;
                      case tpUInt4:
                          arr = new ScalarArray<unsigned int>(len, len);
                          {
                              unsigned int* dst = (unsigned int*)arr->pointer();
                              for (int i = 0; i < len; i++) { 
                                  dst[i] = (unsigned int)unpackInt(buf);
                                  buf += 4;
                              }
                          }
                          break;
                      case tpInt8:
                          arr = new ScalarArray<int64_t>(len, len);
                          {
                              int64_t* dst = (int64_t*)arr->pointer();
                              for (int i = 0; i < len; i++) { 
                                  dst[i] = unpackLong(buf);
                                  buf += 8;
                              }
                          }
                          break;
                      case tpUInt8:
                          arr = new ScalarArray<uint64_t>(len, len);
                          {
                              uint64_t* dst = (uint64_t*)arr->pointer();
                              for (int i = 0; i < len; i++) { 
                                  dst[i] = (uint64_t)unpackLong(buf);
                                  buf += 8;
                              }
                          }
                          break;
                      case tpReal4:
                          arr = new ScalarArray<float>(len, len);
                          {
                              float* dst = (float*)arr->pointer();
                              for (int i = 0; i < len; i++) { 
                                  dst[i] = unpackFloat(buf);
                                  buf += 4;
                              }
                          }
                          break;
                       case tpReal8:
                          arr = new ScalarArray<double>(len, len);
                          {
                              double* dst = (double*)arr->pointer();
                              for (int i = 0; i < len; i++) { 
                                  dst[i] = unpackDouble(buf);
                                  buf += 8;
                              }
                          }
                          break;
                      default:
                      {
                          Vector<Value>* vec = Vector < Value > ::create(len);
                          arr = new ArrayStub(vec);
                          for (int i = 0; i < len; i++)
                          {
                              Value* v = deserialize(buf, pos);
                              vec->items[i] = v;
                              if (v->type() == tpStruct)
                              {
                                  ((StructStub*)v)->outer = arr;
                              }
                              buf += pos;
                          }
                      }
                    }
                    pos = (buf - start) + 6;
                    return arr;
                }

            case tpStruct:
                {
                    int len = * buf++ &0xFF;
                    char* start = buf;
                    Vector < Value > * arr = Vector < Value > ::create(len);
                    StructStub* s = new StructStub();
                    for (int i = 0; i < len; i++)
                    {
                        Value* v = deserialize(buf, pos);
                        arr->items[i] = v;
                        if (v->type() == tpStruct)
                        {
                            ((StructStub*)v)->outer = s;
                        }
                        buf += pos;
                    }
                    s->outer = NULL;
                    s->items = arr;
                    pos = (buf - start) + 2;
                    return s;
                }

            case tpBlob:
                {
                    int len = unpackInt(buf);
                    BlobStub* blob = new BlobStub();
                    blob->content = String::create(len);
                    blob->pos = 0;
                    memcpy(blob->content->body(), buf + 4, len);
                    pos = 5+len;
                    return blob;
                }

            case tpReference:
                pos = 9;
                return new ReferenceStub(unpackLong(buf));

            case tpNull:
                pos = 1;
                return  &Null;

            case tpDataSource:
                {
                    int i;
                    char* start = buf;
                    DataSourceStub* ds = new DataSourceStub();
                    FieldStub** fpp = &ds->columns;
                    for (i = 0; *buf != (char)EOF; i++)
                    {
                        FieldStub* field = new FieldStub();
                        field->fieldNo = i;
                        buf = field->deserialize(buf);
                        *fpp = field;
                        fpp = &field->next;
                    }
                    *fpp = NULL;
                    ds->nColumns = i;
                    buf += 1; // skip fields terminator
                    ds->size = unpackInt(buf);
                    buf += 4;
                    ds->content = NULL;
                    ds->buf = buf;
                    ds->reader = NULL;
                    ds->destructor = NULL;
                    ds->segmentId = 0;
                    ds->mark = 0;
                    pos = buf - start + 1;
                    return ds;
                }
            default:
                assert(false);
        }
        return NULL;
    }


    void Value::output(FormattedOutput *streams, size_t n_streams)
    {
        char buf[BUF_SIZE];
        toString(buf, BUF_SIZE - 1);
        buf[BUF_SIZE - 1] = '\0';
        for (size_t i = 0; i < n_streams; ++i) {
            streams[i].print(buf);
        }
    }

    bool IterableValue::isIterable()
    {
        return true;
    }

    SortOrder DataSource::sortOrder() 
    {
        return UNSPECIFIED_ORDER;
    }

    bool DataSource::invert(Transaction*) 
    {
        return false;
    }

    DataSource::DataSource()
    {
        allocator = MemoryManager::getAllocator();
    }

    Type DataSource::type()
    {
        return tpDataSource;
    }

    Field* DataSource::findField(char const* name)
    {
        return findField(String::create(name));
    }

    Field* DataSource::findField(String* fieldName)
    {
        Iterator < Field > * iterator = fields();
        while (iterator->hasNext())
        {
            Field* f = iterator->next();
            if (fieldName->equals(f->name()))
            {
                return f;
            }
        }
        return NULL;
    }

    size_t DataSource::serialize(char* buf, size_t bufSize)
    {
        if (bufSize >= 1)
        {
            char* start = buf;
            *buf++ = tpDataSource;
            bufSize -= 1;
            Iterator < Field > * iterator = fields();
            while (iterator->hasNext())
            {
                size_t len = iterator->next()->serialize(buf, bufSize);
                if (len == 0)
                {
                    return 0;
                }
                buf += len;
                bufSize -= len;
            }
            if (bufSize < 5)
            {
                return 0;
            }
            *buf++ = (char)EOF; // terminator
            buf = packInt(buf, isNumberOfRecordsKnown() ? (int)nRecords(currentTransaction()):  - 1);
            bufSize -= 5;
            Cursor* cursor = records();
            while (cursor->hasNext())
            {
                size_t len = cursor->next()->serialize(buf, bufSize);
                if (len == 0)
                {
                    cursor->release();
                    return 0;
                }
                buf += len;
                bufSize -= len;
            }
            cursor->release();
            if (bufSize == 0)
            {
                return 0;
            }
            *buf++ = (char)EOF;
            return buf - start;
        }
        return 0;
    }


    static size_t extractStruct(Struct* rec, Iterator < Field > * fi, void* dst, size_t offs, size_t &maxAlignment,
                                size_t appSize, bool*  &nullIndicators, ExtractMode mode)
    {
        for (int i = 0; fi->hasNext(); i++)
        {
            Field* f = fi->next();
            Type fieldType = f->type();
            int fixedSize = 0;
            size_t alignment;
            if (mode == emCopyFixedSizeStrings && (fieldType == tpString || fieldType == tpUnicode) && (fixedSize = f->fixedSize()) > 0)
            {
                alignment = (fieldType == tpUnicode) ? sizeof(wchar_t): sizeof(char);
            }
            else
            {
                alignment = typeAlignment[fieldType];
            }
            if (alignment > maxAlignment)
            {
                maxAlignment = alignment;
            }
            offs = DOALIGN(offs, alignment);

            if (offs == appSize && f->isAutoGenerated())
            {
                continue;
            }
            else if (offs >= appSize)
            {
                MCO_THROW RuntimeError(String::format(
                                   "Size of extracted record is greater than size of destination structure %d", (int)appSize)->cstr());
            }
            Value* value = rec == NULL ? NULL : rec->get(i);
            if (fieldType != tpStruct) { 
                if (value == NULL || value->isNull())
                {
                    if (nullIndicators != NULL)
                    {
                        *nullIndicators++ = true;
                    }
                    else if (isScalarType(fieldType) || fixedSize != 0)
                    {
                        MCO_THROW RuntimeError("NULL value found and nullIndicators is not specified");
                    }
                    if (isScalarType(fieldType))
                    {
                        offs += typeSizeof[fieldType];
                        continue;
                    }
                    value = NULL;
                }
                else if (nullIndicators != NULL)
                {
                    *nullIndicators++ = false;
                }
            } else { 
                if (value == &Null) { 
                    value = NULL;
                }
            }
            char* ptr = (char*)dst + offs;
            switch (fieldType)
            {
                case tpInt1:
                case tpUInt1:
                    *ptr = (char)value->intValue();
                    offs += 1;
                    break;
                case tpInt2:
                case tpUInt2:
                    *(short*)ptr = (short)value->intValue();
                    offs += 2;
                    break;
                case tpInt4:
                case tpUInt4:
                    *(int*)ptr = (int)value->intValue();
                    offs += 4;
                    break;
                case tpNumeric:
                    *(int64_t*)ptr = ((NumericValue*)value)->intValue(((NumericValue*)value)->precision);
                    offs += 8;
                    break;
                case tpInt8:
                case tpUInt8:
                case tpReference:
                        *(int64_t*)ptr = value == NULL ? 0 : value->intValue();
                    offs += 8;
                    break;
                #ifndef NO_FLOATING_POINT
                case tpReal4:
                    *(float*)ptr = (float)value->realValue();
                    offs += 4;
                    break;
                case tpReal8:
                    *(double*)ptr = value->realValue();
                    offs += 8;
                    break;
                #endif 
                case tpDateTime:
                    *(time_t*)ptr = (time_t)value->intValue();
                    offs += sizeof(time_t);
                    break;
                #ifdef UNICODE_SUPPORT
                case tpUnicode:
                    switch (mode)
                    {
                        case emCloneStrings:
                            if (value == NULL)
                            {
                                *(wchar_t**)ptr = NULL;
                            }
                            else
                            {
                                UnicodeString* str = value->unicodeStringValue();
                                int len = str->size();
                                wchar_t* body = new wchar_t[len + 1];
                                memcpy(body, str->body(), sizeof(wchar_t)* len);
                                body[len] = 0;
                                *(wchar_t**)ptr = body;
                            }
                            break;
                        case emCopyFixedSizeStrings:
                            if (fixedSize > 0)
                            {
                                if (value != NULL)
                                {
                                    memcpy(ptr, value->unicodeStringValue()->body(), fixedSize*sizeof(wchar_t));
                                }
                                offs += fixedSize * sizeof(wchar_t);
                                continue;
                            }
                            // no break
                        default:
                            *(wchar_t**)ptr = value == NULL ? NULL : value->unicodeStringValue()->wcstr();
                        }
                        offs += sizeof(wchar_t*);
                        break;
                    #endif 
                case tpString:
                    switch (mode)
                    {
                    case emCloneStrings:
                        if (value == NULL)
                        {
                            *(char**)ptr = NULL;
                        }
                        else
                        {
                            String* str = value->stringValue();
                            int len = str->size();
                            char* body = new char[len + 1];
                            memcpy(body, str->body(), len);
                            body[len] = 0;
                            *(char**)ptr = body;
                        }
                        break;
                    case emCopyFixedSizeStrings:
                        if (fixedSize > 0)
                        {
                            if (value != NULL) {
                                memcpy(ptr, value->stringValue()->body(), fixedSize);
                            }
                            offs += fixedSize;
                            continue;
                        }
                        // no break
                    default:
                        *(char**)ptr = value == NULL ? NULL : value->stringValue()->cstr();
                    }
                    offs += sizeof(char*);
                    break;
                case tpStruct:
                    alignment = f->calculateStructAlignment();
                    offs = DOALIGN(offs, alignment);
                    offs = extractStruct((Struct*)value, f->components(), dst, offs, maxAlignment, appSize,
                                         nullIndicators, mode);
                    break;
                default:
                    *(Value**)ptr = value;
                    offs += sizeof(Value*);
                    break;
            }
        }
        return offs;
    }

    void DataSource::setMark(size_t mark) {}

    void DataSource::extract(Record* rec, void* dst, size_t appSize, bool nullIndicators[], ExtractMode mode)
    {
        size_t alignment = 1;
        size_t dbSize = extractStruct(rec, fields(), dst, 0, alignment, appSize, nullIndicators, mode);
        dbSize = DOALIGN(dbSize, alignment);
        if (dbSize != appSize)
        {
            MCO_THROW RuntimeError(String::format(
                               "Size of extracted record %d doesn't match with size of destination structure %d", (int)
                               dbSize, (int)appSize)->cstr());
        }
    }

    static size_t storeStruct(Struct* rec, Iterator < Field > * fi, void const* src, size_t offs, size_t &maxAlignment,
                              size_t appSize, bool const* &nullIndicators, ExtractMode mode)
    {
        for (int i = 0; fi->hasNext(); i++)
        {
            Field* f = fi->next();
            Type fieldType = f->type();
            int fixedSize = 0;
            size_t alignment;
            if (mode == emCopyFixedSizeStrings && (fieldType == tpString || fieldType == tpUnicode) && (fixedSize = f->fixedSize()) > 0)
            {
                alignment = (fieldType == tpUnicode) ? sizeof(wchar_t): sizeof(char);
            }
            else
            {
                alignment = typeAlignment[fieldType];
            }
            if (alignment > maxAlignment)
            {
                maxAlignment = alignment;
            }
            offs = DOALIGN(offs, alignment);

            if (offs == appSize && f->isAutoGenerated())
            {
                continue;
            }
            else if (offs >= appSize)
            {
                MCO_THROW RuntimeError(String::format(
                                   "Size of stored record %d doesn't match with size of source structure %d", 
                                   (int)offs, (int)appSize)->cstr());
            }
            Value* value = &Null;
            if (nullIndicators != NULL && *nullIndicators++) {
                if (isScalarType(fieldType)) {
                    offs += typeSizeof[fieldType];
                } else if (fixedSize > 0) {
                    offs += fixedSize * ((fieldType == tpUnicode) ? sizeof(wchar_t) : 1);
                } else { 
                    offs += sizeof(void*);
                }
            } else { 
                char* ptr = (char*)src + offs;
                switch (fieldType)
                {
                  case tpInt1:
                    value = new IntValue(*ptr);
                    offs += 1;
                    break;
                  case tpUInt1:
                    value = new IntValue(*(unsigned char*)ptr);
                    offs += 1;
                    break;
                  case tpInt2:
                    value = new IntValue(*(short*)ptr);
                    offs += 2;
                    break;
                  case tpUInt2:
                    value = new IntValue(*(unsigned short*)ptr);
                    offs += 2;
                    break;
                  case tpInt4:
                    value = new IntValue(*(unsigned*)ptr);
                    offs += 4;
                    break;
                  case tpUInt4:
                    value = new IntValue(*(int*)ptr);
                    offs += 4;
                    break;
                  case tpNumeric:
                    value = new NumericValue(*(int64_t*)ptr, f->precision());
                    offs += 8;
                    break;
                  case tpInt8:
                  case tpUInt8:
                    value = new IntValue(*(int64_t*)ptr);
                    offs += 8;
                    break;
                  case tpReference:
                    value = new ReferenceStub(*(int64_t*)ptr);
                    offs += 8;
                    break;
                  #ifndef NO_FLOATING_POINT
                  case tpReal4:
                    value = new RealValue(*(float*)ptr);
                    offs += 4;
                    break;
                  case tpReal8:
                    value = new RealValue(*(double*)ptr);
                    offs += 8;
                    break;
                  #endif 
                  case tpDateTime:
                    value = new DateTime(*(int64_t*)ptr);
                    offs += 8;
                    break;
                  #ifdef UNICODE_SUPPORT
                  case tpUnicode:
                    if (mode == emCopyFixedSizeStrings) { 
                        if (fixedSize > 0) { 
                            value = UnicodeString::create((wchar_t*)ptr, fixedSize);
                            ptr += sizeof(wchar_t)*fixedSize;
                        } else { 
                            value = UnicodeString::create(*(wchar_t**)ptr);
                            ptr += sizeof(wchar_t)*((UnicodeString*)value)->size();
                        }
                    } else {
                        value = UnicodeString::create(*(wchar_t**)ptr);
                        ptr += sizeof(wchar_t*);
                    }
                    #endif 
                  case tpString:
                    if (mode == emCopyFixedSizeStrings) { 
                        if (fixedSize > 0) { 
                            value = String::create(ptr, fixedSize);
                            ptr += fixedSize;
                        } else { 
                            value = String::create(*(char**)ptr);
                            ptr += ((String*)value)->size();
                        }
                    } else {
                        value = String::create(*(char**)ptr);
                        ptr += sizeof(char*);
                    }
                  case tpStruct:
                    alignment = f->calculateStructAlignment();
                    offs = DOALIGN(offs, alignment);
                    if (rec != NULL) { 
                        value = rec->update(i);
                    }
                    offs = storeStruct((Struct*)value, f->components(), src, offs, maxAlignment, appSize,
                                       nullIndicators, mode);
                    break;
                  default:
                    value = *(Value**)ptr;
                    if (value == NULL) {
                        value = &Null;
                    }
                    offs += sizeof(Value*);
                    break;
                }
            }
            if (rec != NULL) {
                rec->set(i, value);
            }
        }
        return offs;
    }

    void DataSource::store(Record* rec, void const* src, size_t appSize, bool const nullIndicators[], ExtractMode mode)
    {
        size_t alignment = 1;
        size_t dbSize = storeStruct(rec, fields(), src, 0, alignment, appSize, nullIndicators, mode);
        dbSize = DOALIGN(dbSize, alignment);
        if (dbSize != appSize)
        {
            MCO_THROW RuntimeError(String::format(
                               "Size of stored record %d doesn't match with size of source structure %d", (int)
                               dbSize, (int)appSize)->cstr());
        }
    }

    bool DataSource::isResultSet()
    {
        return false;
    }

    Cursor* DataSource::internalCursor(Transaction* trans)
    {
        return records(trans);
    }

    Transaction* DataSource::currentTransaction()
    {
        MCO_THROW InvalidOperation("DataSource::currentTransaction");
    }

    Table* DataSource::sourceTable()
    {
        return NULL;
    }

    void DataSource::release(){}

    int DataSource::compare(Value* x)
    {
        MCO_THROW InvalidOperation("DataSource::compare");
    }

    size_t DataSource::toString(char* buf, size_t bufSize)
    {
        MCO_THROW InvalidOperation("DataSource::toString");
    }

    void Table::checkpointRecord(Transaction*, Record*)
    {
    }

    bool Table::temporary()
    {
        return true;
    }

    bool Table::commit(int)
    {
        return true;
    }

    void Table::rollback()
    {
    }

    void Table::updateStatistic(Transaction* trans, Record* stat)
    {
    }

    Table* Table::sourceTable()
    {
        return this;
    }

    Cursor* Table::records()
    {
        MCO_THROW InvalidOperation("Table::records");
    }

    void Index::updateStatistic(Transaction* trans, Record* stat)
    {
    }

    NullValue* NullValue::create() 
    {
        return &Null;
    }

    void* NullValue::pointer()
    {
        return NULL;
    }

    Type NullValue::type()
    {
        return tpNull;
    }

    bool NullValue::isNull()
    {
        return true;
    }

    int NullValue::compare(Value* x)
    {
        return x->isNull() ? 0 :  - 1;
    }

    size_t Value::fillBuf(char* dst, char const* src, size_t size)
    {
        size_t i;
        for (i = 0; i < size && (dst[i] = src[i]) != 0; i++)
            ;
        return i;
    }


    String* NullValue::stringValue()
    {
        return String::create("null");
    }

    int64_t NullValue::intValue()
    {
        return 0;
    }

    Value* NullValue::clone(AbstractAllocator* alloc)                
    {
        return this;
    }

    uint64_t NullValue::hashCode()
    {
        return 0;
    }

    #ifndef NO_FLOATING_POINT
        double NullValue::realValue()
        {
            return 0;
        }
    #endif 

    size_t NullValue::toString(char* buf, size_t bufSize)
    {
        return fillBuf(buf, "null", bufSize);
    }

    size_t NullValue::serialize(char* buf, size_t size)
    {
        if (size >= 1)
        {
            *buf = tpNull;
            return 1;
        }
        return 0;
    }

    void* BoolValue::pointer()
    {
        return (void*) &val;
    }

    Type BoolValue::type()
    {
        return tpBool;
    }

    bool BoolValue::isTrue()
    {
        return val;
    }

    int BoolValue::compare(Value* x)
    {
        return x->isNull() ? 1 : (int)val - (int)((BoolValue*)x)->val;
    }

    BoolValue* BoolValue::create(bool val)
    {
        return val ? &True: &False;
    }

    int64_t BoolValue::intValue()
    {
        return val ? 1 : 0;
    }

    uint64_t BoolValue::hashCode()
    {
        return val;
    }

    Value* BoolValue::clone(AbstractAllocator* alloc)
    {
        return this;
    }

    #ifndef NO_FLOATING_POINT
        double BoolValue::realValue()
        {
            return val ? 1 : 0;
        }
    #endif 

    String* BoolValue::stringValue()
    {
        return String::create(val ? "true" : "false");
    }

    size_t BoolValue::toString(char* buf, size_t bufSize)
    {
        return fillBuf(buf, val ? "true" : "false", bufSize);
    }

    size_t BoolValue::serialize(char* buf, size_t size)
    {
        if (size >= 2)
        {
            *buf++ = tpBool;
            *buf = (char)val;
            return 2;
        }
        return 0;
    }


    IntValue* IntValue::create(int64_t v) { 
        return new IntValue(v);
    }

    void* IntValue::pointer()
    {
        return (void*) &val;
    }

    Type IntValue::type()
    {
        return tpInt8;
    }

    int IntValue::compare(Value* x)
    {
        return x->isNull() ? 1 : val < ((IntValue*)x)->val ?  - 1: val == ((IntValue*)x)->val ? 0 : 1;
    }

    int64_t IntValue::intValue()
    {
        return val;
    }

    Value* IntValue::clone(AbstractAllocator* allocator)
    {
        return new (allocator) IntValue(val);
    }

    uint64_t IntValue::hashCode()
    {
        return val;
    }

    bool IntValue::isTrue()
    {
        return val != 0;
    }

    time_t IntValue::timeValue()
    {
        return (time_t)val;
    }

    #ifndef NO_FLOATING_POINT
        double IntValue::realValue()
        {
            return (double)val;
        }
    #endif 

    String* IntValue::stringValue()
    {
        char tmpBuf[32];
        return String::create(int64ToString(val, tmpBuf, sizeof(tmpBuf)));
    }

    size_t IntValue::toString(char* buf, size_t bufSize)
    {
        char tmpBuf[32];
        char* p = int64ToString(val, tmpBuf, sizeof(tmpBuf));
        return fillBuf(buf, p, bufSize);
    }

    size_t IntValue::serialize(char* buf, size_t size)
    {
        if (size >= 9)
        {
            *buf++ = tpInt8;
            packLong(buf, val);
            return 9;
        }
        return 0;
    }


    NumericValue* NumericValue::create(int64_t scaledVal, int prec) { 
        return new NumericValue(scaledVal, prec);
    }

    void* NumericValue::pointer()
    {
        return (void*) &val;
    }

    Type NumericValue::type()
    {
        return tpNumeric;
    }

    int NumericValue::compare(Value* x)
    {
        if (x->isNull()) {
            return 1;
        }        
        NumericValue* other = (NumericValue*)x;
        int64_t leftVal = scale(other->precision);
        int64_t rightVal = other->scale(precision);
        return leftVal < rightVal ? -1 : leftVal == rightVal ? 0 : 1;
    }

    Value* NumericValue::clone(AbstractAllocator* allocator)
    {
        return new (allocator) NumericValue(val, precision);
    }

    uint64_t NumericValue::hashCode()
    {
        return val;
    }

    int64_t NumericValue::intValue(int prec)
    {
        int64_t v = val;
        while (prec > precision) { 
            v *= 10;
            prec -= 1;
        }
        while (prec < precision) { 
            v /= 10;
            prec += 1;
        }
        return v;
    }

    int64_t NumericValue::intValue()
    {
        return val / scale();
    }

    bool NumericValue::isTrue()
    {
        return val != 0;
    }

    time_t NumericValue::timeValue()
    {
        return (time_t)val;
    }

    #ifndef NO_FLOATING_POINT
        double NumericValue::realValue()
        {
            return (double)val/scale();
        }
    #endif 

    String* NumericValue::stringValue()
    {
        char tmpBuf[32];
        toString(tmpBuf, sizeof tmpBuf);
        return String::create(tmpBuf);
    }

    size_t NumericValue::toString(char* buf, size_t bufSize)
    {
        char tmpBuf[64];
        int64_t s = scale();
        char* p = int64ToString(val / s, tmpBuf, sizeof(tmpBuf)/2);
        if (precision > 0) { 
            char* frac = &tmpBuf[sizeof(tmpBuf)/2-1];
            *frac++ = '.';
            for (int i = precision; --i >= 0; frac[i] = '0');
            int64ToString((int64_t)((uint64_t)(val < 0 ? -val : val) % s), frac, precision+1);        
        }
        return fillBuf(buf, p, bufSize);
    }

    size_t NumericValue::serialize(char* buf, size_t size)
    {
        if (size >= 10)
        {
            *buf++ = tpNumeric;
            *buf++ = precision;
            packLong(buf, val);
            return 10;
        }
        return 0;
    }

    #ifndef NO_FLOATING_POINT
    NumericValue::NumericValue(double realVal, int prec) : precision(prec)
    {
        val = int64_t(realVal*scale());
    }
    #endif

    NumericValue::NumericValue(char const* str, int prec) : precision(prec) 
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
        if (*str == '.') { 
            str += 1;
            while (*str >= '0' && * str <= '9')
            {
                if (prec == 0) { 
                    break;
                }
                prec -= 1;                    
                v = v * 10+* str++ - '0';
            }
        }
        while (--prec >= 0) {
            v *= 10;
        }
        val = v*sign;
    }
           
    NumericValue::NumericValue(char const* str)
    {
        if (!parse(str)) {
            MCO_THROW InvalidTypeCast("String", "Numeric");
        }
    }

    bool NumericValue::parse(char const* str)
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
        int prec = 0;
        if (*str == '.') { 
            str += 1;
            while (*str >= '0' && * str <= '9')
            {
                prec += 1;                    
                v = v * 10+* str++ - '0';
            }
        }
        val = v*sign;
        precision = prec;
        return *str == '\0';
    }
           
    NumericValue::NumericValue(int64_t intPart, int64_t fracPart, int prec) : precision(prec)
    {
        val = intPart*scale()+fracPart;
    }

    void* RawValue::pointer()
    {
        return (void*)ptr;
    }

    Type RawValue::type()
    {
        return tpRaw;
    }

    int RawValue::compare(Value* x)
    {
        return x->isNull() ? 1 : memcmp(ptr, ((RawValue*)x)->ptr, length);
    }

    String* RawValue::stringValue()
    {
        return String::create("<opaque>");
    }

    size_t RawValue::toString(char* buf, size_t bufSize)
    {
        return fillBuf(buf, "<opaque>", bufSize);
    }

    size_t RawValue::serialize(char* buf, size_t size)
    {
        if (size >= (size_t)(2+length))
        {
            *buf++ = tpRaw;
            *buf++ = (char)length;
            memcpy(buf, ptr, length);
            return 2+length;
        }
        return 0;
    }

    char const* DateTime::format = "%c";

    DateTime* DateTime::create(int64_t val) { 
        return new DateTime(val);
    }

    Value* DateTime::clone(AbstractAllocator* allocator)
    {
        return new (allocator) DateTime(val);
    }

    Type DateTime::type()
    {
        return tpDateTime;
    }

    String* DateTime::stringValue()
    {
        char tmpBuf[MCO_SQL_TIME_BUF_SIZE];
        timeToString((time_t)val, tmpBuf, sizeof(tmpBuf));
        return String::create(tmpBuf);
    }

    size_t DateTime::toString(char* buf, size_t bufSize)
    {
        return timeToString((time_t)val, buf, bufSize);
    }

    size_t DateTime::serialize(char* buf, size_t size)
    {
        if (size >= 9)
        {
            *buf++ = tpDateTime;
            packLong(buf, val);
            return 9;
        }
        return 0;
    }


    #ifndef NO_FLOATING_POINT
        char const* RealValue::format = "%.15g";

        RealValue* RealValue::create(double v) { 
            return new RealValue(v);
        }

        void* RealValue::pointer()
        {
            return (void*) &val;
        }

        Type RealValue::type()
        {
            return tpReal8;
        }

        bool RealValue::isTrue()
        {
            return val != 0;
        }

        int RealValue::compare(Value* x)
        {
            return x->isNull() ? 1 : isnan(val) ? isnan(((RealValue*)x)->val) ? 0 : -1 : isnan(((RealValue*)x)->val) ? 1 : val < ((RealValue*)x)->val ?  - 1: val == ((RealValue*)x)->val ? 0 : 1;
        }
  
        Value* RealValue::clone(AbstractAllocator* allocator)
        {
            return new (allocator) RealValue(val);
        }

        uint64_t RealValue::hashCode()
        {
            return *(uint64_t*)&val;
        }

        int64_t RealValue::intValue(int precision)
        {
            double v = val;
            while (precision > 0) { 
                v *= 10;
                precision -= 1;
            }
            return (int64_t)v;
        }

        int64_t RealValue::intValue()
        {
            return (int64_t)val;
        }

        double RealValue::realValue()
        {
            return val;
        }

        size_t RealValue::serialize(char* buf, size_t size)
        {
            if (size >= 9)
            {
                *buf++ = tpReal8;
                packDouble(buf, val);
                return 9;
            }
            return 0;
        }

        String* RealValue::stringValue()
        {
            char tmpBuf[32];
            sprintf(tmpBuf, format, val);
            return String::create(tmpBuf);
        }

        size_t RealValue::toString(char* buf, size_t bufSize)
        {
            char tmpBuf[32];
            sprintf(tmpBuf, format, val);
            return fillBuf(buf, tmpBuf, bufSize);
        }

        size_t AvgValue::serialize(char* buf, size_t size)
        {
            if (size >= 17)
            {
                *buf++ = tpAverage;
                buf = packDouble(buf, val);
                packLong(buf, count);
                return 17;
            }
            return 0;
        }     
    #endif 

    bool IterableValueIterator::hasNext()
    {
        if (curr == NULL) {
            curr = iterator->next();
        }
        return curr != NULL;
    }
        
    Value* IterableValueIterator::next() 
    {
        if (!hasNext()) {
            MCO_THROW NoMoreElements();
        }
        Value* v = curr;
        curr = NULL;
        return v;
    }

    IterableValueIterator::IterableValueIterator(IterableValue* iterable)
    {
        curr = NULL;
        iterator = iterable;
    }

    Value* List::next() 
    { 
        return currPos < size() ? getAt(currPos++) : NULL;
    }

    Type String::type()
    {
        return tpString;
    }

    bool String::startsWith(String* prefix)
    {
        return size() >= prefix->size() && STRNCMP(body(), prefix->body(), prefix->size()) == 0;
    }

    bool String::isTrue()
    {
        char* str = cstr();
        return STRCMP(str, "1") == 0 || STRCMP(str, "true") == 0;
    }

    int String::indexOf(String* s)
    {
        char* p = cstr();
        char* q = STRSTR(p, s->cstr());
        return q == NULL ?  - 1: (int)(q - p);
    }

    uint64_t String::hashCode()
    {
        uint64_t h = 0;
        size_t len = size();
        unsigned char* str = (unsigned char*)body();
        for (size_t i = 0; i < len; i++) {
            h = h*31 + str[i];
        }
        return h;
    }

    int64_t String::intValue(int precision)
    {
        int64_t val;
        if (stringToNumeric(cstr(), val, precision))
        {
            return val;
        }
        MCO_THROW InvalidTypeCast("String", "numeric");
    }
        
    int64_t String::intValue()
    {
        int64_t val;
        if (stringToInt64(cstr(), val))
        {
            return val;
        }
        MCO_THROW InvalidTypeCast("String", "int");
    }

    time_t String::timeValue()
    {
        time_t val;
        if (stringToTime(cstr(), val))
        {
            return val;
        }
        MCO_THROW InvalidTypeCast("String", "time_t");
    }


    #ifndef NO_FLOATING_POINT
    double String::realValue()
    {
        double val = 0;
        int n;
        char* str = cstr();
        if (*str == '\0' || (sscanf(str, "%lf%n", &val, &n) == 1 && str[n] == '\0'))
        {
            return val;
        }
        MCO_THROW InvalidTypeCast("String", "double");
    }
    #endif 


    String* String::substr(int pos, int len)
    {
        if (pos < 0 || len < 0 || pos + len > size())
        {
            MCO_THROW IndexOutOfBounds(pos, size());
        }
        return String::create(body() + pos, len);
    }

    void* String::pointer()
    {
        return cstr();
    }

    String* String::create(int len)
    {
        return new(len)StringLiteral(len);
    }

    String* String::create(int len, AbstractAllocator* allocator)
    {
        return new(len,allocator) StringLiteral(len);
    }

    String* String::create(char const* str, int len)
    {
        return new(len)StringLiteral(str, len);
    }

    String* String::create(char const* str, int len, AbstractAllocator* allocator)
    {
        return new(len, allocator) StringLiteral(str, len);
    }

    String* String::create(char const* str)
    {
        return create(str, (int)STRLEN(str));
    }

    String* String::create(char const* str, AbstractAllocator* allocator)
    {
        return create(str, (int)STRLEN(str), allocator);
    }

    int String::compare(char const* str)
    {
        int len1 = size();
        int len2 = (int)STRLEN(str);
        int n = min(len1, len2);
        int diff = STRNCMP(body(), str, n);
        if (diff != 0)
        {
            return diff;
        }
        return len1 - len2;
    }

    int String::compare(Value* x)
    {
        if (x == NULL || x->isNull())
        {
            return 1;
        }
        String* s = (String*)x;
        int len1 = size();
        int len2 = s->size();
        int n = min(len1, len2);
        int diff = STRNCMP(body(), s->body(), n);
        if (diff != 0)
        {
            return diff;
        }
        return len1 - len2;
    }

    String* String::concat(String* head, String* tail)
    {
        int headLength = head->size();
        int tailLength = tail->size();
        StringLiteral* s = new(headLength + tailLength)StringLiteral(headLength + tailLength);
        memcpy(s->chars, head->body(), headLength);
        memcpy(s->chars + headLength, tail->body(), tailLength);
        s->chars[headLength + tailLength] = '\0';
        return s;
    }

    String* String::toUpperCase()
    {
        int n = size();
        StringLiteral* dst = new(n)StringLiteral(n);
        unsigned char* src = (unsigned char*)body();
        for (int i = 0; i < n; i++)
        {
            dst->chars[i] = toupper(src[i]);
        }
        dst->chars[n] = '\0';
        return dst;
    }

    String* String::toLowerCase()
    {
        int n = size();
        StringLiteral* dst = new(n)StringLiteral(n);
        unsigned char* src = (unsigned char*)body();
        for (int i = 0; i < n; i++)
        {
            dst->chars[i] = tolower(src[i]);
        }
        dst->chars[n] = '\0';
        return dst;
    }

    Value* String::getAt(int index)
    {
        int length = size();
        if ((unsigned)index >= (unsigned)length)
        {
            MCO_THROW IndexOutOfBounds(index, length);
        }
        return new IntValue((unsigned char)body()[index]);
    }

    String* String::stringValue()
    {
        return this;
    }

    size_t String::toString(char* buf, size_t bufSize)
    {
        size_t length = size();
        if (length < bufSize)
        {
            memcpy(buf, body(), length);
            buf[length] = '\0';
            return length;
        }
        else
        {
            memcpy(buf, body(), bufSize);
            return bufSize;
        }
    }

    void String::output(FormattedOutput *streams, size_t n_streams)
    {
        for (size_t i = 0; i < n_streams; ++i) {
            streams[i].print(cstr());
        }
    }


    String* String::format(char const* fmt, ...)
    {
        char buf[BUF_SIZE];
        va_list params;
        va_start(params, fmt);
        vsprintf(buf, fmt, params);
        va_end(params);
        return create(buf);
    }

    size_t String::serialize(char* buf, size_t bufSize)
    {
        int len = size();
        if (bufSize >= (size_t)(6+len))
        {
            *buf++ = tpString;
            buf = packInt(buf, len);
            memcpy(buf, body(), len);
            buf[len] = '\0';
            return 6+len;
        }
        return 0;
    }

    Value* StringLiteral::clone(AbstractAllocator* allocator)
    {
        StringLiteral* str = new (length, allocator) StringLiteral(chars, length);
        memcpy(str->chars, chars, length+1);
        return str;
    }

    int StringLiteral::size()
    {
        return length;
    }

    char* StringLiteral::body()
    {
        return chars;
    }

    char* StringLiteral::cstr()
    {
        return chars;
    }

    int UserString::size()
    {
        return length;
    }

    Value* UserString::clone(AbstractAllocator* allocator)
    {
        return new (allocator) UserString(chars, length);
    }

    UserString::UserString(char const* s, int l): length(l)
    {
        chars = new char[l + 1];
        memcpy(chars, s, l);
        chars[l] = '\0';
    }

    UserString::~UserString()
    {
        delete [] chars;
    }

    char* UserString::body()
    {
        return chars;
    }

    char* UserString::cstr()
    {
        return chars;
    }

    Value* StringRef::clone(AbstractAllocator* allocator)
    {
        return new (allocator) StringRef(chars, length, zeroTerminated);
    }

    int StringRef::size()
    {
        return length;
    }

    char* StringRef::body()
    {
        return chars;
    }

    char* StringRef::cstr()
    {
        if (!zeroTerminated)
        {
            String* s = String::create(chars, length);
            chars = s->cstr();
            zeroTerminated = true;
        }
        return chars;
    }

    #ifdef UNICODE_SUPPORT
        UnicodeString* Value::unicodeStringValue()
        {
            return UnicodeString::create(stringValue());
        }

        uint64_t UnicodeString::hashCode()
        {
            uint64_t h = 0;
            size_t len = size();
            wchar_t* str = (wchar_t*)body();
            for (size_t i = 0; i < len; i++) {
                h = h*31 + str[i];
            }
            return h;
        }

        UnicodeString* UnicodeString::unicodeStringValue()
        {
            return this;
        }

        Type UnicodeString::type()
        {
            return tpUnicode;
        }

        bool UnicodeString::startsWith(UnicodeString* prefix)
        {
            return size() >= prefix->size() && wcsncmp(body(), prefix->body(), prefix->size()) == 0;
        }

        int UnicodeString::indexOf(UnicodeString* s)
        {
            wchar_t* p = wcstr();
            wchar_t* q = wcsstr(p, s->wcstr());
            return q == NULL ?  - 1: (int)(q - p);
        }

        UnicodeString* UnicodeString::substr(int pos, int len)
        {
            if (pos < 0 || len < 0 || pos + len > size())
            {
                MCO_THROW IndexOutOfBounds(pos, size());
            }
            return UnicodeString::create(body() + pos, len);
        }

        void* UnicodeString::pointer()
        {
            return wcstr();
        }

        UnicodeString* UnicodeString::create(int len)
        {
            return new(len*sizeof(wchar_t))UnicodeStringLiteral(len);
        }

        UnicodeString* UnicodeString::create(int len, AbstractAllocator* allocator)
        {
            return new(len*sizeof(wchar_t), allocator) UnicodeStringLiteral(len);
        }

        UnicodeString* UnicodeString::create(wchar_t const* str, int len)
        {
            return new(len* sizeof(wchar_t))UnicodeStringLiteral(str, len);
        }

        UnicodeString* UnicodeString::create(wchar_t const* str)
        {
            return create(str, (int)wcslen(str));
        }



        UnicodeString* UnicodeString::create(String* mbs)
        {
            wchar_t buf[BUF_SIZE];
            int len = mbs->size();
            if (len < BUF_SIZE)
            {
                len = (int)mbstowcs(buf, mbs->cstr(), BUF_SIZE);
                if (len < 0) { 
                    MCO_THROW InvalidArgument("Invalid multibyte string");
                }
                return new(len* sizeof(wchar_t))UnicodeStringLiteral(buf, len);
            }
            else
            {
                UnicodeString* wcs = create(len);
                len = (int)mbstowcs(wcs->body(), mbs->cstr(), len + 1);
                if (len < 0) { 
                    MCO_THROW InvalidArgument("Invalid multibyte string");
                }
                return new UnicodeStringRef(wcs->wcstr(), len, true);
            }
        }

        int UnicodeString::compare(wchar_t const* str)
        {
            int len1 = size();
            int len2 = (int)wcslen(str);
            int n = min(len1, len2);
            int diff = wcsncmp(body(), str, n);
            if (diff != 0)
            {
                return diff;
            }
            return len1 - len2;
        }

        int UnicodeString::compare(Value* x)
        {
            if (x == NULL || x->isNull())
            {
                return 1;
            }
            UnicodeString* s = (UnicodeString*)x;
            int len1 = size();
            int len2 = s->size();
            int n = min(len1, len2);
            int diff = wcsncmp(body(), s->body(), n);
            if (diff != 0)
            {
                return diff;
            }
            return len1 - len2;
        }

        UnicodeString* UnicodeString::concat(UnicodeString* head, UnicodeString* tail)
        {
            int headLength = head->size();
            int tailLength = tail->size();
            UnicodeStringLiteral* s = new((headLength + tailLength)* sizeof(wchar_t))UnicodeStringLiteral(headLength +
                                          tailLength);
            memcpy(s->chars, head->body(), headLength* sizeof(wchar_t));
            memcpy(s->chars + headLength, tail->body(), tailLength* sizeof(wchar_t));
            s->chars[headLength + tailLength] = '\0';
            return s;
        }

        UnicodeString* UnicodeString::toUpperCase()
        {
            int n = size();
            UnicodeStringLiteral* dst = new(n)UnicodeStringLiteral(n);
            wchar_t* src = body();
            for (int i = 0; i < n; i++)
            {
                dst->chars[i] = towupper(src[i]);
            }
            dst->chars[n] = '\0';
            return dst;
        }

        UnicodeString* UnicodeString::toLowerCase()
        {
            int n = size();
            UnicodeStringLiteral* dst = new(n)UnicodeStringLiteral(n);
            wchar_t* src = body();
            for (int i = 0; i < n; i++)
            {
                dst->chars[i] = towlower(src[i]);
            }
            dst->chars[n] = '\0';
            return dst;
        }

        Value* UnicodeString::getAt(int index)
        {
            int length = size();
            if ((unsigned)index >= (unsigned)length)
            {
                MCO_THROW IndexOutOfBounds(index, length);
            }
            return new IntValue((wchar_t)body()[index]);
        }

        String* UnicodeString::stringValue()
        {
            wchar_t* src = wcstr();
            int len = (int)wcstombs(NULL, src, 0);
            String* mbs = String::create(len);
            wcstombs(mbs->body(), src, len + 1);
            return mbs;
        }

        size_t UnicodeString::toString(char* buf, size_t bufSize)
        {
            return wcstombs(buf, wcstr(), bufSize);
        }

        UnicodeString* UnicodeString::format(wchar_t const* fmt, ...)
        {
            wchar_t buf[BUF_SIZE];
            va_list params;
            va_start(params, fmt);
            #ifdef _WIN32
                vswprintf(buf, fmt, params);
            #else 
                buf[BUF_SIZE - 1] = '\0';
                vswprintf(buf, BUF_SIZE - 1, fmt, params);
            #endif 
            va_end(params);
            return create(buf);
        }

        size_t UnicodeString::serialize(char* buf, size_t bufSize)
        {
            int len = size();
            if (bufSize >= (size_t)(7+len * 2))
            {
                *buf++ = tpUnicode;
                buf = packInt(buf, len);
                wchar_t* ptr = body();
                for (int i = 0; i < len; i++)
                {
                    buf = packShort(buf, ptr[i]);
                }
                *buf++ = 0;
                *buf = 0;
                return 7+len * 2;
            }
            return 0;
        }

        Value* UnicodeStringLiteral::clone(AbstractAllocator* allocator)
        {
            return new (length*sizeof(wchar_t), allocator) UnicodeStringLiteral(chars, length);
        }

        int UnicodeStringLiteral::size()
        {
            return length;
        }

        wchar_t* UnicodeStringLiteral::body()
        {
            return chars;
        }

        wchar_t* UnicodeStringLiteral::wcstr()
        {
            return chars;
        }

        int UnicodeStringRef::size()
        {
            return length;
        }

        wchar_t* UnicodeStringRef::body()
        {
            return chars;
        }

        Value* UnicodeStringRef::clone(AbstractAllocator* allocator)
        {
            return new (allocator) UnicodeStringRef(chars, length, zeroTerminated);
        }

        wchar_t* UnicodeStringRef::wcstr()
        {
            if (!zeroTerminated)
            {
                UnicodeString* s = UnicodeString::create(chars, length);
                chars = s->wcstr();
                zeroTerminated = true;
            }
            return chars;
        }
    #endif 

    Type Reference::type()
    {
        return tpReference;
    }

    uint64_t Reference::hashCode()
    {
        return intValue();
    }

    size_t Reference::serialize(char* buf, size_t size)
    {
        if (size >= 9)
        {
            *buf++ = tpReference;
            packLong(buf, intValue());
            return 9;
        }
        return 0;
    }

    Value* Struct::clone(AbstractAllocator* alloc) 
    {
        StructStub* copy = new (alloc->allocate(sizeof(StructStub))) StructStub();
        int n = nComponents();
        Vector<Value>* arr = Vector<Value>::create(n, alloc);
        for (int i = 0; i < n; i++) {
            Value* v = get(i)->clone(alloc);
            arr->items[i] = v;
            if (v->type() == tpStruct) {
                ((StructStub*)v)->outer = copy;
            }
        }
        copy->items = arr;
        copy->outer = NULL;
        return copy;
    }
        

    Value* Struct::next() 
    { 
        return currComponent < nComponents() ? get(currComponent++) : NULL;
    }

    Type Struct::type()
    {
        return tpStruct;
    }

    Value* Struct::scope()
    {
        return NULL;
    }

    size_t Struct::toString(char* buf, size_t bufSize)
    {
        char* dst = buf;
        if (bufSize > 0)
        {
            *dst++ = '{';
            bufSize -= 1;
            for (int i = 0, n = nComponents(); i < n; i++)
            {
                if (bufSize > 0 && i != 0)
                {
                    *dst++ = ',';
                    bufSize -= 1;
                }
                size_t len = get(i)->toString(dst, bufSize);
                bufSize -= len;
                dst += len;
            }
            if (bufSize > 0)
            {
                *dst++ = '}';
                bufSize -= 1;
            }
            if (bufSize > 0)
            {
                *dst = '\0';
            }
        }
        return dst - buf;
    }

    void Struct::output(FormattedOutput *streams, size_t n_streams)
    {
        for (int i = 0, n = nComponents(); i < n; i++) {
            for (size_t s = 0; s < n_streams; ++s) {
                streams[s].print(i == 0 ? "{" : ", ");
            }
            get(i)->output(streams, n_streams);
        }
        for (size_t s = 0; s < n_streams; ++s) {
            streams[s].print("}");
        }
    }

    int Struct::compare(Value* x)
    {
        if (x->isNull())
        {
            return 1;
        }
        Struct* s = (Struct*)x;
        assert(s->type() == tpStruct && s->nComponents() == nComponents());

        for (int i = 0, n = nComponents(); i < n; i++)
        {
            int diff = get(i)->compare(s->get(i));
            if (diff != 0)
            {
                return diff;
            }
        }
        return 0;
    }

    size_t Struct::serialize(char* buf, size_t bufSize)
    {
        if (bufSize >= 2)
        {
            int n = nComponents();
            char* start = buf;
            *buf++ = tpStruct;
            *buf++ = (char)n;
            bufSize -= 2;
            for (int i = 0; i < n; i++)
            {
                size_t pos = get(i)->serialize(buf, bufSize);
                if (pos == 0)
                {
                    return 0;
                }
                buf += pos;
                bufSize -= pos;
            }
            return buf - start;
        }
        return 0;
    }

    bool Array::isPlain() 
    {
        return false;
    }
    
    Type Array::type()
    {
        return tpArray;
    }

    int Array::compare(Value* x)
    {
        if (x->isNull())
        {
            return 1;
        }
        assert(x->type() == tpArray);
        Array* arr = (Array*)x;
        int n1 = size();
        int n2 = arr->size();
        for (int i = 0, n = min(n1, n2); i < n; i++)
        {
            int diff = getAt(i)->compare(arr->getAt(i));
            if (diff != 0)
            {
                return diff;
            }
        }
        return n1 - n2;
    }

    size_t Array::toString(char* buf, size_t bufSize)
    {
        char* dst = buf;
        if (bufSize > 1)
        {
            *dst++ = '[';
            bufSize -= 1;
            for (int i = 0, n = size(); i < n; i++)
            {
                if (bufSize > 0 && i != 0)
                {
                    *dst++ = ',';
                    bufSize -= 1;
                }
                size_t len = getAt(i)->toString(dst, bufSize);
                bufSize -= len;
                dst += len;
            }
            if (bufSize > 0)
            {
                *dst++ = ']';
                bufSize -= 1;
            }
            if (bufSize > 0)
            {
                *dst = '\0';
            }
        }
        return dst - buf;
    }

    void Array::output(FormattedOutput *streams, size_t n_streams)
    {
        size_t j, sz = size();
        bool hasActive = false;
        for (size_t i = 0; i < n_streams; ++i) {
            if (streams[i].isActive()) {
                hasActive = true;
                break;
            }
        }
        if (! hasActive) { return; }

        for (size_t i = 0; i < n_streams; ++i) {
            streams[i].print("[");
        }
        for (j = 0; j < sz; ++j) {
            for (size_t i = 0; i < n_streams; ++i) {
                if (j > 0) streams[i].print(", ");
                if (streams[i].array_first + streams[i].array_last < sz) {
                    if (j == streams[i].array_first) {
                        streams[i].disable();
                    }
                    if (j == sz - streams[i].array_last) {
                        streams[i].enable();
                        streams[i].print("...<%d element(s)>..., ", sz - streams[i].array_last - streams[i].array_first);
                    }
                }
                if (streams[i].array_show_indexes) {
                    streams[i].print("%d:", j);
                }
            }
            getAt(j)->output(streams, n_streams);
        }

        for (size_t i = 0; i < n_streams; ++i) {
            if (streams[i].array_first + streams[i].array_last < sz && streams[i].array_last == 0) {
                streams[i].enable();
                streams[i].print("...<%d element(s)>...", sz - streams[i].array_last - streams[i].array_first);
            }
            streams[i].print("]");
        }
    }



    size_t Array::serialize(char* buf, size_t bufSize)
    {
        if (bufSize >= 6)
        {
            size_t len = size();
            Type elemType = getElemType();
            char* start = buf;
            size_t i;
            *buf++ = tpArray;
            *buf++ = (char)elemType;
            buf = packInt(buf, (int)len);
            bufSize -= 6;            
            switch (elemType) { 
              case tpBool:
              case tpInt1:
              case tpUInt1:
                if (bufSize < len) {
                    return 0;
                } else { 
                    getBody(buf, 0, len);
                    buf += len;
                }
                break;
              case tpInt2:
              case tpUInt2:
                if (bufSize < len*2) {
                    return 0;
                } else { 
                    if (isPlain()) {                     
                        short* src = (short*)pointer();
                        for (i = 0; i < len; i++) {
                            buf = packShort(buf, src[i]);
                        }
                    } else { 
                        short src[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (size_t offs = 0; offs < len; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            size_t n = len - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? len - offs : MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            getBody(src, offs, n);
                            for (i = 0; i < n; i++) { 
                                buf = packShort(buf, src[i]);
                            }        
                        }
                    }
                }
                break;
              case tpReal4:
              case tpInt4:
              case tpUInt4:
                if (bufSize < len*4) {
                    return 0;
                } else {  
                    if (isPlain()) {                     
                        int* src = (int*)pointer();
                        for (i = 0; i < len; i++) {
                            buf = packInt(buf, src[i]);
                        }        
                    } else { 
                        int src[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (size_t offs = 0; offs < len; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            size_t n = len - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? len - offs : MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            getBody(src, offs, n);
                            for (i = 0; i < n; i++) { 
                                buf = packInt(buf, src[i]);
                            }        
                        }
                    }
                }
                break;
              case tpDateTime:
              case tpReal8:
              case tpInt8:
              case tpUInt8:
                if (bufSize < len*8) {
                    return 0;
                } else { 
                    if (isPlain()) {                     
                        int64_t* src = (int64_t*)pointer();
                        for (i = 0; i < len; i++) {
                            buf = packLong(buf, src[i]);
                        }
                    } else { 
                        int64_t src[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (size_t offs = 0; offs < len; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            size_t n = len - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? len - offs : MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            getBody(src, offs, n);
                            for (i = 0; i < n; i++) { 
                                buf = packLong(buf, src[i]);
                            }        
                        }
                    }
                }
                break;
              default:
                for (i = 0; i < len; i++) {
                    size_t pos = getAt(i)->serialize(buf, bufSize);
                    if (pos == 0)
                    {
                        return 0;
                    }
                    buf += pos;
                    bufSize -= pos;
                }
            }
            return buf - start;
        }
        return 0;
    }

    Type Blob::type()
    {
        return tpBlob;
    }

    size_t Blob::toString(char* buf, size_t bufSize)
    {
        return fillBuf(buf, "<BLOB>", bufSize);
    }

    int Blob::compare(Value* x)
    {
        return x->isNull() ? 1 : 0;
    }

    size_t Blob::serialize(char* buf, size_t bufSize)
    {
        reset();
        int len = available();
        if (bufSize >= (size_t)(5+len))
        {
            *buf++ = tpBlob;
            buf = packInt(buf, len);
            int n = get(buf, len);
            assert(n == len);
            return 5+len;
        }
        return 0;
    }

    Field* Field::findComponent(String*)
    {
        MCO_THROW InvalidOperation("Field::findComponent");
    }

    Field* Field::findComponent(char const* componentName)
    {
        return findComponent(String::create(componentName));
    }

    size_t Field::calculateStructAlignment()
    {
        Iterator < Field > * fi = components();
        size_t maxAlignment = 1;
        while (fi->hasNext())
        {
            Field* f = fi->next();
            Type fieldType = f->type();
            size_t alignment = typeAlignment[fieldType];
            if (fieldType == tpStruct)
            {
                alignment = f->calculateStructAlignment();
            }
            if (alignment > maxAlignment)
            {
                maxAlignment = alignment;
            }
        }
        return maxAlignment;
    }

    size_t Field::serialize(char* buf, size_t bufSize)
    {
        if (bufSize < 8)
        {
            return 0;
        }
        char* start = buf;
        Type t = type();
        *buf++ = t;
        *buf++ = isAutoGenerated();
        *buf++ = isNullable();
        *buf++ = precision();
        bufSize -= 4;
        if (t == tpArray || t == tpString || t == tpUnicode)
        {
            buf = packInt(buf, fixedSize());
            bufSize -= 4;
        }
        size_t len;
        if (name() == NULL)
        {
            if (bufSize < 2)
            {
                return 0;
            }
            *buf++ = tpNull;
            bufSize -= 1;
        }
        else
        {
            len = name()->serialize(buf, bufSize);
            if (len == 0)
            {
                return 0;
            }
            bufSize -= len;
            buf += len;
        }
        switch (t)
        {
            case tpStruct:
                {
                    Iterator < Field > * iterator = components();
                    while (iterator->hasNext())
                    {
                        len = iterator->next()->serialize(buf, bufSize);
                        if (len == 0)
                        {
                            return 0;
                        }
                        buf += len;
                        bufSize -= len;
                    }
                    break;
                }
            case tpArray:
                len = element()->serialize(buf, bufSize);
                if (len == 0)
                {
                    return 0;
                }
                buf += len;
                bufSize -= len;
            default:
                break;
        }
        if (bufSize > 0)
        {
            *buf++ = (char)EOF;
            return buf - start;
        }
        return 0;
    }

    Array* createScalarArray(AbstractAllocator* alloc, Type elemType, int elemSize, int size)
    {
        switch (elemType) { 
          case tpInt1:
              return new (alloc) ScalarArray<signed char>(size, alloc);
          case tpInt2:
            return new (alloc) ScalarArray<short>(size, alloc);
          case tpInt4:
            return new (alloc) ScalarArray<int>(size, alloc);
          case tpInt8:
            return new (alloc) ScalarArray<int64_t>(size, alloc);
          case tpUInt1:
            return new (alloc) ScalarArray<unsigned char>(size, alloc);
          case tpUInt2:
            return new (alloc) ScalarArray<unsigned short>(size, alloc);
          case tpUInt4:
            return new (alloc) ScalarArray<unsigned int>(size, alloc);
          case tpUInt8:
            return new (alloc) ScalarArray<uint64_t>(size, alloc);
          case tpReal4:
            return new (alloc) ScalarArray<float>(size, alloc);
          case tpReal8:
            return new (alloc) ScalarArray<double>(size, alloc);
          case tpString:
            return new (alloc) StringArray(size, elemSize, alloc);
          default:
            assert(false);
        }
    }

    template<>
    ScalarArray<bool>* ScalarArray<bool>::create(bool* body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<char>* ScalarArray<char>::create(char* body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<unsigned char>* ScalarArray<unsigned char>::create(unsigned char* body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<signed char>* ScalarArray<signed char>::create(signed char* body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<signed short>* ScalarArray<signed short>::create(signed short* body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<unsigned short>* ScalarArray<unsigned short>::create(unsigned short* body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<signed int>* ScalarArray<signed int>::create(signed int* body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<unsigned int>* ScalarArray<unsigned int>::create(unsigned int* body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<int64_t>* ScalarArray<int64_t>::create(int64_t* body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<uint64_t>* ScalarArray<uint64_t>::create(uint64_t* body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<float>* ScalarArray<float>::create(float* body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<double>* ScalarArray<double>::create(double* body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<String*>* ScalarArray<String*>::create(String** body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }
    template<>
    ScalarArray<Array*>* ScalarArray<Array*>::create(Array** body, int len, bool copy) { 
        return new ScalarArray(body, len, copy);
    }

    void FormattedOutput::initFormat(bool full) 
    {
        seq_first    = (full) ? UNLIMITED : DEFAULT_SEQ_FIRST;
        seq_last     = (full) ? UNLIMITED : DEFAULT_SEQ_LAST;
        array_first  = (full) ? UNLIMITED : DEFAULT_ARRAY_FIRST;
        array_last   = (full) ? UNLIMITED : DEFAULT_ARRAY_LAST;
        seq_show_indexes   = full;
        array_show_indexes = full;
    }

    void FormattedOutput::setFd(FILE *f) 
    {
        if (fd && fd != stdout) {
            fclose(fd);
        }
        fd = f;
    }

    void FormattedOutput::print(const char *format, va_list *list) 
    {
        if (fd && disabled == 0) {
            if (htmlEncode) {
                char buf[ENCODE_BUF_SIZE];
                int len = vsnprintf(buf, sizeof(buf), format, *list);
                for (int i = 0; i < len; ++i) {
                    encode(buf[i]);
                }
            } else {
                vfprintf(fd, format, *list);
            }
        }
    }

    void FormattedOutput::print(const char *format, ...) 
    {
        if (fd && disabled == 0) {
            va_list list;
            va_start(list, format);
            if (htmlEncode) {
                char buf[ENCODE_BUF_SIZE];
                int len = vsnprintf(buf, sizeof(buf), format, list);
                for (int i = 0; i < len; ++i) {
                    encode(buf[i]);
                }
            } else {
                vfprintf(fd, format, list);
            }
            va_end(list);
        }
    }

    void FormattedOutput::put(const char *data, size_t len) 
    {
        if (fd && disabled == 0) {
            if (htmlEncode) {
                for (size_t i = 0; i < len; ++i) {
                    encode(data[i]);
                }
            } else {
                fwrite(data, 1, len, fd); 
            }
        }
    }

    void FormattedOutput::encode(char c) 
    {
        switch(c) {
            case '&':
                fputs("&amp;", fd);
                break;
            case '<':
                fputs("&lt;", fd);
                break;
            case '>':
                fputs("&gt;", fd);
                break;
            case '"':
                fputs("&quot;", fd);
                break;
            default:
                fputc(c, fd);
        }
    }
}

