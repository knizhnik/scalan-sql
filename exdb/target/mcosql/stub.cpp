#include <ctype.h>
#include <dbapi.h>
#include <memmgr.h>
#include "stub.h"
#include "convert.h"

namespace McoSql
{
    ArrayStub* ArrayStub::create(Vector<Value>* vec, Type type) { 
        return new ArrayStub(vec, type);
    }

    Value* ArrayStub::clone(AbstractAllocator* alloc)
    {
        int len = items->length;
        Vector<Value>* vec = Vector < Value > ::create(len, alloc);
        ArrayStub *ret = new (alloc->allocate(sizeof(*this))) ArrayStub(vec);

        for (int i = 0; i < len; i++) {
            vec->items[i] = items->items[i]->clone(alloc);
            if (vec->items[i]->type() == tpStruct)
            {
                ((StructStub*)vec->items[i])->outer = ret;
            }
        }
        return ret;
    }

    void ArrayStub::setAt(int index, Value* value)
    {
        MCO_THROW InvalidOperation("ArrayStub::setAt");
    }

    Value* ArrayStub::updateAt(int index)
    {
        MCO_THROW InvalidOperation("ArrayStub::updateAt");
    }

    void ArrayStub::setSize(int newSize)
    {
        MCO_THROW InvalidOperation("ArrayStub::setSize");
    }

    void ArrayStub::getBody(void* dst, int offs, int len)
    {
        MCO_THROW InvalidOperation("ArrayStub::getBody");
    }

    void ArrayStub::setBody(void* src, int offs, int len)
    {
        MCO_THROW InvalidOperation("ArrayStub::setBody");
    }

    Value* ArrayStub::getAt(int index)
    {
        return items->getAt(index);
    }

    int ArrayStub::getElemSize() const
    {
        return typeSizeof[elemType];
    }

    Type ArrayStub::getElemType() const
    {
        return elemType;
    }


    int ArrayStub::size()
    {
        return items->length;
    }

    int StructStub::nComponents()
    {
        return items->length;
    }

    Value* StructStub::get(int index)
    {
        return items->getAt(index);
    }

    void StructStub::set(int index, Value* value)
    {
        items->items[index] = value;
    }

    Value* StructStub::update(int index)
    {
        MCO_THROW InvalidOperation("StructStub::update");
    }


    Struct* StructStub::source()
    {
        return NULL;
    }

    Value* StructStub::scope()
    {
        return outer;
    }

    void StructStub::deleteRecord()
    {
    }

    void StructStub::updateRecord()
    {
    }
  
    Record* ReferenceStub::dereference()
    {
        MCO_THROW InvalidOperation("ReferenceStub::dereference");
    }

    int ReferenceStub::compare(Value* v)
    {
        return v->isNull() ? 1 : oid < ((ReferenceStub*)v)->oid ?  - 1: oid == ((ReferenceStub*)v)->oid ? 0 : 1;
    }

    size_t ReferenceStub::toString(char* buf, size_t bufSize)
    {
        IntValue iv(oid);
        return iv.toString(buf, bufSize);
    }

    int64_t ReferenceStub::intValue()
    {
        return oid;
    }

    String* ReferenceStub::stringValue()
    {
        IntValue iv(oid);
        return iv.stringValue();
    }

    Value* ReferenceStub::clone(AbstractAllocator* alloc)
    {
        return new (alloc) ReferenceStub(oid);
    }


    int BlobStub::available()
    {
        return content->size() - pos;
    }

    int BlobStub::get(void* buffer, int size)
    {
        int n = pos + size <= content->size() ? size : pos < content->size() ? content->size() - pos : 0;
        memcpy(buffer, content->body() + pos, n);
        pos += n;
        return n;
    }

    void BlobStub::append(void const* buffer, int size)
    {
        StringRef tail((const char*)buffer, size);
        content = String::concat(content, &tail);
    }

    void BlobStub::reset()
    {
        pos = 0;
    }

    void BlobStub::truncate()
    {
        content = String::create(0);
        pos = 0;
    }

    void DataSourceStub::setMark(size_t mark) 
    { 
        this->mark = mark;
    }

    void DataSourceStub::release()
    {
        AllocatorContext ctx(allocator);
        if (destructor != NULL)
        {
            destructor(this, arg);
        }
        allocator->deleteSegment(segmentId, mark);
    }

    int DataSourceStub::nFields()
    {
        return nColumns;
    }

    size_t DataSourceStub::nRecords(Transaction*)
    {
        return size;
    }

    Iterator < Field > * DataSourceStub::fields()
    {
        AllocatorContext ctx(allocator);
        return (Iterator < Field > *)new ListIterator < FieldStub > (columns);
    }

    Cursor* DataSourceStub::records(Transaction* trans)
    {
        return records();
    }

    Cursor* DataSourceStub::records()
    {
        AllocatorContext ctx(allocator);
        return new CursorStub(this);
    }

    bool DataSourceStub::isNumberOfRecordsKnown()
    {
        return size >= 0;
    }

    bool DataSourceStub::isRIDAvailable()
    {
        return false;
    }

    int DataSourceStub::compareRID(Record* r1, Record* r2)
    {
        MCO_THROW InvalidOperation("DataSourceStub:compareRID");
    }

    Reference* DataSourceStub::getRID(Record* rec)
    {
        MCO_THROW InvalidOperation("DataSourceStub:getRID");
    }


    String* FieldStub::name()
    {
        return fieldName;
    }

    Type FieldStub::type()
    {
        return fieldType;
    }

    SortOrder FieldStub::order()
    {
        return UNSPECIFIED_ORDER;
    }

    Table* FieldStub::table()
    {
        MCO_THROW InvalidOperation("FieldStub::table");
    }

    Field* FieldStub::scope()
    {
        return fieldScope;
    }

    Value* FieldStub::get(Struct* rec)
    {
        return rec->get(fieldNo);
    }

    void FieldStub::set(Struct* rec, Value* val)
    {
        MCO_THROW InvalidOperation("FieldStub::set");
    }

    Value* FieldStub::update(Struct* rec)
    {
        MCO_THROW InvalidOperation("FieldStub::update");
    }

    String* FieldStub::referencedTableName()
    {
        return NULL;
    }

    Iterator < Field > * FieldStub::components()
    {
        if (fieldType != tpStruct)
        {
            MCO_THROW InvalidOperation("FieldDescriptor::components");
        }
        return (Iterator < Field > *)new ListIterator < FieldStub > (members);
    }

    int FieldStub::elementSize() 
    { 
        return -1;
    }
        
    Type FieldStub::elementType()
    {
        if (fieldType != tpArray)
        {
            return tpNull;
        }
        return members->fieldType;
    }

    Field* FieldStub::element()
    {
        if (fieldType != tpArray)
        {
            MCO_THROW InvalidOperation("ArrayFieldDescriptor::element");
        }
        return members;
    }

    int FieldStub::fixedSize()
    {
        if (fieldType != tpArray && fieldType != tpString && fieldType != tpUnicode)
        {
            MCO_THROW InvalidOperation("ArrayFieldDescriptor::fixedSize");
        }
        return fixedArraySize;
    }

    int FieldStub::precision()
    {
        return numericPrecision;
    }

    int FieldStub::width()
    {
        return -1;
    }

    bool FieldStub::isNullable()
    {
        return nullable;
    }

    bool FieldStub::isAutoGenerated()
    {
        return synthetic;
    }

    char* FieldStub::deserialize(char* buf)
    {
        size_t pos;
        fieldType = (Type)*buf++;
        synthetic = *buf++ != 0;
        nullable = *buf++ != 0;
        numericPrecision  = *buf++;
        if (fieldType == tpArray || fieldType == tpString || fieldType == tpUnicode)
        {
            fixedArraySize = unpackInt(buf);
            buf += 4;
        }
        else
        {
            fixedArraySize = 0;
        }
        fieldScope = NULL;
        fieldName = (String*)Value::deserialize(buf, pos);
        if (fieldName->isNull())
        {
            fieldName = NULL;
        }
        else
        {
            assert(fieldName->type() == tpString);
        }
        buf += pos;
        FieldStub** fpp = &members;
        for (int i = 0; *buf != (char)EOF; i++)
        {
            FieldStub* f = new FieldStub();
            f->fieldNo = i;
            f->fieldScope = this;
            buf = f->deserialize(buf);
            *fpp = f;
            fpp = &f->next;
        }
        *fpp = NULL;
        return buf + 1;
    }

    CursorStub::CursorStub(DataSourceStub* ds)
    {
        this->ds = ds;
        buf = ds->buf;
        curr = NULL;
    }

    bool CursorStub::hasNext()
    {
        if (curr == NULL)
        {
            if (buf == NULL)
            {
                return false;
            }
            AllocatorContext ctx(allocator);
            while (true) { 
                switch (*buf)
                {
                  case tpNull:
                    if (ds->reader == NULL)
                    {
                        return false;
                    }
                    ds->reader(ds, ds->arg);
                    buf = ds->buf;
                    continue;
                  case (char)EOF:
                    buf = NULL;
                    return false;
                }
                break;
            }
            size_t pos;
            curr = Value::deserialize(buf, pos);
            if (curr->type() == tpString) {  
                MCO_THROW RuntimeError(curr->stringValue()->cstr());
            }
            assert(curr->type() == tpStruct);
            buf += pos;
        }
        return true;
    }

    Record* CursorStub::next()
    {
        if (!hasNext())
        {
            MCO_THROW NoMoreElements();
        }
        Value* v = curr;
        curr = NULL;
        return (Record*)v;
    }

}
