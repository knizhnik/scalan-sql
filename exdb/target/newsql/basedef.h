class DynamicObject
{
  public:
    void* operator new(size_t size);
    
    void* operator new(size_t size, size_t varPart)
    {
        return operator new(size + varPart);
    }
    
    void* operator new(size_t size, AbstractAllocator* allocator) { 
        allocator->allocate(size);
    }
    
    void* operator new(size_t fixedSize, size_t varyingPart, AbstractAllocator* allocator)
    {
        return allocator->allocate(fixedSize + varyingPart);
    }
    
    void operator delete (void* ptr, AbstractAllocator*)
    {
        allocator->free(ptr);
    }

    void operator delete (void* ptr, size_t, AbstractAllocator*)
    {
        allocator->free(ptr);
    }
    
    virtual void destroy(AbstractAllocator* allocator) 
    { 
        delete (allocator) this;
    }

    virtual ~DynamicObject() {}
};
    
class Value : public DynamicObject
{
    virtual Value* clone(AbstractAllcoator*) = 0;
};


class NullValue : public Value
{
  public:
    Value* clone(AbstractAllcoator*) 
    {
        return this;
    }
    void destroy(AbstractAllocator*) {}
};

class BoolValue : public Value
{
  public:
    Value* clone(AbstractAllcoator*) 
    {
        return this;
    }
    BoolValue* create(bool val);
    void destroy(AbstractAllocator* allocator) {}
};


class DataSource 
{
  public:
    virtual Vector<Field> columns() = 0;
    virtual Cursor* cursor() = 0;
};



class CurrentRecord 
{
    Runtime* runtime;
    Record*  save;
  public:
    CurrentRecord(Runtime* rt, Record* rec) {
        runtime= rt;
        save = rt->record;
        rt->record = rec;
    }
    ~CurrentRecord() { 
        runtime->record = save;
    }
};
