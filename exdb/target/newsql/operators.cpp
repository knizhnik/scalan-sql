//
// Cross join
//

CrossJoin::CursorImpl::CursorImpl(Runtime* _runtime, CrossJoin* _ds) 
: runtime(_runtime), ds(_ds), outerCursor(NULL), innerCursor(NULL), outerTuple(NULL)
{
    outerColumns = ds->outer->nFields();
    innerColumns = ds->inner->nFields();
}

Record* CrossJoin::CursorImpl::next()
{    
    if (outerCursor == NULL) { 
        outerCursor = ds->outer->cursor();
        innerCursor = ds->inner->cursor();
        outerTuple = outerCursor->next();
    }
        
    while (true) { 
        if (outerTuple == NULL) { 
            return NULL;
        }
        Record* innerTuple = innerCursor->next();
        if (innerTuple == NULL) { 
            delete (runtime->allocator) outerTuple;
            delete (runtime->allocator) innerCursor;
            innerCursor = ds->inner->cursor();
            outerTuple = outerCursor->next();
        } else { 
            Tuple* tuple = Tuple::create(outerColumns + innerColumns, runtime->allocator);
            for (int i = 0; i < outerColumns; i++) { 
                tuple->set(i, outerTuple->get(i)->clone(runtime->allocator));
            }
            for (int i = 0; i < innerColumns; i++) { 
                tuple->set(i + outerColumns, innerTuple->get(i)->clone(runtime->allocator));
            }
            return tuple;            
        }
    }
}


CrossJoin::CursorImpl::CursorImpl()
{
    delete (runtime->allocator) innerCursor;
    delete (runtime->allocator) outerCursor;
    delete (runtime->allocator) outerTuple;   
}

CrossJoin::CrossJoin(DataSource* _outer, DataSource* _inner) : outer(_outer), inner(_inner) {}


CrossJoin::~CrossJoin()
{
    delete (runtime->allocator) outer;    
    delete (runtime->allocator) inner;
}

Cursor* CrossJoin::cursor(Runtime* runtime) 
{
    return new (runtime->allocator) CursorImpl(runtime, this); 
}

//
// Indexed join
//

IndexedJoin::CursorImpl::CursorImpl(Runtime* _runtime, CrossJoin* _ds) 
: runtime(_runtime), ds(_ds), outerCursor(NULL), innerCursor(NULL), innerDataSource(NULL), outerTuple(NULL)
{
    outerColumns = ds->outer->nFields();
    innerColumns = ds->inner->nFields();
}

Record* IndexedJoin::CursorImpl::next()
{    
    if (outerCursor == NULL) { 
        outerCursor = ds->outer->cursor();
        outerTuple = outerCursor->next();
    }
        
    while (true) { 
        if (outerTuple == NULL) { 
            return NULL;
        }
        if (innerCursor == NULL) { 
            Vector<Value>* keys = Vector<Value>::create(ds->outerKeys->size);
            ds->record = outerTuple;
            for (int i = 0; i < ds->outerKeys->size; i++) { 
                keys->items[i] = ds->outerKeys->items[i]->evaluate(runtime);
            }
            innerDataSource = ds->index->search(runtime, Index::Equals, keys, false);
            innerCursor = innerDataSource->cursor();
        }
        Record* innerTuple = innerCursor->next();
        if (innerTuple == NULL) { 
            delete (runtime->allocator) innerCursor;
            delete (runtime->allocator) innerDataSource;
            delete (runtime->allocator) outerTuple;
            innerCursor = NULL;
            innerDataSource = NULL;
            outerTuple = outerCursor->next();
        } else { 
            Tuple* tuple = Tuple::create(outerColumns + innerColumns, runtime->allocator);
            for (int i = 0; i < outerColumns; i++) { 
                tuple->set(i, outerTuple->get(i)->clone(runtime->allocator));
            }
            for (int i = 0; i < innerColumns; i++) { 
                tuple->set(i + outerColumns, innerTuple->get(i)->clone(runtime->allocator));
            }
            return tuple;            
        }
    }
}


IndexedJoin::CursorImpl::CursorImpl()
{
    delete (runtime->allocator) innerCursor;
    delete (runtime->allocator) innerDataSource;
    delete (runtime->allocator) outerCursor;
    delete (runtime->allocator) outerTuple;   
}


Cursor* IndexedJoin::cursor(Runtime* runtime)
{
    return new (runtime->allocator) CursorImpl(runtime, this); 
}


IndexedJoin::IndexedJoin(DataSource* _outer, DataSource* _inner, Vector<ExprNode>* _outerKeys, bool _isOuterJoin)
: outer(_outer), inner(_inner), outerKeys(_outerKeys), isOuterKey(_isOuterKey) {}


IndexedJoin::~IndexedJoin()
{
    delete (runtime->allocator) outer;    
    delete (runtime->allocator) inner;
    delete (runtime->allocator) outerKeys;
}

//
// Hash join
//

HashedJoin::CursorImpl::CursorImpl(Runtime* _runtime, CrossJoin* _ds) 
: runtime(_runtime), ds(_ds), outerCursor(NULL), innerCursor(NULL), outerTuple(NULL), hash(NULL), outerKeyValues(NULL)
{
    outerColumns = ds->outer->nFields();
    innerColumns = ds->inner->nFields();
}

Record* HashedJoin::CursorImpl::next()
{    
    if (outerCursor == NULL) {         
        outerCursor = ds->outer->cursor();
        outerTuple = outerCursor->next();
        hash = new DynamicHashTable();

        innerCursor = ds->inner->cursor();
        Record* innerTyple;        
        while ((innerTuple = innerCursor->next()) != NULL) { 
            CurrentRecord ctx(ds->runtime, innerTuple);
            Value* innerKey;
            if (ds->innerKeys->size > 1) { 
                Vector<Value>* keys = Vector<Value>::create(ds->innerKeys->size, runtime->allocator);        
                for (int i = 0; i < keys->size; i++) { 
                    keys->items[i] = ds->innerKeys->item[i]->evaluate(runtime);
                }
                innerKey = new (runtime) ArrayStub(keys);
            } else { 
                innerKey = ds->innerKeys->item[0]->evaluate(runtime);
            }
            hash->put(innerKey, innerTuple);
        }
        outerKeyValues = Vector<Value>::create(ds->outerKeys, runtime->allocator);
        delete (runtime) innerCursor;
        innerCursor = NULL;
    }
        
    while (true) { 
        if (outerTuple == NULL) { 
            return NULL;
        }
        if (innerCursor == NULL) { 
            CurrentRecord ctx(ds->runtime, outerTuple);
            if (outerKeys->size == 1) { 
                innerCursor = innerDataSource->get(ds->outerKeys->items->evaluate(runtime));
            } else {
                for (int i = 0; i < outerKeys->size; i++) { 
                    outerKeys->items[i] = ds->outerKeys->items[i]->evaluate(runtime);
                }                         
                innerCursor = hash->get(outerKeys);
           }
        }
        Record* innerTuple = innerCursor->next();
        if (innerTuple == NULL) { 
            Tuple* outer = outerTuple;
            delete (runtime->allocator) innerCursor;
            delete (runtime->allocator) outerTuple;
            innerCursor = NULL;
            outerTuple = NULL;
            if (ds->outerJoin) {
                Tuple* tuple = Tuple::create(outerColumns + innerColumns, runtime->allocator);
                for (int i = 0; i < outerColumns; i++) { 
                    tuple->set(i, outer->get(i)->clone(runtime->allocator));
                }
                for (int i = 0; i < innerColumns; i++) { 
                    tuple->set(i + outerColumns, &Null);
                }
                return tuple;  
            }
        } else { 
            Tuple* tuple = Tuple::create(outerColumns + innerColumns, runtime->allocator);
            for (int i = 0; i < outerColumns; i++) { 
                tuple->set(i, outerTuple->get(i)->clone(runtime->allocator));
            }
            for (int i = 0; i < innerColumns; i++) { 
                tuple->set(i + outerColumns, innerTuple->get(i)->clone(runtime->allocator));
            }
            return tuple;            
        }
    }
}


HashedJoin::CursorImpl::CursorImpl()
{
    delete (runtime->allocator) innerCursor;
    delete (runtime->allocator) outerCursor;
    delete (runtime->allocator) outerTuple;   
    delete (runtime->allocator) hash;   
    delete (runtime->allocator) outerKeyValues;   
}


Cursor* HashedJoin::cursor(Runtime* runtime)
{
    return new (runtime->allocator) CursorImpl(runtime, this); 
}


HashedJoin::HashedJoin(DataSource* _outer, DataSource* _inner, Vector<ExprNode>* _outerKeys, Vector<ExprNode>* innerKeys, bool _isOuterJoin)
: outer(_outer), inner(_inner), outerKeys(_outerKeys), innerKeys(_innerKeys), isOuterJoin(_isOuterJoin) {}


HashedJoin::~HashedJoin()
{
    delete (runtime->allocator) outer;    
    delete (runtime->allocator) inner;
    delete (runtime->allocator) outerKeys;
    delete (runtime->allocator) innerKeys;
}
