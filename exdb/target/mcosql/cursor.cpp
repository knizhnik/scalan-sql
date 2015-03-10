/*******************************************************************
 *                                                                 *
 *  cursor.cpp                                                      *
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
 * MODULE:    cursor.cpp
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#include "cursor.h"
#include <assert.h>
#if MCO_CFG_SQL_MALLOC_PROFILE
#include <unistd.h>
#include <fcntl.h>
#endif

namespace McoSql
{

    Runtime::Runtime(DatabaseDescriptor* db, SqlEngine* engine, size_t segmentId, size_t mark, bool traceEnabled, Transaction* trans, bool
                     autoCommit)
    {
        this->db = db;
        this->engine = engine;
        this->segmentId = segmentId;
        this->mark = mark;
        this->trans = trans;
        this->autoCommit = autoCommit;
        this->traceEnabled = traceEnabled;
        groupBegin = 0;
        groupEnd = 0;
        result = NULL;
        dependentDataSources = NULL;
    }

    void Runtime::linkDependentDataSource(DataSource* ds)
    {
        ds->next = dependentDataSources;
        dependentDataSources = ds;
    }

    void Runtime::unlinkDependentDataSource(DataSource* ds)
    {
        DataSource** dpp;
        for (dpp = &dependentDataSources; *dpp != ds; dpp = &(*dpp)->next);
        *dpp = ds->next;
    }

    int Runtime::memProfileTop = 100;

    void Runtime::release()
    {
        AbstractAllocator* alloc = allocator;
        if (alloc != NULL) {
            AllocatorContext ctx(alloc);
            bool conflict = false;

#if MCO_CFG_SQL_MALLOC_PROFILE
            static int reqNo;
            char file[64];
            sprintf(file, "profile-%d.acc", ++reqNo);
            int fd = open(file, O_CREAT|O_RDWR|O_TRUNC, 0777);
            if (fd >= 0) {
                AbstractAllocator::accumulativeProfile->dump(fd, memProfileTop);
                close(fd);
            }
            sprintf(file, "profile-%d.fin", reqNo);
            fd = open(file, O_CREAT|O_RDWR|O_TRUNC, 0777);
            if (fd >= 0) {
                MemoryProfile* profile = alloc->profile(NULL);
                for (DataSource* ds = dependentDataSources; ds != NULL; ds = ds->next) {
                    ds->allocator->profile(profile);
                }
                profile->dump(fd, memProfileTop);
                close(fd);
            }
#endif
            allocator = NULL;
            while (dependentDataSources != NULL) {
                dependentDataSources->release();
            }
            if (autoCommit)
            {
                conflict = !trans->commit();
                trans->release();
                engine->currTrans = NULL;
            }
            trans = NULL;
            alloc->deleteSegment(segmentId, mark);

#if MCO_CFG_SQL_MALLOC_PROFILE
            sprintf(file, "profile-%d.liv", reqNo);
            fd = open(file, O_CREAT|O_RDWR|O_TRUNC, 0777);
            if (fd >= 0) {
                MemoryProfile* profile = alloc->profile(NULL);
                profile->dump(fd, memProfileTop);
                close(fd);
            }
#endif
            if (conflict) {
                MCO_THROW TransactionConflict();
            }
        }
    }

    Value* Tuple::clone(AbstractAllocator* allocator)
    {
        Tuple* tuple = new ((nValues - 1)*sizeof(Value*), allocator) Tuple(nValues);
        for (int i = 0; i < nValues; i++) {
            tuple->values[i] = values[i]->clone(allocator);
        }
        return tuple;
    }

    void* Tuple::pointer()
    {
        MCO_THROW InvalidOperation("Tuple::pointer");
    }

    int Tuple::nComponents()
    {
        return nValues;
    }

    Value* Tuple::get(int index)
    {
        if ((unsigned)index >= (unsigned)nValues)
        {
            MCO_THROW IndexOutOfBounds(index, nValues);
        }
        return values[index];
    }

    Value* Tuple::update(int index)
    {
        return get(index);
    }

    void Tuple::set(int index, Value* value)
    {
        if ((unsigned)index >= (unsigned)nValues)
        {
            MCO_THROW IndexOutOfBounds(index, nValues);
        }
        values[index] = value;
    }

    void Tuple::deleteRecord()
    {
        MCO_THROW InvalidOperation("Tuple::deleteRecord");
    }

    void Tuple::updateRecord()
    {
        MCO_THROW InvalidOperation("Tuple::updateRecord");
    }

    Struct* Tuple::source()
    {
        return this;
    }

    bool ResultSet::isResultSet()
    {
        return true;
    }

    bool ResultSet::isHash()
    {
        return false;
    }

    void ResultSet::setMark(size_t mark)
    {
        runtime->mark = mark;
    }

    Cursor* ResultSet::records(Transaction* trans)
    {
        return records();
    }

    Cursor* ResultSet::records()
    {
        AllocatorContext ctx(allocator);
        if (recordArray != NULL)
        {
            return recordArray->iterator(size);
        }
        else
        {
            return new(allocator)ListIterator < Record > (first);
        }
    }

    Transaction* ResultSet::currentTransaction()
    {
        return runtime->trans;
    }

    void ResultSet::add(int pos, Record* rec)
    {
        if (pos == 0) {
            if (last == NULL) {
                last = rec;
            }
            rec->next = first;
            first = rec;
        } else {
            Record* before = first;
            while (--pos != 0) {
                before = before->next;
            }
            rec->next = before->next;
            before->next = rec;
            if (last == before) {
                last = rec;
            }
        }
        recordArray = NULL;
        size += 1;
    }

    bool ResultSet::add(Record* rec)
    {
        if (last == NULL)
        {
            first = rec;
        }
        else
        {
            last->next = rec;
        }
        rec->next = NULL;
        last = rec;
        size += 1;
        recordArray = NULL;
        return true;
    }

    void ResultSet::toArray()
    {
        if (recordArray == NULL)
        {
            recordArray = Vector < Record > ::create(size);
            Record** items = recordArray->items;
            Record* t = first;
            for (int i = 0; t != NULL; t = t->next)
            {
                items[i++] = t;
            }
        }
    }

    Value* ResultSet::get(int row, int column)
    {
        assert(recordArray != NULL);
        return recordArray->items[row]->get(column);
    }

    void ResultSet::set(int row, int column, Value* val)
    {
        assert(recordArray != NULL);
        recordArray->items[row]->set(column, val);
    }

    void ResultSet::release()
    {
        if (runtime) {
            runtime->release();
        }
    }

    bool ResultSet::isNumberOfRecordsKnown()
    {
        return true;
    }

    size_t ResultSet::nRecords(Transaction*)
    {
        return size;
    }

    bool ResultSet::isRIDAvailable()
    {
        return false;
    }

    Reference* ResultSet::getRID(Record*)
    {
        MCO_THROW InvalidOperation("ResultSet::getRID");
    }

    int ResultSet::compareRID(Record* r1, Record* r2)
    {
        MCO_THROW InvalidOperation("ResultSet::compareRID");
    }

    void ResultSet::reset()
    {
        size = 0;
        first = last = NULL;
        recordArray = NULL;
        orderBy = NULL;
    }

    ResultSet::ResultSet(Runtime* runtime)
    {
        this->runtime = runtime;
        reset();
    }


    bool ColumnIterator::hasNext()
    {
        return i < columns->length;
    }

    Field* ColumnIterator::next()
    {
        if (i >= columns->length)
        {
            MCO_THROW NoMoreElements();
        }
        FieldDescriptor* fd = columns->items[i]->field;
        if (fd == NULL || (columns->items[i]->name != NULL && columns->items[i]->name != fd->_name))
        {
            if (fd == NULL) {
                fd = new(allocator) FieldDescriptor(NULL, columns->items[i]->name, columns->items[i]->type, tpRaw);
            } else {
                fd = new(allocator)FieldDescriptor(fd->_table, columns->items[i]->name, fd->_type, fd->_elemType);
            }
        }
        i += 1;
        return fd;
    }


    TemporaryResultSet::TemporaryResultSet(Vector < ColumnNode > * columns, Runtime* runtime): ResultSet(runtime)
    {
        columnArray = columns;
    }

    Iterator < Field > * TemporaryResultSet::fields()
    {
        return new(allocator)ColumnIterator(allocator, columnArray);
    }

    int TemporaryResultSet::nFields()
    {
        return columnArray->length;
    }

    ResultSet* TemporaryResultSet::clone()
    {
        return new(allocator)TemporaryResultSet(columnArray, runtime);
    }

    static const size_t primeNumbers[] =
    {
        1361,           /* 6 */
        2729,           /* 7 */
        5471,           /* 8 */
        10949,          /* 9 */
        21911,          /* 10 */
        43853,          /* 11 */
        87719,          /* 12 */
        175447,         /* 13 */
        350899,         /* 14 */
        701819,         /* 15 */
        1403641,        /* 16 */
        2807303,        /* 17 */
        5614657,        /* 18 */
        11229331,       /* 19 */
        22458671,       /* 20 */
        44917381,       /* 21 */
        89834777,       /* 22 */
        179669557,      /* 23 */
        359339171,      /* 24 */
        718678369,      /* 25 */
        1437356741,     /* 26 */
        2147483647,     /* 27 */
        4294967291U     /* 28 */
    };

    HashResultSet::HashResultSet(Vector < ColumnNode > * columns, Runtime* runtime, GroupNode* agg, OrderNode* grp)
    :  TemporaryResultSet(columns, runtime)
    {
        int nGroupByKeys = 0;
        aggregates = agg;
        groupBy = grp;
        while (grp != NULL) {
            nGroupByKeys += 1;
            grp = grp->next;
        }
        hashKeyShift = nGroupByKeys > 0 ? 64 / nGroupByKeys : 0;

        sizeLog = (nGroupByKeys > 5) ? 5 : (nGroupByKeys != 0) ? (nGroupByKeys-1) : 0;
        size = 0;
        hashTable = Vector<Tuple>::create(primeNumbers[sizeLog], &hashAllocator);
        memset(hashTable->items, 0, primeNumbers[sizeLog]*sizeof(Tuple*));

        runtime->linkDependentDataSource(this);
        allocator = &hashAllocator;
    }

    bool HashResultSet::isHash()
    {
        return true;
    }

    bool HashResultSet::add(Record* rec)
    {
        uint64_t hash = 0;
        OrderNode* g;
        Tuple* tuple = (Tuple*)rec;
        size_t shift = hashKeyShift;
        for (g = groupBy; g != NULL; g = g->next) {
            hash = ((hash >> shift) | (hash << (64 - shift))) ^ tuple->values[g->columnNo]->hashCode();
        }
        size_t h = hash % hashTable->length;
        for (Tuple* t = hashTable->items[h]; t != NULL; t = (Tuple*)t->next) {
            for (g = groupBy; g != NULL && tuple->values[g->columnNo]->compare(t->values[g->columnNo]) == 0; g = g->next);
            if (g == NULL) { // match
                for (GroupNode* agg = aggregates; agg != NULL; agg = agg->next) {
                    agg->accumulate(t, tuple, &hashAllocator);
                }
                return false;
            }
        }
        tuple = (Tuple*)tuple->clone(&hashAllocator);
        tuple->next = hashTable->items[h];
        hashTable->items[h] = tuple;
        for (GroupNode* agg = aggregates; agg != NULL; agg = agg->next) {
            agg->init(tuple, &hashAllocator);
        }
        if (++size > hashTable->length) {
            size_t newHashTableSize = primeNumbers[++sizeLog];
            Vector<Tuple>* newHashTable = Vector<Tuple>::create(newHashTableSize, &hashAllocator);
            memset(newHashTable->items, 0, newHashTableSize*sizeof(Tuple*));
            for (int i = 0, n = hashTable->length; i < n; i++) {
                Tuple* next;
                for (Tuple* t = hashTable->items[i]; t != NULL; t = next) {
                    next = (Tuple*)t->next;
                    hash = 0;
                    for (g = groupBy; g != NULL; g = g->next) {
                        hash = ((hash >> shift) | (hash << (64 - shift))) ^ t->values[g->columnNo]->hashCode();
                    }
                    h = hash % newHashTableSize;
                    t->next = newHashTable->items[h];
                    newHashTable->items[h] = t;
                }
            }
            hashTable = newHashTable;
        }
        return false;
    }

    void HashResultSet::release()
    {
        runtime->unlinkDependentDataSource(this);
        hashAllocator.release();
        ResultSet::release();
    }

    void HashResultSet::toArray()
    {
        if (recordArray == NULL)
        {
            recordArray = Vector<Record>::create(size);
            Record** items = recordArray->items;
            Record *t = NULL, **tpp = &first;
            for (int i = 0, n = hashTable->length; i < n; i++) {
                for (t = hashTable->items[i]; t != NULL; t = t->next) {
                    *tpp = t;
                    tpp = &t->next;
                    *items++ = t;
                }
            }
            assert(items - recordArray->items == size);
            *tpp = NULL;
            last = t;
        }
    }

    Iterator < Field > * TableResultSet::fields()
    {
        AllocatorContext ctx(allocator);
        return table->fields();
    }

    Field* TableResultSet::findField(String* fieldName)
    {
        AllocatorContext ctx(allocator);
        return table->findField(fieldName);
    }

    int TableResultSet::nFields()
    {
        return table->nFields();
    }

    TableResultSet::TableResultSet(TableDescriptor* table, Runtime* runtime): ResultSet(runtime)
    {
        this->table = table;
    }

    bool TableResultSet::isRIDAvailable()
    {
        return table->isRIDAvailable();
    }

    Reference* TableResultSet::getRID(Record* rec)
    {
        AllocatorContext ctx(allocator);
        return table->getRID(rec);
    }

    int TableResultSet::compareRID(Record* r1, Record* r2)
    {
        return table->compareRID(r1, r2);
    }

    Table* TableResultSet::sourceTable()
    {
        return table;
    }

    void TableResultSet::extract(Record* rec, void* dst, size_t size, bool nullIndicators[], ExtractMode mode)
    {
        table->extract(rec, dst, size, nullIndicators, mode);
    }

    ResultSet* TableResultSet::clone()
    {
        return new(allocator)TableResultSet(table, runtime);
    }

    void InternalCursor::release()
    {
        iterator->release();
    }

    bool InternalCursor::hasNext()
    {
        if (curr == NULL)
        {
            FilterDataSource* src = source;
            while (iterator->hasNext())
            {
                size_t mark = allocator->mark();
                curr = iterator->next();
                src->select->setCurrentRecord(src->runtime, src->tableNo, curr);
                if (src->condition->evaluate(src->runtime)->isTrue())
                {
                    return true;
                }
                allocator->reset(mark);
            }
            curr = NULL;
            return false;
        }
        return true;
    }

    Record* InternalCursor::next()
    {
        if (!hasNext())
        {
            MCO_THROW NoMoreElements();
        }
        Record* rec = curr;
        curr = NULL;
        return rec;
    }

    InternalCursor::InternalCursor(FilterDataSource* src)
    {
        iterator = src->source->records(src->runtime->trans);
        source = src;
        curr = NULL;
    }

    FilterIterator::FilterIterator(FilterDataSource* source): InternalCursor(source)
    {
        segment = allocator->currentSegment();
        mark = allocator->mark();
    }

    bool FilterIterator::hasNext()
    {
        if (curr == NULL)
        {
            AllocatorContext ctx(allocator);
            allocator->reset(mark, segment);
            FilterDataSource* src = source;
            while (iterator->hasNext())
            {
                curr = iterator->next();
                if (src->condition == NULL)
                {
                    return true;
                }
                src->select->setCurrentRecord(src->runtime, src->tableNo, curr);
                if (src->condition->evaluate(src->runtime)->isTrue())
                {
                    return true;
                }
                allocator->reset(mark, segment);
            }
            curr = NULL;
            return false;
        }
        return true;
    }

    Table* FilterDataSource::sourceTable()
    {
        return table;
    }

    void FilterDataSource::extract(Record* rec, void* dst, size_t size, bool nullIndicators[], ExtractMode mode)
    {
        source->extract(rec, dst, size, nullIndicators, mode);
    }

    SortOrder FilterDataSource::sortOrder()
    {
        return source->sortOrder();
    }

    bool FilterDataSource::invert(Transaction* t) 
    {
        return source->invert(t);
    }

    Iterator < Field > * FilterDataSource::fields()
    {
        AllocatorContext ctx(allocator);
        return table->fields();
    }

    Field* FilterDataSource::findField(String* fieldName)
    {
        AllocatorContext ctx(allocator);
        return table->findField(fieldName);
    }

    int FilterDataSource::nFields()
    {
        return table->nFields();
    }

    size_t FilterDataSource::nRecords(Transaction* trans)
    {
        if (condition != NULL)
        {
            MCO_THROW InvalidOperation("FilterDataSource::nRecords");
        }
        return source->nRecords(trans);
    }

    bool FilterDataSource::isNumberOfRecordsKnown()
    {
        return condition != NULL ? false : source->isNumberOfRecordsKnown();
    }

    bool FilterDataSource::isRIDAvailable()
    {
        return source->isRIDAvailable();
    }

    Reference* FilterDataSource::getRID(Record* rec)
    {
        return source->getRID(rec);
    }

    int FilterDataSource::compareRID(Record* r1, Record* r2)
    {
        return source->compareRID(r1, r2);
    }

    Cursor* FilterDataSource::records(Transaction*)
    {
        return records();
    }

    Transaction* FilterDataSource::currentTransaction()
    {
        return runtime->trans;
    }

    Cursor* FilterDataSource::records()
    {
        return new(allocator)FilterIterator(this);
    }

    Cursor* FilterDataSource::internalCursor(Transaction* trans)
    {
        if (condition != NULL)
        {
            return new(allocator)InternalCursor(this);
        }
        return source->internalCursor(trans);
    }

    void FilterDataSource::release()
    {
        source->release();
        runtime->release();
    }

    void FilterDataSource::setMark(size_t mark)
    {
        runtime->mark = mark;
    }

    //
    // IndexMergeDataSource
    //
    IndexMergeDataSource::IndexMergeCursor::IndexMergeCursor(Transaction* trans,IndexMergeDataSource* source)
    {
        this->source = source;
        keyIterator = source->alternatives->type() == tpDataSource
            ? new ScalarSetIterator(trans, ((DataSource*)source->alternatives))
            : ((Array*)source->alternatives)->iterator();
        valueIterator = NULL;
        curr = NULL;
    }
    
    Record* IndexMergeDataSource::IndexMergeCursor::next()
    {
        if (!hasNext())
        {
            MCO_THROW NoMoreElements();
        }
        Record* rec = curr;
        curr = NULL;
        return rec;
    }

    bool  IndexMergeDataSource::IndexMergeCursor::hasNext()
    {
        if (curr == NULL)
        {
            IndexMergeDataSource* src = source;
            while (true) {
                while (valueIterator == NULL || !valueIterator->hasNext()) {
                    if (!keyIterator->hasNext()) {
                        return false;
                    }
                    {
                        AllocatorContext ctx(&source->mergeAllocator);
                        source->keys[source->nKeys-1].lowBound = source->keys[source->nKeys-1].highBound = keyIterator->next();
                        valueIterator = src->index->select(src->runtime->trans, Index::opEQ, source->nKeys, source->keys)->records(src->runtime->trans);
                    }
                }
                size_t mark = allocator->mark();
                Record* rec = valueIterator->next();
                if (src->filter != NULL) {
                    src->select->setCurrentRecord(src->runtime, src->tableNo, rec);
                    if (!src->filter->evaluate(src->runtime)->isTrue())
                    {
                        allocator->reset(mark);
                        continue;
                    }
                }
                curr = rec;
                break;
            }
        }
        return true;
    }


    IndexMergeDataSource::IndexMergeDataSource(SelectNode* select, IndexDescriptor* index, int tableNo, int nKeys, Range* keys, Value* alternatives, ExprNode* filter, Runtime* runtime)
    {
        this->index = index;
        this->table = (TableDescriptor*)index->table();
        this->select = select;
        this->tableNo = tableNo;
        this->alternatives = alternatives;
        this->filter = filter;
        this->runtime = runtime;
        this->nKeys = nKeys;
        memcpy(this->keys, keys, (nKeys-1)*sizeof(Range));
        runtime->linkDependentDataSource(this);
        allocator = &mergeAllocator;
    }

    Table* IndexMergeDataSource::sourceTable()
    {
        return table;
    }


    Iterator<Field>* IndexMergeDataSource::fields()
    {
        return table->fields();
    }

    Field* IndexMergeDataSource::findField(String* fieldName)
    {
        return table->findField(fieldName);
    }

    int IndexMergeDataSource::nFields()
    {
        return table->nFields();
    }

    size_t IndexMergeDataSource::nRecords(Transaction* trans)
    {
        MCO_THROW InvalidOperation("IndexMergeDataSource::nRecords");
    }

    bool IndexMergeDataSource::isNumberOfRecordsKnown()
    {
        return false;
    }

    bool IndexMergeDataSource::isRIDAvailable()
    {
        return table->isRIDAvailable();
    }

    Reference* IndexMergeDataSource::getRID(Record* rec)
    {
        return table->getRID(rec);
    }

    int IndexMergeDataSource::compareRID(Record* r1, Record* r2)
    {
        return table->compareRID(r1, r2);
    }

    Cursor* IndexMergeDataSource::records(Transaction* trans)
    {
        return new IndexMergeCursor(trans, this);
    }

    Transaction* IndexMergeDataSource::currentTransaction()
    {
        return runtime->trans;
    }

    Cursor* IndexMergeDataSource::records()
    {
        return records(runtime->trans);
    }

    Cursor* IndexMergeDataSource::internalCursor(Transaction* trans)
    {
        return records(trans);
    }

    void IndexMergeDataSource::release()
    {
        runtime->unlinkDependentDataSource(this);
        mergeAllocator.release();
        runtime->release();
    }

    void IndexMergeDataSource::setMark(size_t mark)
    {
        runtime->mark = mark;
    }

    //
    // MergeDataSource
    //
    MergeDataSource::MergeCursor::MergeCursor(MergeDataSource* source)
    {
        currCursor = leftCursor = source->left->internalCursor(source->runtime->trans);
        rightCursor = source->right->internalCursor(source->runtime->trans);
    }

    Record* MergeDataSource::MergeCursor::next()
    {
        if (!hasNext())
        {
            MCO_THROW NoMoreElements();
        }
        return currCursor->next();
    }

    bool  MergeDataSource::MergeCursor::hasNext()
    {
        while (!currCursor->hasNext()) {
            if (currCursor == leftCursor) {
                currCursor = rightCursor;
            } else {
                return false;
            }
        }
        return true;
    }

    MergeDataSource::MergeDataSource(Runtime* runtime, DataSource* left, DataSource* right)
    {
        this->runtime = runtime;
        this->left = left;
        this->right = right;
    }

    Table* MergeDataSource::sourceTable()
    {
        return left->sourceTable();
    }


    Iterator<Field>* MergeDataSource::fields()
    {
        return left->fields();
    }

    Field* MergeDataSource::findField(String* fieldName)
    {
        Field* f = left->findField(fieldName);
        if (f == NULL) {
            f = right->findField(fieldName);
        }
        return f;
    }

    int MergeDataSource::nFields()
    {
        return left->nFields();
    }

    size_t MergeDataSource::nRecords(Transaction* trans)
    {
        return left->nRecords() + right->nRecords();
    }

    bool MergeDataSource::isNumberOfRecordsKnown()
    {
        return left->isNumberOfRecordsKnown() && right->isNumberOfRecordsKnown();
    }

    bool MergeDataSource::isRIDAvailable()
    {
        return left->isRIDAvailable() && right->isRIDAvailable();
    }

    Reference* MergeDataSource::getRID(Record* rec)
    {
        return left->getRID(rec);
    }

    int MergeDataSource::compareRID(Record* r1, Record* r2)
    {
        return left->compareRID(r1, r2);
    }

    Cursor* MergeDataSource::records(Transaction*)
    {
        return records();
    }

    Transaction* MergeDataSource::currentTransaction()
    {
        return runtime->trans;
    }

    Cursor* MergeDataSource::records()
    {
        return new MergeCursor(this);
    }

    Cursor* MergeDataSource::internalCursor(Transaction* trans)
    {
        return records(trans);
    }

    void MergeDataSource::release()
    {
        left->release();
        right->release();
    }

    void MergeDataSource::setMark(size_t mark)
    {
        runtime->mark = mark;
    }

    //
    // Projection record
    //

    int ProjectionRecord::nComponents()
    {
        return columns->length;
    }

    Value* ProjectionRecord::get(int index)
    {
        AllocatorContext ctx(allocator);
        ColumnNode* column = columns->getAt(index);
        if (column->field != NULL) {
            return column->field->get(rec);
        }
        if (!columnsInitialized) {
            ds->select->setCurrentRecord(ds->runtime, 1, rec);
            columnsInitialized = true;
        }
        return column->evaluate(ds->runtime);
        //return column->value;
    }

    Value* ProjectionRecord::update(int index)
    {
        AllocatorContext ctx(allocator);
        return columns->getAt(index)->field->update(rec);
    }

    void ProjectionRecord::set(int index, Value* value)
    {
        columns->getAt(index)->field->set(rec, value);
    }

    void ProjectionRecord::deleteRecord()
    {
        rec->deleteRecord();
    }

    void ProjectionRecord::updateRecord()
    {
        rec->updateRecord();
    }

    Struct* ProjectionRecord::source()
    {
        return rec->source();
    }

    void* ProjectionRecord::pointer()
    {
        return rec->pointer();
    }

    bool ProjectionCursor::hasNext()
    {
        return cursor->hasNext();
    }

    void ProjectionCursor::release()
    {
        cursor->release();
    }

    Record* ProjectionCursor::next()
    {
        Record* rec = cursor->next();
        return new(allocator)ProjectionRecord(recordAllocator, columns, rec, ds);
    }

    SortOrder ProjectionDataSource::sortOrder()
    {
        return source->sortOrder();
    }

    bool ProjectionDataSource::invert(Transaction* t) 
    {
        return source->invert(t);
    }

    Table* ProjectionDataSource::sourceTable()
    {
        return source->sourceTable();
    }

    Iterator < Field > * ProjectionDataSource::fields()
    {
        return new(allocator)ColumnIterator(allocator, columns);
    }

    int ProjectionDataSource::nFields()
    {
        return columns->length;
    }

    size_t ProjectionDataSource::nRecords(Transaction* trans)
    {
        return source->nRecords(trans);
    }

    bool ProjectionDataSource::isNumberOfRecordsKnown()
    {
        return source->isNumberOfRecordsKnown();
    }

    bool ProjectionDataSource::isRIDAvailable()
    {
        return source->isRIDAvailable();
    }

    Reference* ProjectionDataSource::getRID(Record* rec)
    {
        return source->getRID(rec);
    }

    int ProjectionDataSource::compareRID(Record* r1, Record* r2)
    {
        return source->compareRID(r1, r2);
    }

    Cursor* ProjectionDataSource::records(Transaction*)
    {
        return records();
    }

    Transaction* ProjectionDataSource::currentTransaction()
    {
        return source->currentTransaction();
    }

    Cursor* ProjectionDataSource::records()
    {
        AllocatorContext ctx(allocator);
        ProjectionCursor* cursor = new(allocator)ProjectionCursor();
        cursor->init(columns, source->records(), this);
        return cursor;
    }

    Cursor* ProjectionDataSource::internalCursor(Transaction* trans)
    {
        ProjectionCursor* cursor = new(allocator)ProjectionCursor();
        cursor->init(columns, source->internalCursor(trans), this);
        return cursor;
    }

    void ProjectionDataSource::setMark(size_t mark)
    {
        runtime->mark = mark;
    }

    void ProjectionDataSource::release()
    {
        source->release();
    }


    void LimitCursor::init(Cursor* cursor, int start, int limit)
    {
        this->cursor = cursor;
        this->limit = limit;
        while (start > 0 && cursor->hasNext())
        {
            start -= 1;
            cursor->next();
        }
    }

    bool LimitCursor::hasNext()
    {
        return limit > 0 && cursor->hasNext();
    }

    Record* LimitCursor::next()
    {
        if (limit <= 0)
        {
            MCO_THROW NoMoreElements();
        }
        limit -= 1;
        return cursor->next();
    }

    void LimitCursor::release()
    {
        cursor->release();
    }

    Table* LimitDataSource::sourceTable()
    {
        return source->sourceTable();
    }

    Iterator < Field > * LimitDataSource::fields()
    {
        AllocatorContext ctx(allocator);
        return source->fields();
    }

    int LimitDataSource::nFields()
    {
        return source->nFields();
    }

    size_t LimitDataSource::nRecords(Transaction* trans)
    {
        size_t n = source->nRecords(trans);
        return (n <= start) ? 0 : n - start > limit ? limit : n - start;
    }

    bool LimitDataSource::isNumberOfRecordsKnown()
    {
        return source->isNumberOfRecordsKnown();
    }

    bool LimitDataSource::isRIDAvailable()
    {
        return source->isRIDAvailable();
    }

    Reference* LimitDataSource::getRID(Record* rec)
    {
        return source->getRID(rec);
    }

    int LimitDataSource::compareRID(Record* r1, Record* r2)
    {
        return source->compareRID(r1, r2);
    }

    Cursor* LimitDataSource::records(Transaction*)
    {
        return records();
    }

    Transaction* LimitDataSource::currentTransaction()
    {
        return source->currentTransaction();
    }

    Cursor* LimitDataSource::records()
    {
        AllocatorContext ctx(allocator);
        LimitCursor* cursor = new(allocator)LimitCursor();
        cursor->init(source->records(), (int)start, (int)limit);
        return cursor;
    }

    Cursor* LimitDataSource::internalCursor(Transaction* trans)
    {
        LimitCursor* cursor = new(allocator)LimitCursor();
        cursor->init(source->internalCursor(trans), (int)start, (int)limit);
        return cursor;
    }

    void LimitDataSource::release()
    {
        source->release();
    }

    void LimitDataSource::setMark(size_t mark)
    {
        runtime->mark = mark;
    }

    //
    // Distrinct data source

    Iterator < Field > * DistinctDataSource::fields() {
        return new(allocator)ColumnIterator(allocator, select->resultColumns);
    }

    int DistinctDataSource::nFields() {
        return 1;
    }

    Field* DistinctDataSource::findField(String* fieldName) {
        return select->resultColumns->items[0]->name->equals(fieldName) ? select->resultColumns->items[0]->field : NULL;
    }

    size_t DistinctDataSource::nRecords(Transaction* trans) {
        MCO_THROW InvalidOperation("DistinctDataSource::nRecords");
    }

    bool DistinctDataSource::isNumberOfRecordsKnown() {
        return false;
    }

    bool DistinctDataSource::isRIDAvailable() {
        return false;
    }

    int DistinctDataSource::compareRID(Record* r1, Record* r2) {
        MCO_THROW InvalidOperation("DistinctDataSource::compareRID");
    }

    Reference* DistinctDataSource::getRID(Record* rec) {
        MCO_THROW InvalidOperation("DistinctDataSource::getRID");
    }

    Cursor* DistinctDataSource::internalCursor(Transaction* trans)
    {
        return records(trans);
    }

    Cursor* DistinctDataSource::records()
    {
        return new(allocator)DistinctIterator(this);
    }

    Cursor* DistinctDataSource::records(Transaction* trans)
    {
        return records();
    }

    void DistinctDataSource::release()
    {
        runtime->release();
    }

    void DistinctDataSource::setMark(size_t mark)
    {
        runtime->mark = mark;
    }

    Table* DistinctDataSource::sourceTable()
    {
        return index->_table;
    }

    Transaction* DistinctDataSource::currentTransaction()
    {
        return runtime->trans;
    }



    DistinctIterator::DistinctIterator(DistinctDataSource* src)
    {
        source = src;
        segment = allocator->currentSegment();
        mark = allocator->mark();
        prev = curr = NULL;
    }

    bool DistinctIterator::hasNext()
    {
        if (curr == NULL) {
            AllocatorContext ctx(allocator);
            DataSource* ds;
            if (prev != NULL) {
                char buf[1024];
                Value* prevKey = source->index->_keys->_field->get(prev);
                if (prevKey->serialize(buf, sizeof(buf)) != 0) {
                    size_t pos = 0;
                    // value saved in buffer: safe to release stack allocator
                    allocator->reset(mark, segment);
                    prevKey = Value::deserialize(buf, pos);
                }
                Range range;
                range.isLowInclusive = range.isHighInclusive = false;
                if (source->index->_keys->_order == ASCENT_ORDER) {
                    range.lowBound = prevKey;
                    ds = source->index->select(source->runtime->trans, Index::opGT, 1, &range);
                } else {
                    range.highBound = prevKey;
                    ds = source->index->select(source->runtime->trans, Index::opLT, 1, &range);
                }
            } else {
                ds = source->index->select(source->runtime->trans, Index::opNop, 0, NULL);
            }
            Cursor* cursor = ds->records(source->runtime->trans);
            curr = cursor->hasNext() ? cursor->next() : NULL;
        }
        return curr != NULL;
    }

    Record* DistinctIterator::next()
    {
        if (!hasNext())
        {
            MCO_THROW NoMoreElements();
        }
        prev = curr;
        curr = NULL;
        return prev;
    }

#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT

    int FlattenedRecord::nComponents()
    {
        return columns->length;
    }

    Value* FlattenedRecord::get(int index)
    {
        AllocatorContext ctx(allocator);
        if (sequences->items[index]) {
            Value* seqElem = nextSeqElems->items[index];
            return seqElem->type() == tpReal || seqElem->type() == tpString ? seqElem : new RealValue(seqElem->realValue());
        } else {
            return rec->get(index);
        }
    }

    Value* FlattenedRecord::update(int index)
    {
        return rec->update(index);
    }

    void FlattenedRecord::set(int index, Value* value)
    {
        rec->set(index, value);
    }

    void FlattenedRecord::deleteRecord()
    {
        rec->deleteRecord();
    }

    void FlattenedRecord::updateRecord()
    {
        rec->updateRecord();
    }

    Struct* FlattenedRecord::source()
    {
        return rec->source();
    }

    void* FlattenedRecord::pointer()
    {
        return rec->pointer();
    }

    bool FlattenedRecord::moveNext()
    {
        AllocatorContext ctx(allocator);
        bool hasNextSeqElem = true;
        size_t nColumns = sequences->length;
        for (size_t i = 0; i < nColumns; i++) {
            if (sequences->items[i]) {
                Value* v = sequences->items[i]->next();
                if (v == NULL) {
                    hasNextSeqElem = false;
                }
                nextSeqElems->items[i] = v;
            }
        }
        return hasNextSeqElem;
    }


    FlattenedRecord::FlattenedRecord(AbstractAllocator* allocator, Vector < ColumnNode > * columns, Record* rec)
    {
        AllocatorContext ctx(allocator);
        size_t nColumns = columns->length;
        this->allocator = allocator;
        this->columns = columns;
        this->rec = rec;
        sequences = Vector<McoGenericSequence>::create(nColumns);
        nextSeqElems = Vector<Value>::create(nColumns);
        for (size_t i = 0; i < nColumns; i++) {
            if (columns->items[i]->flattened) {
                sequences->items[i] = (McoGenericSequence*)rec->get(i);
                sequences->items[i]->getIterator();
            } else {
                sequences->items[i] = NULL;
            }
        }
    }

    void FlattenedCursor::init(Vector < ColumnNode > * columns, Cursor* cursor)
    {
        this->columns = columns;
        this->cursor = cursor;
        hasNextElem = false;
        currRecord = NULL;
        segment = allocator->currentSegment();
        mark = allocator->mark();
    }

    bool FlattenedCursor::hasNext()
    {
        if (hasNextElem) {
            return true;
        }
        if (currRecord != NULL) {
            if (currRecord->moveNext()) {
                hasNextElem = true;
                return true;
            }
        }
        do {
            currRecord = NULL;
            if (!cursor->hasNext()) {
                return false;
            }
            currRecord = new(allocator)FlattenedRecord(allocator, columns, cursor->next());
        } while (!currRecord->moveNext());

        hasNextElem = true;
        return true;
    }

    void FlattenedCursor::release()
    {
        cursor->release();
    }

    Record* FlattenedCursor::next()
    {
        if (!hasNext()) {
            MCO_THROW NoMoreElements();
        }
        hasNextElem = false;
        return currRecord;
    }

    Table* FlattenedDataSource::sourceTable()
    {
        return source->sourceTable();
    }

    Iterator < Field > * FlattenedDataSource::fields()
    {
        return source->fields();
    }

    int FlattenedDataSource::nFields()
    {
        return source->nFields();
    }

    size_t FlattenedDataSource::nRecords(Transaction* trans)
    {
        MCO_THROW InvalidOperation("FlattenedDataSource::nRecords");
    }

    bool FlattenedDataSource::isNumberOfRecordsKnown()
    {
        return false;
    }

    bool FlattenedDataSource::isRIDAvailable()
    {
        return source->isRIDAvailable();
    }

    Reference* FlattenedDataSource::getRID(Record* rec)
    {
        return source->getRID(rec);
    }

    int FlattenedDataSource::compareRID(Record* r1, Record* r2)
    {
        return source->compareRID(r1, r2);
    }

    Cursor* FlattenedDataSource::records(Transaction*)
    {
        return records();
    }

    Transaction* FlattenedDataSource::currentTransaction()
    {
        return source->currentTransaction();
    }

    Cursor* FlattenedDataSource::records()
    {
        AllocatorContext ctx(allocator);
        FlattenedCursor* cursor = new(allocator)FlattenedCursor();
        cursor->init(columns, source->records());
        return cursor;
    }

    Cursor* FlattenedDataSource::internalCursor(Transaction* trans)
    {
        FlattenedCursor* cursor = new(allocator)FlattenedCursor();
        cursor->init(columns, source->internalCursor(trans));
        return cursor;
    }

    void FlattenedDataSource::release()
    {
        source->release();
    }

    void FlattenedDataSource::setMark(size_t mark)
    {
        runtime->mark = mark;
    }

#endif
}
