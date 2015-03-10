/*******************************************************************
 *                                                                 *
 *  scheme.cpp                                                      *
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
 * MODULE:    scheme.cpp
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#include "scheme.h"
#include "cursor.h"
#include "memmgr.h"

namespace McoSql
{
    const size_t MAX_TABLE_NAME_LEN = 256;
    const size_t MAX_TABLE_VERSION_LEN = 8;
    const size_t MAX_CSV_LINE_LEN = 65536;


    Iterator < Table > * DatabaseDescriptor::tables()
    {
        return (Iterator < Table > *)new ListIterator < TableDescriptor > (_tables);
    }

    Table* DatabaseDescriptor::findTable(String* name)
    {
        for (TableDescriptor* td = _tables; td != NULL; td = td->next)
        {
            if (name->equals(td->_name))
            {
                return td;
            }
        }
        return NULL;
    }

    Table* DatabaseDescriptor::createTable(Transaction* trans, Table* table)
    {
        TableDescriptor* newTable = (TableDescriptor*)table;
        renameOldTable(newTable);
        newTable = new TableDescriptor(this, impl->createTable(trans, newTable));
        updateMetadata(newTable);
        _schemaVersion = impl->getSchemaVersion();

        *_tablesTail = newTable;
        _tablesTail = &newTable->next;

        return newTable;
    }

    Index* DatabaseDescriptor::createIndex(Transaction* trans, Index* index)
    {
        Index* indexImpl = impl->createIndex(trans, index);
        TableDescriptor* table = (TableDescriptor*)index->table();
        IndexDescriptor* indexDesc = new IndexDescriptor(table, indexImpl);

        int indexNo = 0;
        int keyNo = 0;
        TableDescriptor* metatable = (TableDescriptor*)_tables;
        TableDescriptor* metaindex = (TableDescriptor*)metatable->next;
        TableDescriptor* statistic = (TableDescriptor*)metaindex->next;
        TableDescriptor* indexStatistic = (TableDescriptor*)statistic->next;
        TemporaryResultSet* metaindexRows = (TemporaryResultSet*)metaindex->impl;
        TemporaryResultSet* indexStatisticRows = (TemporaryResultSet*)indexStatistic->impl;
        TableDescriptor* td = indexStatistic;
        IndexDescriptor* prev = NULL;
        do {
            td = td->next;
            prev = NULL;
            for (IndexDescriptor* id = td->_indices; id != NULL; prev = id, id = id->next, indexNo++) {
                for (KeyDescriptor* key = id->_keys; key != NULL; keyNo++, key = key->next);
            }
        } while (td != table);


        indexDesc->next = NULL;
        if (prev != NULL) {
            prev->next = indexDesc;
        } else {
            table->_indices = indexDesc;
        }

        Tuple* indexStatTuple = Tuple::create(6);
        indexStatTuple->values[0] = table->_name;
        indexStatTuple->values[1] = indexDesc->_name;
        indexStatTuple->values[2] = new IntValue(0);
        indexStatTuple->values[3] = new IntValue(0);
        indexStatTuple->values[4] = new IntValue(0);
        indexStatTuple->values[5] = new IntValue(0);
        indexStatisticRows->add(indexNo, indexStatTuple);

        int keyPos = 1;
        for (KeyDescriptor* key = indexDesc->_keys; key != NULL; keyPos++, key = key->next)
        {
            Tuple* tuple = Tuple::create(7);
            tuple->values[0] = table->_name;
            tuple->values[1] = indexDesc->_name;
            tuple->values[2] = key->_field->_name;
            tuple->values[3] = new IntValue(keyPos);
            tuple->values[4] = BoolValue::create(key->_order != DESCENT_ORDER);
            tuple->values[5] = BoolValue::create(!indexDesc->_unique);
            tuple->values[6] = BoolValue::create(indexDesc->_ordered);
            metaindexRows->add(keyNo++, tuple);
        }
        _schemaVersion = impl->getSchemaVersion();
        return indexDesc;
    }

    void DatabaseDescriptor::close()
    {
        impl->close();
    }

    bool DatabaseDescriptor::checkSchema()
    {
        if (impl->getSchemaVersion() != _schemaVersion) { 
            loadSchema();
            return true;
        }
        return false;
    }        

    Transaction* DatabaseDescriptor::beginTransaction(Transaction::Mode mode, int priority, Transaction::IsolationLevel level)
    {
        NewMemorySegment mem;
        Transaction* t = impl->beginTransaction(mode, priority, level);
        t->mark = mem.mark;
        t->segmentId = mem.segmentId;
        mem.detach();
        return t;
    }

    Reference* DatabaseDescriptor::createReference(int64_t id)
    {
        return impl->createReference(id);
    }


    void DatabaseDescriptor::removeTable(TableDescriptor* table)
    {
        TableDescriptor** tpp, *tp;
        for (tpp = &_tables; (tp = *tpp) != table; tpp = &tp->next);
        *tpp = tp->next;
        if (tp->next == NULL) { 
            _tablesTail = tpp;
        }
    }

    DatabaseDescriptor::DatabaseDescriptor(Database* db, DatabaseDescriptor* desc)
    {
        impl = db;
        _tables = desc->_tables;
        _tablesTail = desc->_tablesTail;
        _schemaVersion = desc->_schemaVersion;
    }

    void DatabaseDescriptor::updateMetadata(TableDescriptor* table)
    {
        TableDescriptor* metatable = (TableDescriptor*)_tables;
        TableDescriptor* metaindex = (TableDescriptor*)metatable->next;
        TableDescriptor* statistic = (TableDescriptor*)metaindex->next;
        TableDescriptor* indexStatistic = (TableDescriptor*)statistic->next;

        TemporaryResultSet* metatableRows = (TemporaryResultSet*)metatable->impl;
        TemporaryResultSet* metaindexRows = (TemporaryResultSet*)metaindex->impl;
        TemporaryResultSet* statisticRows = (TemporaryResultSet*)statistic->impl;
        TemporaryResultSet* indexStatisticRows = (TemporaryResultSet*)indexStatistic->impl;

        String* tableName = table->_name;
        int nColumns = table->columns->length;
        FieldDescriptor** columns = table->columns->items;
        for (int i = 0; i < nColumns; i++)
        {
            Tuple* tuple = Tuple::create(13);
            tuple->values[0] = tableName;
            tuple->values[1] = new IntValue(i + 1);
            tuple->values[2] = columns[i]->_name;
            tuple->values[3] = String::create(typeMnemonic[columns[i]->_type]);
            tuple->values[4] = new IntValue((int)columns[i]->_type);
            tuple->values[5] = new IntValue(columns[i]->_fixedSize);
            tuple->values[6] = BoolValue::create(columns[i]->isAutoGenerated());
            tuple->values[7] = columns[i]->_referencedTableName == NULL ? (Value*) &Null: (Value*)columns[i]->_referencedTableName;
            tuple->values[8] = BoolValue::create(columns[i]->isNullable());
            tuple->values[9] = new IntValue(columns[i]->_precision);
            tuple->values[10] = new IntValue(columns[i]->_width);
            tuple->values[11] = new IntValue((int)columns[i]->_elemType);
            tuple->values[12] = String::create(typeMnemonic[columns[i]->_elemType]);
            metatableRows->add(tuple);
        }
        Tuple* statTuple = Tuple::create(6);
        statTuple->values[0] = tableName;
        statTuple->values[1] = new IntValue(0);
        statTuple->values[2] = new IntValue(0);
        statTuple->values[3] = new IntValue(0);
        statTuple->values[4] = new IntValue(0);
        statTuple->values[5] = new IntValue(0);
        statisticRows->add(statTuple);

        for (IndexDescriptor* index = table->_indices; index != NULL; index = index->next)
        {
            int keyPos = 1;
            for (KeyDescriptor* key = index->_keys; key != NULL; keyPos++, key = key->next)
            {
                Tuple* tuple = Tuple::create(7);
                tuple->values[0] = tableName;
                tuple->values[1] = index->_name;
                tuple->values[2] = key->_field->_name;
                tuple->values[3] = new IntValue(keyPos);
                tuple->values[4] = BoolValue::create(key->_order != DESCENT_ORDER);
                tuple->values[5] = BoolValue::create(!index->_unique);
                tuple->values[6] = BoolValue::create(index->_ordered);
                metaindexRows->add(tuple);
            }
            Tuple* indexStatTuple = Tuple::create(6);
            indexStatTuple->values[0] = tableName;
            indexStatTuple->values[1] = index->_name;
            indexStatTuple->values[2] = new IntValue(0);
            indexStatTuple->values[3] = new IntValue(0);
            indexStatTuple->values[4] = new IntValue(0);
            indexStatTuple->values[5] = new IntValue(0);
            indexStatisticRows->add(indexStatTuple);
        }
    }
    
    int DatabaseDescriptor::getSchemaVersion() 
    {
        return _schemaVersion;
    }

    DatabaseDescriptor::DatabaseDescriptor(Database* db)
    {
        impl = db;
        loadSchema();
    }
    
    void DatabaseDescriptor::loadSchema()
    {    
        int i;
        _schemaVersion = impl->getSchemaVersion();
        const int nMetatableColumns = 13;
        Vector < FieldDescriptor > * metatableFields = Vector < FieldDescriptor > ::create(nMetatableColumns);
        Vector < ColumnNode > * metatableColumns = Vector < ColumnNode > ::create(nMetatableColumns);
        TemporaryResultSet* metatableRows = new TemporaryResultSet(metatableColumns, NULL);
        SyntheticTableDescriptor* metatable = new SyntheticTableDescriptor(this, String::create("Metatable"),
            metatableFields, metatableRows);
        metatableColumns->items[0] = new ColumnNode(1, tpString, String::create("TableName"));
        metatableColumns->items[1] = new ColumnNode(2, tpInt2, String::create("FieldNo"));
        metatableColumns->items[2] = new ColumnNode(3, tpString, String::create("FieldName"));
        metatableColumns->items[3] = new ColumnNode(4, tpString, String::create("FieldTypeName"));
        metatableColumns->items[4] = new ColumnNode(5, tpInt4, String::create("FieldType"));
        metatableColumns->items[5] = new ColumnNode(6, tpInt4, String::create("FieldSize"));
        metatableColumns->items[6] = new ColumnNode(7, tpBool, String::create("AutoGenerated"));
        metatableColumns->items[7] = new ColumnNode(8, tpString, String::create("ReferencedTable"));
        metatableColumns->items[8] = new ColumnNode(9, tpBool, String::create("Nullable"));
        metatableColumns->items[9] = new ColumnNode(10, tpInt4, String::create("Precision"));
        metatableColumns->items[10] = new ColumnNode(11, tpInt4, String::create("Width"));
        metatableColumns->items[11] = new ColumnNode(12, tpInt4, String::create("ElementType"));
        metatableColumns->items[12] = new ColumnNode(13, tpString, String::create("ElementTypeName"));
        for (i = 0; i < nMetatableColumns; i++)
        {
            metatableColumns->items[i]->field = metatableFields->items[i] =
                new SyntheticFieldDescriptor(metatable,
                                             metatableColumns->items[i]->name, metatableColumns->items[i]->type, tpNull, i);
        }

        const int nMetaindexColumns = 7;
        Vector < FieldDescriptor > * metaindexFields = Vector < FieldDescriptor > ::create(nMetaindexColumns);
        Vector < ColumnNode > * metaindexColumns = Vector < ColumnNode > ::create(nMetaindexColumns);
        TemporaryResultSet* metaindexRows = new TemporaryResultSet(metaindexColumns, NULL);
        SyntheticTableDescriptor* metaindex = new SyntheticTableDescriptor(this, String::create("Metaindex"),
            metaindexFields, metaindexRows);
        metaindexColumns->items[0] = new ColumnNode(1, tpString, String::create("TableName"));
        metaindexColumns->items[1] = new ColumnNode(2, tpString, String::create("IndexName"));
        metaindexColumns->items[2] = new ColumnNode(3, tpString, String::create("FieldName"));
        metaindexColumns->items[3] = new ColumnNode(4, tpInt2, String::create("FieldPos"));
        metaindexColumns->items[4] = new ColumnNode(5, tpBool, String::create("Ascending"));
        metaindexColumns->items[5] = new ColumnNode(6, tpBool, String::create("NonUnique"));
        metaindexColumns->items[6] = new ColumnNode(7, tpBool, String::create("Ordered"));
        for ( i = 0; i < nMetaindexColumns; i++)
        {
            metaindexColumns->items[i]->field = metaindexFields->items[i] =
                new SyntheticFieldDescriptor(metaindex,
                                             metaindexColumns->items[i]->name, metaindexColumns->items[i]->type, tpNull, i);
        }

        const int nStatisticColumns = 6;
        Vector < FieldDescriptor > * statisticFields = Vector < FieldDescriptor > ::create(nStatisticColumns);
        Vector < ColumnNode > * statisticColumns = Vector < ColumnNode > ::create(nStatisticColumns);
        TemporaryResultSet* statisticRows = new TemporaryResultSet(statisticColumns, NULL);
        StatisticTableDescriptor* statistic = new StatisticTableDescriptor(this, String::create("Statistic"),
            statisticFields, statisticRows);
        statisticColumns->items[0] = new ColumnNode(1, tpString, String::create("TableName"));
        statisticColumns->items[1] = new ColumnNode(2, tpInt8, String::create("ObjectsNum"));
        statisticColumns->items[2] = new ColumnNode(3, tpInt8, String::create("VersionsNum"));
        statisticColumns->items[3] = new ColumnNode(4, tpInt8, String::create("CorePages"));
        statisticColumns->items[4] = new ColumnNode(5, tpInt8, String::create("BlobPages"));
        statisticColumns->items[5] = new ColumnNode(6, tpInt8, String::create("CoreSpace"));
        for (i = 0; i < nStatisticColumns; i++)
        {
            statisticColumns->items[i]->field = statisticFields->items[i] =
                new SyntheticFieldDescriptor(statistic,
                                             statisticColumns->items[i]->name, statisticColumns->items[i]->type, tpNull, i);
        }

        const int nIndexStatisticColumns = 6;
        Vector < FieldDescriptor > * indexStatisticFields = Vector < FieldDescriptor > ::create(nIndexStatisticColumns);
        Vector < ColumnNode > * indexStatisticColumns = Vector < ColumnNode > ::create(nIndexStatisticColumns);
        TemporaryResultSet* indexStatisticRows = new TemporaryResultSet(indexStatisticColumns, NULL);
        IndexStatisticTableDescriptor* indexStatistic = new IndexStatisticTableDescriptor(this, String::create("IndexStatistic"),
            indexStatisticFields, indexStatisticRows);
        indexStatisticColumns->items[0] = new ColumnNode(1, tpString, String::create("TableName"));
        indexStatisticColumns->items[1] = new ColumnNode(2, tpString, String::create("IndexName"));
        indexStatisticColumns->items[2] = new ColumnNode(3, tpInt8, String::create("KeysNum"));
        indexStatisticColumns->items[3] = new ColumnNode(4, tpInt8, String::create("PagesNum"));
        indexStatisticColumns->items[4] = new ColumnNode(5, tpInt8, String::create("AvgCmp"));
        indexStatisticColumns->items[5] = new ColumnNode(6, tpInt8, String::create("MaxCmp"));
        for (i = 0; i < nIndexStatisticColumns; i++)
        {
            indexStatisticColumns->items[i]->field = indexStatisticFields->items[i] =
                new SyntheticFieldDescriptor(indexStatistic,
                                             indexStatisticColumns->items[i]->name, indexStatisticColumns->items[i]->type, tpNull, i);
        }

        _tables = metatable;
        metatable->next = metaindex;
        metaindex->next = statistic;
        statistic->next = indexStatistic;

        TableDescriptor **tpp = &indexStatistic->next; 
        Iterator < Table > * tableIterator = impl->tables();
        while (tableIterator->hasNext())
        {
            TableDescriptor* table = new TableDescriptor(this, tableIterator->next());
            *tpp = NULL;
            renameOldTable(table);
            updateMetadata(table);
            *tpp = table;
            tpp = &table->next;
        }
        *tpp = NULL;
        _tablesTail = tpp;
        indexStatisticRows->toArray();
        statisticRows->toArray();
        metatableRows->toArray();
        metaindexRows->toArray();
    }

    void DatabaseDescriptor::renameOldTable(TableDescriptor* newTable) 
    {
        TableDescriptor* oldTable  = (TableDescriptor*)findTable(newTable->_name);
        if (oldTable != NULL) { // table with such name alreay exists: rename it to *$.N
            for (int i = 1; ; i++) {
                char buf[MAX_TABLE_NAME_LEN];
                int len = sprintf(buf, "%s$%d", newTable->_name->cstr(), i);
                String* version = String::create(buf);
                if (!findTable(version)) {
                    // Hack: replace table name in place to update table name in all metatables.
                    mco_memcpy(oldTable->_name, version, sizeof(StringLiteral)+len);
                    break;
                }
            }
        }    
    }

    SortOrder KeyDescriptor::order()
    {
        return _order;
    }

    Field* KeyDescriptor::field()
    {
        return _field;
    }


    String* IndexDescriptor::name()
    {
        return _name;
    }

    int IndexDescriptor::nKeys()
    {
        return _nKeys;
    }

    Iterator < Key > * IndexDescriptor::keys()
    {
        return (Iterator < Key > *)new ListIterator < KeyDescriptor > (_keys);
    }

    bool IndexDescriptor::isApplicable(SearchOperation cop, int nRanges, Range ranges[])
    {
        return impl->isApplicable(cop, nRanges, ranges);
    }

    bool IndexDescriptor::isOrdered()
    {
        return _ordered;
    }


    bool IndexDescriptor::isSpatial()
    {
        return _spatial;
    }

    bool IndexDescriptor::isTrigram()
    {
        return _trigram;
    }

    bool IndexDescriptor::isUnique()
    {
        return _unique;
    }

    DataSource* IndexDescriptor::select(Transaction* trans, SearchOperation cop, int nRanges, Range ranges[])
    {
        return impl->select(trans, cop, nRanges, ranges);
    }

    void IndexDescriptor::drop(Transaction* trans)
    {
        impl->drop(trans);
    }


    FieldDescriptor* IndexDescriptor::locateKey(FieldDescriptor* field, int &index)
    {
        if (field->_type == tpStruct)
        {
            for (FieldDescriptor* fd = field->_components; fd != NULL; fd = fd->next)
            {
                FieldDescriptor* key = locateKey(fd, index);
                if (key != NULL)
                {
                    return key;
                }
            }
        }
        else if (--index < 0)
        {
            return field;
        }
        return NULL;
    }

    FieldDescriptor* IndexDescriptor::getKey(int index)
    {
        for (KeyDescriptor* kd = _keys; kd != NULL; kd = kd->next)
        {
            FieldDescriptor* key = locateKey(kd->_field, index);
            if (key != NULL)
            {
                return key;
            }
        }
        return NULL;
    }

    Table* IndexDescriptor::table()
    {
        return _table;
    }

    IndexDescriptor::IndexDescriptor(TableDescriptor* table, String* name, KeyDescriptor* keys)
    {
        _name = name;
        _keys = keys;
        _table = table;
        _ordered = true;
        _unique = false;
        _spatial = false;
        _trigram = false;
        _nKeys = 0;
        while (keys != NULL) {
            _nKeys += 1;
            keys = keys->next;
        }
    }


    IndexDescriptor::IndexDescriptor(TableDescriptor* table, Index* index)
    {
        impl = index;
        _name = index->name();
        _table = table;
        _ordered = index->isOrdered();
        _spatial = index->isSpatial();
        _trigram = index->isTrigram();
        _unique = index->isUnique();
        Iterator < Key > * iterator = index->keys();
        KeyDescriptor** kpp = &_keys;
        _nKeys = 0;
        while (iterator->hasNext())
        {
            Key* key = iterator->next();
            FieldDescriptor* fd = table->findFieldDescriptor(key->field());
            assert(fd != NULL);
            KeyDescriptor* kp = new KeyDescriptor(fd, key->order());
            *kpp = kp;
            kpp = &kp->next;
            _nKeys += 1;
        }
        *kpp = NULL;
    }


    Constraint::ConstraintType ConstraintDescriptor::type()
    {
        return _type;
    }

    Table* ConstraintDescriptor::table()
    {
        return _table;
    }

    String* ConstraintDescriptor::name()
    {
        return _name;
    }

    Iterator < Field > * ConstraintDescriptor::fields()
    {
        return _fields->iterator();
    }

    ConstraintDescriptor::ConstraintDescriptor(TableDescriptor* table, String* name, ConstraintType type, Vector < Field > * fields,
                                               ConstraintDescriptor* chain, bool onDeleteCascade, String*
                                               primaryKeyTable, Vector < String > * primaryKey)
    {
        _type = type;
        _name = name ? name : fields->items[0]->name();
        _fields = fields;
        _table = table;
        next = chain;
        _onDeleteCascade = onDeleteCascade;
        _primaryKeyTable = primaryKeyTable;
        _primaryKey = primaryKey;
    }

    ConstraintDescriptor::ConstraintDescriptor(TableDescriptor* table, Constraint* constraint)
    {
        int i;
        Field* cf[MAX_KEYS];
        String* pk[MAX_KEYS];

        _type = constraint->type();
        _name = constraint->name();
        _table = table;
        Iterator < Field > * iterator = constraint->fields();
        for (i = 0; iterator->hasNext(); i++)
        {
            assert(i < MAX_KEYS);
            cf[i] = iterator->next();
        }
        _fields = Vector < Field > ::create(i, cf);
        if (_type == FOREIGN_KEY)
        {
            _onDeleteCascade = constraint->onDeleteCascade();
            _primaryKeyTable = constraint->primaryKeyTable();
            Iterator < String > * pkIterator = constraint->primaryKey();
            for (i = 0; pkIterator->hasNext(); i++)
            {
                assert(i < MAX_KEYS);
                pk[i] = pkIterator->next();
            }
            _primaryKey = Vector < String > ::create(i, pk);
        }
    }

    bool ConstraintDescriptor::onDeleteCascade()
    {
        if (_type != FOREIGN_KEY)
        {
            MCO_THROW InvalidOperation("ConstraintDescriptor::onDeleteCascade");
        }
        return _onDeleteCascade;
    }

    String* ConstraintDescriptor::primaryKeyTable()
    {
        if (_type != FOREIGN_KEY)
        {
            MCO_THROW InvalidOperation("ConstraintDescriptor::primaryKeyTable");
        }
        return _primaryKeyTable;
    }

    Iterator < String > * ConstraintDescriptor::primaryKey()
    {
        if (_type != FOREIGN_KEY)
        {
            MCO_THROW InvalidOperation("ConstraintDescriptor::primaryKey");
        }
        return _primaryKey != NULL ? _primaryKey->iterator(): NULL;
    }

    String* TableDescriptor::name()
    {
        return _name;
    }

    Iterator < Index > * TableDescriptor::indices()
    {
        return (Iterator < Index > *)new ListIterator < IndexDescriptor > (_indices);
    }

    void TableDescriptor::extract(Record* rec, void* dst, size_t size, bool nullIndicators[], ExtractMode mode)
    {
        impl->extract(rec, dst, size, nullIndicators, mode);
    }


    IndexDescriptor* TableDescriptor::findIndex(FieldDescriptor* fd, SortOrder order, IndexOperationKind kind)
    {
        IndexDescriptor* bestIndex = NULL;
        for (IndexDescriptor* index = _indices; index != NULL; index = index->next)
        {
            if (index->_keys->_field == fd
                && (kind == IDX_EXACT_MATCH || kind == IDX_UNIQUE_MATCH || index->_ordered)
                && (kind != IDX_UNIQUE_MATCH || index->_unique)
                && (bestIndex == NULL
                    || (kind == IDX_SUBSTRING && !bestIndex->_trigram && index->_trigram)
                    || (kind != IDX_SUBSTRING && bestIndex->_trigram && !index->_trigram)
                    || bestIndex->_nKeys > index->_nKeys
                    || (order != UNSPECIFIED_ORDER
                        && bestIndex->_keys->_order != order
                        && index->_keys->_order == order)))
            {
                bestIndex = index;
            }
        }
        return bestIndex;
    }

    FieldDescriptor* TableDescriptor::findFieldDescriptor(Field* field)
    {
        Field* scope = field->scope();
        String* name = field->name();
        if (scope != NULL)
        {
            FieldDescriptor* scopeDesc = findFieldDescriptor(scope);
            return (FieldDescriptor*)scopeDesc->findComponent(name);
        }
        else
        {
            return (FieldDescriptor*)findField(name);
        }
    }

    Field* TableDescriptor::findField(String* name)
    {
        int i = columns->length;
        FieldDescriptor** fields = columns->items;
        while (--i >= 0)
        {
            if (name->equals(fields[i]->_name))
            {
                return fields[i];
            }
        }
        return NULL;
    }

    void TableDescriptor::drop(Transaction* trans)
    {
        db->removeTable(this);
        impl->drop(trans);
    }

    void TableDescriptor::deleteAllRecords(Transaction* trans)
    {
        impl->deleteAllRecords(trans);
    }

    void TableDescriptor::deleteRecord(Transaction* trans, Record* rec)
    {
        assert(rec->source() != NULL);
        impl->deleteRecord(trans, (Record*)rec->source());
    }

    void TableDescriptor::updateRecord(Transaction* trans, Record* rec)
    {
        assert(rec->source() != NULL);
        impl->updateRecord(trans, (Record*)rec->source());
    }

    void TableDescriptor::checkpointRecord(Transaction* trans, Record* rec)
    {
        assert(rec->source() != NULL);
        impl->checkpointRecord(trans, (Record*)rec->source());
    }

    Iterator < Constraint > * TableDescriptor::constraints()
    {
        return (Iterator < Constraint > *)new ListIterator < ConstraintDescriptor > (_constraints);
    }

    Iterator < Field > * TableDescriptor::fields()
    {
        return (Iterator < Field > *)columns->iterator();
    }

    Cursor* TableDescriptor::records(Transaction* trans)
    {
        return impl->records(trans);
    }

    Cursor* TableDescriptor::internalCursor(Transaction* trans)
    {
        return impl->internalCursor(trans);
    }

    int TableDescriptor::nFields()
    {
        return columns->length;
    }

    void TableDescriptor::release()
    {
        impl->release();
    }

    bool TableDescriptor::isNumberOfRecordsKnown()
    {
        return impl->isNumberOfRecordsKnown();
    }

    size_t TableDescriptor::nRecords(Transaction* trans)
    {
        return impl->nRecords(trans);
    }

    bool TableDescriptor::isRIDAvailable()
    {
        return impl->isRIDAvailable();
    }

    Reference* TableDescriptor::getRID(Record* rec)
    {
        assert(rec->source() != NULL);
        return impl->getRID((Record*)rec->source());
    }

    int TableDescriptor::compareRID(Record* r1, Record* r2)
    {
        assert(r1->source() != NULL && r2->source() != NULL);
        return impl->compareRID((Record*)r1->source(), (Record*)r2->source());
    }

    Record* TableDescriptor::createRecord(Transaction* trans)
    {
        return impl->createRecord(trans);
    }

    bool TableDescriptor::temporary()
    {
        return _temporary;
    }

    TableDescriptor::TableDescriptor(DatabaseDescriptor* dd, String* name, bool temporary)
    {
        db = dd;
        _temporary = temporary;
        _name = new (name->size() + MAX_TABLE_VERSION_LEN) StringLiteral(name->body(), name->size()); // reserve space for version name
        impl = NULL;
        next = NULL;
        columns = NULL;
        _constraints = NULL;
        _indices = NULL;
    }

    TableDescriptor::TableDescriptor(DatabaseDescriptor* dd, Table* table)
    {
        db = dd;
        String* name = table->name();
        _temporary = table->temporary();
        _name = new (name->size() + MAX_TABLE_VERSION_LEN) StringLiteral(name->body(), name->size()); // reserve space for version name
        impl = table;
        next = NULL;

        int nFields = table->nFields();
        columns = Vector < FieldDescriptor > ::create(nFields);
        Iterator < Field > * fieldIterator = table->fields();
        for (int i = 0; i < nFields; i++)
        {
            columns->items[i] = new FieldDescriptor(this, fieldIterator->next(), NULL);
        }

        Iterator < Constraint > * constraintIterator = table->constraints();
        ConstraintDescriptor** cpp = &_constraints;
        while (constraintIterator->hasNext())
        {
            ConstraintDescriptor* cp = new ConstraintDescriptor(this, constraintIterator->next());
            *cpp = cp;
            cpp = &cp->next;
        }
        *cpp = NULL;

        Iterator < Index > * indexIterator = table->indices();
        IndexDescriptor** ipp = &_indices;
        while (indexIterator->hasNext())
        {
            IndexDescriptor* ip = new IndexDescriptor(this, indexIterator->next());
            *ipp = ip;
            ipp = &ip->next;
        }
        *ipp = NULL;
    }

    void SyntheticTableDescriptor::drop(Transaction*)
    {
        MCO_THROW InvalidOperation("SyntheticTableDescriptor::drop");
    }

    void SyntheticTableDescriptor::deleteAllRecords(Transaction*)
    {
        MCO_THROW InvalidOperation("SyntheticTableDescriptor::deleteAllRecords");
    }

    void SyntheticTableDescriptor::deleteRecord(Transaction* trans, Record* record)
    {
        MCO_THROW InvalidOperation("SyntheticTableDescriptor::deleteRecord");
    }

    void SyntheticTableDescriptor::updateRecord(Transaction* trans, Record* record)
    {
        MCO_THROW InvalidOperation("SyntheticTableDescriptor::updateRecord");
    }

    void SyntheticTableDescriptor::checkpointRecord(Transaction* trans, Record* record)
    {
        MCO_THROW InvalidOperation("SyntheticTableDescriptor::checkpointRecord");
    }

    void SyntheticTableDescriptor::release()
    {
        impl->release();
    }

    Record* SyntheticTableDescriptor::createRecord(Transaction* trans)
    {
        MCO_THROW InvalidOperation("SyntheticTableDescriptor::createRecord");
    }

    SyntheticTableDescriptor::SyntheticTableDescriptor(DatabaseDescriptor* db, String* name, Vector < FieldDescriptor >
                                                       * columns, DataSource* source): TableDescriptor(db, name)
    {
        this->columns = columns;
        impl = (Table*)source;
    }

#if MCO_CFG_CSV_IMPORT_SUPPORT
    CsvTableDescriptor::CsvTableDescriptor(DatabaseDescriptor* db, String* csvFile, TableDescriptor* pattern, String* delimiter, int skip)
    : SyntheticTableDescriptor(db, pattern->_name, pattern->columns, NULL)
    {
        fileName = csvFile;
        this->delimiter = delimiter;
        this->skip = skip;
        file = fopen(csvFile->cstr(), "r");
        if (file == NULL) {
            MCO_THROW RuntimeError(String::format("Failed to open file %s", csvFile->cstr())->cstr());
        }
        int nColumns = 0;
        for (int i = 0; i < columns->length; i++) {
            nColumns += !columns->items[i]->isAutoGenerated();
        }
        columns = Vector<FieldDescriptor>::create(nColumns);
        isFixedSizeRecord = true;
        size_t fixedSize = 0;
        for (int i = 0, j = 0; i < pattern->columns->length; i++) {
            if (!pattern->columns->items[i]->isAutoGenerated()) {
                columns->items[j] = new SyntheticFieldDescriptor(this, pattern->columns->items[i]->_name, pattern->columns->items[i]->_type, pattern->columns->items[i]->_elemType, j,  pattern->columns->items[i]->_precision);
                columns->items[j]->_fixedSize = pattern->columns->items[i]->_fixedSize;
                if (pattern->columns->items[i]->_type != tpString || pattern->columns->items[i]->_fixedSize == 0) { 
                    isFixedSizeRecord = false;
                } else {  
                    fixedSize += pattern->columns->items[i]->_fixedSize;
                } 
                j += 1;
                
            }
        }
        if (fixedSize >= MAX_CSV_LINE_LEN) { 
             MCO_THROW RuntimeError(String::format("Fixed record size %d is larger than input buffer size %d", 
                                                   (int)fixedSize, MAX_CSV_LINE_LEN)->cstr());
        }
    }

    void CsvTableDescriptor::release() {
        fclose(file);
    }

    bool CsvTableDescriptor::isNumberOfRecordsKnown() {
        return false;
    }

    Cursor* CsvTableDescriptor::records(Transaction* trans) {
        return new CsvCursor(this);
    }

    Cursor* CsvTableDescriptor::internalCursor(Transaction* trans) {
        return records(trans);
    }

    size_t CsvTableDescriptor::nRecords(Transaction* trans) {
        MCO_THROW InvalidOperation("CsvTableDescriptor::nRecords");
    }

    bool CsvTableDescriptor::isRIDAvailable() {
        return false;
    }

    Reference* CsvTableDescriptor::getRID(Record* rec) {
        MCO_THROW InvalidOperation("CsvTableDescriptor::getRID");
    }

    int CsvTableDescriptor::compareRID(Record* r1, Record* r2) {
        MCO_THROW InvalidOperation("CsvTableDescriptor::compareRID");
    }

    bool CsvTableDescriptor::CsvCursor::hasNext() {
        if (curr == NULL) {
            return moveNext();
        }
        return true;
    }

    Record* CsvTableDescriptor::CsvCursor::next()
    {
        if (!hasNext()) {
            MCO_THROW NoMoreElements();
        }
        Record* rec = curr;
        curr = NULL;
        return rec;
    }

    void CsvTableDescriptor::CsvCursor::release() {}

    void CsvTableDescriptor::CsvCursor::reportError(char const* msg)
    {
        MCO_THROW InvalidCsvFormat(desc->fileName, line, msg);
    }

    bool CsvTableDescriptor::CsvCursor::moveNext()
    {
        char buf[MAX_CSV_LINE_LEN];
        line += 1;
        if (fgets(buf, sizeof buf, desc->file) == NULL) {
            curr = NULL;
            return false;
        }
        int nColumns = desc->columns->length;
        Tuple* tuple = Tuple::create(nColumns, allocator);
        char* p = buf;
        MCO_TRY {
            for (int i = 0; i < nColumns; i++) {
                if (!desc->columns->items[i]->isAutoGenerated()) {
                    Value* v;
                    if (sep == '\0') { // fixed size format 
                        int width = desc->columns->items[i]->_fixedSize;
                        int len = width;
                        while (len != 0 && p[len-1] == ' ') {
                            len -= 1;
                        }
                        v = String::create(p, len, allocator);
                        p += width;
                    } else {     
                        char* str;
                        if (*p == '\'' || *p == '"') {
                            char quote = *p++;
                            str = p;
                            while (*p != quote) {
                                if (*p++ == '\0') {
                                    reportError("Unterminated string");
                                }
                            }
                            *p++ = '\0';
                            if (*p == sep) {
                                p += 1;
                            } else if (i+1 != nColumns) {
                                reportError("Too few values");
                            }
                        } else {
                            str = p;
                            while (*p != sep && *p != '\n' && *p != '\r' && *p != '\0') {
                                p += 1;
                            }
                            if (*p == sep) {
                                *p++ = '\0';
                            } else {
                                *p = '\0';
                                if (i+1 != nColumns) {
                                    reportError("Too few values");
                                }
                            }
                        }
                        if (*str == '\0') {
                            if (desc->columns->items[i]->_type == tpString || desc->columns->items[i]->_type == tpUnicode) {
                                v = String::create(0, allocator);
                            } else {
                                v = &Null;
                            }
                        } else {
                            v = String::create(str, allocator);
                            switch (fieldValueType[desc->columns->items[i]->_type]) {
                            case tpBool:
                                v = BoolValue::create(v->isTrue());
                                break;
                            case tpInt:
                              v = new (allocator) IntValue(v->intValue());
                                break;
                            case tpReal:
                                v = new (allocator) RealValue(v->realValue());
                                break;
                            case tpDateTime:
                                v = new (allocator) DateTime(v->timeValue());
                                break;
                            case tpNumeric:
                            {
                                int prec = desc->columns->items[i]->_precision;
                                v = new (allocator) NumericValue(v->intValue(prec), prec);
                                break;
                            }
                            case tpUnicode:
                            case tpString:
                                break;
                            default:
                                reportError("Invalid column value");
                            }
                        }
                    }
                    tuple->set(i, v);
                }
            }
#if 0
            if (sep == '\0') { // fixed size format 
                if ((mco_size_t)(p - buf) >= strlen(buf) - 1) { 
                    reportError("Too short string");
                }
            }
#endif
        }
#if MCO_CFG_USE_EXCEPTIONS
        catch (McoSqlException& x)
        {
            if (x.code != McoSqlException::CSV_FORMAT_EXCEPTION) {
                reportError(x.what());
            } else {
                MCO_THROW;
            }
        }
#endif
        curr = tuple;
        return true;
    }



    CsvTableDescriptor::CsvCursor::CsvCursor(CsvTableDescriptor* csv) : desc(csv), curr(NULL), line(0), allocator(MemoryManager::getAllocator())
    {
        char buf[MAX_CSV_LINE_LEN];
        if (csv->delimiter) { 
            sep = csv->delimiter->size() == 0 ? '\0' : *csv->delimiter->body();
            for (int i = csv->skip; i != 0; i--) {
                if (fgets(buf, sizeof buf, desc->file) == NULL) {
                    MCO_THROW RuntimeError(String::format("Failed to read CSV file header in %s", desc->fileName->cstr())->cstr());
                }
            }
        } else {
            if (fgets(buf, sizeof buf, desc->file) == NULL) {
                MCO_THROW RuntimeError(String::format("Failed to read CSV file header in %s", desc->fileName->cstr())->cstr());
            }
            char const* separators = ";,|\t";
            sep = '\0';
            while (*separators != '\0') {
                if (strchr(buf, *separators) != NULL) {
                    sep = *separators;
                    break;
                }
                separators += 1;
            }
        }
        if (sep == '\0') {
            if (!desc->isFixedSizeRecord) { 
	      MCO_THROW RuntimeError(String::format("Invalid CSV file header in %s : %s", desc->fileName->cstr(), buf)->cstr());
            }
        }
    }
#endif

    ArrayDataSource::ArrayDataSource(DatabaseDescriptor* db, Array* a, Type elemType, Type subarrayElemType)
    : SyntheticTableDescriptor(db, String::create("array"), Vector<FieldDescriptor>::create(1), NULL), arr(a)
    {
        columns->items[0] = new SyntheticFieldDescriptor(this, String::create("element"), elemType, subarrayElemType, 0); 
    }

    bool ArrayDataSource::isNumberOfRecordsKnown() {
        return true;
    }

    Cursor* ArrayDataSource::records(Transaction* trans) {
        return new ArrayCursor(arr);
    }

    Cursor* ArrayDataSource::internalCursor(Transaction* trans) {
        return records(trans);
    }

    size_t ArrayDataSource::nRecords(Transaction* trans) {
        return arr->size();
    }

    bool ArrayDataSource::isRIDAvailable() {
        return false;
    }

    Reference* ArrayDataSource::getRID(Record* rec) {
        MCO_THROW InvalidOperation("ArrayDataSource::getRID");
    }

    int ArrayDataSource::compareRID(Record* r1, Record* r2) {
        MCO_THROW InvalidOperation("ArrayDataSource::compareRID");
    }

    bool ArrayDataSource::ArrayCursor::hasNext() {
        return curr < size;
    }

    Record* ArrayDataSource::ArrayCursor::next()
    {
        if (curr >= size) { 
            MCO_THROW NoMoreElements();
        }
        Tuple* tuple = Tuple::create(1);
        tuple->values[0] = arr->getAt(curr++);
        return tuple;
    }


    StatisticTableDescriptor::StatisticTableDescriptor(DatabaseDescriptor* db, String* name,
                                                       Vector < FieldDescriptor > * columns, DataSource* source)
    : SyntheticTableDescriptor(db, name, columns, source) {}



    Cursor* StatisticTableDescriptor::records(Transaction* trans)
    {
        Record* rec = ((TemporaryResultSet*)impl)->first;
        for (TableDescriptor* table = db->_tables->next->next->next->next; table != NULL; table = table->next, rec = rec->next) {
            table->impl->updateStatistic(trans, rec);
        }
        return impl->records(trans);
    }

    Cursor* StatisticTableDescriptor::internalCursor(Transaction* trans)
    {
        Record* rec = ((TemporaryResultSet*)impl)->first;
        for (TableDescriptor* table = db->_tables->next->next->next->next; table != NULL; table = table->next, rec = rec->next) {
            table->impl->updateStatistic(trans, rec);
        }
        return impl->internalCursor(trans);
    }

    Cursor* IndexStatisticTableDescriptor::records(Transaction* trans)
    {
        Record* rec = ((TemporaryResultSet*)impl)->first;
        for (TableDescriptor* table = db->_tables->next->next->next->next; table != NULL; table = table->next) {
           for (IndexDescriptor* index = table->_indices; index != NULL; index = index->next, rec = rec->next) {
               index->impl->updateStatistic(trans, rec);
           }
        }
        return impl->records(trans);
    }

    Cursor* IndexStatisticTableDescriptor::internalCursor(Transaction* trans)
    {
        Record* rec = ((TemporaryResultSet*)impl)->first;
        for (TableDescriptor* table = db->_tables->next->next->next->next; table != NULL; table = table->next) {
           for (IndexDescriptor* index = table->_indices; index != NULL; index = index->next, rec = rec->next) {
               index->impl->updateStatistic(trans, rec);
           }
        }
        return impl->internalCursor(trans);
    }


    IndexStatisticTableDescriptor::IndexStatisticTableDescriptor(DatabaseDescriptor* db, String* name,
                                                       Vector < FieldDescriptor > * columns, DataSource* source)
    : SyntheticTableDescriptor(db, name, columns, source) {}



    DynamicTableDescriptor::DynamicTableDescriptor(DatabaseDescriptor* db, QueryNode* stmt): SyntheticTableDescriptor
                                                   (db, String::create("DYNAMIC"), NULL, NULL)
    {
        Vector < ColumnNode > * resultColumns = stmt->resultColumns;
        int nColumns = resultColumns->length;
        columns = Vector < FieldDescriptor > ::create(nColumns);
        for (int i = 0; i < nColumns; i++)
        {
            columns->items[i] = new SyntheticFieldDescriptor(this, resultColumns->items[i]->name, resultColumns->items[i]->type, tpRaw, i);
        }
        this->stmt = stmt;
    }

    void DynamicTableDescriptor::evaluate(Runtime* runtime)
    {
        impl = (Table*)stmt->executeQuery(runtime);
    }


    FieldDescriptor::FieldDescriptor(TableDescriptor* table, String* name, Type type, Type elemType, int precision, bool nullable)
    {
        _table = table;
        _scope = NULL;
        _type = type;
        _elemType = elemType;
        _elemSize = -1;
        _name = name;
        _nullable = nullable;
        _referencedTableName = NULL;
        referencedTable = NULL;
        _components = (type == tpArray) ? new FieldDescriptor(table, NULL, elemType, tpNull) : NULL;
        _fixedSize = 0;
        _order = UNSPECIFIED_ORDER;
        impl = NULL;
        next = NULL;
        _precision = precision;
        _width = -1;
    }

    FieldDescriptor::FieldDescriptor(TableDescriptor* table, Field* field, FieldDescriptor* scope)
    {
        _name = field->name();
        _type = field->type();
        _elemType = field->elementType();
        _elemSize = field->elementSize();
        _order = field->order();
        _table = table;
        _scope = scope;
        _nullable = field->isNullable();
        _referencedTableName = NULL;
        referencedTable = NULL;
        _components = NULL;
        _fixedSize = 0;
        _precision = field->precision();
        _width = field->width();
        impl = field;
        next = NULL;

        switch (_type)
        {
            case tpStruct:
                {
                    Iterator < Field > * iterator = field->components();
                    FieldDescriptor** fpp = &_components;
                    while (iterator->hasNext())
                    {
                        FieldDescriptor* fd = new FieldDescriptor(table, iterator->next(), this);
                        *fpp = fd;
                        fpp = &fd->next;
                    }
                    *fpp = NULL;
                    break;
                }
            case tpArray:
                _components = new FieldDescriptor(table, field->element(), this);
                // no break
            case tpString:
            case tpUnicode:
                _fixedSize = field->fixedSize();
                break;
            case tpReference:
                _referencedTableName = field->referencedTableName();
                break;
            default:
                ;
        }
    }

    Field* FieldDescriptor::findComponent(String* name)
    {
        if (_type != tpStruct)
        {
            MCO_THROW InvalidOperation("TableDescriptor::findComponent");
        }
        for (FieldDescriptor* fd = _components; fd != NULL; fd = fd->next)
        {
            if (name->equals(fd->_name))
            {
                return fd;
            }
        }
        return NULL;
    }

    String* FieldDescriptor::name()
    {
        return _name;
    }

    Type FieldDescriptor::type()
    {
        return _type;
    }

    Type FieldDescriptor::elementType()
    {
        return _elemType;
    }

    int FieldDescriptor::elementSize()
    {
        return _elemSize;
    }

    SortOrder FieldDescriptor::order()
    {
        return _order;
    }

    Table* FieldDescriptor::table()
    {
        return _table;
    }

    Field* FieldDescriptor::scope()
    {
        return _scope;
    }

    Value* FieldDescriptor::get(Struct* rec)
    {
        assert(rec->source() != NULL);
        return impl->get(rec->source());
    }

    Value* FieldDescriptor::update(Struct* rec)
    {
        assert(rec->source() != NULL);
        return impl->update(rec->source());
    }

    void FieldDescriptor::set(Struct* rec, Value* val)
    {
        assert(rec->source() != NULL);
        impl->set(rec->source(), val);
    }


    bool FieldDescriptor::isNullable() 
    { 
        return _nullable;
    }

    bool FieldDescriptor::isAutoGenerated()
    {
        return impl != NULL && impl->isAutoGenerated();
    }

    String* FieldDescriptor::referencedTableName()
    {
        if (_type != tpReference)
        {
            MCO_THROW InvalidOperation("FieldDescriptor::referencedTableName");
        }
        return _referencedTableName;
    }

    int FieldDescriptor::precision()
    {
        return _precision;
    }

    int FieldDescriptor::width()
    {
        return _width;
    }

     Iterator < Field > * FieldDescriptor::components()
    {
        if (_type != tpStruct)
        {
            MCO_THROW InvalidOperation("FieldDescriptor::components");
        }
        return (Iterator < Field > *)new ListIterator < FieldDescriptor > (_components);
    }

    Field* FieldDescriptor::element()
    {
        if (_type != tpArray)
        {
            MCO_THROW InvalidOperation("ArrayFieldDescriptor::element");
        }
        return _components;
    }

    int FieldDescriptor::fixedSize()
    {
        if (_type != tpArray && _type != tpString && _type != tpUnicode && _type != tpSequence)
        {
            MCO_THROW InvalidOperation("ArrayFieldDescriptor::fixedSize");
        }
        return _fixedSize;
    }

    SyntheticFieldDescriptor::SyntheticFieldDescriptor(TableDescriptor* table, String* name, Type type, Type elemType, int fieldNo, int precision)
        : FieldDescriptor(table, name, type, elemType, precision)
    {
        this->fieldNo = fieldNo;
    }

    Value* SyntheticFieldDescriptor::get(Struct* rec)
    {
        return rec->get(fieldNo);
    }

}
