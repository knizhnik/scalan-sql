/*******************************************************************
 *                                                                 *
 *  sqlc.cpp                                                      *
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
 * MODULE:    sqlc.cpp
 *
 * ABSTRACT:  
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#include "sqlc.h"
#include "sqlcpp.h"

using namespace McoSql;

SqlEngine mcosql_engine_singleton;
StaticAllocator mcosql_static_allocator;
DynamicAllocator mcosql_dynamic_allocator;

status_t mcosql_initialize_static_memory_manager(void* memory, size_t memory_size)
{
    MCO_TRY
    {
        mcosql_static_allocator.init(memory, memory_size);
        MemoryManager::setAllocator(&mcosql_static_allocator);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_initialize_dynamic_memory_manager(alloc_t alloc, free_t free, size_t quantum, size_t retain)
{
    MCO_TRY
    {
        mcosql_dynamic_allocator.init(alloc, free, quantum, retain);
        MemoryManager::setAllocator(&mcosql_dynamic_allocator);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_release_memory()
{
    MCO_TRY
    {
        MemoryManager::allocator->release();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_open(database_t* database)
{
    MCO_TRY
    {
        mcosql_engine_singleton.open();
        *database = (database_t) &mcosql_engine_singleton;
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_open2(database_t* database, storage_t storage)
{
    MCO_TRY
    {
        SqlEngine* engine = new SqlEngine();
        engine->open((Database*)storage);
        *database = (database_t)engine;
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}


status_t mcosql_begin_transaction_ex(database_t database, transaction_t* trans, enum mcosql_transaction_mode mode, int priority, enum mcosql_transaction_isolation_level level)
{
    MCO_TRY
    {
        *trans = (transaction_t)((SqlEngine*)database)->database()->beginTransaction((Transaction::Mode)mode, priority, (Transaction::IsolationLevel)level);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_begin_transaction(database_t database, transaction_t* trans, enum mcosql_transaction_mode mode, int priority)
{
    MCO_TRY
    {
        *trans = (transaction_t)((SqlEngine*)database)->database()->beginTransaction((Transaction::Mode)mode, priority, Transaction::DefaultIsolationLevel);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}


status_t mcosql_commit_transaction(transaction_t trans)
{
    MCO_TRY
    {
        return ((Transaction*)trans)->commit() ? SQL_OK : SQL_CONFLICT;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_commit_transaction_phase(transaction_t trans, int phase)
{
    MCO_TRY
    {
        return ((Transaction*)trans)->commit(phase) ? SQL_OK : SQL_CONFLICT;
    }
    MCO_RETURN_ERROR_CODE;
}


status_t mcosql_rollback_transaction(transaction_t trans)
{
    MCO_TRY
    {
        ((Transaction*)trans)->rollback();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_upgrade_transaction(transaction_t trans)
{
    MCO_TRY
    {
        return ((Transaction*)trans)->upgrade() ? SQL_OK : UPGRAGE_NOT_POSSIBLE;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_release_transaction(transaction_t trans)
{
    MCO_TRY
    {
        ((Transaction*)trans)->release();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_prepare_statement(database_t database, prepared_statement_t* stmt, char const* sql, ...)
{
    MCO_TRY
    {
        va_list list;
        va_start(list, sql);
        PreparedStatement* pstmt = new PreparedStatement();
        ((SqlEngine*)database)->vprepare(*pstmt, sql, &list);
        *stmt = (prepared_statement_t) pstmt;
        va_end(list);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_execute_prepared_statement(database_t database, transaction_t trans, prepared_statement_t stmt, int* n_records)
{
    MCO_TRY
    {
        int n = ((SqlEngine*)database)->executePreparedStatement(*(PreparedStatement*)stmt, (Transaction*)trans);
        if (n_records != NULL)
        {
            *n_records = n;
        }
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}
        
status_t mcosql_execute_prepared_query(database_t database, transaction_t trans, prepared_statement_t stmt, data_source_t* data_source)
{
    MCO_TRY
    {
        *data_source = (data_source_t)((SqlEngine*)database)->executePreparedQuery(*(PreparedStatement*)stmt, (Transaction*)trans);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_release_prepared_statement(prepared_statement_t stmt)
{
    delete (PreparedStatement*)stmt;
    return SQL_OK;
}

status_t mcosql_execute_statement(database_t database, transaction_t trans, int* n_records, char const* sql, ...)
{
    MCO_TRY
    {
        va_list list;
        va_start(list, sql);
        int n = ((SqlEngine*)database)->vexecuteStatement((Transaction*)trans, sql, &list);
        if (n_records != NULL)
        {
            *n_records = n;
        }
        va_end(list);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_execute_query(database_t database, transaction_t trans, data_source_t* data_source, char const* sql,
                              ...)

{
    MCO_TRY
    {
        va_list list;
        va_start(list, sql);
        *data_source = (data_source_t)((SqlEngine*)database)->vexecuteQuery((Transaction*)trans, sql, &list);
        va_end(list);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_current_transaction(data_source_t data_source, transaction_t* trans)
{
    MCO_TRY
    {
        *trans = (transaction_t)((DataSource*)data_source)->currentTransaction();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_cursor(data_source_t data_source, cursor_t* cursor)
{
    MCO_TRY
    {
        *cursor = (cursor_t)((DataSource*)data_source)->records();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_number_of_columns(data_source_t data_source, int* n_columns)
{
    MCO_TRY
    {
        *n_columns = ((DataSource*)data_source)->nFields();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_column_iterator(data_source_t data_source, column_iterator_t* iterator)
{
    MCO_TRY
    {
        *iterator = (column_iterator_t)((DataSource*)data_source)->fields();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_column_info(column_iterator_t iterator, type_t* type, char** name)
{
    MCO_TRY
    {
        Iterator < Field > * fields = (Iterator < Field > *)iterator;
        if (fields->hasNext())
        {
            Field* field = fields->next();
            *type = (type_t)field->type();
			*name = field->name() != NULL ? field->name()->cstr() : NULL;
            return SQL_OK;
        }
        return NO_MORE_ELEMENTS;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_cursor_move_next(cursor_t cursor, record_t* record)
{
    MCO_TRY
    {
        Cursor* iterator = (Cursor*)cursor;
        if (iterator->hasNext())
        {
            *record = (record_t)iterator->next();
            return SQL_OK;
        }
        return NO_MORE_ELEMENTS;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_extract_struct(data_source_t data_source, record_t record, void* dst, size_t dst_size, char
                               null_indicators[])
{
    MCO_TRY
    {
        ((DataSource*)data_source)->extract((Record*)record, dst, dst_size, (bool*)null_indicators);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}


static Value* create_value(type_t type, void* value)
{
    switch (type)
    {
        case CT_NULL:
            return  &Null;
        case CT_BOOL:
            return BoolValue::create(*(bool*)value);
        case CT_INT1:
            return new IntValue(*(signed char*)value);
        case CT_UINT1:
            return new IntValue(*(unsigned char*)value);
        case CT_INT2:
            return new IntValue(*(signed short*)value);
        case CT_UINT2:
            return new IntValue(*(unsigned char*)value);
        case CT_INT4:
            return new IntValue(*(signed int*)value);
        case CT_UINT4:
            return new IntValue(*(unsigned int*)value);
        case CT_INT8:
        case CT_UINT8:
            return new IntValue(*(int64_t*)value);
        #ifndef NO_FLOATING_POINT
        case CT_REAL4:
            return new RealValue(*(float*)value);
        case CT_REAL8:
            return new RealValue(*(double*)value);
        #endif 
        case CT_TIME:
            return new DateTime(*(time_t*)value);
        case CT_STRING:
            return String::create((char*)value);
        default:
            return (Value*)value;
    }
}


status_t mcosql_get_column_value(record_t record, int columnNo, type_t* type, void* value)
{
    MCO_TRY
    {
        Value* cv = ((Record*)record)->get(columnNo);
        *type = (type_t)cv->type();
        *(void**)value = cv->pointer();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_column_value_as(record_t record, int columnNo, type_t type, void* buffer)
{
    MCO_TRY
    {
        Value* cv = ((Record*)record)->get(columnNo);
        if (cv->isNull()) { 
            return SQL_NULL_VALUE;
        }
        switch (type) { 
          case CT_BOOL:
            *(bool*)buffer = cv->isTrue();
            break;
          case CT_INT1:
          case CT_UINT1: 
            *(char*)buffer = (char)cv->intValue();
            break;
          case CT_UINT2:
          case CT_INT2: 
            *(short*)buffer = (short)cv->intValue();
            break;
          case CT_UINT4:
          case CT_INT4:
            *(int*)buffer = (int)cv->intValue();
            break;
          case CT_INT8:
          case CT_UINT8:
          case CT_REFERENCE:
            *(int64_t*)buffer = cv->intValue();
            break;
            #ifndef NO_FLOATING_POINT
          case CT_REAL4:
            *(float*)buffer = (float)cv->realValue();
            break;
          case CT_REAL8:
            *(double*)buffer = cv->realValue();
            break;
            #endif
           case CT_TIME:
            *(time_t*)buffer = cv->timeValue();
            break;
            #ifdef UNICODE_SUPPORT
          case CT_UNICODE:
            *(wchar_t**)buffer = cv->unicodeStringValue()->wcstr();
            break;
            #endif
          case CT_STRING:
            *(char**)buffer = cv->stringValue()->cstr();
            break;
          case CT_RAW:
            *(void**)buffer = cv->pointer();
            break;
          default:
            return SQL_INVALID_OPERAND;
        }
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_set_column_value(record_t record, int columnNo, type_t type, void* value)
{
    MCO_TRY
    {
        ((Record*)record)->set(columnNo, create_value(type, value));
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_column_value_for_update(record_t record, int columnNo, type_t* type, void* value)
{
    MCO_TRY
    {
        Value* cv = ((Record*)record)->update(columnNo);
        *type = (type_t)cv->type();
        *(void**)value = (void*)cv;
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_release_query_result(data_source_t data_source)
{
    MCO_TRY
    {
        ((DataSource*)data_source)->release();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

#if SQL_INTERPRETER_SUPPORT
void mcosql_interpreter(database_t database, char const* prompt, FILE* in, FILE* out, FILE* err)
{
    ((SqlEngine*)database)->main(prompt, in, out, err);
}
#endif


status_t mcosql_close(database_t database)
{
    MCO_TRY
    {
        SqlEngine* db = (SqlEngine*)database;
        db->close();
        if (db !=  &mcosql_engine_singleton)
        {
            delete db;
        }
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_release(database_t database)
{
    MCO_TRY
    {
        ((SqlEngine*)database)->close();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_array_element_value(void* array, int index, type_t* type, void* value)
{
    assert(((Value*)array)->type() == tpArray);
    MCO_TRY
    {
        Value* elem = ((Array*)array)->getAt(index);
        *type = (type_t)elem->type();
        *(void**)value = elem->pointer();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_array_element_value_for_update(void* array, int index, type_t* type, void* value)
{
    assert(((Value*)array)->type() == tpArray);
    MCO_TRY
    {
        Value* elem = ((Array*)array)->updateAt(index);
        *type = (type_t)elem->type();
        *(Value**)value = elem;
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_set_array_element_value(void* array, int index, type_t type, void* value)
{
    assert(((Value*)array)->type() == tpArray);
    MCO_TRY
    {
        ((Array*)array)->setAt(index, create_value(type, value));
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_array_length(void* array, int* length)
{
    assert(((Value*)array)->type() == tpArray);
    MCO_TRY
    {
        *length = ((Array*)array)->size();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_set_array_length(void* array, int length)
{
    assert(((Value*)array)->type() == tpArray);
    MCO_TRY
    {
        ((Array*)array)->setSize(length);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_array_body(void* array, void* dst, int offs, int len)
{
    assert(((Value*)array)->type() == tpArray);
    MCO_TRY
    {
        ((Array*)array)->getBody(dst, offs, len);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_set_array_body(void* array, void* src, int offs, int len)
{
    assert(((Value*)array)->type() == tpArray);
    MCO_TRY
    {
        ((Array*)array)->setBody(src, offs, len);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_referenced_record(void* ref, record_t* record)
{
    assert(((Value*)ref)->type() == tpReference);
    MCO_TRY
    {
        *(Record**)record = ((Reference*)ref)->dereference();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_record_id(void* ref, int64_t* id)
{
    assert(((Value*)ref)->type() == tpReference);
    MCO_TRY
    {
        *id = ((Reference*)ref)->intValue();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_struct_component_value(void* s, int index, type_t* type, void* value)
{
    assert(((Value*)s)->type() == tpStruct);
    MCO_TRY
    {
        Value* c = ((Struct*)s)->get(index);
        *type = (type_t)c->type();
        *(void**)value = c->pointer();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_struct_component_value_for_update(void* s, int index, type_t* type, void* value)
{
    assert(((Value*)s)->type() == tpStruct);
    MCO_TRY
    {
        Value* c = ((Struct*)s)->update(index);
        *type = (type_t)c->type();
        *(Value**)value = c;
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_set_struct_component_value(void* s, int index, type_t type, void* value)
{
    assert(((Value*)s)->type() == tpStruct);
    MCO_TRY
    {
        ((Struct*)s)->set(index, create_value(type, value));
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_get_struct_size(void* s, int* size)
{
    assert(((Value*)s)->type() == tpStruct);
    MCO_TRY
    {
        *size = ((Struct*)s)->nComponents();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_blob_available_size(void* blob, int* size)
{
    assert(((Value*)blob)->type() == tpBlob);
    MCO_TRY
    {
        *size = ((Blob*)blob)->available();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_blob_get_data(void* blob, void* buffer, int buffer_size, int* n_bytes)
{
    assert(((Value*)blob)->type() == tpBlob);
    MCO_TRY
    {
        *n_bytes = ((Blob*)blob)->get(buffer, buffer_size);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_blob_append_data(void* blob, void const* buffer, int size)
{
    assert(((Value*)blob)->type() == tpBlob);
    MCO_TRY
    {
        ((Blob*)blob)->append(buffer, size);
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcosql_blob_reset(void* blob)
{
    assert(((Value*)blob)->type() == tpBlob);
    MCO_TRY
    {
        ((Blob*)blob)->reset();
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}
