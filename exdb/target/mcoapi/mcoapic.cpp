/*******************************************************************
 *                                                                 *
 *  mcoapic.cpp                                                      *
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
 * MODULE:    mcoapic.cpp
 *
 * ABSTRACT:  
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#include "mcosql.h"
#include "memmgr.h"
#include "mcoapi.h"
#include "mcoapic.h"

McoDatabase *api_database = 0;

status_t mcoapi_initialize(mco_db_h db)
{
    if (api_database == 0)
    {
        api_database = new McoDatabase;
    }
    MCO_TRY
    {
        api_database->open(db);
        return SQL_OK;
    } 
    MCO_RETURN_ERROR_CODE;
}

status_t mcoapi_create(mco_db_h db, storage_t* storage)
{
    MCO_TRY
    {
        McoDatabase* database = new McoDatabase();
        database->open(db);
        *storage = (storage_t)database;
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcoapi_destroy(storage_t storage)
{
    MCO_TRY
    {
        delete (McoDatabase*)storage;
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

extern McoSql::DynamicAllocator mcosql_dynamic_allocator;

class SqlEngineWrapper : public McoMultithreadedSqlEngine
{
  protected:
    McoSql::DynamicAllocator customAllocator;

    virtual McoSql::AbstractAllocator* getDefaultAllocator() {
        return &customAllocator;
    }
  public:
    SqlEngineWrapper() : customAllocator(mcosql_dynamic_allocator) {}
};

status_t mcoapi_create_engine(mco_db_h dbh, database_t* database)
{
    MCO_TRY
    {
        McoSqlEngine* engine = new SqlEngineWrapper();
        engine->open(dbh);
        *database = (database_t)engine;
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}

status_t mcoapi_destroy_engine(database_t database)
{
    MCO_TRY
    {
        McoSqlEngine* engine = (McoSqlEngine*)database;
        delete engine;
        return SQL_OK;
    }
    MCO_RETURN_ERROR_CODE;
}
