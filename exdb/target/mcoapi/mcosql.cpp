/*******************************************************************
 *                                                                 *
 *  mcosql.cpp                                                     *
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
 * MODULE:    mcosql.cpp
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 *            1.0- 2 SS 29-Aug-2005 McoSqlEngine constructor added in order to clear mco_db_h db
 * --
 */
#include <mco.h>
#include <mcowrap.h>
#include "mcocfg.h"
#include "mcoerr.h"
#include "mcoconst.h"
#include "mcosys.h"
#include "mcomem.h"
#include "mcodb.h"
#include "mcocobj.h"
#include "mcoconn.h"
#include "mcoindex.h"
#include "mcoabst.h"
#include "mcocsr.h"
#include "mcotmgr.h"
#include "mcotrans.h"
#include "mcoobj.h"

#include "mcosql.h"
#include "mcodisk.h"
#include "mcodiskbtree.h"
#include "sync.h"

using namespace McoSql;

void McoSqlOpenParameters::setDefaultValues()
{
    databaseName = NULL;
    dictionary = NULL;
    mainMemoryDatabaseSize = 0;
#ifndef _AIX
    mainMemoryDatabaseAddress = (void*)0x20000000;
#else
    mainMemoryDatabaseAddress = (void*)0x30000000;
#endif
    maxTransactionSize = 0;
    maxConnections = 100;
    mainMemoryPageSize = 128;
    hashLoadFactor = 100;
    flags = DEFAULT_OPEN_FLAGS;
    modeMask = 0;
    savedImage = NULL;
    diskDatabaseFile = NULL;
    diskDatabaseLogFile = NULL;
    diskDatabaseMaxSize = MCO_INFINITE_DATABASE_SIZE;
    diskCacheMemoryAddress = 0;
    diskCacheSize = 0;
    diskPageSize = 4096;
    streamReader = NULL;
    stream = NULL;
    allocator = NULL;
    redoLogLimit = MCO_DEFAULT_REDO_LOG_LIMIT;
    logType = REDO_LOG;
    defaultCommitPolicy = MCO_COMMIT_SYNC_FLUSH;
    transSchedPolicy = MCO_SCHED_FIFO;
    devices = NULL;
    n_devices = 0;
    tables = NULL;
    n_tables = 0;
    maxClasses = 0;    maxIndexes = 0;
    maxDictionarySize = 0;
    maxTransTime = 0;
}

McoSqlOpenParameters::McoSqlOpenParameters(char const* name, mco_dictionary_h dictionary, size_t size, size_t pageSize,
                                           void* mapAddress, size_t maxTransSize, int flags, char const* savedImage)
{
    setDefaultValues();
    this->databaseName = (char*)name;
    this->dictionary = dictionary;
    this->mainMemoryDatabaseSize = size;
    this->mainMemoryDatabaseAddress = mapAddress;
    this->maxTransactionSize = maxTransSize;
    this->mainMemoryPageSize = pageSize;
    this->flags = flags;
    this->savedImage = (char*)savedImage;
}


size_t McoSqlEngine::getPageSize()
{
    return pageSize;
}

McoSqlEngine::McoSqlEngine()
{
    dbh = NULL;
    addr = NULL;
    diskDatabase = false;
    license_key = NULL;
    flags = 0;
}

McoSqlEngine::McoSqlEngine(McoSqlEngine* engine, AbstractAllocator* allocator): SqlEngine(&db, engine, allocator)
{
    addr = engine->addr;
    pageSize = engine->pageSize;
    license_key = NULL;
    flags = 0;
}

static McoSql::Value* mco_sql_save_snapshot(SqlFunctionDeclaration* fdecl, McoSql::Value* path)
{
    McoSqlEngine* engine = (McoSqlEngine*)fdecl->getContext();
    engine->save(path->stringValue()->cstr());
    return path;
}

static McoSql::Value* mco_sql_save_metadata(SqlFunctionDeclaration* fdecl, McoSql::Value* path)
{
    McoSqlEngine* engine = (McoSqlEngine*)fdecl->getContext();
    engine->saveMetadata(path->stringValue()->cstr());
    return path;
}

void McoSqlEngine::init()
{
    SqlEngine::init();
    registerFunction(tpString, "save_snapshot", (void*)mco_sql_save_snapshot, 1, this);
    registerFunction(tpString, "save_metadata", (void*)mco_sql_save_metadata, 1, this);
}

McoSqlEngine::~McoSqlEngine(){}

#ifdef MCO_CFG_SAVELOAD_SUPPORTED
static mco_size_sig_t freader(void* stream, void* to, mco_size_t max_nbytes)
{
    return fread(to, 1, max_nbytes, (FILE*)stream);
}

static mco_size_sig_t fwriter(void* stream, void const* from, mco_size_t n)
{
    return fwrite(from, 1, n, (FILE*)stream);
}
#endif /* MCO_CFG_SAVELOAD_SUPPORTED */

void McoSqlEngine::open(char const* name, mco_dictionary_h dictionary, size_t size, size_t pageSize, void* mapAddress,
                        size_t maxTransSize, int flags, char const* savedImage)
{
    McoSqlOpenParameters params;
    params.databaseName = (char*)name;
    params.dictionary = dictionary;
    params.mainMemoryDatabaseSize = size;
    params.mainMemoryDatabaseAddress = mapAddress;
    params.maxTransactionSize = maxTransSize;
    params.mainMemoryPageSize = pageSize;
    params.flags = flags;
    params.savedImage = (char*)savedImage;
    open(params);
}

void McoSqlEngine::open(McoSqlOpenParameters const &params)
{
    char* startAddress = 0;
    mco_runtime_info_t info;
    int flags = params.flags;
    this->flags = flags;
    optimizerParams = params.optimizerParams;
    pageSize = params.mainMemoryPageSize;
    diskDatabase = false;

    mco_get_runtime_info(&info);
    if (info.mco_shm_supported || params.devices != NULL || !(flags & McoSqlOpenParameters::ALLOCATE_MEMORY))
    {
        addr = NULL;
        startAddress = (char*)params.mainMemoryDatabaseAddress;
    }
    else
    {
        addr = mco_sys_malloc(params.mainMemoryDatabaseSize + params.diskCacheSize);
        startAddress = (char*)addr;
        if (startAddress == NULL)
        {
            MCO_THROW NotEnoughMemory(0, 0, params.mainMemoryDatabaseSize + params.diskCacheSize);
        }
    }

    /* start MCO runtime */
    if ((flags & McoSqlOpenParameters::START_MCO_RUNTIME) && mco_runtime_start() != MCO_S_OK)
    {
        if (addr != NULL)
        {
            mco_sys_free(addr);
        }
        MCO_THROW RuntimeError("Failed to start runtime");
    }

    if (flags & McoSqlOpenParameters::SET_ERROR_HANDLER)
    {
        /* set fatal error handler */
        mco_error_set_handler((mco_error_handler_f) & McoDatabase::checkStatus);
    }

    AbstractAllocator* engineAllocator = params.allocator;

    if (flags & McoSqlOpenParameters::SET_SQL_ALLOCATOR)
    {
        /* Set memory allocator */
        if (engineAllocator == NULL)
        {
            engineAllocator = getDefaultAllocator();
        }
        setAllocator(engineAllocator);
        MemoryManager::setAllocator(engineAllocator);
    }

    mco_db_params_t   db_params;
    mco_device_t      dev[4];
    mco_device_t*     devices;
    int               n_devices;
    void             *cache = params.diskCacheMemoryAddress != NULL ? params.diskCacheMemoryAddress
        : startAddress != NULL ? startAddress + params.mainMemoryDatabaseSize : NULL;

    mco_db_params_init ( &db_params );
    db_params.mem_page_size            = (uint2)params.mainMemoryPageSize;
    db_params.disk_page_size           = ( params.diskDatabaseFile != NULL ) ? params.diskPageSize : 0;
    db_params.db_log_type              = params.logType;
    db_params.log_params.default_commit_policy = params.defaultCommitPolicy;
    db_params.disk_max_database_size   = params.diskDatabaseMaxSize;
    db_params.hash_load_factor         = params.hashLoadFactor;
    db_params.license_key              = license_key;
    db_params.mode_mask                = params.modeMask;

    db_params.max_classes              = params.maxClasses;
    db_params.max_indexes              = params.maxIndexes;
    db_params.ddl_dict_size            = params.maxDictionarySize;

    db_params.db_max_connections       = params.maxConnections;

    db_params.trans_sched_policy       = params.transSchedPolicy;
    db_params.max_trans_time           = params.maxTransTime;

    if ( params.redoLogLimit )
    {
        db_params.log_params.redo_log_limit = params.redoLogLimit;
    }

    if (params.devices != NULL) {
        devices = params.devices;
        n_devices = params.n_devices;
        for (int i = 0; i < n_devices; i++) {
            if (devices[i].assignment == MCO_MEMORY_ASSIGN_PERSISTENT) {
                db_params.disk_page_size = params.diskPageSize;
                break;
            }
        }
    } else {
        devices = dev;
        n_devices = 1;
        dev[0].assignment = MCO_MEMORY_ASSIGN_DATABASE;
        dev[1].assignment = MCO_MEMORY_ASSIGN_CACHE;
        dev[2].assignment = MCO_MEMORY_ASSIGN_PERSISTENT;
        dev[3].assignment = MCO_MEMORY_ASSIGN_LOG;

        dev[0].size       = params.mainMemoryDatabaseSize;
        dev[1].size       = params.diskCacheSize;

        if (info.mco_shm_supported)
        {
            dev[0].type           = MCO_MEMORY_NAMED;
            dev[1].type           = MCO_MEMORY_NAMED;

            dev[0].dev.named.hint = startAddress;
            dev[1].dev.named.hint = cache;

            dev[0].dev.named.flags = 0;
            dev[1].dev.named.flags = 0;

            sprintf( dev[0].dev.named.name, "%s", params.databaseName );
            sprintf( dev[1].dev.named.name, "%s_cache", params.databaseName );
        }
        else
        {
            dev[0].type         = MCO_MEMORY_CONV;
            dev[1].type         = MCO_MEMORY_CONV;

            dev[0].dev.conv.ptr = startAddress;
            dev[1].dev.conv.ptr = cache;
        }

        dev[2].type           = MCO_MEMORY_FILE;
        dev[3].type           = MCO_MEMORY_FILE;
        dev[2].dev.file.flags = MCO_FILE_OPEN_DEFAULT;
        dev[3].dev.file.flags = MCO_FILE_OPEN_DEFAULT;

        if ( params.diskDatabaseFile != NULL )
        {
            sprintf( dev[2].dev.file.name, "%s", params.diskDatabaseFile );
            if ( params.diskDatabaseLogFile != NULL )
            {
                sprintf( dev[3].dev.file.name, "%s", params.diskDatabaseLogFile );
                n_devices = 4;
            }
            else
                n_devices = 3;
        }
    }
    /* Try to connect to existed database */
    if (!(info.mco_shm_supported && mco_db_connect(params.databaseName, &dbh) == MCO_S_OK))
    {
#ifdef MCO_CFG_SAVELOAD_SUPPORTED
        mco_stream_read reader = params.streamReader;
        void* stream = params.stream;
        FILE* f = NULL;

        if (reader == NULL && params.savedImage != NULL)
        {
            f = fopen(params.savedImage, "rb");
            if (f == NULL)
            {
                if (!(flags & McoSqlOpenParameters::INITIALIZE_DATABASE))
                {
                    MCO_THROW RuntimeError("Failed to open file");
                }
            }
            else
            {
                stream = f;
                reader = freader;
            }
        }

        if (stream != NULL)
        {
            McoDatabase::checkStatus(mco_db_load(stream, reader, params.databaseName, params.dictionary, devices, n_devices, &db_params));
            flags &= ~McoSqlOpenParameters::INITIALIZE_DATABASE;
            if (f != NULL)
            {
                fclose(f);
            }
        }
#endif /* MCO_CFG_SAVELOAD_SUPPORTED */

        if (flags & McoSqlOpenParameters::INITIALIZE_DATABASE)
        {
            /* Create a database, using first memory segment */
            McoDatabase::checkStatus(mco_db_open_dev(params.databaseName, params.dictionary, devices, n_devices, &db_params));
        }
        /* connect to the database, obtain a database handle */
        McoDatabase::checkStatus(mco_db_connect(params.databaseName, &dbh));
    }
    else if (params.streamReader != NULL || params.savedImage != NULL)
    {
        MCO_THROW RuntimeError("Attempt to create db instance with duplicate name");
    }

    /* Open SQL mapper */
    db.open(dbh);

    if (params.n_tables != 0) {
        db.registerExternalTables(params.tables, params.n_tables);
    }

    /* McoSQL engine */
    SqlEngine::open(&db);
}

void McoSqlEngine::open(mco_db_h dbh)
{
    this->dbh = dbh;
    this->flags = McoSqlOpenParameters::FOREIGN_CONNECTION;
    setAllocator(getDefaultAllocator());
    MemoryManager::setAllocator(getDefaultAllocator());
    db.open(dbh);
    SqlEngine::open(&db);
}

void McoSqlEngine::createConnectionPool()
{
    db.createConnectionPool();
}

void McoSqlEngine::destroyConnectionPool()
{
    db.destroyConnectionPool();
}

void McoSqlEngine::save(char const *databaseFile)
{
#ifdef MCO_CFG_SAVELOAD_SUPPORTED
        FILE* f = fopen(databaseFile, "wb");
        if (f == NULL)
        {
            MCO_THROW RuntimeError("Failed to create file");
        }
        McoDatabase::checkStatus(mco_db_save(f, fwriter, dbh));
        if (fclose(f) != 0)
        {
            MCO_THROW RuntimeError("Failed to close file");
        }
#endif /* MCO_CFG_SAVELOAD_SUPPORTED */
}

void McoSqlEngine::save(void *stream, mco_stream_write streamWriter)
{
#ifdef MCO_CFG_SAVELOAD_SUPPORTED
        McoDatabase::checkStatus(mco_db_save(stream, streamWriter, dbh));
#endif /* MCO_CFG_SAVELOAD_SUPPORTED */
}

void McoSqlEngine::saveMetadata(char const *metadataFile)
{
#ifdef MCO_CFG_SAVELOAD_SUPPORTED
        FILE* f = fopen(metadataFile, "wb");
        if (f == NULL)
        {
            MCO_THROW RuntimeError("Failed to create file");
        }
        McoDatabase::checkStatus(mco_db_save_metadata(f, fwriter, dbh, MCO_YES));
        if (fclose(f) != 0)
        {
            MCO_THROW RuntimeError("Failed to close file");
        }
#endif /* MCO_CFG_SAVELOAD_SUPPORTED */
}

void McoSqlEngine::saveMetadata(void *stream, mco_stream_write streamWriter)
{
#ifdef MCO_CFG_SAVELOAD_SUPPORTED
        McoDatabase::checkStatus(mco_db_save_metadata(stream, streamWriter, dbh, MCO_YES));
#endif /* MCO_CFG_SAVELOAD_SUPPORTED */
}

char* McoSqlEngine::getName()
{
    mco_database_h hdb = dbptr_by_handle(dbh);
    return hdb->name;
}

void McoSqlEngine::detach()
{
    flags |= McoSqlOpenParameters::PRESERVE_SHARED_DATABASE;
}

int64_t McoSqlEngine::getLastAutoid()
{
    uint8 autoid;
    MCO_RET ret = mco_get_last_autoid(dbh, &autoid);
    McoDatabase::checkStatus(ret);
    return (int64_t)autoid;
}


MCO_COMMIT_POLICY McoSqlEngine::setTransactionPolicy(MCO_COMMIT_POLICY policy)
{
    return mco_disk_transaction_policy(dbh, policy);
}

void McoSqlEngine::flush()
{
    MCO_RET ret = mco_disk_flush(dbh);
    McoDatabase::checkStatus(ret);
}


void McoSqlEngine::close()
{
    char name[MCO_CFG_DB_NAME_MAXSIZE + 2];
    strcpy(name, getName());
    /* close McoSql engine */
    if ((flags & McoSqlOpenParameters::FOREIGN_CONNECTION) == 0) {
        SqlEngine::close();
    }

    /* Deallocate memory used by McoSQL */
    AbstractAllocator* engineAllocator = getAllocator();
    if (engineAllocator != NULL)
    {
        engineAllocator->release();
    }
    if ((flags & (McoSqlOpenParameters::FOREIGN_CONNECTION|McoSqlOpenParameters::INITIALIZE_DATABASE|McoSqlOpenParameters::PRESERVE_SHARED_DATABASE)) == McoSqlOpenParameters::INITIALIZE_DATABASE) {
        MCO_RET ret = mco_db_close(name);
        if (ret != MCO_E_OPENED_SESSIONS)
        {
            McoDatabase::checkStatus(ret);
        }
    }

    /* mco_sys_free the memory */
    if (addr != NULL)
    {
        mco_sys_free(addr);
        addr = NULL;
    }

    /* shutdown database engine */
    if (flags & McoSqlOpenParameters::START_MCO_RUNTIME) {
        mco_runtime_stop();
    }
}

void McoSqlEngine::disconnect()
{
    /* close McoSql engine */
    SqlEngine::close();

    /* Deallocate memory used by McoSQL */
    AbstractAllocator* engineAllocator = getAllocator();
    if (engineAllocator != NULL)
    {
        engineAllocator->release();
    }

    /* mco_sys_free the memory */
    if (addr != NULL)
    {
        mco_sys_free(addr);
        addr = NULL;
    }
}

#ifdef MULTITHREADING_SUPPORT

    McoSqlSession::McoSqlSession(McoSqlEngine* engine): McoSqlEngine(engine, ((AbstractDynamicAllocator*)engine
                                 ->getAllocator())->clone())
    {
        MemoryManager::allocator->attach();
        McoDatabase::checkStatus(mco_db_connect(engine->getName(), &dbh));
        db.connect(engine->db, dbh);
        saveAllocator = MemoryManager::allocator->replace(getAllocator());
    }

    McoSqlSession::~McoSqlSession()
    {
        McoDatabase::checkStatus(mco_db_disconnect(dbh));
        ((AbstractDynamicAllocator*)getAllocator())->destroy();
        MemoryManager::allocator->replace(saveAllocator);
        MemoryManager::allocator->detach();
    }


#endif

#ifdef MCO_CFG_SAVELOAD_SUPPORTED
static bool saveCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
{
    ((McoSqlEngine*)engine)->save(params[1]);
    return true;
}

static InteractiveSqlExtension saveCmd("save", "save <file-path>", "Save database image to specified file",
                                       &saveCommand, 1, 1);

static bool saveMetadataCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
{
    ((McoSqlEngine*)engine)->saveMetadata(params[1]);
    return true;
}

static InteractiveSqlExtension saveMetadataCmd("savemeta", "savemeta <file-path>", "Save database metadata to specified file",
                                       &saveMetadataCommand, 1, 1);

static bool detachCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
{
    ((McoSqlEngine*)engine)->detach();
    return false;
}
static InteractiveSqlExtension detachCmd("detach", "detach", "Do not close database on exit (leave it in shared memory)",  &detachCommand, 0, 0);
#endif


#ifdef MCO_CFG_SUPPORT_DISK_OBJECTS

#define BACKUP_BUFFER_SIZE (1024*1024)

struct CopyFileContext {
    MCO_RET    ret;
    int        threadId;
    int        nThreads;
    mco_file_h srcFile;
    mco_file_h dstFile;
    mco_offs_t fileSize;
    Thread     thread;
};

struct BackupUnwind
{
    mco_disk_manager_h dm;
    mco_file_h dbsFile;
    mco_file_h logFile;
    mco_offs_t redoLogLimit;
    CopyFileContext* context;
    bool normal;

    BackupUnwind(mco_disk_manager_h mgr, CopyFileContext* ctx, mco_file_h dbs, mco_file_h log, mco_offs_t limit)
    : dm(mgr), dbsFile(dbs), logFile(log), redoLogLimit(limit), context(ctx), normal(false) {}

    ~BackupUnwind() {
        mco_disk_sync_lock(dm->mutex);
        dm->hdr->redo_log_limit = redoLogLimit;
        mco_disk_sync_unlock(dm->mutex);

        if (dbsFile->close(dbsFile) != 0 && normal) {
           MCO_THROW RuntimeError("Failed to close database backup file");
        }
        if (logFile->close(logFile) != 0 && normal) {
            MCO_THROW RuntimeError("Failed to close log backup file");
        }
        delete[] context;
     }
};


#ifndef _INTEGRITY
void concurrenFileCopy(void* arg)
#else
void concurrenFileCopy(void)
#endif
{
    CopyFileContext* ctx;
    char* buf = new char[BACKUP_BUFFER_SIZE];
#ifdef _INTEGRITY
    Address arg;
    Error   e;
    Task    task = CurrentTask();

    e = GetTaskIdentification( task, &arg );
    assert ( e == Success );
#endif
    ctx = (CopyFileContext*)arg;

    ctx->ret = MCO_S_OK;
    for (mco_offs_t pos = ctx->threadId*BACKUP_BUFFER_SIZE; pos < ctx->fileSize; pos += ctx->nThreads*BACKUP_BUFFER_SIZE) {
        mco_size_sig_t chunk_size = pos + BACKUP_BUFFER_SIZE > ctx->fileSize ? (mco_size_t)(ctx->fileSize - pos) : BACKUP_BUFFER_SIZE;
        mco_size_sig_t rc = ctx->srcFile->pread(ctx->srcFile, pos, buf, chunk_size);
        if (rc < 0) {
            ctx->ret = MCO_E_DISK_READ;
            break;
        } else if (rc == 0) {
            break; /* end of file */
        }
        chunk_size = rc;
        rc = ctx->dstFile->pwrite(ctx->dstFile, pos, buf, chunk_size);
        if (rc != chunk_size) {
            ctx->ret = MCO_E_DISK_WRITE;
            break;
        }
    }
    delete[] buf;
}


void  McoSqlEngine::backup(char const *dbsFile, char const* logFile, int nThreads)
{
    mco_db_connection_h con = (mco_db_connection_h)dbh;
    mco_disk_manager_h dm = disk_manager_by_handle(con);
    mco_file_h dbsBck;
    mco_file_h logBck;
    mco_offs_t fileSize;
    int i;

    if (!dm->hdr || !*mco_open_file_ptr) {
        MCO_THROW RuntimeError("Online backup is possible only for disk database");
    }
    if (dm->hdr->log_type != REDO_LOG) {
        MCO_THROW RuntimeError("Online backup is possible only for REDO log");
    }
    dbsBck = (*mco_open_file_ptr)(dbsFile, MCO_FILE_OPEN_TRUNCATE, NULL);
    if (dbsBck == NULL) {
        MCO_THROW RuntimeError("Failed to create database backup file");
    }
    logBck = (*mco_open_file_ptr)(logFile, MCO_FILE_OPEN_TRUNCATE, NULL);
    if (logBck == NULL) {
        dbsBck->close(dbsBck);
        MCO_THROW RuntimeError("Failed to create log backup file");
    }
    CopyFileContext* ctx = new CopyFileContext[nThreads];
    BackupUnwind unwind(dm, ctx, dbsBck, logBck, dm->hdr->redo_log_limit);

    mco_disk_sync_lock(dm->mutex);
    dm->hdr->redo_log_limit = (mco_offs_t)-1; /* infinity */
    fileSize = dm->root_page->database_size;
    mco_disk_sync_unlock(dm->mutex);

    // Copy database file
    for (i = 0; i < nThreads; i++) {
        ctx[i].threadId = i;
        ctx[i].nThreads = nThreads;
        ctx[i].srcFile = dm->file;
        ctx[i].dstFile = dbsBck;
        ctx[i].fileSize = fileSize;
        ctx[i].thread.create(concurrenFileCopy, &ctx);
    }
    for (i = 0; i < nThreads; i++) {
        ctx[i].thread.join();
    }
    for (i = 0; i < nThreads; i++) {
        McoDatabase::checkStatus(ctx[i].ret);
    }

    // Copy log file
    fileSize = (dm->hdr->log_size + dm->hdr->page_size - 1) & ~(dm->hdr->page_size-1);
    for (i = 0; i < nThreads; i++) {
        ctx[i].threadId = i;
        ctx[i].nThreads = nThreads;
        ctx[i].srcFile = dm->log;
        ctx[i].dstFile = logBck;
        ctx[i].fileSize = fileSize;
        ctx[i].thread.create(concurrenFileCopy, &ctx);
    }
    for (i = 0; i < nThreads; i++) {
        ctx[i].thread.join();
    }
    for (i = 0; i < nThreads; i++) {
        McoDatabase::checkStatus(ctx[i].ret);
    }
    unwind.normal = true;
}


static bool backupCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
{

    ((McoSqlEngine*)engine)->backup(params[1], params[2], nParams >= 3 ? atoi(params[3]) : 1);
    return true;
}

static InteractiveSqlExtension backupCmd("backup", "backup <database-file-path> <log-file-path> <n-threads=1>", "Online backup of disk database to the specified files",
                                         &backupCommand, 2, 3);

#endif

