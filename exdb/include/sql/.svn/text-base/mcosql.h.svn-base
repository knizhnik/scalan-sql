/*******************************************************************
 *                                                                 *
 *  mcosql.h                                                       *
 *                                                                 *
 *  This file is a part of the eXtremeDB source code               *
 *  Copyright (c) 2001-2005 McObject LLC                           * 
 *  All Rights Reserved                                            *
 *                                                                 *
 *******************************************************************/
#ifndef __MCOSQL_H__
    #define __MCOSQL_H__

    #include "sqlcpp.h"
    #include "mcoapi.h"

    class McoSqlSession;

    class McoSqlOpenParameters
    {
        public:
            enum OpenFlags
            {
                    ALLOCATE_MEMORY = 1,  
                      /* Allocate memory buffer of specified size for eXtremeDB database*/
                      /* using malloc function (value of mapAddress parameter is ignored).*/
                      /* If this flag is not set, specified value of mapAddress parameter is used.*/
                      /* This flag is ignored for shared memory database, in which case mapAddress*/
                      /* always specifies mapping address. */
                    SET_ERROR_HANDLER = 2,  /* Set McoSql specific error handler. */
                    START_MCO_RUNTIME = 4,  /* Start MCO runtime.*/
                    SET_SQL_ALLOCATOR = 8,  /* Set McoSql allocator.*/
                    INITIALIZE_DATABASE = 16,  /* Initialize new database instance (call mco_db_open).*/
                    PRESERVE_SHARED_DATABASE = 32, /* Do not close database, keeing it in shared memory*/
                    FOREIGN_CONNECTION = 64, /* Connection was eestablished outside SqlEngine: do not perfrom disconnect and database close */
                    DEFAULT_OPEN_FLAGS = ALLOCATE_MEMORY | SET_ERROR_HANDLER | START_MCO_RUNTIME | SET_SQL_ALLOCATOR | INITIALIZE_DATABASE 
            };

            char const* databaseName;
            mco_dictionary_h dictionary;
            size_t mainMemoryDatabaseSize;
            void* mainMemoryDatabaseAddress;
            size_t maxConnections;
            size_t maxTransactionSize;
            size_t mainMemoryPageSize;
            uint2 hashLoadFactor;
            int flags;
            int modeMask;

            char* savedImage;

            char* diskDatabaseFile;
            char* diskDatabaseLogFile;
            mco_offs_t diskDatabaseMaxSize;
            void* diskCacheMemoryAddress;
            size_t diskCacheSize;
            int diskPageSize;
            MCO_LOG_TYPE logType;
            mco_offs_t redoLogLimit;
            MCO_COMMIT_POLICY defaultCommitPolicy;
            MCO_TRANS_SCHED_POLICY transSchedPolicy;
            mco_stream_read streamReader;
            void* stream;

            uint8 maxTransTime; /* maximal transaction time, 0 to disable */

            mco_device_t* devices;
            size_t        n_devices;

            McoSql::Table** tables;
            size_t          n_tables;

            McoSql::SqlOptimizerParameters optimizerParams;

            McoSql::DynamicAllocator* allocator;

            size_t maxClasses;
            size_t maxIndexes;
            size_t maxDictionarySize;

            void setDefaultValues();

            McoSqlOpenParameters()
            {
                    setDefaultValues();
            }

            McoSqlOpenParameters(char const* name, mco_dictionary_h dictionary, size_t size, size_t pageSize = 128,
#ifndef _AIX
                                 void* mapAddress = (void*)0x20000000, 
#else
                                 void* mapAddress = (void*)0x30000000, 
#endif
                                 size_t maxTransSize = 0, 
                                 int flags = DEFAULT_OPEN_FLAGS, 
                                 char const* savedImage = NULL);
    };

    class McoSqlEngine: public McoSql::SqlEngine
    {
            friend class McoSqlSession;
        protected:
            mco_db_h dbh;
            McoDatabase db;
            void* addr;
            size_t pageSize;
            bool diskDatabase;
            int flags;
            char * license_key;
            McoSqlEngine(McoSqlEngine* engine, McoSql::AbstractAllocator* allocator);
            virtual void init();

        public:
            McoSqlEngine(); /* default constructor*/

            /**
             * Open database: open eXtremeDB database, eXtremeDB SQL mapper and McoSql engine.
             * @param openParams parameters of database open
             * @throws RuntimeException in case of error
             */
            void open(McoSqlOpenParameters const &params);

            /**
             * Open database: open eXtremeDB SQL mapper and McoSql engine given eXtremeDB database handle
             * @param dbh eXtremeDB database handle
             * @throws RuntimeException in case of error
             */
             void open(mco_db_h dbh);

            /**
             * Open database: open eXtremeDB database, eXtremeDB SQL mapper and McoSql engine.
             * @param name database name 
             * @param dictionary database scheme definition
             * @param size eXtremeDB database size (bytes)
             * @param pageSize eXtremeDB page size (bytes)
             * @param mapAddress eXtremeDB map address (for shared memory database)
             * @param maxTransSize eXtremeDB limit for transaction size (object)
             * If value of this parameter is 0, then default transaction size limit is used.
             * @param flags optional open flags (see OpenFlags enum)
             * @param databaseFile database file to be loaded 
             * @throws RuntimeException in case of error
             */
            void open(char const* name, mco_dictionary_h dictionary, size_t size, size_t pageSize = 128, void*
#ifndef _AIX
                      mapAddress = (void*)0x20000000, 
#else
                      mapAddress = (void*)0x30000000, 
#endif
                      size_t maxTransSize = 0, int flags = McoSqlOpenParameters
                      ::DEFAULT_OPEN_FLAGS, char const* databaseFile = NULL);


            /**
             * Create connection pool needed for parallel query execution.
             */
            void createConnectionPool();

            /**
             * Destroy connection pool created by  createConnectionPool()
             */
            void destroyConnectionPool();

            /**
             * Save database to the specified file.
             * @param databaseFile path to the file where database has to be stored
             */
            void save(char const *databaseFile);
            void save(void *stream, mco_stream_write streamWriter);

            /**
             * Save database metadata compatible with xsqlcmd utility to the specified file.
             * @param metadataFile path to the file where database metadata has to be stored
             */
            void saveMetadata(char const *metadataFile);
            void saveMetadata(void *stream, mco_stream_write streamWriter);

            /**
             * Online backup of disk database
             * @param dbFile path to database backup file
             * @param logFile path to transaction log backup file
             * @param nThreads number of concurrent threads
             */
            void backup(char const *dbFile, char const* logFile, int nThreads = 1);
        
            /**
             * Close database.
             */
            virtual void close();

            /**
             * Diconnect database.
             */
            void disconnect();

            /**
             * Get database handle which can be used by eXtremeDB API functions.
             */
            mco_db_h getHandle()
            {
                    return dbh;
            }

            /**
             * Get autoid of last allocated objects 
             */
            int64_t getLastAutoid();
            
            /**
             * Change transaction commit policy for the specified database connections
             * @param policy one of MCO_COMMIT_ASYNC_FLUSH, MCO_COMMIT_SYNC_FLUSH or MCO_COMMIT_NO_FLUSH
             * @return previous policy
             */
            MCO_COMMIT_POLICY setTransactionPolicy(MCO_COMMIT_POLICY policy);

            /**
             * Flush all changes done by committed transactions to the disk.
             * This method can be used to save to the disk changes when MCO_COMMIT_NO_FLUSH transaction policy is used
             */
            void flush();

            /**
             * Get database name
             */
            char* getName();

            /** 
             * Do not close eXtremeDB database kleeping it in cshared memory
             */
            void detach();

            /**
             * Get eXtremeDB page size.
             */
            size_t getPageSize();

            /**
             * Set license key (for license-protected packages only)
             * The license key have to be set before call of any of open() method
             */
            void license( char * key ) {
                license_key = key;
            }
 
            virtual ~McoSqlEngine();
    };

    #if MULTITHREADING_SUPPORT 
        /**
         * SQL engine for multithreaded applications.
         * This engine defines a multithreaded allocator which provides
         * each threads with its own local allocator.
         * This class shall be used in conjunction with the McoSqlSession class. Newer versions
         * of the eXtremeDB require each thread to have its own connection to the database
         */
        class McoMultithreadedSqlEngine: public McoSqlEngine{}
        ;

        /**
         * This class must be used to share a single database connection between multiple threads. 
         * Multithreaded applications can have their own SQL engines for each connected thread (i.e. create
         * their own instance of McoSqlEngine). It is, however, not convenient and not very
         * efficient. In many cases the application does not start threads by itself, rather the thread pool is
         * maintained by some external component and the user code is executed in the context of
         * one of those threads. A typical example is Apache servlets.<p>
         * McoSqlSession allows multiple threads to share a single McoMultithreadedSqlEngine instance.
         * The constructor of this class uses the <code>mco_db_connect</code> function to establish a connection for 
         * a particular thread.
         * The destructor of this class releases the connection by calling the <code>mco_db_disconnect</code> function
         */
        class McoSqlSession: public McoSqlEngine
        {
			    McoSql::AbstractAllocator* saveAllocator;
            public:
                /**
                 * Establish a connection for the thread via the <code>mco_db_connect</code> function 
                 * @param engine shared instance of the McoSqlEngine (it is assumed
                 * to be the McoMultithreadedSqlEngine) 
                 */
                McoSqlSession(McoSqlEngine* engine);

                /**
                 * Release the connection using the <code>mco_db_disconnect</code> function
                 */
                ~McoSqlSession();
        };

    #endif 

#endif
