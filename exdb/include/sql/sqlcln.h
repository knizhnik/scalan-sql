#ifndef __SQLCLN_H__
    #define __SQLCLN_H__

    #include "sqlcpp.h"

    namespace McoSql
    {

        class TcpIpSocket;
        class DataSourceStub;

        /**
         * Client interface for sending SQL requests to the server
         */
        class RemoteSqlEngine: public SqlEngine
        {
                TcpIpSocket* socket;
                size_t txBufSize;
                char* txBuf;
                size_t dataSourceMark;
                DataSourceStub* activeDataSource;

                static void responseReader(DataSourceStub* ds, void* arg);
                static void responseDestructor(DataSourceStub* ds, void* arg);
                String* readResponse();

            public:
                /**
                 * Prepared queries are not supported
                 */
                void prepare(PreparedStatement &stmt, char* sql, ...);

                /**
                 * Remotely execute query at server
                 */
                DataSource* vexecute(Transaction* trans, char const* sql, va_list* list, Value** array, size_t
                                     &nRecords);

                /**
                 * Open connection with server
                 * @param hostname name of the where server is located
                 * @param port server port
                 * @param maxConnectAttempts maximal attempts of connection to server, interval between
                 * each connection attempt is one second, so totally connection will be performed
                 * during maxConnectionAttempts seconds
                 * @return <code>true</code> if connection successfully established, <code>false</code> otherwise
                 */
                bool open(char const* hostname, int port, int maxConnectAttempts = 10);

                /**
                 * Close connection with server
                 */
                void close();

                /**
                 * Check if engine is local or remote
                 */
                virtual bool isRemote();

                /** Constructor of SQL engine remote interface
                 * @param txBufSize size of transmit buffer (used to serialize requests to be sent to the server)
                 */
                RemoteSqlEngine(size_t txBufSize = 64 * 1024);

                /**
                 * Destructor of SQL engine remote interface
                 */
                ~RemoteSqlEngine();
        };


        /**
         * Client interface for dsitrbuted execution of SQL queries
         */
        class DistributedSqlEngine: public SqlEngine
        {
            struct Node {
                TcpIpSocket* socket;
                char* address;
                int port;
                int reconnectCount;
                int reconnectInterval;

                Node();
                ~Node();
            };

            struct Shard {
                int master;
                int executor;
                int nLiveReplicas;
                DataSourceStub* result;

                Shard() : master(0), nLiveReplicas(0), result(NULL) {}
            };

            size_t txBufSize;
            char* txBuf;
            size_t dataSourceMark;
            int currDataSource;
            int nShards;
            int nReplicas;
            bool replication;
            unsigned currShard;
            Node* nodes;
            Shard* shards;

            static void responseReader(DataSourceStub* ds, void* arg);
            static void responseDestructor(DataSourceStub* ds, void* arg);
            char* readResponse(bool throwException = true);
            bool socketRead(int i, void* buf, size_t bufSize, int& errorCode);
            void disconnectReplica(int node);
            void reconnectReplica(int node);

            DataSource* mergeResults(NewMemorySegment& mem, char const* sql, DataSourceStub* ds);
            size_t copy(DataSource* ds, char const* sql, int dst);

          public:
            /**
             * Prepared queries are not supported
             */
            void prepare(PreparedStatement &stmt, char* sql, ...);

            /**
             * Remotely execute query at server
             */
            DataSource* vexecute(Transaction* trans, char const* sql, va_list* list, Value** array, size_t& nRecords);


            typedef enum { SQL_REPLICATION = 0, HA_REPLICATION } ReplicationType;
            /**
             * Open connection with servers
             * @param nodes array with node names and ports ("NAME:PORT")
             * @param nNodes number of nodes
             * @param nReplicas number of replicas, should be diveder of nNodes
             * @param replType replication method
             * @param maxConnectAttempts maximal attempts of connection to server, interval between
             * each connection attempt is one second, so totally connection will be performed
             * during maxConnectionAttempts seconds
             * @param bad_node if this parameter is not null and open is failed, then index of not unavailable node is stored at this location 
             * @return <code>true</code> if connection successfully established, <code>false</code> otherwise
             */
            bool open(char const* const* nodes, int nNodes, int nReplicas = 1, ReplicationType replType = SQL_REPLICATION, int maxConnectAttempts = 10, int* badNode = NULL);

            /**
             * Close connection with servers
             */
            void close();

            /**
             * Check if engine is local or remote
             */
            virtual bool isRemote();

            /**
             * Handler of replica connection failure.
             * Can be redefined in derived class.
             * By default just write message in the log file
             */
            virtual void onReplicaConnectionFailure(int node, int errorCode);

            /**
             * Handler of replica reconect
             * Can be redefined in derived class.
             * By default just write message in the log file
             */
            virtual void onReplicaReconnect(int node);

            /** Constructor of SQL engine remote interface
             * @param txBufSize size of transmit buffer (used to serialize requests to be sent to the server)
             */
            DistributedSqlEngine(size_t txBufSize = 64 * 1024);

            /**
             * Destructor of SQL engine remote interface
             */
            ~DistributedSqlEngine();
        };
    }

#endif
