#ifndef __SQLSRV_H__
    #define __SQLSRV_H__

    #include "mcosql.h"

    #if MULTITHREADING_SUPPORT 

        namespace McoSql
        {
            class Server;

            /**
             * Class performing execution of client SQL requests
             */
            class SqlServer
            {
                public:
                    typedef void(*error_handler_t)(char const* msg);

                    typedef enum { s_wait, s_active, s_done, s_canceled } session_state_t;
                    typedef struct {
                        session_state_t state;
                        unsigned int peer_addr;
                        int peer_port;
                    } session_info_t;
                    typedef void(*session_info_handler_t)(const session_info_t* info, void* context);

                    /**
                     * Starts server. This method cause server to open socket at specified port and 
                     * accept client connection requests
                     */
                    void start();

                    /**
                     * Stops server. This method stop accept thread and close server socket.
                     */
                    void stop();

                    /**
                     * Enumerates sessions in wait state then active sessions.
                     */
                    void getSessionsInfo(session_info_handler_t handler, void* context = NULL);

                    /**
                     * Registers (unregister) callback to receive new session start\stop events.
                     */
                    void regSessionEvent(session_info_handler_t handler, void* context = NULL);

                    /**
                     * Constructor of SQL server
                     * @param engine locql SQL engine
                     * @param port server port
                     * @param bufferSize size of transfer buffer. Result of client query execution will be placed 
                     * in this buffer. If result data source can not fit in buffer then query result will be 
                     * delivered to client by parts. In this case transaction lock is kept until all
                     * results are sent to the client.
                     * @param nThreads optimal number of thread spawned by server. Server will spawn as many threads
                     * as client requests arrives. Each thread process requests from the client until 
                     * client closes connection. After it thread is return to pool of idle threads or
                     * is terminated if number of threads exceeds optimal number of threads.
                     * @param listenQueueSize paremeter of socket listen function, meaning maximal number of 
                     * connections which can be concurrently accepted by server
                     * @param handler procedure for reporting client session errors
                     */
                    SqlServer(McoMultithreadedSqlEngine* engine, int port, size_t bufferSize = 64 * 1024, size_t
                              nThreads = 8, int listenQueueSize = 5, error_handler_t handler = NULL);

                    /**
                     * Server destructor
                     */
                    ~SqlServer();

                private:
                    Server* server;
            };
        }
    #endif 

#endif
