/*******************************************************************
 *                                                                 *
 *  sqlcpp.h                                                       *
 *                                                                 *
 *  This file is a part of the eXtremeDB source code               *
 *  Copyright (c) 2001-2005 McObject LLC                           * 
 *  All Rights Reserved                                            *
 *                                                                 *
 *******************************************************************/
#ifndef __SQL_H__
#define __SQL_H__

#include "dbapi.h"
#include "memmgr.h"

namespace McoSql
{

    class HashTable;
    class SymbolTable;
    class SqlEngine;
    class StmtNode;
    class TransactionNode;
    class DatabaseDescriptor;
    class Server;
    class Runtime;
    class Finally;
    class CreateDomainNode;

    #define MCO_DATABASE_NAME "eXtremeDB"

    /**
     * Placeholder for prepared statement
     */
    class PreparedStatement
    {
        friend class SqlEngine;
        AbstractAllocator* allocator;
        DynamicAllocator defaultAllocator;
        StmtNode* node;

      public:
        /**
         * Return information about result columns (for non-selectable statement returns NULL)
         */
        Iterator < Field > * describeResultColumns();
        
        /**
         * Check if prepared node is select 
         */
        bool isQuery() const;

        /**
         * Create prepared statement with user defined allocator
         * @param allocator provided by user allocator
         */
        PreparedStatement(AbstractAllocator* allocator)
        {
            node = NULL;
            this->allocator = allocator;
        }

        /**
         * Create prepared statement with default allocator
         */
        PreparedStatement()
        {
            node = NULL;
            allocator = &defaultAllocator;
        }
    };

    /**
     * Descriptor of parameter
     */
    struct ParamDesc
    {
        Type type;
        void* ptr;
        int* lenptr;
    };


    /**
     * Class for user-defined function which can be used in McoSQL
     */
    class SqlFunctionDeclaration: public DynamicObject
    {
        friend class SqlEngine;
        friend class Compiler;
        friend class FuncCallNode;
        friend class GroupNode;
      public:
        enum FuncArgs { 
            FUNC_VARARG = -1,   /* function accepting varying number of arguments */
            FUNC_AGGREGATE = -2,/* function with two arguments which can be used for aggregate calculation: call of this function in as expression with single
                                 *  argument is tranformed to function(accumulator, current_value) 
                                 */
            FUNC_MAX_ARGS = 4   /* Maximal number of function parameters */
        };
        /**
         * Global user function declaration constructor 
         * User-defined function should 
         * receive from 0 to 4 arguments of <code>Value*</code> type and return result of 
         * <code>Value*</code> type. Type of return result should be the same as declared or tpNull.
         * It is possible to return NULL, which is treated as return of SQL Null value.
         * @param type function return type
         * @param name function name
         * @param func pointer to the function
         * @param nArgs number of function arguments (if nArgs = FUNC_VARARG, then function accepts vector with varying 
         * number of arguments, if nArgs == FUNC_AGGREGATE then function should accept to arguments, but can be used in expressoin just with one argument 
         */
        SqlFunctionDeclaration(Type type, char const* name, void* func, int nArgs = FUNC_VARARG);

        /**
         * Per-engine user function declaration constructor 
         * User-defined function should 
         * receive from 0 to 4 arguments of <code>Value*</code> type and return result of 
         * <code>Value*</code> type. Type of return result should be the same as declared or tpNull.
         * It is possible to return NULL, which is treated as return of SQL Null value.
         * @param type function return type
         * @param name function name
         * @param func pointer to the function
         * @param nArgs number of function arguments (if nArgs = FUNC_VARARG, then function accepts vector with varying 
         * number of arguments, if nArgs == FUNC_AGGREGATE then function should accept to arguments, but can be used in expressoin just with one argument 
         * @param ctx function call context  (is passed to function as first parameter if not null)
         */
        SqlFunctionDeclaration(Type type, char const* name, void* func, int nArgs, void* ctx);

        /**
         * Dynamically create function constructor 
         * User-defined function should 
         * receive from 0 to 4 arguments of <code>Value*</code> type and return result of 
         * <code>Value*</code> type. Type of return result should be the same as declared or tpNull.
         * It is possible to return NULL, which is treated as return of SQL Null value.
         * @param type function return type
         * @param elemType function return element type (is used only for arrays)
         * @param name function name
         * @param func pointer to the function
         * @param args vector of function arguments 
         * @param engine SqlEngine in which this function is registered
         */
        SqlFunctionDeclaration(Type type, Type elemType, char const* name, void* func, Vector<Field>* args);

        Type getType() const { return type; }
        char const* getName() const { return name; } 
        int getNumberOfArguments() const { return nArgs; }
        void* getContext() const { return ctx; }

        Type const type;
        Type const elemType;
        char const* const name;
        int const nArgs;
        Vector<Field>* args;
        void* const ctx;

      private:
        static SqlFunctionDeclaration* chain;
        SqlFunctionDeclaration* next;
        void* func;
    };

    #if SQL_INTERPRETER_SUPPORT
    /**
     * Class for interactive SQL extension definition
     */
    class InteractiveSqlExtension: public DynamicObject
    {
        friend class SqlEngine;
      public:
        /**
         * Callback function prototype
         * @param engine reference to SqlEngine instance
         * @param nParams number of command parameters
         * @param params array with parsed command parameters (quote character is removed)
         * params[0] contains name of the command
         * @param in input stream as passed to SqlEngine::main method
         * @param out output stream as passed to SqlEngine::main method
         * @param err error stream as passed to SqlEngine::main method
         * @return <code>true</code> to continue interactive SQL loop, <code>false</code> to terminate
         */
        typedef bool(*command_t)(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*
                                 &err);

        /**
         * Constructor of interactive SQL extension
         * This mechanism can be used to implement database-specific commands for interactive SQL.
         * (SqlEngine::main method) 
         * @param name command name 
         * @param syntax command syntax
         * @param description command description
         * @param callback pointer to the function
         * @param minArgs minimum number of command arguments
         * @param maxArgs maximum number of command arguments
         */
        InteractiveSqlExtension(char const* name, char const* syntax, char const* description, command_t
                                callback, int minArgs, int maxArgs);

      private:
        InteractiveSqlExtension(){}

        static InteractiveSqlExtension* chain;
        InteractiveSqlExtension* next;

        char const* name;
        char const* syntax;
        char const* description;
        command_t callback;
        int minArgs;
        int maxArgs;
    };
    #endif
    /**
     * Tune parameter for SQL optimizer
     */
    class SqlOptimizerParameters
    {
      public:
        /**
         * If optimization is enabled SQL engine will choose order of query conjuncts execution based 
         * on the estimation of their execution cost, if optimization is disable, 
         * then conjuncts will be executed in the same order as them are specified in the query.
         */
        bool enableCostBasedOptimization;

        /**
         * Index is not applicable
         */
        int sequentialSearchCost;
        
        /**
         * Cost of searching using non-unique index. It is used only of equality comparisons and is added to eqCost, eqStringConst...
         */
        int notUniqCost;
            
        /**
         * Cost of index search of key of scalar, reference or date type 
         */
        int eqCost;
            
        /**
         * Cost of search in boolean index
         */
        int eqBoolCost;
        
        /**
         * Cost of search in string index
         */
        int eqStringCost;
        
        /**
         * Cost of search in real index
         */
        int eqRealCost;
        
        /**
         * Cost for the following comparison operations: &lt; &lt;= &gt; &gt;=
         */
        int openIntervalCost;
            
        /**
         * Cost for BETWEEN operation
         */
        int closeIntervalCost;
        
        /**
         * Cost of index search of collection elements
         */
        int scanSetCost;
        
        /**
         * Cost of boolean OR operator
         */
        int orCost;
        
        /**
         * Cost of boolean AND operator
         */
        int andCost;
        
        /**
         * Cost of IS NULL operator
         */
        int isNullCost;
        
        /**
         * Cost of LIKE operator
         */
        int patternMatchCost;
        
        /**
         * Cost of each extra level of indirection, for example in condition (x.y.z = 1) indirection level is 2 and in condition (x = 2) it is 0.
         */
        int indirectionCost;
        
        /**
         * Cost of prefix search using Patricia trie
         */
        int prefixMatchCost;
        
        /**
         * Cost of exact match search using Patricia trie
         */
        int exactMatchCost;

        /**
         * Default constructor setting default values of parameters
         */
        SqlOptimizerParameters();
    };

    /**
     * SQL engine
     * Only one instance of this class can be used at the same time.
     */
    class SqlEngine
    {
        friend class Server;
        friend class Runtime;
        friend class Finally;
        friend class Compiler;
        friend class DatabaseDescriptor;
        friend class CreateDomainNode;
        friend class CreateFunctionNode;
        friend class DropFunctionNode;

      public:
        /**
         * Check if engine is local or remote
         */
        virtual bool isRemote();

        /**
         * Default constructor
         */
        SqlEngine();


        virtual ~SqlEngine();

        /**
         * Open engine - this method loads database scheme from underlying storage.
         * @param db implementation of underlying storage level
         * If this parameter is not specified, 
         * then value of static field <code>Database::instance</code> will be used.
         * <code>OutOfMemory</code> exception will be thrown
         */
        virtual void open(Database* db = NULL);

        /**
         * Trigger trace output
         * @param enable if <code>true</code> then engine will output trace information about
         *  query execution plans
         */
        void trace(bool enable);

        /**
         * Get database.
         * @return database
         */
        Database* database();

        /**
         * Execute SQL query statement with varying list of parameters.
         * Statement will be executed in separate transaction which will be automatically committed after the statement execution.
         * It is possible to specify varying list of parameters.
         * Use the following placeholders for parameters in query:
         * <table border>
         * <tr><th>Placeholder</th><th>Parameter C type</th></tr>
         * <tr><td><code>%b</code></td><td><code>bool</code></td></tr>
         * <tr><td><code>%i</code></td><td><code>int</code></td></tr>
         * <tr><td><code>%u</code></td><td><code>unsigned</code></td></tr>
         * <tr><td><code>%l</code></td><td><code>int64_t</code></td></tr>
         * <tr><td><code>%p</code></td><td><code>int64_t* (treated as reference)</code></td></tr>
         * <tr><td><code>%f</code></td><td><code>double</code></td></tr>
         * <tr><td><code>%t</code></td><td><code>time_t</code></td></tr>
         * <tr><td><code>%s</code></td><td><code>char*</code></td></tr>
         * <tr><td><code>%w</code></td><td><code>wchar_t*</code></td></tr>
         * <tr><td><code>%v</code></td><td><code>Value*</code></td></tr>
         * </table>
         * @param sql string with SQL statement
         * @return In case of queries, returns result data source. In all other cases returns NULL.
         */
        DataSource* executeQuery(char const* sql, ...);

        /**
         * Execute SQL query statement with extracted list of parameters.
         * Statement will be executed in separate transaction which will be automatically committed after the statement execution.
         * @param sql string with SQL statement
         * @param params varying list of SQL statement parameters
         * @return In case of queries, returns result data source. In all other cases returns NULL.
         * @see DataSource* executeQuery(char const* sql, ...)
         */
        DataSource* vexecuteQuery(char const* sql, va_list* params);

        /**
         * Execute SQL query statement with array of parameter values.
         * Statement will be executed in separate transaction which will be automatically committed after the statement execution.
         * @param sql string with SQL statement
         * This string can contains references
         * to array elements represented as %0...%9. There can be no more than 10 parameters.
         * @param params array of parameter values
         * @return In case of queries, returns result data source. In all other cases returns NULL.
         */
        DataSource* vexecuteQuery(char const* sql, Value** params);

        /**
         * Execute SQL update or delete statement with varying list of parameters.
         * Statement will be executed in separate transaction which will be automatically committed after the statement execution.
         * It is possible to specify varying list of parameters.
         * Use the following placeholders for parameters in query:
         * <table border>
         * <tr><th>Placeholder</th><th>Parameter C type</th></tr>
         * <tr><td><code>%b</code></td><td><code>bool</code></td></tr>
         * <tr><td><code>%i</code></td><td><code>int</code></td></tr>
         * <tr><td><code>%u</code></td><td><code>unsigned</code></td></tr>
         * <tr><td><code>%l</code></td><td><code>int64_t</code></td></tr>
         * <tr><td><code>%p</code></td><td><code>int64_t* (treated as reference)</code></td></tr>
         * <tr><td><code>%f</code></td><td><code>double</code></td></tr>
         * <tr><td><code>%t</code></td><td><code>time_t</code></td></tr>
         * <tr><td><code>%s</code></td><td><code>char*</code></td></tr>
         * <tr><td><code>%w</code></td><td><code>wchar_t*</code></td></tr>
         * <tr><td><code>%v</code></td><td><code>Value*</code></td></tr>
         * <tr><td><code>%r</code></td><td><code>struct mapped to the record (record fields with string type should have "char*" type)</code></td></tr>
         * <tr><td><code>%R</code></td><td><code>struct mapped to the record (record fields with char&lt;N&gt; type should have "char[N]" type)</code></td></tr>
         * </table>
         * @param sql string with SQL statement
         * @return number of affected records or -1 if precise number is unknown
         */
        int executeStatement(char const* sql, ...);

        /**
         * Execute SQL UPDATE or DELETE statement with extracted list of parameters.
         * Statement will be executed in separate transaction which will be automatically committed after the statement execution.
         * @param sql string with SQL statement
         * @param params varying list of SQL statement parameters
         * @return number of affected records or -1 if precise number is unknown
         * @see int executeStatement(char const* sql, ...)
         */
        int vexecuteStatement(char const* sql, va_list* params);

        /**
         * Execute SQL update or delete statement with array of parameter values.
         * Statement will be executed in separate transaction which will be automatically committed after the statement execution.
         * @param sql string with SQL statement
         * This string can contain references
         * to array elements represented as %0...%9. There can be no more than 10 parameters.
         * @param params array of parameter values
         * @return number of affected records or -1 if precise number is unknown
         */
        int vexecuteStatement(char const* sql, Value** params);

        /**
         * Execute SQL query statement with varying list of parameters.
         * Statement will be executed in the context of specified transaction.
         * It is possible to specify varying list of parameters.
         * Use the following placeholders for parameters in query:
         * <table border>
         * <tr><th>Placeholder</th><th>Parameter C type</th></tr>
         * <tr><td><code>%b</code></td><td><code>bool</code></td></tr>
         * <tr><td><code>%i</code></td><td><code>int</code></td></tr>
         * <tr><td><code>%u</code></td><td><code>unsigned</code></td></tr>
         * <tr><td><code>%l</code></td><td><code>int64_t</code></td></tr>
         * <tr><td><code>%p</code></td><td><code>int64_t*</code></td></tr>
         * <tr><td><code>%f</code></td><td><code>double</code></td></tr>
         * <tr><td><code>%t</code></td><td><code>time_t</code></td></tr>
         * <tr><td><code>%s</code></td><td><code>char*</code></td></tr>
         * <tr><td><code>%w</code></td><td><code>wchar_t*</code></td></tr>
         * <tr><td><code>%v</code></td><td><code>Value*</code></td></tr>
         * </table>
         * @param trans current transaction
         * @param sql string with SQL statement
         * @return In case of queries, returns result data source. In all other cases returns NULL.
         */
        DataSource* executeQuery(Transaction* trans, char const* sql, ...);

        /**
         * Execute SQL query statement with extracted list of parameters.
         * Statement will be executed in the context of specified transaction.
         * @param trans current transaction
         * @param sql string with SQL statement
         * @param params varying list of SQL statement parameters
         * @return In case of queries, returns result data source. In all other cases returns NULL.
         * @see DataSource* executeQuery(char const* sql, ...)
         */
        DataSource* vexecuteQuery(Transaction* trans, char const* sql, va_list* params);

        /**
         * Execute SQL QUERY statement with array of parameter values.
         * Statement will be executed in separate transaction which will be automatically committed after the statement execution.
         * @param trans current transaction
         * @param sql string with SQL statement. This string can contain references
         * to array elements represented as %0...%9. There can be no more than 10 parameters.
         * @param params array of parameter values
         * @return In case of queries, returns result data source. In all other cases returns NULL.
         */
        DataSource* vexecuteQuery(Transaction* trans, char const* sql, Value** params);


        /**
         * Execute SQL UPDATE or DELETE statement with varying list of parameters.
         * Statement will be executed in the context of specified transaction.
         * It is possible to specify varying list of parameters.
         * Use the following placeholders for parameters in query:
         * <table border>
         * <tr><th>Placeholder</th><th>Parameter C type</th></tr>
         * <tr><td><code>%b</code></td><td><code>bool</code></td></tr>
         * <tr><td><code>%i</code></td><td><code>int</code></td></tr>
         * <tr><td><code>%u</code></td><td><code>unsigned</code></td></tr>
         * <tr><td><code>%l</code></td><td><code>int64_t</code></td></tr>
         * <tr><td><code>%p</code></td><td><code>int64_t*</code></td></tr>
         * <tr><td><code>%f</code></td><td><code>double</code></td></tr>
         * <tr><td><code>%t</code></td><td><code>time_t</code></td></tr>
         * <tr><td><code>%s</code></td><td><code>char*</code></td></tr>
         * <tr><td><code>%w</code></td><td><code>wchar_t*</code></td></tr>
         * <tr><td><code>%v</code></td><td><code>Value*</code></td></tr>
         * <tr><td><code>%r</code></td><td><code>struct mapped to the record (record fields with string type should have "char*" type)</code></td></tr>
         * <tr><td><code>%R</code></td><td><code>struct mapped to the record (record fields with char&lt;N&gt; type should have "char[N]" type)</code></td></tr>
         * </table>
         * @param trans current transaction
         * @param sql string with SQL statement
         * @return number of affected records or -1 if precise number is unknown
         */
        int executeStatement(Transaction* trans, char const* sql, ...);

        /**
         * Execute SQL UPDATE or DELETE statement with extracted list of parameters.
         * Statement will be executed in the context of specified transaction.
         * @param trans current transaction
         * @param sql string with SQL statement
         * @param params varying list of SQL statement parameters
         * @return number of affected records or -1 if precise number is unknown
         * @see int executeStatement(char const* sql, ...)
         */
        int vexecuteStatement(Transaction* trans, char const* sql, va_list* params);

        /**
         * Execute SQL UPDATE or DELETE statement with array of parameter values.
         * Statement will be executed in separate transaction which will be automatically committed after the statement execution.
         * @param trans current transaction
         * @param sql string with SQL statement. This string can contain references
         * to array elements represented as %0...%9. There can be no more than 10 parameters.
         * @param params array of parameter values
         * @return number of affected records or -1 if precise number is unknown
         */
        int vexecuteStatement(Transaction* trans, char const* sql, Value** params);

        /**
         * Prepare statement using default allocator
         * It is possible to specify varying list of parameters.
         * Use the following placeholders for parameters in query:
         * <table border>
         * <tr><th>Placeholder</th><th>Parameter C type</th></tr>
         * <tr><td><code>%b</code></td><td><code>bool</code></td></tr>
         * <tr><td><code>%i</code></td><td><code>int</code></td></tr>
         * <tr><td><code>%u</code></td><td><code>unsigned</code></td></tr>
         * <tr><td><code>%l</code></td><td><code>int64_t</code></td></tr>
         * <tr><td><code>%p</code></td><td><code>int64_t*</code></td></tr>
         * <tr><td><code>%f</code></td><td><code>double</code></td></tr>
         * <tr><td><code>%t</code></td><td><code>time_t</code></td></tr>
         * <tr><td><code>%s</code></td><td><code>char*</code></td></tr>
         * <tr><td><code>%w</code></td><td><code>wchar_t*</code></td></tr>
         * <tr><td><code>%r</code></td><td><code>struct mapped to the record (record fields with string type should have "char*" type)</code></td></tr>
         * <tr><td><code>%R</code></td><td><code>struct mapped to the record (record fields with char&lt;N&gt; type should have "char[N]" type)</code></td></tr>
         * <tr><td><code>%*b</code></td><td><code>bool*</code></td></tr>
         * <tr><td><code>%*i</code></td><td><code>int*</code></td></tr>
         * <tr><td><code>%*u</code></td><td><code>unsigned*</code></td></tr>
         * <tr><td><code>%*l</code></td><td><code>int64_t*</code></td></tr>
         * <tr><td><code>%*p</code></td><td><code>int64_t*</code></td></tr>
         * <tr><td><code>%*f</code></td><td><code>double*</code></td></tr>
         * <tr><td><code>%*t</code></td><td><code>time_t*</code></td></tr>
         * <tr><td><code>%*s</code></td><td><code>char**</code></td></tr>
         * <tr><td><code>%*w</code></td><td><code>wchar_t**</code></td></tr>
         * </table>
         * @param stmt prepared statement allocated by user
         * @param sql SQL statement text
         */
        virtual void prepare(PreparedStatement &stmt, char const* sql, ...);

        /**
         * Prepare statement using default allocator
         * @param stmt prepared statement allocated by user
         * @param sql SQL statement text
         * @param params varying list of SQL statement parameters
         */
        virtual void vprepare(PreparedStatement &stmt, char const* sql, va_list* list);

        /**
         * Prepare statement using default allocator
         * @param stmt prepared statement allocated by user
         * @param sql SQL statement text
         * @param params descriptors of parameters
         */
        virtual void vprepare(PreparedStatement &stmt, char const* sql, ParamDesc* params);

        /**
         * Execute parepared select statement
         * @param stmt statement prepared usign prepare method
         * @param trans current transaction (optional, if NULL then new transaction is created for execution of this statement)
         * @return result data source.
         */
        DataSource* executePreparedQuery(PreparedStatement &stmt, Transaction* trans = NULL);

        /**
         * Execute parepared insert/update/delete statement
         * @param stmt statement prepared usign prepare method
         * @param trans current transaction (optional, if NULL then new transaction is created for execution of this statement)
         * @return number of affected records
         */
        int executePreparedStatement(PreparedStatement &stmt, Transaction* trans = NULL);

        /**
         * Register user function
         * User defined function should 
         * receive from 0 to 4 arguments of <code>Value*</code> type and return result also of 
         * <code>Value*</code> type. Type of return result should be the same as declared or tpNull.
         * It is also possible to return NULL, which will be treated as return of SQL Null value.
         * @param type function return type
         * @param name function name
         * @param func pointer to the function
         * @param nArgs number of function arguments (if nArgs == -1, then function accepts vector with varying 
         * number of arguments)
         * @param ctx optional function call context (is passed to function as first parameter if not null)
         */
        void registerFunction(Type type, char const* name, void* func, int nArgs =  - 1, void* ctx = NULL);

        /**
         * Register dynamcically loaded user defined function
         * User defined function should 
         * receive from 0 to 4 arguments of <code>Value*</code> type and return result also of 
         * <code>Value*</code> type. Type of return result should be the same as declared or tpNull.
         * It is also possible to return NULL, which will be treated as return of SQL Null value.
         * @param type function return type
         * @param type function return element type (used only for arrays)
         * @param name function name
         * @param func pointer to the function
         * @param args vectors with arguments descriptors
         */
        void registerFunction(Type type, Type elemType, char const* name, void* func, Vector<Field>* args);


        /**
         * Unregister function with specified name
         * @param name function name
         */
        void unregisterFunction(char const* name);


        /**
         * Register interactive SQL extension
         * This mechanism can be used to implement database-specific commands for interactive SQL. 
         * (SqlEngine::main method) 
         * @param name command name 
         * @param syntax command syntax
         * @param description command description
         * @param callback pointer to the function
         * @param minArgs minimum number of command arguments
         * @param maxArgs maximum number of command arguments
         */
        void registerExtension(char const* name, char const* syntax, char const* description,
                               InteractiveSqlExtension::command_t callback, int minArgs, int maxArgs);

        /**
         * Close engine.
         */
        virtual void close();

        /**
         * Output format for interactive SQL
         */
        enum OutputFormat
        {
            TEXT, HTML, XML, CSV, LIST
        };

        /**
         * Set output format for interactive SQL.
         * This format specifies how result of query will be represented.
         * @param format output format
         */
        void setOutputFormat(OutputFormat format);

        /**
         * Print column value to the specified buffer
         * @param buf destibation buffer
         * @param bufSize destination buffer size
         * @param v printed value
         * @return pointer to the zero terminated string (it may be placed in the buffer or be a string constant)
         */
        virtual char* printValue(char* buf, size_t bufSize, Value* v);

#if SQL_INTERPRETER_SUPPORT
        /**
         * Interactive SQL
         * This method inputs SQL statements from specified input stream
         * and prints results to the specified output stream.
         * Engine should be previously opened. Control is returned from this function 
         * only in case of closing input stream or execution of "exit" command.
         * Commands "trace on" and "trace off" can be used to toggle tracing option.
         * @param prompt interactive SQL prompt, if value of this parameter is <code>NULL</code> then     
         *  prompt is not printed
         * @param in input stream to read SQL statement
         * @param out output stream to write results of SQL statement execution
         * @param err stream to print error messages
         */
        void main(char const* prompt, FILE* in, FILE* out, FILE* err);

        /**
         * Create default engine for interactive SQL.
         * This method initializes engine with default parameters.
         * Then it starts <code>main(stdin, stdout, stderr)</code> which reads requests from 
         * standard input stream and write results to standard output.
         * When input stream is closed, engine is also closed and control is returned from this function.
         */
        static void defaultEngine();

        static bool helpCommand(SqlEngine* engine, int nParams, char* params[], FILE* &in, FILE* &out, FILE* &err);
        static bool logSessionCommand(SqlEngine* engine, int nParams, char* params[], FILE* &in, FILE* &out, FILE* &err);
        static bool seqFormatCommand(SqlEngine* engine, int nParams, char* params[], FILE* &in, FILE* &out, FILE* &err);
        static bool arrayFormatCommand(SqlEngine* engine, int nParams, char* params[], FILE* &in, FILE* &out, FILE* &err);

#endif
        virtual DataSource* vexecute(Transaction* trans, char const* sql, va_list* list, Value** array, size_t
                                     &nRecords);

        /**
         * Get memory allocator
         */
        AbstractAllocator* getAllocator() const
        {
            return allocator;
        }

        /**
         * Set memory allocator
         */
        void setAllocator(AbstractAllocator* allocator)
        {
            this->allocator = allocator;
        }

        /**
         * Tune parameter for SQL optimizer
         */
        SqlOptimizerParameters optimizerParams;

        bool isDatabaseSchemaUpdated() { 
            return schemaUpdated;
        }


        Transaction* getCurrentTransaction() { 
            return currTrans;
        }


      private:
        struct CompilerContext { 
            SymbolTable* symtab;
            HashTable* domainHash;
            bool loadingFunction;
        };
        HashTable* userFuncs;
        HashTable* extensions;
        CompilerContext compilerCtx;
        DatabaseDescriptor* db;
        bool traceEnabled;
        bool schemaUpdated;
        OutputFormat format;
        AbstractAllocator* allocator;
        Transaction* currTrans;
        int defaultPriority;
        Transaction::IsolationLevel defaultIsolationLevel;
        void manageTransaction(TransactionNode* tnode);

        SqlFunctionDeclaration* findFunction(char const* name, bool tryToLoad = false);
        void registerFunction(SqlFunctionDeclaration* decl);
#if SQL_INTERPRETER_SUPPORT
        void registerExtension(InteractiveSqlExtension* decl);
        bool readStatement(char const* prompt, FILE*  &in, FILE*  &out, FILE*  &err, char* buf, size_t bufSize);
        int parseArguments(char* buf, char* cmd, char* args[]);

        void print(const char *format, ...);
        FormattedOutput outs[2]; // 0 - 'main', 1 - 'session'
        int nOuts;
#endif

      protected:
        DynamicAllocator defaultAllocator;
        virtual AbstractAllocator* getDefaultAllocator();

        DataSource* executePrepared(PreparedStatement &stmt, Transaction* trans, int &rc);
        virtual void init();
        SqlEngine(Database* db, SqlEngine* engine, AbstractAllocator* allocator);
    };

    class SqlEngineAllocatorContext: public AllocatorContext
    {
      public:
        SqlEngineAllocatorContext(SqlEngine* engine): AllocatorContext(engine->getAllocator()){}
    };

}

#endif
