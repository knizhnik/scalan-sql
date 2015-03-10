/*******************************************************************
 *                                                                 *
 *  sql.cpp                                                        *
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
 * MODULE:    sql.cpp
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */

#if !defined(MCO_CFG_XSQL_USE_READLINE) && defined(_LINUX)
#define MCO_CFG_XSQL_USE_READLINE 1
#endif

#if !defined(MCO_CFG_XSQL_USE_EDITLINE) && defined(__APPLE__)
#undef  MCO_CFG_XSQL_USE_READLINE
#define MCO_CFG_XSQL_USE_EDITLINE 1
#endif

#if MCO_CFG_XSQL_USE_READLINE || MCO_CFG_XSQL_USE_EDITLINE
#include <dlfcn.h>
#endif
#include "sqlcpp.h"
#include <mcotime.h>
#include "compiler.h"

#if MCO_CFG_XSQL_USE_EDITLINE
#include <histedit.h>
#if defined(__APPLE__)
#define MCO_CFG_XSQL_EDITLINE_NAME "libedit.dylib"
#else
#define MCO_CFG_XSQL_EDITLINE_NAME "libedit.so"
#endif
#endif

namespace McoSql
{

    const int MAX_COMMAND_ARGS = 8;
    const int BUF_SIZE = 64*1024;
    const int MAX_CSV_COLUMNS = 256;
    const int MAX_CSV_LINE_LEN = 4*1024;

#if MCO_CFG_XSQL_USE_READLINE

    typedef char* (*readline_t)(const char* prompt);
    typedef void (*add_history_t)(const char* history);
    typedef void (*free_t)(char* line);

    struct ReadLine {
        readline_t readline;
        add_history_t add_history;
        free_t freeline;
        void* hnd;

        ReadLine() {
            hnd = dlopen("libreadline.so", RTLD_NOW);
            if (hnd != NULL) {
                readline = (readline_t)dlsym(hnd, "readline");
                add_history = (add_history_t)dlsym(hnd, "add_history");
                freeline = (free_t)dlsym(hnd, "free");
            }
        }

        ~ReadLine() {
            if (hnd != NULL)
                dlclose(hnd);
        }
    };

    static ReadLine readlineLib;
#endif

#if MCO_CFG_XSQL_USE_EDITLINE

    typedef EditLine * (*el_init_t) (const char *prog, FILE *fin, FILE *fout, FILE *ferr);
    typedef void (*el_end_t) (EditLine *e);
    typedef const char * (*el_gets_t)(EditLine *e, int *count);
    typedef int (*el_set_t)(EditLine *e, int op, ...);
    typedef History * (*history_init_t)();
    typedef void (*history_end_t)(History *h);
    typedef int (*history_t)(History *h, HistEvent *ev, int op, ...);

    static const char * edit_line_prompt;
	
    static const char * get_prompt(EditLine * el ) {
        return edit_line_prompt;
    }

    struct EditLineCtx {
        int        initialized;
        EditLine * el;
        History  * hs;
        HistEvent  ev;
        void     * hnd;
        el_init_t f_el_init;
        el_end_t  f_el_end;
        el_gets_t f_el_gets;
        el_set_t  f_el_set;
        history_init_t f_history_init;
        history_end_t  f_history_end;
        history_t      f_history;

        int initialize( FILE *fin, FILE *fout, FILE *ferr, const char * sprompt ) {
            
            edit_line_prompt = sprompt;
            
            if ( f_el_init ) {
                char const* exec_path = getenv("_");
                if (exec_path == NULL) { 
                    exec_path = ".";
                }
                el = f_el_init(exec_path, fin, fout, ferr );
                if ( el ) {
                    f_el_set( el, EL_PROMPT, get_prompt );
                    f_el_set( el, EL_EDITOR, "emacs" );
                }
                
                hs = f_history_init();
                if ( hs ) {
                    f_history( hs, &ev, H_SETSIZE, 800);
                    f_el_set(el, EL_HIST, f_history, hs);
                }
                
                if ( hs && el ) {
                    initialized = MCO_YES;
                    return 0; /* initialized succ */
                }
                /* failed to initialize */
                f_el_gets = 0;
            }
            /* library was not found */
            return 1;
        }
        
        EditLineCtx() {
            edit_line_prompt = NULL;
            initialized = MCO_NO;
            hnd = dlopen( MCO_CFG_XSQL_EDITLINE_NAME, RTLD_NOW);
            if ( hnd != NULL ) {
                f_el_init      = (el_init_t)dlsym( hnd, "el_init" );
                f_el_end       = (el_end_t)dlsym( hnd, "el_end" );
                f_el_gets      = (el_gets_t)dlsym( hnd, "el_gets" );
                f_el_set       = (el_set_t)dlsym( hnd, "el_set" );
                f_history_init = (history_init_t)dlsym( hnd, "history_init" );
                f_history_end  = (history_end_t)dlsym( hnd, "history_end" );
                f_history      = (history_t)dlsym( hnd, "history" );
            } else {
                f_el_init      = NULL;
                f_el_gets      = NULL;
                f_el_set       = NULL;
                f_history_init = NULL;
                f_history_end  = NULL;
                f_history      = NULL;
            }
        }
        
        ~EditLineCtx() {
            if ( hs ) f_history_end( hs );
            if ( el ) f_el_end( el );
        }
    };

    static EditLineCtx editlineLib;

#endif

    bool PreparedStatement::isQuery() const
    {
        return node->isQueryNode();
    }


    Iterator < Field > * PreparedStatement::describeResultColumns()
    {
        return node->isQueryNode() ? new(allocator)ColumnIterator(allocator, ((QueryNode*)node)->resultColumns) : NULL;
    }


    SqlOptimizerParameters::SqlOptimizerParameters()
    {
        enableCostBasedOptimization = false;
        sequentialSearchCost = 1000;
        openIntervalCost = 100;
        scanSetCost = 50;
        orCost = 10;
        andCost = 10;
        isNullCost = 6;
        closeIntervalCost = 5;
        prefixMatchCost = 4;
        exactMatchCost = 3;
        patternMatchCost = 2;
        eqCost = 1;
        eqRealCost = 2;
        eqStringCost = 3;
        eqBoolCost = 200;
    }


    SqlEngine::SqlEngine()
    {
        db = NULL;
        traceEnabled = false;
        format = TEXT;
        allocator = NULL;
        currTrans = NULL;
        defaultIsolationLevel = Transaction::DefaultIsolationLevel;
        defaultPriority = 0;
        schemaUpdated = false;
    }

    SqlEngine::SqlEngine(Database* db, SqlEngine* engine, AbstractAllocator* allocator)
    {
        setAllocator(allocator);
        this->db = new(allocator)DatabaseDescriptor(db, engine->db);
        userFuncs = engine->userFuncs;
        extensions = engine->extensions;
        traceEnabled = engine->traceEnabled;
        format = engine->format;
        currTrans = NULL;
        defaultIsolationLevel = Transaction::DefaultIsolationLevel;
        defaultPriority = 0;
        compilerCtx = engine->compilerCtx; // Struct copy
        schemaUpdated = false;
    }

    AbstractAllocator* SqlEngine::getDefaultAllocator()
    {
        return  &defaultAllocator;
    }

    bool SqlEngine::isRemote()
    {
        return false;
    }

    void SqlEngine::setOutputFormat(OutputFormat format)
    {
        this->format = format;
    }

    void SqlEngine::init()
    {
        Compiler::initializeSymbolTable(compilerCtx);
        userFuncs = new HashTable(INIT_HASH_SIZE);
        extensions = new HashTable(INIT_HASH_SIZE);
        for (InteractiveSqlExtension* ex = InteractiveSqlExtension::chain; ex != NULL; ex = ex->next)
        {
            registerExtension(ex);
        }
#if SQL_INTERPRETER_SUPPORT
        outs[0].initFormat(false);
        outs[1].initFormat(true);
        nOuts = 1;
#endif
    }

    void SqlEngine::open(Database* db)
    {
        SqlEngineAllocatorContext ctx(this);

        this->db = new DatabaseDescriptor(db == NULL ? Database::instance: db);
        init();
        for (SqlFunctionDeclaration* fd = SqlFunctionDeclaration::chain; fd != NULL; fd = fd->next)
        {
            registerFunction(fd);
        }
    }

    SqlFunctionDeclaration* SqlEngine::findFunction(char const* name, bool tryToLoad)
    {
        SqlFunctionDeclaration* fdecl = (SqlFunctionDeclaration*)userFuncs->get(name);
        if (fdecl == NULL && tryToLoad && db->findTable(String::create("Functions")))
        {
            char* stmt = NULL;
            {
                QueryResult result(executeQuery(currTrans, "select stmt from Functions where name=%s", name));
                Cursor* cursor = result->records();
                if (cursor->hasNext()) {
                    String* str = cursor->next()->get(0)->stringValue();
                    stmt = new char[str->size()+1];
                    memcpy(stmt, str->cstr(), str->size()+1);
                }
            }
            if (stmt != NULL) { 
                delete[] stmt;
                executeStatement(currTrans, stmt);
                fdecl - (SqlFunctionDeclaration*)userFuncs->get(name);
            }
        }
        return fdecl;
    }

    void SqlEngine::registerFunction(SqlFunctionDeclaration* decl)
    {
        userFuncs->put(decl->name, decl);
    }

    void SqlEngine::unregisterFunction(char const* name)
    {
        userFuncs->remove(name);
    }

    void SqlEngine::registerExtension(InteractiveSqlExtension* ext)
    {
        extensions->put(ext->name, ext);
    }

    void SqlEngine::registerFunction(Type type, char const* name, void* func, int nArgs, void* ctx)
    {
        registerFunction(new SqlFunctionDeclaration(type, name, func, nArgs, ctx));
    }

    void SqlEngine::registerFunction(Type type, Type elemType, char const* name, void* func, Vector<Field>* args)
    {
        registerFunction(new SqlFunctionDeclaration(type, elemType, name, func, args));
    }

    void SqlEngine::registerExtension(char const* name, char const* syntax, char const* description,
                                      InteractiveSqlExtension::command_t callback, int minArgs = 1, int maxArgs = 1)
    {
        InteractiveSqlExtension* ext = new InteractiveSqlExtension();
        ext->name = name;
        ext->syntax = syntax;
        ext->description = description;
        ext->callback = callback;
        ext->minArgs = minArgs;
        ext->maxArgs = maxArgs;
        registerExtension(ext);
    }

    DataSource* SqlEngine::executeQuery(char const* sql, ...)
    {
        va_list list;
        size_t rc;
        va_start(list, sql);
        DataSource* result = vexecute(NULL, sql, &list, NULL, rc);
        va_end(list);
        return result;
    }

    DataSource* SqlEngine::vexecuteQuery(char const* sql, va_list* list)
    {
        size_t rc;
        return vexecute(NULL, sql, list, NULL, rc);
    }

    DataSource* SqlEngine::vexecuteQuery(char const* sql, Value** params)
    {
        size_t rc;
        return vexecute(NULL, sql, NULL, params, rc);
    }

    int SqlEngine::executeStatement(char const* sql, ...)
    {
        va_list list;
        size_t rc;
        va_start(list, sql);
        DataSource* result = vexecute(NULL, sql, &list, NULL, rc);
        if (result != NULL)
        {
            result->release();
        }
        va_end(list);
        return (int)rc;
    }

    int SqlEngine::vexecuteStatement(char const* sql, va_list* list)
    {
        size_t rc;
        DataSource* result = vexecute(NULL, sql, list, NULL, rc);
        if (result != NULL)
        {
            result->release();
        }
        return (int)rc;
    }

    int SqlEngine::vexecuteStatement(char const* sql, Value** params)
    {
        size_t rc;
        DataSource* result = vexecute(NULL, sql, NULL, params, rc);
        if (result != NULL)
        {
            result->release();
        }
        return (int)rc;
    }

    DataSource* SqlEngine::executeQuery(Transaction* trans, char const* sql, ...)
    {
        va_list list;
        size_t rc;
        va_start(list, sql);
        DataSource* result = vexecute(trans, sql, &list, NULL, rc);
        va_end(list);
        return result;
    }

    DataSource* SqlEngine::vexecuteQuery(Transaction* trans, char const* sql, va_list* list)
    {
        size_t rc;
        return vexecute(trans, sql, list, NULL, rc);
    }

    DataSource* SqlEngine::vexecuteQuery(Transaction* trans, char const* sql, Value** params)
    {
        size_t rc;
        return vexecute(trans, sql, NULL, params, rc);
    }

    int SqlEngine::executeStatement(Transaction* trans, char const* sql, ...)
    {
        va_list list;
        size_t rc;
        va_start(list, sql);
        DataSource* result = vexecute(trans, sql, &list, NULL, rc);
        if (result != NULL)
        {
            result->release();
        }
        va_end(list);
        return (int)rc;
    }

    int SqlEngine::vexecuteStatement(Transaction* trans, char const* sql, va_list* list)
    {
        size_t rc;
        DataSource* result = vexecute(trans, sql, list, NULL, rc);
        if (result != NULL)
        {
            result->release();
        }
        return (int)rc;
    }

    int SqlEngine::vexecuteStatement(Transaction* trans, char const* sql, Value** params)
    {
        size_t rc;
        DataSource* result = vexecute(trans, sql, NULL, params, rc);
        if (result != NULL)
        {
            result->release();
        }
        return (int)rc;
    }

    void SqlEngine::trace(bool enable)
    {
        traceEnabled = enable;
    }

    Database* SqlEngine::database()
    {
        return db;
    }


    class Finally
    {
      private:
        SqlEngine*   engine;
        bool         rollback;

      public:
        Finally(SqlEngine* engine, bool rollback = true)
        {
            this->engine = engine;
            this->rollback = rollback;
        }

        void refuse()
        {
            engine = NULL;
        }

        ~Finally()
        {
            if (engine != NULL)
            {
                Transaction* trans = engine->currTrans;
                if (trans != NULL) {
                    if (rollback) {
                        trans->rollback();
                    }
                    trans->release();
                    engine->currTrans = NULL;
                }
            }
        }
    };

    void SqlEngine::manageTransaction(TransactionNode* tnode)
    {
        switch (tnode->tag) {
          case ExprNode::opStartTransaction:
            if (currTrans != NULL) {
                MCO_THROW InvalidState("transaction already active");
            } else {
                currTrans = db->beginTransaction(tnode->mode, tnode->priority, tnode->isolationLevel);
            }
            break;
          case ExprNode::opCommitTransaction:
            if (currTrans == NULL) {
                MCO_THROW InvalidState("no active transaction");
            }
            currTrans->commit();
            currTrans->release();
            currTrans = NULL;
            break;
          case ExprNode::opRollbackTransaction:
            if (currTrans == NULL) {
                MCO_THROW InvalidState("no active transaction");
            }
            currTrans->rollback();
            currTrans->release();
            currTrans = NULL;
            break;
          case ExprNode::opSetDefaultIsolationLevel:
            defaultIsolationLevel = tnode->isolationLevel;
            break;
          case ExprNode::opSetDefaultPriority:
            defaultPriority = tnode->priority;
            break;
          default:
            break;
        }
    }
    
    DataSource* SqlEngine::vexecute(Transaction* trans, char const* sql, va_list* list, Value** array, size_t &nRecords)
    {
        SqlEngineAllocatorContext ctx(this);
        bool schemaReloaded = db->checkSchema();
        Compiler compiler(this, sql, list, array, NULL);
        NewMemorySegment mem;
        StmtNode* node = compiler.statement();
        bool autoCommit = false;
        bool conflict = false;
        if (node->isTransactionNode()) {
            Finally finally(this, false);
            manageTransaction((TransactionNode*)node);
            finally.refuse();
            return NULL;
        }
        do {
            if (autoCommit || trans == NULL)
            {
                if (currTrans == NULL) {
                    currTrans = db->beginTransaction(node->isReadOnly() ? Transaction::ReadOnly 
                                                     : node->isDDLNode() ? Transaction::Exclusive : Transaction::ReadWrite,
                                                     defaultPriority, defaultIsolationLevel);
                    autoCommit = true;
                }
                trans = currTrans;
            }
            else if (!node->isReadOnly())
            {
                if (!trans->upgrade()) {
                    MCO_THROW UpgradeNotPossible();
                }
            }
            {
                Runtime* runtime = new Runtime(db, this, mem.segmentId, mem.mark, traceEnabled, trans, autoCommit);
                Finally finally(autoCommit ? this : NULL);
                schemaUpdated = schemaReloaded || node->isCreateNode();
                if (node->isQueryNode())
                {
                    DataSource* result = ((QueryNode*)node)->executeQuery(runtime);
                    nRecords = result->isNumberOfRecordsKnown() ? (int)result->nRecords(trans):  - 1;
                    finally.refuse();
                    mem.detach();
                    return result;
                }
                nRecords = node->executeStmt(runtime);
                if (traceEnabled)
                {
                    printf("Update %ld records\n", (long)nRecords);
                }
                finally.refuse();
                if (autoCommit) {
                    conflict = !trans->commit();
                    currTrans = NULL;
                } else if (schemaUpdated) { 
                    trans->hold();
                }
                mem.release(!node->isCreateNode());
            }
        } while (conflict);

        return NULL;
    }


    void SqlEngine::close()
    {
        if (db != NULL) {
            SqlEngineAllocatorContext ctx(this);
            db->close();
            db = NULL;
        }
    }

    SqlEngine::~SqlEngine(){}


    void SqlEngine::prepare(PreparedStatement &stmt, char const* sql, ...)
    {
        va_list list;
        va_start(list, sql);
        vprepare(stmt, sql, &list);
        va_end(list);
    }

    void SqlEngine::vprepare(PreparedStatement &stmt, char const* sql,  va_list* list)
    {
        stmt.allocator->reset(0);
        AllocatorContext ctx(stmt.allocator);
        schemaUpdated = db->checkSchema();
        Compiler compiler(this, sql, list, NULL, NULL);
        stmt.node = compiler.statement();
    }

    void SqlEngine::vprepare(PreparedStatement &stmt, char const* sql, ParamDesc* params)
    {
        stmt.allocator->reset(0);
        AllocatorContext ctx(stmt.allocator);
        schemaUpdated = db->checkSchema();
        Compiler compiler(this, sql, NULL, NULL, params);
        stmt.node = compiler.statement();
    }

    DataSource* SqlEngine::executePreparedQuery(PreparedStatement &stmt, Transaction* trans)
    {
        int rc;
        return executePrepared(stmt, trans, rc);
    }

    int SqlEngine::executePreparedStatement(PreparedStatement &stmt, Transaction* trans)
    {
        int rc;
        DataSource* result = executePrepared(stmt, trans, rc);
        if (result != NULL)
        {
            result->release();
        }
        return rc;
    }

    DataSource* SqlEngine::executePrepared(PreparedStatement &stmt, Transaction* trans, int &nRecords)
    {
        SqlEngineAllocatorContext ctx(this);
        NewMemorySegment mem;
        bool autoCommit = false;
        StmtNode* node = stmt.node;
        bool conflict = false;
        if (node == NULL)
        {
            MCO_THROW NotPrepared();
        }
        do {
            node->cleanup();
            if (node->isTransactionNode()) {
                Finally finally(this, false);
                manageTransaction((TransactionNode*)node);
                finally.refuse();
                return NULL;
            }
            if (autoCommit || trans == NULL)
            {
                if (currTrans == NULL) {
                    currTrans = db->beginTransaction(node->isReadOnly() ? Transaction::ReadOnly
                                                     : node->isDDLNode() ? Transaction::Exclusive : Transaction::ReadWrite,
                                                     defaultPriority, defaultIsolationLevel);
                    autoCommit = true;
                }
                trans = currTrans;
            }
            else if (!node->isReadOnly())
            {
                if (!trans->upgrade()) {
                    MCO_THROW UpgradeNotPossible();
                }
            }
            {
                Runtime* runtime = new Runtime(db, this, mem.segmentId, mem.mark, traceEnabled, trans, autoCommit);
                Finally finally(autoCommit ? this : NULL);
                if (node->isQueryNode())
                {
                    DataSource* result = ((QueryNode*)node)->executeQuery(runtime);
                    nRecords = result->isNumberOfRecordsKnown() ? (int)result->nRecords(trans):  - 1;
                    finally.refuse();
                    mem.detach();
                    return result;
                }
                nRecords = node->executeStmt(runtime);
                if (traceEnabled)
                {
                    printf("Update %ld records\n", (long)nRecords);
                }
                finally.refuse();
                schemaUpdated |= node->isCreateNode();
                if (autoCommit) {
                    conflict = !trans->commit();
                    currTrans = NULL;
                } else if (schemaUpdated) {
                    trans->hold();
                }
                mem.release(!schemaUpdated);
            }
        } while (conflict);

        return NULL;
    }

    #if SQL_INTERPRETER_SUPPORT

        void SqlEngine::defaultEngine()
        {
            SqlEngine engine;
            engine.setAllocator(engine.getDefaultAllocator());
            engine.open();
            engine.main("XSQL> ", stdin, stdout, stderr);
            MCO_TRY
            {
                engine.close();
            }
#if MCO_CFG_USE_EXCEPTIONS
            catch (McoSqlException const &x)
            {
                fprintf(stderr, "ERROR: %s\n", x.getMessage()->cstr());
            }
#endif
        }

        static FILE* saveInputStream;

        static bool exitCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
        {
            if (saveInputStream != NULL)
            {
                in = saveInputStream;
                return true;
            }
            return false;
        }
        static InteractiveSqlExtension exitCmd("exit", "exit", "Terminate interactive SQL session",  &exitCommand, 0, 0);

        static bool pauseCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
        {
            char buf[64];
            fputs("Press any key to continue...\n", stdout);
            return fgets(buf, sizeof buf, stdin) != NULL;
        }
        static InteractiveSqlExtension pauseCmd("pause", "pause", "Pause interactive SQL session",  &pauseCommand, 0, 0)
                                               ;


        static bool traceCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
        {
            engine->trace(STRCMP(params[1], "on") == 0 ? true : false);
            return true;
        }
        static InteractiveSqlExtension traceCmd("trace", "trace (on|off)", "Toggle query execution trace",
                                                &traceCommand, 1, 1);

        static bool inputCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
        {
            if (in != NULL && in != stdin)
            {
                fclose(in);
            }
            if (STRCMP(params[1], "console") == 0)
            {
                saveInputStream = in;
                in = stdin;
            }
            else
            {
                FILE* f = fopen(params[1], "r");
                if (f == NULL)
                {
                    fprintf(err, "Failed to open file '%s'\n", params[1]);
                }
                else
                {
                    saveInputStream = in;
                    in = f;
                }
            }
            return true;
        }
        static InteractiveSqlExtension inputCmd("input", "input (<file>|console)", "Redirect input from specified file",
                                                &inputCommand, 1, 1);

        static bool outputCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
        {
            if (out != NULL && out != stdout)
            {
                fclose(out);
            }
            if (STRCMP(params[1], "console") == 0)
            {
                out = stdout;
            }
            else if (STRCMP(params[1], "null") == 0)
            {
                out = NULL;
            }
            else
            {
                FILE* f = fopen(params[1], "w");
                if (f == NULL)
                {
                    fprintf(err, "Failed to open file '%s'\n", params[1]);
                }
                else
                {
                    out = f;
                }
            }
            return true;
        }
        static InteractiveSqlExtension outputCmd("output", "output (<file>|console|null)",
                                                 "Redirect output to specified file",  &outputCommand, 1, 1);


        bool SqlEngine::logSessionCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
        {
            FILE *f = 0;
            if (STRCMP(params[1], "on") == 0) {
                char *filename;
                char tmp_filename[128];
                if (nParams == 3) {
                    filename = params[2];
                } else {
                    sprintf(tmp_filename, "xsqlcmd.session.%ld", time(0));
                    filename = tmp_filename;
                }
                f = fopen(filename, "a+");
                if (f == NULL) {
                    fprintf(err, "Failed to open file '%s'\n", filename);
                }
            }
            engine->outs[1].setFd(f);
            engine->nOuts = (f == 0) ? 1 : 2;
            return true;
        }

        static InteractiveSqlExtension logSessionCmd("logsession", "logsession (on|off) [<file>]",
                                                 "Save session to specified file",  &SqlEngine::logSessionCommand, 1, 2);



        static bool formatCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
        {
            engine->setOutputFormat(STRCMP(params[1], "HTML") == 0 ? SqlEngine::HTML
                                    : STRCMP(params[1], "XML") == 0 ? SqlEngine::XML
                                    : STRCMP(params[1], "CSV") == 0 ? SqlEngine::CSV
                                    : STRCMP(params[1], "LIST") == 0 ? SqlEngine::LIST : SqlEngine::TEXT);
            return true;
        }

        static InteractiveSqlExtension formatCmd("format", "format (TEXT|HTML|XML|CSV|LIST)", "Select query output format",
                                                 &formatCommand, 1, 1);


        const int MAX_FORMAT_LEN = 256;
        static bool dateFormatCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
        {
            static char format[MAX_FORMAT_LEN];
            strcpy(format, params[1]);
            DateTime::format = format;
            return true;
        }

        static InteractiveSqlExtension dateFormatCmd("dtformat", "dtformat FORMAT", "Output date/time format (see strftime)",
                                                     &dateFormatCommand, 1, 1);

        static bool numFormatCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
        {
            static char format[MAX_FORMAT_LEN];
            strcpy(format, params[1]);
            RealValue::format = format;
            return true;
        }

        static InteractiveSqlExtension numFormatCmd("numformat", "numformat FORMAT", "Output real number format (see sprintf)",
                                                     &numFormatCommand, 1, 1);


        bool SqlEngine::seqFormatCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
        {
            if (STRCMP(params[1], "default") == 0) {
                engine->outs[0].seq_first = FormattedOutput::DEFAULT_SEQ_FIRST;
                engine->outs[0].seq_last  = FormattedOutput::DEFAULT_SEQ_LAST;
                engine->outs[0].seq_show_indexes = false;
            } else {
                if (STRCMP(params[1], "short") == 0) {
                    engine->outs[0].seq_show_indexes = false;
                } else if (STRCMP(params[1], "long") == 0) {
                    engine->outs[0].seq_show_indexes = true;
                } else {
                    fprintf(err, "Wrong '%s' argument. Valid values are 'default', 'short' or 'long'\n", params[1]);
                }
                if (nParams >= 4) {
                    engine->outs[0].seq_first = atoi(params[2]);
                    engine->outs[0].seq_last  = atoi(params[3]);
                } else {
                    engine->outs[0].seq_first = FormattedOutput::UNLIMITED;
                    engine->outs[0].seq_last  = FormattedOutput::UNLIMITED;
                }
            }
            return true;
        }

        static InteractiveSqlExtension seqFormatCmd("seqformat", "seqformat {default|short|long} [<first> <last>]", "Output sequences format.",
                                                     &SqlEngine::seqFormatCommand, 1, 3);

        bool SqlEngine::arrayFormatCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*  &err)
        {
            if (STRCMP(params[1], "default") == 0) {
                engine->outs[0].array_first = FormattedOutput::DEFAULT_ARRAY_FIRST;
                engine->outs[0].array_last  = FormattedOutput::DEFAULT_ARRAY_LAST;
                engine->outs[0].array_show_indexes = false;
            } else {
                if (STRCMP(params[1], "short") == 0) {
                    engine->outs[0].array_show_indexes = false;
                } else if (STRCMP(params[1], "long") == 0) {
                    engine->outs[0].array_show_indexes = true;
                } else {
                    fprintf(err, "Wrong '%s' argument. Valid values are 'default', 'short' or 'long'\n", params[1]);
                }
                if (nParams >= 4) {
                    engine->outs[0].array_first = atoi(params[2]);
                    engine->outs[0].array_last  = atoi(params[3]);
                } else {
                    engine->outs[0].array_first = FormattedOutput::UNLIMITED;
                    engine->outs[0].array_last  = FormattedOutput::UNLIMITED;
                }
            }
            return true;
        }

        static InteractiveSqlExtension arrayFormatCmd("arrayformat", "arrayformat {default|short|long} [<first> <last>]", "Output arrays format.",
                                                     &SqlEngine::arrayFormatCommand, 1, 3);




        bool SqlEngine::helpCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE*
                                    &err)
        {
            FILE* f = out != NULL ? out : stderr;
            fprintf(f, "%-25sany valid SQL statement\n", "<sql-statement>';'");
            HashTable::Iterator iterator = engine->extensions->iterator();
            while (iterator.hasNext())
            {
                InteractiveSqlExtension* ext = (InteractiveSqlExtension*)iterator.next()->value;
                fprintf(f, "%-25s%s\n", ext->syntax, ext->description);
            }
            return true;
        }
        static InteractiveSqlExtension helpCmd("help", "help", "Print supported commands",  &SqlEngine::helpCommand, 0,
                                               0);


        static bool echoCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE* &err)
        {
            FILE* f = out != NULL ? out : stderr;
            char timeBuf[MCO_SQL_TIME_BUF_SIZE];
            timeToString(time(NULL), timeBuf, sizeof(timeBuf));
            fprintf(f, "[%s]:", timeBuf);
            for (int i = 1; i < nParams; i++) {
                fprintf(f, " %s", params[i]);
            }
            fprintf(f, "\n");
            return true;
        }
        static InteractiveSqlExtension echoCmd("echo", "echo", "Print arguments",  &echoCommand, 0, 100);


        static mco_bool parseCsvLine(FILE* &err, char* buf, ParamDesc* params, int n_values, char sep)
        {
            char* p = buf;
            int i;
            for (i = 0; i < n_values; i++) {
                if (*p == '\'' || *p == '"') {
                    char quote = *p++;
                    params[i].ptr = p;
                    while (*p != quote) {
                        if (*p++ == '\0') {
                            fprintf(stderr, "Unterminated string\n");
                            return MCO_NO;
                        }
                    }
                    *p++ = '\0';
                    if (*p != sep) {
                        fprintf(err, "'%c' expected at position %d\n", sep, (int)(p - buf));
                        return MCO_NO;
                    }
                    p += 1;
                } else {
                    params[i].ptr = p;
                    while (*p != sep && *p != '\n' && *p != '\r' && *p != '\0') {
                        p += 1;
                    }
                    if (*p == sep) {
                        *p++ = '\0';
                    } else {
                        *p = '\0';
                        if (i+1 != n_values) {
                            fprintf(err, "Too few values %d\n", i+1);
                            return MCO_NO;
                        }
                        break;
                    }
                }
            }
            return MCO_YES;
        }

        bool importCsvCommand(SqlEngine* engine, int nParams, char* params[], FILE*  &in, FILE*  &out, FILE* &err)
        {
            char const* tableName = params[1];
            FILE* f = fopen(params[2], "r");
            char buf[MAX_CSV_LINE_LEN];
            char sql[MAX_CSV_LINE_LEN];
            char sep = '\0';
            char const* separators = ";,|\t";
            int i, n, nColumns;
            ParamDesc columns[MAX_CSV_COLUMNS];
            Value* values[MAX_CSV_COLUMNS];
            bool useHeader = false;
            bool skipHeader = false;
            int commit_period = 0;

            for (i = 3; i < nParams; i++) {
                if (strcmp(params[i], "use") == 0) {
                    useHeader = true;
                } else if (strcmp(params[i], "skip") == 0) {
                    skipHeader = true;
                } else if (strcmp(params[i], "commit") == 0) {
                    if (++i == nParams) {
                        fprintf(err, "Expect commit period parameter\n");
                        return true;
                    }
                    if ((commit_period = atoi(params[i])) == 0) {
                        fprintf(err, "Invalid commit period: %s\n", params[i]);
                        return true;
                    }
                    continue;
                } else {
                    fprintf(err, "Invalid parameter %s: expect 'use', 'skip' or 'commit'\n", params[i]);
                    return true;
                }
                if (++i == nParams) {
                    fprintf(err, "Expect 'header' parameter\n");
                    return true;
                } else if (strcmp(params[i], "header") != 0) {
                    fprintf(err, "Invalid parameter %s: expect 'header'\n", params[i]);
                    return true;
                }
            }
            if (f == NULL) {
                fprintf(err, "Failed to open file '%s'\n", params[2]);
                return true;
            }
            if (fgets(buf, sizeof buf, f) == NULL) {
                fprintf(err, "Failed to read CSV file header\n");
                return true;
            }
            while (*separators != '\0') {
                if (strchr(buf, *separators) != NULL) {
                    sep = *separators;
                    break;
                }
                separators += 1;
            }
            if (sep == '\0') {
                fprintf(err, "Invalid header of CSV file: %s\n", buf);
                return true;
            }

            char* hdr = buf;
            for (nColumns = 0; (hdr = strchr(hdr, sep)) != NULL; hdr++, nColumns += 1);
            hdr = buf + strlen(buf);
            while (isspace(*--hdr));
            if (*hdr != sep) {
                nColumns += 1;
                hdr += 1;
            }
            *hdr = '\0';
            if (nColumns > MAX_CSV_COLUMNS) {
                fprintf(err, "Number of columns %d exeeds limit %d\n", nColumns, MAX_CSV_COLUMNS);
                return true;
            }

            if (useHeader) {
                if (sep != ',') {
                    for (hdr = buf; (hdr = strchr(hdr, sep)) != NULL; *hdr++ = ',');
                }
                n = sprintf(sql, "insert into %s (%s) values (", tableName, buf);
                skipHeader = true;
            } else {
                n = sprintf(sql, "insert into %s values (", tableName);
            }

            PreparedStatement stmt;

            timer_unit start = mco_system_get_current_time();
            Transaction* trans = NULL;

            if (engine->isRemote()) {
                strcpy(sql + n, "%v");
                n += 2;
                for (i = 1; i < nColumns; i++, n += 3) {
                    strcpy(sql + n, ",%v");
                }
                strcpy(sql + n, ")");
                engine->executeStatement("start transaction");
            } else {
                sql[n++] = '?';
                for (i = 1; i < nColumns; i++, n += 2) {
                    strcpy(sql + n, ",?");
                }
                strcpy(sql + n, ")");
                for (i = 0; i < nColumns; i++) {
                    columns[i].type = tpString;
                    columns[i].lenptr = NULL;
                }
                engine->vprepare(stmt, sql, columns);
                trans = engine->database()->beginTransaction(Transaction::ReadWrite);
            }
            for (i = 1; !skipHeader || fgets(buf, sizeof buf, f) != NULL; i++) {
                if (!parseCsvLine(err, buf, columns, nColumns, sep)) {
                    fprintf(err, "Failed to parse line %d of CSV file: %s\n", i, buf);
                    break;
                }
                skipHeader = true;
                if (trans != NULL) {
                    n = engine->executePreparedStatement(stmt, trans);
                } else {
                    for (int j = 0; j < nColumns; j++) {
                        values[j] = String::create((char*)columns[j].ptr);
                    }
                    n = engine->vexecuteStatement(sql, values);
                }
                if (n != 1) {
                    fprintf(err, "Failed to insert record in the table %s\n", tableName);
                    break;
                }
                if (commit_period != 0 && i % commit_period == 0) {
                    if (trans != NULL) {
                        trans->commit();
                        trans->release();
                        trans = engine->database()->beginTransaction(Transaction::ReadWrite);
                    } else {
                        engine->executeStatement("commit transaction");
                        engine->executeStatement("start transaction");
                    }
                }
            }
            if (trans) {
                trans->commit();
                trans->release();
            } else {
                engine->executeStatement("commit transaction");
            }

            fprintf(out, "Import of %d records completed in %d milliseconds\n",
                    i-1, (int)(mco_system_get_current_time() - start));
            return true;
        }
        static InteractiveSqlExtension importCmd("import", "import TABLE CSV-FILE [(use|skip) header] [commit N]", "Import data from CSV file",  &importCsvCommand, 2, 6);



        int SqlEngine::parseArguments(char* buf, char* cmd, char* args[])
        {
            char* src = buf;
            char* dst = cmd;
            int i = 0;
            char ch;

            while ((ch = * src++) != '\0')
            {
                if (!isspace((unsigned char)ch) && ch != ',' && ch != ';')
                {
                    if (i >= MAX_COMMAND_ARGS)
                    {
                        return 0;
                    }
                    args[i] = dst;
                    if (ch == '\'' || ch == '"')
                    {
                        char quote = ch;
                        while ((ch = * src++) != quote)
                        {
                            if (ch == '\0')
                            {
                                return 0;
                            }
                            *dst++ = ch;
                        }
                    }
                    else
                    {
                        do
                        {
                            *dst++ = ch;
                        }
                        while ((ch = * src++) != '\0' && ch != ',' && ch != ';' && !isspace((unsigned char)ch));
                        src -= 1;
                    }
                    *dst++ = '\0';
                    i += 1;
                }
            }
            if (i > 0 && cmd[0] == '-')
            {
                return 0;
            }
            return i;
        }


        bool SqlEngine::readStatement(char const* prompt, FILE*  &in, FILE*  &out, FILE*  &err, char* buf, size_t bufSize)
        {
            nextLine: int offs = 0;
            while (true) {
#if MCO_CFG_XSQL_USE_READLINE
                if (in == stdin && out == stdout && prompt != NULL && readlineLib.readline != NULL) {
                    char* input = readlineLib.readline(prompt);
                    if (input == NULL) {
                        break;
                    }
                    strncpy(buf + offs, input, bufSize - offs);
                    if (readlineLib.freeline) { 
                        readlineLib.freeline(input);
                    } else { 
                        free(input);
                    }
                }
                else
#endif
#if MCO_CFG_XSQL_USE_EDITLINE
                if (in == stdin && out == stdout && prompt != NULL && editlineLib.f_el_gets != NULL ) {
                    int count = 0;
                    const char* input = editlineLib.f_el_gets( editlineLib.el, &count );
                    if (input == NULL || count < 0) {
                        break;
                    }
                    if (count > 0 && input[count-1] == '\n' ) { 
                        count -= 1;
                    }
                    if (size_t(count) > bufSize - offs ) { 
                        count = bufSize - offs;
                    }
                    strncpy(buf + offs, input, count);
                    if (size_t(offs + count) < bufSize) { 
                        buf[offs+count] = 0;
                    }
                    
                }
                else
#endif
                {
                    if (prompt != NULL && out == stdout)
                    {
                        fputs(prompt, out);
                    }

                    if (fgets(buf + offs, (int)(bufSize - offs), in) == NULL) {
                        break;
                    }
                }
                if (offs == 0)
                {
                    char cmd[BUF_SIZE];
                    char* cmdArgs[MAX_COMMAND_ARGS];
                    int nArgs = parseArguments(buf, cmd, cmdArgs);
                    if (nArgs > 0)
                    {
                        InteractiveSqlExtension* ext = (InteractiveSqlExtension*)extensions->get(cmdArgs[0]);
                        if (ext != NULL)
                        {
                            if (nArgs - 1 < ext->minArgs || nArgs - 1 > ext->maxArgs)
                            {
                                fprintf(stderr, "Incorrect number of command arguments\nit");
                            }
                            else
                            {
                                MCO_TRY {
                                    bool cont = (*ext->callback)(this, nArgs, cmdArgs, in, out, err);
                                    if (!cont)
                                    {
                                        return false;
                                    }
                                }
#if MCO_CFG_USE_EXCEPTIONS
                                catch (McoSqlException const &x)
                                {
                                    fprintf(err, "ERROR: %s\n", x.getMessage()->cstr());
                                }
#endif
                            }
                            goto nextLine;
                        }
                    }
                }
                if (buf[0] == '-' && buf[1] == '-')
                {
                    // skip commented statement
                    goto nextLine;
                }
                int len = (int)STRLEN(buf + offs);
                int i = offs + len;
                while (--i >= offs && isspace((unsigned char)buf[i]))
                    ;
                if (i >= offs)
                {
                    if (buf[i] == ';')
                    {
#if MCO_CFG_XSQL_USE_READLINE
                        if (readlineLib.add_history != NULL) {
                            readlineLib.add_history(buf);
                        }
#endif
#if MCO_CFG_XSQL_USE_EDITLINE
                        if (editlineLib.initialized) {
                            editlineLib.f_history( editlineLib.hs, &editlineLib.ev, H_ENTER, buf);
                        }
#endif
                        buf[i] = '\0';
                        return true;
                    }
                    if (offs + len - 1 == i) { // no separator at end of line: removed by getline
                        if (offs + len < (int)bufSize) {
                            buf[offs + len] = '\n'; // just add it to avoid false concatenation of identifiers
                        } else {
                            i -= 1; // no space to add separator, just adjust "i", so that i + 2 produce correct result
                        }
                    }
                    offs = i + 2; // leave separator to avoid false concatenation of identifiers
                }
            }
            return false;
        }

        char* SqlEngine::printValue(char* buf, size_t bufSize, Value* v)
        {
            if (v != NULL) {
                v->toString(buf, bufSize - 1);
                buf[bufSize-1] = '\0';
                return buf;
            } else {
                return (char*)"null";
            }
        }

        void SqlEngine::print(const char *format, ...) 
        {
            va_list list;
            for (int i = 0; i < nOuts; ++i) {
                va_start(list, format);
                outs[i].print(format, &list);
                va_end(list);
            }
        }

        void SqlEngine::main(char const* prompt, FILE* in, FILE* out, FILE* err)
        {
            char buf[BUF_SIZE];
            buf[BUF_SIZE - 1] = '\0';

            outs[0].setFd(out);

#if MCO_CFG_XSQL_USE_READLINE
            if (in == stdin && out == stdout && prompt != NULL && readlineLib.hnd == NULL) {
                fprintf(err, "Warning : Readline library was not found. Line editor is disabled.\n");
            }
#endif

#if MCO_CFG_XSQL_USE_EDITLINE
            if (in == stdin && out == stdout && prompt != NULL && editlineLib.initialize( in, out, err, prompt ) ) {
                fprintf(err, "Warning : Failed to initialize editline library. Line editor disabled.\n");
            }
#endif

            while (readStatement(prompt, in, outs[0].getFd(), err, buf, BUF_SIZE - 1))
            {
                MCO_TRY
                {
                    //size_t usedBefore = MemoryManager::allocator->mark();
                    outs[0].reset();
                    outs[1].reset();
                    timer_unit start = 0;

                    if (traceEnabled) {
                        start = mco_system_get_current_time();
                    }
                    outs[1].print("%s\n", buf);

                    DataSource* source = executeQuery(buf);
                    if (source == NULL)
                    {
                        #if 0
                            print("Done\n");
                        #endif
                    }
                    else
                    {
                        int nRecords = 0;
                        Cursor* records = source->records();

                        if (out == NULL)
                        {
                            while (records->hasNext())
                            {
                                records->next();
                                nRecords += 1;
                            }
                            fprintf(stdout, "Select %d records\n", nRecords);
                        }
                        else
                        {
                            Iterator < Field > * fields = source->fields();
                            int nFields = 0;
                            char sep;

                            switch (format)
                            {
                                case LIST:
                                    while (records->hasNext())
                                    {
                                        Record* r = records->next();
                                        fields = source->fields();
                                        for (int i = 0; fields->hasNext(); i++)
                                        {
                                            Field* f = fields->next();
                                            if (f->name() != NULL)
                                            {
                                                print("%s: ", f->name()->cstr());
                                            }
                                            else
                                            {
                                                print("#%d: ", i+1);
                                            }
                                            Value *v = r->get(i);
                                            (v ? v : NullValue::create())->output(outs, nOuts);
                                            print("\n");
                                        }
                                        nRecords += 1;
                                        print("-------------------------------------------------------\n");
                                    }
                                    print("\nSelected records: %d\n", nRecords);
                                    break;
                                case TEXT:
                                case CSV:
                                    sep = (format == TEXT) ? '\t' : ',';
                                    while (fields->hasNext())
                                    {
                                        if (nFields != 0)
                                        {
                                            print("%c", sep);
                                        }
                                        Field* f = fields->next();
                                        nFields += 1;
                                        if (f->name() != NULL)
                                        {
                                            print(f->name()->cstr());
                                        }
                                        else
                                        {
                                            print("#%d", nFields);
                                        }
                                    }
                                    if (format == TEXT)
                                    {
                                        print("\n------------------------------------------------------------------------------\n");
                                    }
                                    else
                                    {
                                        print("\n");
                                    }
                                    while (records->hasNext())
                                    {
                                        Record* r = records->next();
                                        for (int i = 0; i < nFields; i++)
                                        {
                                            if (i != 0)
                                            {
                                                print("%c", sep);
                                            }
                                            Value *v = r->get(i);
                                            (v ? v : NullValue::create())->output(outs, nOuts);
                                        }
                                        print("\n");
                                        nRecords += 1;
                                    }
                                    if (format == TEXT)
                                    {
                                        print("\nSelected records: %d\n", nRecords);
                                        //fprintf(out, "Used memory: %d bytes\n", (int)(MemoryManager::allocator->mark() - usedBefore));
                                    }
                                    break;

                                case HTML:
                                    print("<table border>\n<tr>");
                                    while (fields->hasNext())
                                    {
                                        Field* f = fields->next();
                                        nFields += 1;
                                        if (f->name() != NULL)
                                        {
                                            print("<th>%s</th>", f->name()->cstr());
                                        }
                                        else
                                        {
                                            print("<th>column%d</th>", nFields);
                                        }
                                    }
                                    print("</tr>\n");
                                    while (records->hasNext())
                                    {
                                        print("<tr>");
                                        Record* r = records->next();
                                        for (int i = 0; i < nFields; i++)
                                        {
                                            print("<td>");
                                            outs[0].setHTMLEncode(true);
                                            outs[1].setHTMLEncode(true);
                                            Value *v = r->get(i);
                                            (v ? v : NullValue::create())->output(outs, nOuts);
                                            outs[0].setHTMLEncode(false);
                                            outs[1].setHTMLEncode(false);
                                            print("</td>");
                                        }
                                        print("</tr>\n");
                                    }
                                    print("</table>\n");
                                    break;

                                case XML:
                                    while (records->hasNext())
                                    {
                                        Record* r = records->next();
                                        fields = source->fields();
                                        print("<%s>\n", source->sourceTable() != NULL ? source->sourceTable()
                                                ->name()->cstr(): "tuple");
                                        for (int i = 0; fields->hasNext(); i++)
                                        {
                                            Field* f = fields->next();
                                            if (f->name() != NULL)
                                            {
                                                print("  <%s>", f->name()->cstr());
                                            }
                                            else
                                            {
                                                print("  <column%d>", i + 1);
                                            }
                                            outs[0].setHTMLEncode(true);
                                            outs[1].setHTMLEncode(true);
                                            Value *v = r->get(i);
                                            (v ? v : NullValue::create())->output(outs, nOuts);
                                            outs[0].setHTMLEncode(false);
                                            outs[1].setHTMLEncode(false);
                                            if (f->name() != NULL)
                                            {
                                                print("</%s>\n", f->name()->cstr());
                                            }
                                            else
                                            {
                                                print("</column%d>\n", i + 1);
                                            }
                                        }
                                        print("</%s>\n", source->sourceTable() != NULL ? source->sourceTable()
                                                ->name()->cstr(): "tuple");
                                    }
                                    break;
                            }
                        }
                        records->release();
                        source->release();
                    }
                    if (traceEnabled) {
                        print("Elapsed time: %d\n", (int)(mco_system_get_current_time() - start));
                    }
                    outs[1].flush();
                }
#if MCO_CFG_USE_EXCEPTIONS
                catch (McoSqlException const &x)
                {
                    fprintf(err, "ERROR: %s\n", x.getMessage()->cstr());
                }
#endif
            }
        }


    InteractiveSqlExtension::InteractiveSqlExtension(char const* name, char const* syntax, char const* description,
                                                     command_t callback, int minArgs, int maxArgs)
    {
        this->name = name;
        this->syntax = syntax;
        this->description = description;
        this->callback = callback;
        this->minArgs = minArgs;
        this->maxArgs = maxArgs;
        next = chain;
        chain = this;
    }

    InteractiveSqlExtension* InteractiveSqlExtension::chain;

    #endif

    SqlFunctionDeclaration* SqlFunctionDeclaration::chain;

    const int MAX_DATE_TIME_STRING_LEN = 256;

    SqlFunctionDeclaration::SqlFunctionDeclaration(Type retType, char const* funcName, void* fptr, int nFuncArgs, void* funcCtx)
    : type(retType), elemType(tpNull), name(funcName), nArgs(nFuncArgs), args(NULL), ctx(funcCtx)
    {
        func = fptr;
    }

    SqlFunctionDeclaration::SqlFunctionDeclaration(Type retType, char const* funcName, void* fptr, int nFuncArgs)
    : type(retType), elemType(tpNull), name(funcName), nArgs(nFuncArgs), args(NULL), ctx(NULL)
    {
        func = fptr;
        next = chain;
        chain = this;
    }

    SqlFunctionDeclaration::SqlFunctionDeclaration(Type retType, Type retElemType, char const* funcName, void* fptr, Vector<Field>* fargs)
    : type(retType), elemType(retElemType), name(funcName), nArgs(SqlFunctionDeclaration::FUNC_VARARG), args(fargs), ctx(NULL)
    {
        func = fptr;
    }

    static String* format(Value* fmt, Value* val)
    {
        if (fmt->type() != tpString)
        {
            MCO_THROW InvalidArgument("first argument of format function should be format string");
        }
        char* f = fmt->stringValue()->cstr();

        switch (val->type())
        {
            case tpDateTime:
                {
                    char buf[MAX_DATE_TIME_STRING_LEN];
                    time_t timestamp = (time_t)val->intValue();
                    #ifdef HAVE_LOCALTIME_R
                        struct tm t;
                        strftime(buf, sizeof(buf), f, localtime_r(&timestamp, &t));
                    #else
                        strftime(buf, sizeof(buf), f, localtime(&timestamp));
                    #endif
                    return String::create(buf);
                }
            case tpInt:
                return String::format(f, val->intValue());
                #ifndef NO_FLOATING_POINT
                case tpReal:
                    return String::format(f, val->realValue());
                #endif
            default:
                return String::format(f, val->stringValue()->cstr());
        }
    }
    static SqlFunctionDeclaration f1(tpString, "format", (void*)format, 2);

    static String* choice(Value* cond, Value* thenVal, Value* elseVal)
    {
        return cond->isTrue() ? thenVal->stringValue(): elseVal->stringValue();
    }
    static SqlFunctionDeclaration f2(tpString, "choice", (void*)choice, 3);

    static String* trim(Value* str)
    {
        String* s = str->stringValue();
        char* beg = s->body();
        int len = s->size();

        while (len > 0 && isspace(beg[0] &0xFF))
        {
            len -= 1;
            beg += 1;
        }
        while (len > 0 && isspace(beg[len - 1] &0xFF))
        {
            len -= 1;
        }
        return String::create(beg, len);
    }
    static SqlFunctionDeclaration f3(tpString, "trim", (void*)trim, 1);

    static McoSql::Value* mod(McoSql::Value* a, McoSql::Value* b)
    {
        if (a->isNull() || b->isNull())
        {
            return NULL;
        }
        return new IntValue(a->intValue() % b->intValue());
    }
    static SqlFunctionDeclaration f4(tpInt, "mod", (void*)mod, 2);

#ifndef NO_FLOATING_POINT
    static McoSql::Value* rnd()
    {
        return new RealValue((double)rand() / RAND_MAX);
    }
    static SqlFunctionDeclaration f5(tpReal, "rnd", (void*)rnd, 0);
#endif

    static IntValue* hashCode(McoSql::Value* val)
    {
        return new IntValue(val->hashCode());
    }
    static SqlFunctionDeclaration hashFunc(tpInt, "hashcode", (void*)hashCode, 1);

    static int64_t sequence_counter;

    static IntValue* seqnumber()
    {
        return new IntValue(++sequence_counter);
    }
    static SqlFunctionDeclaration seqnumberFunc(tpInt, "seqnumber", (void*)seqnumber, 0);

    static IntValue* signature(McoSql::Value* val)
    {
        int64_t signature = 0;
        String* str = val->stringValue();
        char* data = str->body();
        for (int i = 0, len = str->size(); i < len && data[i]; i++) { 
            signature |= 1 << (data[i] & 63);
        }        
        return new IntValue(signature);
    }
    static SqlFunctionDeclaration signatureFunc(tpInt, "signature", (void*)signature, 1);

#ifdef UNICODE_SUPPORT
    static IntValue* unicodeSignature(McoSql::Value* val)
    {
        int64_t signature = 0;
        UnicodeString* str = val->unicodeStringValue();
        wchar_t* data = str->body();
        for (int i = 0, len = str->size(); i < len && data[i]; i++) { 
            signature |= 1 << (data[i] & 63);
        }
        return new IntValue(signature);
    }
    static SqlFunctionDeclaration unicodeSignatureFunc(tpInt, "usignature", (void*)unicodeSignature, 1);
#endif

    static IntValue* patternSignature(McoSql::Value* val)
    {
        int64_t signature = 0;
        String* str = val->stringValue();
        char* data = str->body();
        for (int i = 0, len = str->size(); i < len; i++) { 
            char ch = data[i];
            if (ch == 0) { 
                break;
            } else if (ch == '\\') {
                i += 1;
            } else if (ch != '%' && ch != '_') {
                signature |= 1 << (ch & 63);
            }
        }
        return new IntValue(signature);
    }
    static SqlFunctionDeclaration patternSignatureFunc(tpInt, "psignature", (void*)patternSignature, 1);

#ifdef UNICODE_SUPPORT
    static IntValue* unicodePatternSignature(McoSql::Value* val)
    {
        int64_t signature = 0;
        UnicodeString* str = val->unicodeStringValue();
        wchar_t* data = str->body();
        for (int i = 0, len = str->size(); i < len; i++) { 
            wchar_t ch = data[i];
            if (ch == 0) { 
                break;
            } else if (ch == '\\') {
                i += 1;
            } else if (ch != '%' && ch != '_') {
                signature |= 1 << (ch & 63);
            }
        }
        return new IntValue(signature);
    }
    static SqlFunctionDeclaration unicodePatternSignatureFunc(tpInt, "upsignature", (void*)unicodePatternSignature, 1);
#endif

    static String* env(McoSql::Value* str)
    {
        return String::create(getenv(str->stringValue()->cstr()));
    }

    static SqlFunctionDeclaration f6(tpString, "env", (void*)env, 1);


    static McoSql::Value* vxor(Vector < McoSql::Value > * params)
    {
        int64_t s = 0;
        for (int i = 0; i < params->length; i++)
        {
            if (!params->items[i]->isNull())
            {
                s ^= params->items[i]->intValue();
            }
        }
        return new IntValue(s);
    }

    static SqlFunctionDeclaration f7(tpInt, "xor", (void*)vxor,  - 1);


}
