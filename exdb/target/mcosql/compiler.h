/*******************************************************************
 *                                                                 *
 *  compiler.h                                                      *
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
 * MODULE:    compiler.h
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#ifndef __COMPILER_H__
    #define __COMPILER_H__

    #include <ctype.h>
    #include "nodes.h"
    #include "cursor.h"
    #include "sqlcpp.h"
    #include "sync.h"

    namespace McoSql
    {

        const int MAX_CONJUNCTS = 64;
        const int MAX_TABLE_JOINS = 15;
        const int MAX_KEY_POS = 4;
        const int MAX_FUNC_ARGS = 4;
        const int MAX_FUNC_VARGS = 16;
        const int INIT_HASH_SIZE = 113;


        enum Token
        {
            tknIdent, tknAt, tknFunc, tknLpar, tknRpar, tknLbr, tknRbr, tknDot, tknComma, tknPower, tknIconst, tknSconst,
            tknUSconst, tknFconst, tknValue, tknAdd, tknSub, tknMul, tknDiv, tknAnd, tknOr, tknNot, tknNull, tknNeg,
            tknEq, tknNe, tknGt, tknGe, tknLt, tknLe, tknBetween, tknEscape, tknExists, tknLike, tknILike, tknLimit, tknIn,
            tknLength, tknLower, tknUpper, tknAbs, tknIs, tknInteger, tknReal, tknString, tknBlob, tknCast, tknCol, tknCol2,
            tknTrue, tknFalse, tknWhere, tknOrder, tknAsc, tknDesc, tknSubstr, tknSin, tknCos, tknTan, tknAsin,
            tknAcos, tknAtan, tknSqrt, tknExp, tknLog, tknCeil, tknFloor, tknBy, tknPrimary, tknKey, tknUsing,
            tknCreate, tknDrop, tknDelete, tknUpdate, tknSet, tknValues, tknTable, tknIndex, tknOn, tknInto,
            tknTinyint, tknSmallint, tknBit, tknBigint, tknFloat, tknBoolean, tknDouble, tknChar, tknVarchar,
            tknBinary, tknVarbinary, tknDate, tknTime, tknTimestamp, tknReference, tknFrom, tknRollback, tknCommit,
            tknSelect, tknInsert, tknFor, tknTo, tknDomain, tknAll, tknDistinct, tknSysdate, tknCount, tknMin,
            tknMax, tknAvg, tknSum, tknSequence, tknCor, tknCov, tknAs, tknAny, tknSome, tknHaving, tknGroup, tknJoin, tknNatural, tknUnion,
            tknNumeric, tknDecimal, tknUnique, tknForeign, tknReferences, tknCascade, tknConstraint, tknInt,
            tknLongvarbinary, tknLongvarchar, tknUnsigned, tknMinus, tknExcept, tknIntersect, tknInner, tknOuter,
            tknLeft, tknRight, tknStruct, tknStructFS, tknUnicode, tknParam, tknIndirectParam, tknExactMatch,
            tknPrefixMatch, tknStart, tknTransaction,
            tknCase, tknWhen, tknThen, tknElse, tknEnd, tknView, tknFlattened, tknOverlaps, tknContains,
            tknNear, tknHash, tknConcat, tknRtree, tknTrigram, tknWith, tknContainsAll, tknContainsAny, tknArray, tknIfNull, tknCoalesce, tknExcl,
            tknFunction, tknReturns, tknLocal, tknTemporary, tknPreserve, tknRows, tknNullIfZero,
            tknEof
        };

        class WithContext : public DynamicObject
        {
          public:
            String* name;
            int defPos;
            int usePos;
            int endPos;
            WithContext* outer;
            WithContext* next;
        };

        class Symbol: public DynamicObject
        {
            public:
                Token tkn;

                Symbol(Token tkn)
                {
                        this->tkn = tkn;
                }
        };


        class Binding: public DynamicObject
        {
            public:
                Binding* next;
                String* name;
                bool used;
                int loopId;

                Binding(String* ident, int loop, Binding* chain)
                {
                        name = ident;
                        used = false;
                        next = chain;
                        loopId = loop;
                }
        };

        class SymbolTable: public HashTable
        {
            public:
                Symbol* get(char const* sym)
                {
                        return (Symbol*)HashTable::get(sym);
                }

                SymbolTable();
        };

        class ResultMetadata : public DynamicObject
        {
          public:
            OrderNode* orderBy;
            OrderNode* groupBy;
            GroupNode* aggregate;
            Vector<ColumnNode>* columns;
            int limit;
        };

        class Compiler: public ExprNode
        {
            public:
                int pos;
                char* str;
                Value* value;
                void* structPtr;
                char* query;
                int length;
                String* ident;
                Token lex;
                int vars;
                Binding* bindings;
                SelectNode* select;
                bool columnIsAggregate;
                IndirectParamNode* param;
                WithContext* withContexts;
                WithContext* with;

                SqlFunctionDeclaration* func;

                enum JoinType
                {
                        JOIN = 0x01, CROSS = 0x02, NATURAL = 0x04, LEFT = 0x08, RIGHT = 0x10, OUTER = 0x20, INNER = 0x40
                };

                enum Context
                {
                    CTX_UNKNOWN, CTX_COLUMNS, CTX_WHERE, CTX_ORDER_BY, CTX_GROUP_BY, CTX_HAVING, CTX_INSERT
                };
                SqlFunctionDeclaration* callCtx;
                Context context;
                TableDescriptor* table;
                String* refTableName;
                va_list* params;
                Value** paramArr;
                ParamDesc* paramDescs;
                int paramCount;
                SqlEngine* engine;
                ColumnNode* columnList;
                
                Compiler(SqlEngine* engine, char const* sql, va_list* list, Value** array, ParamDesc* descs = NULL);

                ResultMetadata* getResultMetadata(DataSource* ds);
                String* getTableName();
            
                Token scan(bool skip = false);

                void throwCompileError(char const* msg, int pos) MCO_NORETURN_SUFFIX;
                void throwCompileError(String* msg, int pos) MCO_NORETURN_SUFFIX;

                void expect(Token tkn, char const* what)
                {
                        int p = pos;
                        if ((lex = scan()) != tkn)
                        {
                                throwCompileError(String::format("Expect '%s'", what), p);
                        }
                }

                void expected(Token tkn, char const* what)
                {
                        if (lex != tkn)
                        {
                                throwCompileError(String::format("'%s' expected", what), pos - 1);
                        }
                }

                FieldDescriptor* getColumn();

                OrderNode* orderBy();
                KeyDescriptor* indexKey();

                ExprNode* parseArray(ExprNode* arr, char* p, int pos);

                TableDescriptor* getTable();

                TableDescriptor* lookupTable(String* tableName);

                ExprNode* condition();

                ColumnNode* addColumns(TableNode* tabref, int nColumns, ColumnNode* list, bool join);

                enum SelectAttributes
                {
                        SA_SUBQUERY = 1, SA_EXISTS = 2
                };

                ExprNode* addJoinCondition(TableNode* leftTable, FieldDescriptor* leftColumn, TableNode* rightTable,
                                           FieldDescriptor* rightColumn, ExprNode* joinCondition);

                QueryNode* setOperation(ExprCode cop, char const* opName, QueryNode* left, QueryNode* right);

                QueryNode* selectStatement(int selectAttr);
                QueryNode* tablePrimary();
                QueryNode* intersectTables();
                QueryNode* unionTables();
                SelectNode* joinTables();
                Array* arrayLiteral();

                char* findView(String* viewName);

                StmtNode* statement();


                String* compoundName(char const* what);

                void addConstraint(Constraint::ConstraintType type, Vector < Field > * fields, String* name = NULL);

                void addForeignKeyConstraint(Vector < Field > * fk, String* name = NULL);

                void setColumnType(FieldDescriptor* fd);

                Vector < Field > * getConstraintFields(FieldDescriptor* columns);

                StmtNode* createTable(bool temporary);
                StmtNode* createFunction();

                ExprNode* disjunction();
                ExprNode* conjunction();

                ExprNode* replaceWithBetween(ExprNode* left, ExprNode* right);

                ExprNode* int2ref(ExprNode* expr);

                static ExprNode* wcs2mbs(ExprNode* expr);
                static ExprNode* mbs2wcs(ExprNode* expr);
                static ExprNode* int2real(ExprNode* expr);
                static ExprNode* int2str(ExprNode* expr);
                static ExprNode* real2str(ExprNode* expr);
                static ExprNode* int2time(ExprNode* expr);

                ExprNode* str2int(ExprNode* expr, int p);
                ExprNode* str2real(ExprNode* expr, int p);
                ExprNode* str2time(ExprNode* expr, int p);

                static ExprNode* numeric2str(ExprNode* expr);
                static ExprNode* numeric2time(ExprNode* expr);
                static ExprNode* numeric2real(ExprNode* expr);
                static ExprNode* numeric2int(ExprNode* expr);
                static ExprNode* int2numeric(ExprNode* expr);
                static ExprNode* str2numeric(ExprNode* expr);

                static void extractListItems(BinOpNode* list, Vector < Value > * v);
                static int getConstantListLength(BinOpNode* list, Type selectorType);

                ExprNode* compare(ExprNode* expr, BinOpNode* list);
                ExprNode* compare(ExprNode* expr, BinOpNode* list, int n);

                ExprNode* convertToString(ExprNode* node, int p);
                ExprNode* convertToUnicodeString(ExprNode* node, int p);
                ExprNode* convertToReal(ExprNode* node, int p);
                ExprNode* convertToNumeric(ExprNode* node, int p);
                ExprNode* convertToInt(ExprNode* node, int p);
                ExprNode* convertToRef(ExprNode* node, int p);
                ExprNode* convertToTime(ExprNode* node, int p);

                ExprNode* comparison();
                ExprNode* addition();
                ExprNode* multiplication();
                ExprNode* power();

                ExprNode* invokeSequenceTernaryFunction(char const* name, ExprNode* op1, ExprNode* op2, ExprNode* op3);
                ExprNode* invokeSequenceBinaryFunction(char const* name, ExprNode* left, ExprNode* right);
                ExprNode* invokeSequenceUnaryFunction(char const* name, ExprNode* opd);

                ExprNode* arrayElement(ExprNode* expr);
                ExprNode* cast(ExprNode* expr);

                ExprNode* structComponent(ColumnNode* column);
                ExprNode* structComponent(ExternColumnRefNode* columnRef);
                ExprNode* structComponent(FieldDescriptor* fd, ExprNode* base);
                ExprNode* objectField(ExprNode* base);
                ExprNode* objectField();
                ExprNode* userFunction();



                ExprNode* buildList();
                ExprNode* groupFunction(ExprCode tag, bool wildcat = false);
                ExprNode* term();

                Value* evaluate(Runtime* runtime);

                static void initializeSymbolTable(SqlEngine::CompilerContext& ctx);
        };

    }

#endif
