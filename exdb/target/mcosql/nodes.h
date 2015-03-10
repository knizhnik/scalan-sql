/*******************************************************************
 *                                                                 *
 *  nodes.h                                                      *
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
 * MODULE:    nodes.h
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#ifndef __NODES_H__
    #define __NODES_H__

    #include <math.h>

    #include <sqlcpp.h>
    #include "scheme.h"

    namespace McoSql
    {

        class Runtime;
        class OrderNode;
        class TableNode;
        class ColumnNode;
        class GroupNode;
        class ResultSet;
        class Tuple;
        class SqlFunctionDeclaration;


        class ExprNode: public DynamicObject
        {
            public:
                enum ExprCode
                {
                    opNop, opIntAdd, opIntSub, opIntMul, opIntDiv, opIntAnd, opIntOr, opIntNeg, opIntNot, opIntAbs,
                    opIntPow, opTimeAdd, opTimeSub, opEq, opNe, opGt, opGe, opLt, opLe, opBetweenII,
                    opBetweenEE, opBetweenEI, opBetweenIE, opExactMatchIII,  /* opPrefixMatchIII is invalid*/
                    opExactMatchAII, opPrefixMatchAII, opExactMatchSS, opPrefixMatchSS, opExactMatchUU,
                    opPrefixMatchUU, opStrLike, opStrILike, opUnicodeStrLike, opUnicodeStrILike, opStrLikeEsc, opStrILikeEsc, 
                    opUnicodeStrLikeEsc, opUnicodeStrILikeEsc, opRealAdd,
                    opRealSub, opRealMul, opRealDiv, opRealNeg, opRealAbs, opRealPow, opIntToTime, opIntToReal,
                    opIntToRef, opRefToInt, opRealToInt, opAnyToStr, opStrToInt, opStrToReal, opStrToTime,
                    opMbsToWcs, opWcsToMbs, opIsNull, opIsTrue, opIsFalse, opGetAt, opSysdate, opTable,
                    opColumn, opColumnRef, opExternalField, opIntToBool, opRealToBool, opStrToBool, opLength,
                    opSubStr, opUnicodeSubStr, opSubStr2, opUnicodeSubStr2, opExists, opIndexVar, opFalse, opTrue, opNull, opLoadBlob,
                    opScanArray, opInString, opInUnicodeString, opIntConst, opRealConst, opStrConst,
                    opUnicodeStrConst, opConst, opIndirectParam, opParam, opDeref, opBoolToInt, opBoolToReal,
                    opRealSin, opRealCos, opRealTan, opRealAsin, opRealAcos, opRealAtan, opRealSqrt, opRealExp,
                    opRealLog, opRealCeil, opRealFloor, opBoolAnd, opBoolOr, opBoolNot, opStrLower, opStrUpper,
                    opStrConcat, opUnicodeStrLower, opUnicodeStrUpper, opUnicodeStrConcat, opLoad, opSelect,
                    opUpdate, opDelete, opInsert, opCreateTable, opCreateIndex, opDropTable, opDropIndex,
                    opIntMinAll, opIntMaxAll, opIntAvgAll, opIntSumAll, opIntSequenceAll, opIntCorAll, opIntCovAll, 
                    opIntMinDis, opIntMaxDis, opIntAvgDis, opIntSumDis, opIntSequenceDis, opIntCorDis, opIntCovDis, 
                    opRealMinAll, opRealMaxAll, opRealAvgAll, opRealSumAll, opRealSequenceAll,
                    opRealMinDis, opRealMaxDis, opRealAvgDis, opRealSumDis, opRealSequenceDis,
                    opNumericMinAll, opNumericMaxAll, opNumericAvgAll, opNumericSumAll, opNumericSequenceAll,
                    opNumericMinDis, opNumericMaxDis, opNumericAvgDis, opNumericSumDis, opNumericSequenceDis,
                    opCountAll, opCountDis, opCountNotNull, opSequenceCount,
                    opStrMin, opStrMax, opAvgMerge,
                    opSequenceMinAll, opSequenceMaxAll, opSequenceAvgAll, opSequenceSumAll,  opSequenceSequenceAll, opSequenceCorAll, opSequenceCovAll,
                    opScanSet, opSetNotEmpty, opQuantorAll, opQuantorAny, opSubqueryElem, opCreateDomain, opCreateView,
                    opFuncCall, opIntersect, opMinus, opUnion, opUnionAll, opBinarySearch,
                    opStartTransaction, opCommitTransaction, opRollbackTransaction, opSetDefaultIsolationLevel, opSetDefaultPriority, opCase,
                    opNumericAdd, opNumericSub, opNumericMul, opNumericDiv, opNumericNeg, opNumericAbs,
                    opNumericToTime, opNumericToReal, opNumericToInt, opIntToNumeric, opStrToNumeric, opUserAggr,
                    opOverlaps, opContains, opNear, opContainsAll, opContainsAny, opDropView, opDropFunction, opNewArray, opCastArray, opIfNull, opMark, opTimeToInt, 
                    opCreateFunction, opScaleNumeric, opNullIfZero
                };

                static char const* const opMnemonic[];

                Type type;
                ExprCode tag;

                virtual FieldDescriptor* getField();

                virtual Type getElemType();

                virtual Value* evaluate(Runtime* runtime) = 0;

                virtual int dependsOn();

                virtual bool equalsNode(ExprNode* node);

                virtual int compare(ExprNode* obj);

                virtual String* dump();

                virtual bool isGroupNode();

                virtual bool isCreateNode();

                virtual bool isDDLNode();

                virtual bool isBinOpNode();

                virtual bool isUnaryOpNode();

                virtual bool isSelfLoadNode();

                virtual bool isTripleOpNode();

                virtual bool isLiteralNode();

                virtual bool isConstantExpression();

                virtual bool isQueryNode();

                virtual bool isTransactionNode();

                ExprNode(Type type, ExprCode tag)
                {
                        this->type = type;
                        this->tag = tag;
                }
        };

        class StmtNode: public ExprNode
        {
            public:
                virtual int executeStmt(Runtime* runtime) = 0;

                virtual Value* evaluate(Runtime* runtime);

                virtual bool isReadOnly();

                virtual void cleanup();

                StmtNode(ExprCode tag): ExprNode(tpNull, tag){}
        };

        class TransactionNode: public StmtNode
        {
          public:
            Transaction::Mode mode;
            Transaction::IsolationLevel isolationLevel;
            int priority;

            virtual int executeStmt(Runtime* runtime);
            virtual bool isTransactionNode();

            TransactionNode(ExprCode tag) : StmtNode(tag) {
                mode = Transaction::ReadWrite;
                isolationLevel = Transaction::DefaultIsolationLevel;
                priority = 0;
            }
        };

        class QueryNode: public StmtNode
        {
            public:
                Vector < ColumnNode > * resultColumns;
                bool sortNeeded;
                bool forUpdate;
                OrderNode* order;
                ExprNode* start;
                ExprNode* limit;


                static int compareRecordAllFields(void* p, void* q, void* ctx);
                static int compareRecordOrderByField(void* p, void* q, void* ctx);
                static int compareRecordOrderByFields(void* p, void* q, void* ctx);

                DataSource* subset(DataSource* src, Runtime* runtime);

                virtual DataSource* executeQuery(Runtime* runtime) = 0;
                virtual int executeStmt(Runtime* runtime);
                virtual Value* evaluate(Runtime* runtime);
                virtual bool isQueryNode();
                virtual bool isReadOnly();

                QueryNode(ExprCode cop, Vector < ColumnNode > * columns = NULL);
        };

        class TableOpNode: public QueryNode
        {
            public:
                QueryNode* left;
                QueryNode* right;

                int skipDuplicates(Record** rp, int i, int len, int* nColumns);

                bool isQueryNode();
                DataSource* executeQuery(Runtime* runtime);

                TableOpNode(ExprCode cop, Vector < ColumnNode > * columns, QueryNode* left, QueryNode* right);
        };


        class SelectNode: public QueryNode
        {
            public:
                enum SelectType
                {
                    SELECT_DISTINCT, SELECT_ALL, SELECT_COUNT, SELECT_INCREMENTAL, SELECT_GROUP_BY
                };

                TableDescriptor* table;
                ExprNode* condition;
                SelectType selectType;

                Vector < TableNode > * tables;
                Vector < ColumnNode > * columns;
                Vector < ColumnNode > * groupColumns;

                int firstTableId;
                int outDep;
                SelectNode* outer;
                SelectNode* inners;
                SelectNode* sibling;
                DataSource* preliminaryResult;

                OrderNode* groupBy;
                ExprNode* having;

                GroupNode* groups;
                int traceTableNo;
                int searchTableNo;
                IndexDescriptor* usedIndex;
                bool selectAllColumns;
                bool preliminaryResultsCalculated;
                bool flattened;
                DynamicTableDescriptor* dynamicTables;

                bool sameOrder(Runtime* runtime, IndexDescriptor* index, OrderNode* order, DataSource* ds = NULL);
                bool isProjection();
                ResultSet* createResultSet(Runtime* runtime);
                DataSource* projection(DataSource* src, Runtime* runtime);
                Record* projectRecord(Record* rec, Runtime* runtime);

                void setCurrentRecord(Runtime* runtime, int tableNo, Record* rec);
                IndexDescriptor* locateIndex(ExprNode* expr, IndexOperationKind kind);
                bool canUseIndex(Runtime* runtime, ExprNode* expr, int tableNo, int level, bool& isUnique);
                int calculateCost(SqlOptimizerParameters const& params, ExprNode* expr, int tableNo);
                int calculateCost(SqlOptimizerParameters const& params, ExprNode* expr, bool isUnique);

                DataSource* applyIndex(Runtime* runtime, ExprNode* expr, ExprNode* topExpr, int tableNo, ExprNode*
                                       filter, int level);
                DataSource* intersectDataSources(ResultSet* left, ResultSet* right);
                DataSource* unionDataSources(ResultSet* left, ResultSet* right);
                DataSource* calculateKeyBounds(Runtime* runtime, ExprNode* expr, ExprNode* topExpr, int tableNo,
                                               ExprNode* filter, IndexDescriptor* index, Range* ranges, int keyNo, int
                                               level, bool &searchConstant);
                bool getJoinColumn(TableDescriptor* table, int tableNo, ExprNode* condition, Value* &value, int& column, Runtime* runtime);
                bool joinTables(int tableNo, ExprNode* condition, Runtime* runtime);
                int dependsOn();
                DataSource* executeQuery(Runtime* runtime);
                void init();
                void cleanup();

                static int compareRid(void* p, void* q, void* ctx);

                SelectNode(SelectNode* scope);
                SelectNode(ExprCode tag, TableDescriptor* desc);
        };

        class AddIndexNode: public StmtNode
        {
            public:
                TableDescriptor* table;
                IndexDescriptor* index;

                bool isCreateNode();
                bool isDDLNode();

                int executeStmt(Runtime* runtime);

                AddIndexNode(String* name, TableDescriptor* table, KeyDescriptor* keys, bool unique, bool ordered, bool spatial, bool trigram);
        };


        class BinOpNode: public ExprNode
        {
                static BoolValue* scanArray(Runtime* runtime, Value* val, Array* arr);

            public:
                ExprNode* left;
                ExprNode* right;

                String* dump();

                bool equalsNode(ExprNode* node);

                bool isBinOpNode();

                int dependsOn();

                Value* evaluate(Runtime* runtime);

                BinOpNode(Type type, ExprCode tag, ExprNode* left, ExprNode* right): ExprNode(type, tag)
                {
                        this->left = left;
                        this->right = right;
                }
        };

        class CaseNode: public ExprNode
        {
          public:
            ExprNode* when;
            ExprNode* then;
            ExprNode* otherwise;

            CaseNode(ExprNode* when, ExprNode* then, ExprNode* otherwise)
            : ExprNode(then->type, opCase)
            {
                this->when = when;
                this->then = then;
                this->otherwise = otherwise;
            }

            String* dump();

            bool equalsNode(ExprNode* node);

            int dependsOn();

            Value* evaluate(Runtime* runtime);
        };

        class LogicalOpNode: public BinOpNode
        {
            public:
                Value* evaluate(Runtime* runtime);

                LogicalOpNode(ExprCode tag, ExprNode* left, ExprNode* right): BinOpNode(tpBool, tag, left, right){}
        };

        class IfNullNode: public BinOpNode
        {
          public:
            Value* evaluate(Runtime* runtime);

            IfNullNode(ExprNode* left, ExprNode* right): BinOpNode(left->type, opIfNull, left, right){}
        };


        class ScanSetNode: public BinOpNode
        {
                static BoolValue* scanSet(Runtime* runtime, QueryNode* select, Value* val, DataSource* source);

            public:
                Value* evaluate(Runtime* runtime);

                ScanSetNode(ExprNode* left, ExprNode* right): BinOpNode(tpBool, opScanSet, left, right){}
        };

        class ColumnNode: public ExprNode
        {
            public:
                ColumnNode* next;
                int index;
                FieldDescriptor* field;
                ExprNode* expr;
                String* name;
                int tableDep;
                bool sorted;
                bool aggregate;
                bool constant;
                bool used;
                bool flattened;
                bool materialized;
                Value* value;

                int dependsOn();

                FieldDescriptor* getField();

                bool equalsNode(ExprNode* node);

                String* dump();

                bool isSelfLoadNode();

                void initialize(Runtime* runtime);

                Value* evaluate(Runtime* runtime);

                ColumnNode(int index, ExprNode* expr, String* alias, ColumnNode* chain);
                ColumnNode(int index, Type type, String* alias);
        };

        class ExternColumnRefNode: public ExprNode
        {
            public:
                FieldDescriptor* getField();
                bool equalsNode(ExprNode* node);
                String* dump();
                Value* evaluate(Runtime* runtime);

                ExternColumnRefNode(ColumnNode* column);

                ColumnNode* column;
        };

        class ExternalFieldNode: public ExprNode
        {
            public:
                bool equalsNode(ExprNode* node);
                String* dump();
                Value* evaluate(Runtime* runtime);

                ExternalFieldNode(ExprNode* expr);

                ExprNode* expr;
        };

        class BinarySearchNode: public ExprNode
        {
            public:
                bool equalsNode(ExprNode* node);

                Value* evaluate(Runtime* runtime);

                String* dump();

                int dependsOn();

                BinarySearchNode(ExprNode* selector, Vector < Value > * list): ExprNode(tpBool, opBinarySearch)
                {
                        this->selector = selector;
                        this->list = list;
                }

                Vector < Value > * list;
                ExprNode* selector;
        };

        class CompareNode: public BinOpNode
        {
            public:
                Value* evaluate(Runtime* runtime);

                CompareNode(ExprCode cop, ExprNode* left, ExprNode* right): BinOpNode(tpBool, cop, left, right){}
        };


        class ConstantNode: public ExprNode
        {
            public:
                bool equalsNode(ExprNode* node);

                Value* evaluate(Runtime* runtime);

                ConstantNode(Type type, ExprCode tag): ExprNode(type, tag){}
        };


        class CreateViewNode: public StmtNode
        {
          public:
            String* viewName;
            String* viewBody;


            bool isCreateNode();
            bool isDDLNode();

            int executeStmt(Runtime* runtime);

            CreateViewNode(String* name, String* body): StmtNode(opCreateView), viewName(name), viewBody(body) {}
        };


        class CreateDomainNode: public StmtNode
        {
            public:
                FieldDescriptor* domain;

                bool isCreateNode();
                bool isDDLNode();

                int executeStmt(Runtime* runtime);

                CreateDomainNode(FieldDescriptor* domain): StmtNode(opCreateDomain)
                {
                        this->domain = domain;
                }
        };

        class CreateFunctionNode: public StmtNode
        {
          public:
            String* library;
            String* cName;
            String* sqlName;
            String* sqlStmt;
            Vector<Field>* args;
            Type retType;
            Type retElemType;
            bool replace;

            bool isCreateNode();
            bool isDDLNode();
            
            int executeStmt(Runtime* runtime);

            CreateFunctionNode(Type retType, Type retElemType, String* library, String* sqlName, String* cName, Vector<Field>* args, String* sqlStmt, bool replace): StmtNode(opCreateFunction) {
                this->retType = retType;
                this->retElemType = retElemType;
                this->library = library;
                this->sqlName = sqlName;
                this->cName = cName;
                this->sqlStmt = sqlStmt;
                this->args = args;
                this->replace = replace;
            }
        };

        class CreateTableNode: public StmtNode
        {
            public:
                TableDescriptor* table;

                bool isCreateNode();
                bool isDDLNode();

                int executeStmt(Runtime* runtime);

                CreateTableNode(TableDescriptor* desc): StmtNode(opCreateTable)
                {
                        table = desc;
                }
        };


        class DeleteNode: public SelectNode
        {
            public:
                int executeStmt(Runtime* runtime);

                DeleteNode(TableDescriptor* desc): SelectNode(opDelete, desc){}
        };


        class DerefNode: public ExprNode
        {
            public:
                ExprNode* base;
                FieldDescriptor* field;

                String* dump();

                bool equalsNode(ExprNode* node);

                int dependsOn();

                Value* evaluate(Runtime* runtime);

                DerefNode(ExprNode* ref, FieldDescriptor* fd): ExprNode(tpStruct, opDeref)
                {
                        base = ref;
                        field = fd;
                }
        };



        class DropIndexNode: public StmtNode
        {
            public:
                String* name;

                bool isDDLNode();

                int executeStmt(Runtime* runtime);

                DropIndexNode(String* name): StmtNode(opDropIndex)
                {
                        this->name = name;
                }
        };


        class DropViewNode: public StmtNode
        {
            public:
                String* name;

                bool isDDLNode();

                int executeStmt(Runtime* runtime);

                DropViewNode(String* name): StmtNode(opDropView)
                {
                        this->name = name;
                }
        };


        class DropFunctionNode: public StmtNode
        {
            public:
                char const* name;

                bool isDDLNode();

                int executeStmt(Runtime* runtime);

                DropFunctionNode(char const* name): StmtNode(opDropView)
                {
                        this->name = name;
                }
        };


        class DropTableNode: public StmtNode
        {
            public:
                TableDescriptor* table;

                bool isDDLNode();

                int executeStmt(Runtime* runtime);


                DropTableNode(TableDescriptor* desc): StmtNode(opDropTable)
                {
                        table = desc;
                }
        };


        class ExistsNode: public ExprNode
        {
            public:
                ExprNode* expr;
                int loopId;

                String* dump();

                int dependsOn();

                Value* evaluate(Runtime* runtime);

                ExistsNode(ExprNode* expr, int loopId): ExprNode(tpBool, opExists)
                {
                        this->expr = expr;
                        this->loopId = loopId;
                }
        };


        class GetAtNode: public BinOpNode
        {
            public:
                FieldDescriptor* field;

                FieldDescriptor* getField();

                Value* evaluate(Runtime* runtime);

                GetAtNode(Type type, ExprNode* base, ExprNode* index, FieldDescriptor* element)
                : BinOpNode(fieldValueType[type], opGetAt, base, index)
                {
                        field = element;
                }
        };


        class IndexNode: public ExprNode
        {
            public:
                int loopId;

                String* dump();
                Value* evaluate(Runtime* runtime);

                IndexNode(int loop): ExprNode(tpInt, opIndexVar)
                {
                        loopId = loop;
                }
        };


        class ColumnValue: public DynamicObject
        {
            public:
                ColumnValue* next;
                FieldDescriptor* field;
                ExprNode* value;

                ColumnValue(FieldDescriptor* field)
                {
                        this->field = field;
                        value = NULL;
                        next = NULL;
                }
        };

	    class ExtraValue: public DynamicObject
        {
            public:
                ExtraValue* next;
                ExprNode*   value;
        };


        class InsertNode: public SelectNode
        {
            public:
                ColumnValue* values;
                QueryNode* select;
                void* structPtr;
                ExtraValue* moreValues;
                IndexDescriptor* replaceIndex;
                bool fixedSizeStrings;
                bool bulkInsert;
                int nValues;

                int executeStmt(Runtime* runtime);

                size_t storeField(Runtime* runtime, Record* rec, Field* field, void* src, size_t offs);

            InsertNode(TableDescriptor* table, ColumnValue* values, int nValues): SelectNode(opInsert, table)
                {
                        this->values = values;
                        this->nValues = nValues;
                        structPtr = NULL;
                        select = NULL;
                        moreValues = NULL;
                        replaceIndex = NULL;
                        fixedSizeStrings = false;
                        bulkInsert = false;
                }
        };

        class MarkNode: public ExprNode
        {
          public:
            Value* evaluate(Runtime* runtime);
            MarkNode() : ExprNode(tpInt, opMark) {}
        };

        class LiteralNode: public ExprNode
        {
            public:
                Value* value;

                String* dump();
                bool isLiteralNode();
                bool isConstantExpression();

                bool equalsNode(ExprNode* node);

                Value* evaluate(Runtime* runtime);

                LiteralNode(Type type, ExprCode tag, Value* value): ExprNode(type, tag)
                {
                        this->value = value;
                }
        };

        class ParamNode: public ExprNode
        {
            public:
                ParamDesc* desc;

                String* dump();
                bool isLiteralNode();

                bool equalsNode(ExprNode* node);

                Value* evaluate(Runtime* runtime);

                ParamNode(ParamDesc* desc): ExprNode(fieldValueType[desc->type], opParam)
                {
                        this->desc = desc;
                }
        };

        class IndirectParamNode: public ExprNode
        {
            public:
                void* ptr;
                Type ptrType;
                String* dump();
                bool isLiteralNode();

                bool equalsNode(ExprNode* node);

                Value* evaluate(Runtime* runtime);

                IndirectParamNode(Type ptrType, void* ptr): ExprNode(fieldValueType[ptrType], opIndirectParam)
                {
                        this->ptrType = ptrType;
                        this->ptr = ptr;
                }
        };


        class LoadNode: public ExprNode
        {
            public:
                FieldDescriptor* field;
                ExprNode* base;

                bool isSelfLoadNode();

                FieldDescriptor* getField();

                String* dump();

                bool equalsNode(ExprNode* node);

                int dependsOn();

                Value* evaluate(Runtime* runtime);

                LoadNode(FieldDescriptor* field, ExprNode* base);
        };

        class OrderNode: public DynamicObject
        {
            public:
                OrderNode* next;
                int columnNo;
                SortOrder kind;

                OrderNode(int columnNo, SortOrder kind)
                {
                        this->columnNo = columnNo;
                        this->kind = kind;
                        next = NULL;
                }
        };


        class QuantorNode: public ExprNode
        {
            public:
                ExprNode* condition;
                ExprNode* subquery;
                Value* elem;

                String* dump();

                Value* evaluate(Runtime* runtime);

                QuantorNode(ExprCode tag, ExprNode* subquery): ExprNode(tpBool, tag)
                {
                        this->subquery = subquery;
                        condition = NULL;
                        elem = NULL;
                }
        };

        class SubqueryElemNode: public ExprNode
        {
            public:
                QuantorNode* quantor;

                Value* evaluate(Runtime* runtime);

                SubqueryElemNode(ExprCode tag, ExprNode* expr): ExprNode(expr->type, opSubqueryElem)
                {
                        quantor = new QuantorNode(tag, expr);
                }
        };

        class JoinColumn
        {
            public:
                JoinColumn* next;
                FieldDescriptor* left;
                FieldDescriptor* right;

                JoinColumn(FieldDescriptor* left, FieldDescriptor* right, JoinColumn* chain)
                {
                        next = chain;
                        this->left = left;
                        this->right = right;
                }
        };

        class TableNode: public ExprNode
        {
            public:
                TableNode* next;
                TableDescriptor* table;
                String* name;
                int index;
                DataSource* projection;
                JoinColumn* joinColumns;
                TableNode* joinTable;
                bool sequentialSearch;
                Record* rec;
                int joinType;
                int64_t nSelectedRecords;
                int64_t nIndexSearches;
                int64_t indexSearchTime;

                Value* evaluate(Runtime* runtime);

                String* dump();

                int dependsOn();
                void cleanup();

                TableNode(TableDescriptor* desc, int idx, TableNode* chain, int join);
        };



        class TripleOpNode: public ExprNode
        {
            public:
                ExprNode* o1;
                ExprNode* o2;
                ExprNode* o3;

                String* dump();

                bool isTripleOpNode();

                bool equalsNode(ExprNode* node);

                int dependsOn();

                Value* evaluate(Runtime* runtime);


                TripleOpNode(Type type, ExprCode tag, ExprNode* a, ExprNode* b, ExprNode* c = NULL): ExprNode(type, tag)
                {
                        o1 = a;
                        o2 = b;
                        o3 = c;
                }
        };

        class BetweenNode: public TripleOpNode
        {
            public:
                Value* evaluate(Runtime* runtime);

                BetweenNode(ExprCode cop, ExprNode* op1, ExprNode* op2, ExprNode* op3): TripleOpNode(tpBool, cop, op1,
                            op2, op3){}
        };

        class UnaryOpNode: public ExprNode
        {
            public:
                ExprNode* opd;

                String* dump();

                bool isUnaryOpNode();

                bool equalsNode(ExprNode* node);

                int dependsOn();

                Value* evaluate(Runtime* runtime);

                UnaryOpNode(Type type, ExprCode tag, ExprNode* node): ExprNode(type, tag)
                {
                        opd = node;
                }
        };

        class NewArrayNode : public ExprNode
        {
            int               nElements;
            Vector<ExprNode>* elements;

          public:
            String* dump();
            bool equalsNode(ExprNode* node);

            int dependsOn();

            Value* evaluate(Runtime* runtime);

            NewArrayNode(int nElems, Vector<ExprNode>* elems) : ExprNode(tpArray, opNewArray), nElements(nElems), elements(elems) {}
        };

        class CastArrayNode : public ExprNode
        {
            Type elemType;
            ExprNode* expr;

          public:
            Type getElemType();
            String* dump();
            bool equalsNode(ExprNode* node);

            int dependsOn();

            Value* evaluate(Runtime* runtime);

            CastArrayNode(Type typ, ExprNode* arr) : ExprNode(tpArray, opCastArray), elemType(typ), expr(arr) {}
        };

        class SetNotEmptyNode: public UnaryOpNode
        {
            public:
                Value* evaluate(Runtime* runtime);

                SetNotEmptyNode(QueryNode* query): UnaryOpNode(tpBool, opSetNotEmpty, query){}
        };

        class UpdateNode: public SelectNode
        {
            public:
                ColumnValue* values;

                int executeStmt(Runtime* runtime);

                UpdateNode(TableDescriptor* desc): SelectNode(opUpdate, desc)
                {
                        values = NULL;
                }
        };

        class FuncCallNode: public ExprNode
        {
            public:
                SqlFunctionDeclaration* fdecl;
                Vector < ExprNode > * args;

                Type getElemType();           

                Value* evaluate(Runtime* runtime);
                int dependsOn();

                FuncCallNode(SqlFunctionDeclaration* fdecl, Vector < ExprNode > * args);
        };

        class GroupNode: public ExprNode
        {
            public:
                GroupNode* next;
                ExprNode* expr;
                int columnNo;
                Value* value;

                static int compareColumn(void* p, void* q, void* ctx);
                void accumulate(Tuple* dst, Tuple* src, AbstractAllocator* allocator);
                void init(Tuple* tuple, AbstractAllocator* allocator);
                Value* calculate(Runtime* runtime);
                GroupNode(Type type, ExprCode tag, ExprNode* e = NULL);

                bool isGroupNode();

                String* dump();

                int dependsOn();

                bool equalsNode(ExprNode* node);

                Value* evaluate(Runtime* runtime);
        };

        extern char const* const typeMnemonic[];
    }

#endif
