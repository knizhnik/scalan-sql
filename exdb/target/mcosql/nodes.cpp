/*******************************************************************
 *                                                                 *
 *  nodes.cpp                                                      *
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
 * MODULE:    nodes.cpp
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#ifndef NO_FLOATING_POINT
    #include <math.h>
#endif
#ifdef __APPLE__
#include <inttypes.h>
#endif

#include "nodes.h"
#include "cursor.h"
#include "stub.h"

#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
class McoGenericSequence;
extern McoSql::Value* mco_seq_sql_max(McoGenericSequence* seq);
extern McoSql::Value* mco_seq_sql_min(McoGenericSequence* seq);
extern McoSql::Value* mco_seq_sql_sum(McoGenericSequence* seq);
extern McoSql::Value* mco_seq_sql_var(McoGenericSequence* seq);
extern McoSql::Value* mco_seq_sql_cov(McoGenericSequence* left, McoGenericSequence* right);
extern McoSql::Value* mco_seq_sql_corr(McoGenericSequence* left, McoGenericSequence* right);
extern McoSql::Value* mco_seq_sql_agg_count(McoGenericSequence* seq);
extern void mco_seq_sql_reset(McoGenericSequence* seq);
extern McoGenericSequence* mco_seq_sql_construct_int(mco_int8* elems, mco_size_t nElems);
extern McoGenericSequence* mco_seq_sql_construct_double(mco_double* elems, mco_size_t nElems);
#endif

namespace McoSql
{

    char const* const ExprNode::opMnemonic[] =
    {
        "Nop", "IntAdd", "IntSub", "IntMul", "IntDiv", "IntAnd", "IntOr", "IntNeg", "IntNot", "IntAbs", "IntPow",
        "TimeAdd", "TimeSub", "Eq", "Ne", "Gt", "Ge", "Lt", "Le", "BetweenII", "BetweenEE", "BetweenEI",
        "BetweenIE", "ExactMatchIII", "opExactMatchAII", "PrefixMatchAII", "ExactMatchSS", "PrefixMatchSS",
        "ExactMatchUU", "PrefixMatchUU", "StrLike", "StrILike", "UnicodeStrLike", "UnicodeStrILike",
        "StrLikeEsc", "StrILikeEsc", "UnicodeStrLikeEsc", "UnicodeStrILikeEsc", "RealAdd",
        "RealSub", "RealMul", "RealDiv", "RealNeg", "RealAbs", "RealPow", "IntToTime", "IntToReal", "IntToRef",
        "RefToInt", "RealToInt", "AnyToStr", "StrToInt", "StrToReal", "StrToTime", "MbsToWcs", "WcsToMbs", "IsNull",
        "IsTrue", "IsFalse", "GetAt", "Sysdate", "Table", "Column", "ColumnRef", "ExternalField", "IntToBool",
        "RealToBool", "StrToBool", "Length", "SubStr", "UnicodeSubStr", "SubStr2", "UnicodeSubStr2", "Exists", "IndexVar", "False", "True",
        "Null", "LoadBlob", "ScanArray", "InString", "InUnicodeString", "IntConst", "RealConst", "StrConst",
        "UnicodeStrConst", "Const", "IndirectParam", "Param", "Deref", "BoolToInt", "BoolToReal", "RealSin",
        "RealCos", "RealTan", "RealAsin", "RealAcos", "RealAtan", "RealSqrt", "RealExp", "RealLog", "RealCeil",
        "RealFloor", "BoolAnd", "BoolOr", "BoolNot", "StrLower", "StrUpper", "StrConcat", "UnicodeStrLower",
        "UnicodeStrUpper", "UnicodeStrConcat", "Load", "Select", "Update", "Delete", "Insert", "CreateTable",
        "CreateIndex", "DropTable", "DropIndex",
        "IntMinAll", "IntMaxAll", "IntAvgAll", "IntSumAll", "IntSequenceAll", "IntCorAll", "IntCovAll", 
        "IntMinDis", "IntMaxDis", "IntAvgDis", "IntSumDis", "IntSequenceDis", "IntCorDis", "IntCovDis", 
        "RealMinAll", "RealMaxAll", "RealAvgAll", "RealSumAll", "RealSequenceAll",
        "RealMinDis", "RealMaxDis", "RealAvgDis", "RealSumDis", "RealSequenceDis",
        "NumericMinAll", "NumericMaxAll", "NumericAvgAll", "NumericSumAll", "NumericSequenceAll",
        "NumericMinDis", "NumericMaxDis", "NumericAvgDis", "NumericSumDis", "NumericSequenceDis",
        "CountAll", "CountDis", "CountNotNull", "SequenceCount", "StrMin", "StrMax", "AvgMerge",
        "SequenceMinAll", "SequenceMaxAll", "SequenceAvgAll", "SequenceSumAll", "SequenceSequenceAll", "SequenceCorAll","SequenceCovAll",
        "ScanSet",
        "SetNotEmpty", "QuantorAll", "QuantorAny", "SubqueryElem", "CreateDomain", "CreateView", "FuncCall", "Intersect", "Minus",
        "Union", "UnionAll", "BinarySearch", "StartTransaction", "CommitTransaction", "RollbackTransaction",
        "SetDefaultIsolationLevel", "SetDefaultPriority", "Case",
        "NumericAdd", "NumericSub", "NumericMul", "NumericDiv", "NumericNeg", "NumericAbs",
        "NumericToTime", "NumericToReal", "NumericToInt", "IntToNumeric", "StrToNumeric", "UserAggr", "Overlaps", "Contains", "Near", "ContainsAll", "ContainsAny", "DropView", "DropFunction", "NewArray", "CastArray", "IfNull", "Mark", "TimeToInt", "CreateFunction", "ScaleNumeric", "NullIfZero"
    };

    Type ExprNode::getElemType()
    {
        FieldDescriptor* fd = getField();
        return fd == NULL ? tpNull : fd->_elemType;
    }

    FieldDescriptor* ExprNode::getField()
    {
        return NULL;
    }

    int ExprNode::dependsOn()
    {
        return 0;
    }

    bool ExprNode::equalsNode(ExprNode* node)
    {
        return node == this;
    }

    int ExprNode::compare(ExprNode* obj)
    {
        return dependsOn() - obj->dependsOn();
    }

    String* ExprNode::dump()
    {
        return String::create(opMnemonic[tag]);
    }

    bool ExprNode::isGroupNode()
    {
        return false;
    }


    bool ExprNode::isCreateNode()
    {
        return false;
    }

    bool ExprNode::isDDLNode()
    {
        return false;
    }

    bool ExprNode::isUnaryOpNode()
    {
        return false;
    }

    bool ExprNode::isQueryNode()
    {
        return false;
    }

    bool ExprNode::isTransactionNode()
    {
        return false;
    }

    bool ExprNode::isBinOpNode()
    {
        return false;
    }

    bool ExprNode::isSelfLoadNode()
    {
        return false;
    }

    bool ExprNode::isLiteralNode()
    {
        return false;
    }

    bool ExprNode::isConstantExpression()
    {
        return false;
    }

    bool ExprNode::isTripleOpNode()
    {
        return false;
    }

    int TransactionNode::executeStmt(Runtime* runtime) {
        return 0;
    }

    bool TransactionNode::isTransactionNode()
    {
        return true;
    }


    Value* StmtNode::evaluate(Runtime* runtime)
    {
        return new IntValue(executeStmt(runtime));
    }

    void StmtNode::cleanup(){}

    bool StmtNode::isReadOnly()
    {
        return false;
    }

    bool AddIndexNode::isCreateNode()
    {
        return true;
    }

    bool AddIndexNode::isDDLNode()
    {
        return true;
    }

    int AddIndexNode::executeStmt(Runtime* runtime)
    {
        index->impl = runtime->db->createIndex(runtime->trans, index);
        return 1;
    }

    AddIndexNode::AddIndexNode(String* name, TableDescriptor* table, KeyDescriptor* keys, bool unique, bool ordered, bool spatial, bool trigram) : StmtNode(opCreateIndex)
    {
        this->table = table;
        this->index = new IndexDescriptor(table, name, keys);
        index->_unique = unique;
        index->_ordered = ordered;
        index->_spatial = spatial;
        index->_trigram = trigram;
    }

    String* NewArrayNode::dump()
    {
        const size_t ARRAY_BUF_SIZE = 1024;
        char buf[ARRAY_BUF_SIZE];
        char* cur = buf;
        char* end = buf + sizeof(buf);
        *cur++ = '[';
        for (int i = 0; i < nElements; i++) {
            String* elem = elements->items[i]->dump();
            int elemSize = elem->size();
            if (cur + elemSize + 2 > end) {
                break;
            }
            if (i != 0) {
                *cur++ = ',';
            }
            memcpy(cur, elem->body(), elemSize);
            cur += elemSize;
        }
        *cur++ = ']';
        return String::create(buf, cur - buf);
    }

    bool NewArrayNode::equalsNode(ExprNode* node)
    {
        NewArrayNode* other = (NewArrayNode*)node;
        if (node->tag != tag || nElements != other->nElements) {
            return false;
        }
        for (int i = 0; i < nElements; i++) {
            if (!elements->items[i]->equalsNode(other->elements->items[i])) {
                return false;
            }
        }
        return true;
    }

    int NewArrayNode::dependsOn()
    {
        int maxDep = 0;
        for (int i = 0; i < nElements; i++) {
            int dep = elements->items[i]->dependsOn();
            if (dep > maxDep) {
                maxDep = dep;
            }
        }
        return maxDep;
    }

    Value* NewArrayNode::evaluate(Runtime* runtime)
    {
        Vector<Value>* values = Vector<Value>::create(nElements);
        for (int i = 0; i < nElements; i++) {
            values->items[i] = elements->items[i]->evaluate(runtime);
        }
        return new ArrayStub(values);
    }

    bool CastArrayNode::equalsNode(ExprNode* node)
    {
        CastArrayNode* other = (CastArrayNode*)node;
        return other->tag == tag && other->elemType == elemType && other->expr->equalsNode(expr);
    }

    Type CastArrayNode::getElemType()
    {
        return elemType;
    }

    String* CastArrayNode::dump()
    {
        return String::format("cast(%s as array(%s))", expr->dump()->cstr(), typeMnemonic[elemType]);
    }


    int CastArrayNode::dependsOn()
    {
        return expr->dependsOn();
    }

    Value* CastArrayNode::evaluate(Runtime* runtime)
    {
        return castArray(elemType, expr->evaluate(runtime));
    }

    String* BinOpNode::dump()
    {
        return String::format("(%s %s %s)", opMnemonic[tag], left->dump()->cstr(), right->dump()->cstr());
    }

    bool BinOpNode::isBinOpNode()
    {
        return true;
    }

    bool BinOpNode::equalsNode(ExprNode* node)
    {
        return node->tag == tag && left->equalsNode(((BinOpNode*)node)->left) && right->equalsNode(((BinOpNode*)node)
                                                    ->right);
    }

    int BinOpNode::dependsOn()
    {
        return max(left->dependsOn(), right->dependsOn());
    }

    inline bool overlaps(Array* a, Array* b)
    {
        if (a->size() != b->size() || (a->size() & 1) != 0) {
            MCO_THROW RuntimeError("Mismatched arrays length");
        }
        for (int i = 0, n = a->size()/2; i < n; i++) {
            if (a->getAt(i)->compare(b->getAt(n+i)) > 0 || a->getAt(i+n)->compare(b->getAt(i)) < 0) {
                return false;
            }
        }
        return true;
    }

    inline bool contains(Array* a, Array* b)
    {
        if (a->size() != b->size() || (a->size() & 1) != 0) {
            MCO_THROW RuntimeError("Mismatched arrays length");
        }
        for (int i = 0, n = a->size()/2; i < n; i++) {
            if (b->getAt(i)->compare(a->getAt(i)) < 0 || b->getAt(i+n)->compare(a->getAt(i+n)) > 0) {
                return false;
            }
        }
        return true;
    }

    inline bool containsAll(Array* a, Array* b)
    {
        int a_size = a->size();
        int b_size = b->size();
        int i, j;
        for (i = 0; i < b_size; i++) {
            for (j = 0; j < a_size && !b->getAt(i)->equals(a->getAt(j)); j++);
            if (j == a_size) {
                return false;
            }
        }
        return true;
    }

    inline bool containsAny(Array* a, Array* b)
    {
        int a_size = a->size();
        int b_size = b->size();
        int i, j;
        for (i = 0; i < b_size; i++) {
            for (j = 0; j < a_size && !b->getAt(i)->equals(a->getAt(j)); j++);
            if (j != a_size) {
                return true;
            }
        }
        return false;
    }


    Value* BinOpNode::evaluate(Runtime* runtime)
    {
        AbstractAllocator* allocator = runtime->allocator;
        size_t mark = runtime->allocator->mark();
        Value* l = left->evaluate(runtime);
        Value* r = right->evaluate(runtime);
        Value* result = NULL;

        if (l ==  &Null || r ==  &Null)
        {
            runtime->allocator->reset(mark);
            return  &Null;
        }
        switch (tag)
        {
          case opNear:
            return &BoolValue::True;
          case opOverlaps:
            return BoolValue::create(overlaps((Array*)l, (Array*)r));
          case opContains:
            return BoolValue::create(contains((Array*)l, (Array*)r));
          case opContainsAll:
            return BoolValue::create(containsAll((Array*)l, (Array*)r));
          case opContainsAny:
            return BoolValue::create(containsAny((Array*)l, (Array*)r));
          case opNumericAdd:
            return new (allocator) NumericValue(((NumericValue*)l)->scale(((NumericValue*)r)->precision) + ((NumericValue*)r)->scale(((NumericValue*)l)->precision), max(((NumericValue*)r)->precision, ((NumericValue*)l)->precision));
          case opNumericSub:
            return new (allocator) NumericValue(((NumericValue*)l)->scale(((NumericValue*)r)->precision) - ((NumericValue*)r)->scale(((NumericValue*)l)->precision), max(((NumericValue*)r)->precision, ((NumericValue*)l)->precision));
          case opNumericMul:
            return new (allocator) NumericValue(((NumericValue*)l)->val * ((NumericValue*)r)->val,
                                    ((NumericValue*)r)->precision +  ((NumericValue*)l)->precision);
          case opNumericDiv:
            if (((NumericValue*)r)->val == 0) {
                return &Null;
            }
            return new (allocator) NumericValue(((NumericValue*)l)->scale(((NumericValue*)r)->precision) / ((NumericValue*)r)->val,
                                    max(((NumericValue*)l)->precision - ((NumericValue*)r)->precision, 0));

            case opTimeAdd:
                return new (allocator) DateTime(((DateTime*)l)->val + ((IntValue*)r)->val);
            case opTimeSub:
                return new (allocator) DateTime(((DateTime*)l)->val - ((IntValue*)r)->val);
            case opIntAdd:
                return new (allocator) IntValue(((IntValue*)l)->val + ((IntValue*)r)->val);
            case opIntSub:
                return new (allocator) IntValue(((IntValue*)l)->val - ((IntValue*)r)->val);
            case opIntMul:
                return new (allocator) IntValue(((IntValue*)l)->val *((IntValue*)r)->val);
            case opIntDiv:
                if (((IntValue*)r)->val == 0) {
                    return &Null;
                }
                return new (allocator) IntValue(((IntValue*)l)->val / ((IntValue*)r)->val);
            case opIntAnd:
                return new (allocator) IntValue(((IntValue*)l)->val &((IntValue*)r)->val);
            case opIntOr:
                return new (allocator) IntValue(((IntValue*)l)->val | ((IntValue*)r)->val);
            case opIntPow:
                {
                    int64_t lval = ((IntValue*)l)->val;
                    int64_t rval = ((IntValue*)r)->val;
                    int64_t res = 1;
                    if (rval < 0)
                    {
                        lval = 1 / lval;
                        rval =  - rval;
                    }
                    while (rval != 0)
                    {
                        if ((rval &1) != 0)
                        {
                            res *= lval;
                        }
                        lval *= lval;
                        rval = (uint64_t)rval >> 1;
                    }
                    return new (allocator) IntValue(res);
                }
                #ifndef NO_FLOATING_POINT
                case opRealAdd:
                    return new (allocator) RealValue(((RealValue*)l)->val + ((RealValue*)r)->val);
                case opRealSub:
                    return new (allocator) RealValue(((RealValue*)l)->val - ((RealValue*)r)->val);
                case opRealMul:
                    return new (allocator) RealValue(((RealValue*)l)->val *((RealValue*)r)->val);
                case opRealDiv:
                    return new (allocator) RealValue(((RealValue*)l)->val / ((RealValue*)r)->val);
                case opRealPow:
                    return new (allocator) RealValue(pow(((RealValue*)l)->val, ((RealValue*)r)->val));
                #endif
            case opStrConcat:
                return String::concat((String*)l, (String*)r);
            case opScanArray:
                result = scanArray(runtime, l, (Array*)r);
                break;
            case opInString:
                result = BoolValue::create(((String*)r)->indexOf((String*)l) >= 0);
                break;
                #ifdef UNICODE_SUPPORT
                case opUnicodeStrConcat:
                    return UnicodeString::concat((UnicodeString*)l, (UnicodeString*)r);
                case opInUnicodeString:
                    result = BoolValue::create(((UnicodeString*)r)->indexOf((UnicodeString*)l) >= 0);
                    break;
                #endif
            case opScaleNumeric:
            {
                NumericValue* num = (NumericValue*)l;
                int prec = (int)r->intValue();
                result = new (allocator) NumericValue(num->intValue(prec), prec);
                break;
            }
            default:
                assert(false);
        }
        runtime->allocator->reset(mark);
        return result;
    }


    BoolValue* BinOpNode::scanArray(Runtime* runtime, Value* val, Array* arr)
    {
        for (int i = 0, n = arr->size(); i < n; i++)
        {
            if (arr->getAt(i)->equals(val))
            {
                return  &BoolValue::True;
            }
        }
        return  &BoolValue::False;
    }


    Value* IfNullNode::evaluate(Runtime* runtime)
    {
        Value* val = left->evaluate(runtime);
        return val->isNull() ? right->evaluate(runtime) : val;
    }

    Value* LogicalOpNode::evaluate(Runtime* runtime)
    {
        if (tag == opBoolAnd)
        {
            Value* val = left->evaluate(runtime);
            if (val ==  &BoolValue::False)
            {
                return val;
            }
            else if (val ==  &Null)
            {
                val = right->evaluate(runtime);
                return (val ==  &BoolValue::False) ? val : &Null;
            }
        }
        else
        {
            Value* val = left->evaluate(runtime);
            if (val ==  &BoolValue::True)
            {
                return val;
            }
            else if (val ==  &Null)
            {
                val = right->evaluate(runtime);
                return (val ==  &BoolValue::True) ? val : &Null;
            }
        }
        return right->evaluate(runtime);
    }

    String* CaseNode::dump()
    {
        return otherwise
            ? String::format("(case when %s then %s else %s)", when->dump()->cstr(), then->dump()->cstr(), otherwise->dump()->cstr())
            : String::format("(case when %s then %s)", when->dump()->cstr(), then->dump()->cstr());
    }

    bool CaseNode::equalsNode(ExprNode* node)
    {
        CaseNode* caseNode = (CaseNode*)node;
        return node->tag == tag && when->equalsNode(caseNode->when)
            && then->equalsNode(caseNode->then)
            && ((otherwise == NULL && caseNode->otherwise == NULL)
                || (otherwise != NULL && caseNode->otherwise != NULL && otherwise->equalsNode(caseNode->otherwise)));
    }

    int CaseNode::dependsOn()
    {
        int dep = max(when->dependsOn(), then->dependsOn());
        if (otherwise) {
            dep = max(dep, otherwise->dependsOn());
        }
        return dep;
    }

    Value* CaseNode::evaluate(Runtime* runtime)
    {
        Value* cond = when->evaluate(runtime);
        if (cond == &BoolValue::True) {
            return then->evaluate(runtime);
        } else if (cond == &BoolValue::False && otherwise != NULL) {
            return otherwise->evaluate(runtime);
        }
        return &Null;
    }


    Value* ScanSetNode::evaluate(Runtime* runtime)
    {
        size_t mark = runtime->allocator->mark();
        Value* l = left->evaluate(runtime);
        DataSource* r = ((QueryNode*)right)->executeQuery(runtime);
        BoolValue* result = scanSet(runtime, (QueryNode*)right, l, r);
        runtime->allocator->reset(mark);
        return result;
    }


    BoolValue* ScanSetNode::scanSet(Runtime* runtime, QueryNode* query, Value* val, DataSource* source)
    {
        if (source->isResultSet() && query->tag == opSelect)
        {
            ResultSet* set = (ResultSet*)source;
            SelectNode* select = (SelectNode*)query;
            if (set->isArray() && ((select->order != NULL && select->order->columnNo == 0) || (select->resultColumns
                ->items[0]->field != NULL && select->usedIndex != NULL && select->resultColumns->items[0]->field ==
                select->usedIndex->_keys->_field)))
            {
                set->toArray();
                int l = 0, r = (int)set->nRecords(runtime->trans);
                if ((select->order != NULL && select->order->kind == DESCENT_ORDER) || select->usedIndex->_keys->_order
                    == DESCENT_ORDER)
                {
                    while (l < r)
                    {
                        int i = (l + r) >> 1;
                        int diff = set->get(i, 0)->compare(val);
                        if (diff <= 0)
                        {
                            r = i;
                        }
                        else
                        {
                            l = i + 1;
                        }
                    }
                }
                else
                {
                    while (l < r)
                    {
                        int i = (l + r) >> 1;
                        int diff = set->get(i, 0)->compare(val);
                        if (diff < 0)
                        {
                            l = i + 1;
                        }
                        else
                        {
                            r = i;
                        }
                    }
                }
                return BoolValue::create(r < set->size && set->get(r, 0)->equals(val));
            }
        }
        Cursor* iterator = source->records();
        while (iterator->hasNext())
        {
            Record* rec = iterator->next();
            if (rec->get(0)->equals(val))
            {
                iterator->release();
                return &BoolValue::True;
            }
        }
        iterator->release();
        return  &BoolValue::False;
    }


    int ColumnNode::dependsOn()
    {
        return tableDep;
    }

    bool ColumnNode::isSelfLoadNode()
    {
        return true;
    }

    FieldDescriptor* ColumnNode::getField()
    {
        return field;
    }

    bool ColumnNode::equalsNode(ExprNode* node)
    {
        return node == this;
    }

    String* ColumnNode::dump()
    {
        return String::format("(%s %d)", opMnemonic[tag], index);
    }

    void ColumnNode::initialize(Runtime* runtime)
    {
        value = used ? expr->evaluate(runtime) : NULL;
    }

    Value* ColumnNode::evaluate(Runtime* runtime)
    {
        return value ? value : expr->evaluate(runtime);
    }

    ColumnNode::ColumnNode(int index, Type type, String* alias): ExprNode(type, opColumn)
    {
        expr = NULL;
        this->index = index;
        name = alias;
        field = NULL;
        sorted = false;
        aggregate = false;
        constant = false;
        used = false;
        value = NULL;
        next = NULL;
        flattened = false;
        materialized = false;
    }

    ColumnNode::ColumnNode(int index, ExprNode* expr, String* alias, ColumnNode* chain): ExprNode(expr->type, opColumn)
    {
        this->expr = expr;
        this->index = index;
        field = NULL;
        name = NULL;
        sorted = false;
        aggregate = false;
        constant = expr->isLiteralNode();
        used = false;
        value = NULL;
        flattened = false;
        materialized = false;
        if (expr->isSelfLoadNode())
        {
            field = expr->getField();
            if (field != NULL) {
                name = field->_name;
            }
        }
        tableDep = expr->dependsOn();
        if (alias != NULL)
        {
            name = alias;
        }
        next = chain;
    }


    FieldDescriptor* ExternColumnRefNode::getField()
    {
        return column->getField();
    }

    bool ExternColumnRefNode::equalsNode(ExprNode* node)
    {
        if (tag == node->tag)
        {
            return column->equalsNode(((ExternColumnRefNode*)node)->column);
        }
        return false;
    }

    String* ExternColumnRefNode::dump()
    {
        return column->dump();
    }

    Value* ExternColumnRefNode::evaluate(Runtime* runtime)
    {
        return column->evaluate(runtime);
    }

    ExternColumnRefNode::ExternColumnRefNode(ColumnNode* column): ExprNode(column->type, opColumnRef)
    {
        this->column = column;
    }

    bool ExternalFieldNode::equalsNode(ExprNode* node)
    {
        if (tag == node->tag)
        {
            return expr->equalsNode(((ExternalFieldNode*)node)->expr);
        }
        return false;
    }

    String* ExternalFieldNode::dump()
    {
        return expr->dump();
    }

    Value* ExternalFieldNode::evaluate(Runtime* rt)
    {
        return expr->evaluate(rt);
    }

    ExternalFieldNode::ExternalFieldNode(ExprNode* expr): ExprNode(expr->type, opExternalField)
    {
        this->expr = expr;
    }

    String* BinarySearchNode::dump()
    {
        return String::format("(%s %s)", opMnemonic[tag], selector->dump()->cstr());
    }

    int BinarySearchNode::dependsOn()
    {
        return selector->dependsOn();
    }

    bool BinarySearchNode::equalsNode(ExprNode* node)
    {
        if (node->tag != tag)
        {
            return false;
        }
        BinarySearchNode* bin = (BinarySearchNode*)node;
        if (!selector->equalsNode(bin->selector))
        {
            return false;
        }
        if (list->length != bin->list->length)
        {
            return false;
        }
        for (int i = 0, n = list->length; i < n; i++)
        {
            if (!list->items[i]->equals(bin->list->items[i]))
            {
                return false;
            }
        }
        return true;
    }

    Value* BinarySearchNode::evaluate(Runtime* runtime)
    {
        size_t mark = runtime->allocator->mark();
        Value* v = selector->evaluate(runtime);
        if (v ==  &Null)
        {
            runtime->allocator->reset(mark);
            return  &Null;
        }
        int l = 0, n = list->length, r = n;
        while (l < r)
        {
            int m = (l + r) >> 1;
            if (v->compare(list->items[m]) > 0)
            {
                l = m + 1;
            }
            else
            {
                r = m;
            }
        }
        bool found = l < n && v->equals(list->items[r]);
        runtime->allocator->reset(mark);
        return BoolValue::create(found);
    }


    Value* CompareNode::evaluate(Runtime* runtime)
    {
        size_t mark = runtime->allocator->mark();
        Value* l = left->evaluate(runtime);
        Value* r = right->evaluate(runtime);

        if (l ==  &Null || r ==  &Null)
        {
            runtime->allocator->reset(mark);
            return  &Null;
        }
        int diff = l->compare(r);
        runtime->allocator->reset(mark);
        switch (tag)
        {
            case opEq:
                return BoolValue::create(diff == 0);
            case opNe:
                return BoolValue::create(diff != 0);
            case opGe:
                return BoolValue::create(diff >= 0);
            case opGt:
                return BoolValue::create(diff > 0);
            case opLe:
                return BoolValue::create(diff <= 0);
            case opLt:
                return BoolValue::create(diff < 0);
            default:
                assert(false);
                return NULL;
        }
    }

    bool ConstantNode::equalsNode(ExprNode* node)
    {
        return node->tag == tag;
    }

    Value* ConstantNode::evaluate(Runtime* runtime)
    {
        switch (tag)
        {
            case opNull:
                return  &Null;
            case opSysdate:
              return new (runtime->allocator) IntValue(time(NULL));
            case opTrue:
                return  &BoolValue::True;
            case opFalse:
                return  &BoolValue::False;
            default:
                assert(false);
        }
        return 0;
    }


    bool CreateViewNode::isCreateNode()
    {
        return true;
    }

    bool CreateViewNode::isDDLNode()
    {
        return true;
    }

    int CreateViewNode::executeStmt(Runtime* runtime)
    {
        return runtime->engine->executeStatement(runtime->trans, "insert into Views (name,body) values (%v,%v)", viewName, viewBody);
    }


    bool CreateDomainNode::isCreateNode()
    {
        return true;
    }

    bool CreateDomainNode::isDDLNode()
    {
        return true;
    }

    int CreateDomainNode::executeStmt(Runtime* runtime)
    {
        runtime->engine->compilerCtx.domainHash->put(domain->_name->cstr(), domain);
        return 1;
    }

    bool CreateTableNode::isCreateNode()
    {
        return true;
    }

    bool CreateTableNode::isDDLNode()
    {
        return true;
    }

    int CreateTableNode::executeStmt(Runtime* runtime)
    {
        runtime->db->createTable(runtime->trans, table);
        return 1;
    }

  
    bool CreateFunctionNode::isCreateNode()
    {
        return true;
    }

    bool CreateFunctionNode::isDDLNode()
    {
        return true;
    }

    int CreateFunctionNode::executeStmt(Runtime* runtime)
    {
        void* func = mco_sys_dll_load(library->cstr(), cName->cstr());
        if (func == NULL) { 
            MCO_THROW RuntimeError(String::format("Failed to load function %s from library %s: %s", cName->cstr(), library->cstr(), mco_sys_dll_error())->cstr());
        }            
        runtime->engine->registerFunction(retType, retElemType, sqlName->cstr(), func, args);
        if (replace && runtime->engine->db->findTable(String::create("Functions"))) { 
            runtime->engine->executeStatement(runtime->trans, "insert or update into Functions(name,stmt) values (%v,%v)", sqlName, sqlStmt);
        }
        return 1;
    }

    int DeleteNode::executeStmt(Runtime* runtime)
    {
        int nSelected = 0;
        if (condition == NULL)
        {
            nSelected = table->isNumberOfRecordsKnown() ? (int)table->nRecords(runtime->trans) :  - 1;
            table->deleteAllRecords(runtime->trans);

        }
        else
        {
            DataSource* source = SelectNode::executeQuery(runtime);
            Cursor* iterator = source->records();
            while (iterator->hasNext())
            {
                table->deleteRecord(runtime->trans, iterator->next());
                nSelected += 1;
            }
            iterator->release();
        }
        return nSelected;
    }

    String* DerefNode::dump()
    {
        return String::format("(%s %s)", opMnemonic[tag], base->dump()->cstr());
    }

    bool DerefNode::equalsNode(ExprNode* node)
    {
        return tag == node->tag && base->equalsNode(((DerefNode*)node)->base) && field == ((DerefNode*)node)->field;
    }

    int DerefNode::dependsOn()
    {
        return base->dependsOn();
    }

    Value* DerefNode::evaluate(Runtime* runtime)
    {
        Value* ref = base->evaluate(runtime);
        if (ref ==  &Null)
        {
            MCO_THROW RuntimeError("NULL reference");
        }
        return ((Reference*)ref)->dereference();
    }

    bool DropIndexNode::isDDLNode() 
    {
        return true;
    }

    int DropIndexNode::executeStmt(Runtime* runtime)
    {
        IndexDescriptor* ip, ** ipp;
        for (TableDescriptor* table = runtime->db->_tables; table != NULL; table = table->next)
        {
            for (ipp = &table->_indices; (ip = * ipp) != NULL; ipp = &ip->next)
            {
                if (name->equals(ip->_name))
                {
                    *ipp = ip->next;
                    ip->drop(runtime->trans);
                    return 1;
                }
            }
        }
        return 0;
    }

    bool DropViewNode::isDDLNode() 
    {
        return true;
    }

    int DropViewNode::executeStmt(Runtime* runtime)
    {
        return runtime->engine->executeStatement(runtime->trans, "delete from Views where name=%v", name);
    }

    bool DropTableNode::isDDLNode() 
    {
        return true;
    }

    int DropTableNode::executeStmt(Runtime* runtime)
    {
        table->drop(runtime->trans);
        return 1;
    }

    bool DropFunctionNode::isDDLNode() 
    {
        return true;
    }

    int DropFunctionNode::executeStmt(Runtime* runtime)
    {        
        runtime->engine->userFuncs->remove(name);
        if (runtime->engine->db->findTable(String::create("Functions"))) { 
            return runtime->engine->executeStatement(runtime->trans, "delete from Functions where name=%s", name);
        }
        return 1;
    }


    String* ExistsNode::dump()
    {
        return String::format("(%s %s)", opMnemonic[tag], expr->dump()->cstr());
    }

    int ExistsNode::dependsOn()
    {
        return expr->dependsOn();
    }

    Value* ExistsNode::evaluate(Runtime* runtime)
    {
        runtime->indexVar[loopId] = 0;
        MCO_TRY
        {
            while (!expr->evaluate(runtime)->isTrue())
            {
                runtime->indexVar[loopId] += 1;
            }
            return  &BoolValue::True;
        }
 #if MCO_CFG_USE_EXCEPTIONS
        catch (NullReferenceException &x)
        {
            if (x.loopId != loopId)
            {
                MCO_THROW x;
            }
            return &BoolValue::False;
        }
        catch (IndexOutOfBounds &x)
        {
            if (x.loopId != loopId)
            {
                MCO_THROW x;
            }
            return &BoolValue::False;
        }
#endif
    }

    FieldDescriptor* GetAtNode::getField()
    {
        return field;
    }


    Value* GetAtNode::evaluate(Runtime* runtime)
    {
        Value* l = left->evaluate(runtime);
        Value* r = right->evaluate(runtime);

        if (l == &Null || r == &Null) {
            if (right->tag == opIndexVar) {
                MCO_THROW NullReferenceException(((IndexNode*)right)->loopId);
            }
            return &Null;
        }
        Array* arr = (Array*)l;
        int index = (int)((IntValue*)r)->val;
        int length = arr->size();
        if ((unsigned)index >= (unsigned)length)
        {
            MCO_THROW IndexOutOfBounds(index, length, (right->tag == opIndexVar) ? ((IndexNode*)right)->loopId:  - 1);
        }
        return arr->getAt(index);
    }

    GroupNode::GroupNode(Type type, ExprCode tag, ExprNode* e): ExprNode(type, tag)
    {
        expr = e;
        next = NULL;
        value = NULL;
        if (e != NULL && e->tag == opColumn)
        {
            columnNo = ((ColumnNode*)e)->index - 1;
        }
        else
        {
            columnNo =  - 1;
        }
    }

    bool GroupNode::isGroupNode()
    {
        return true;
    }


    String* GroupNode::dump()
    {
        return (expr != NULL) ? String::format("(%s %s:%d", opMnemonic[tag], expr->dump()->cstr(), columnNo): ExprNode
                ::dump();
    }

    int GroupNode::dependsOn()
    {
        return expr->dependsOn();
    }

    bool GroupNode::equalsNode(ExprNode* node)
    {
        return node->tag == tag && expr->equalsNode(((GroupNode*)node)->expr);
    }

    int GroupNode::compareColumn(void* p, void* q, void* ctx)
    {
        Record* r1 = (Record*)p;
        Record* r2 = (Record*)q;
        int groupByColumn = *(int*)ctx;
        return r1->get(groupByColumn)->compare(r2->get(groupByColumn));
    }

    Value* GroupNode::evaluate(Runtime*)
    {
        return value;
    }

    void GroupNode::init(Tuple* tuple, AbstractAllocator* allocator)
    {
        Value* &acc = tuple->values[columnNo];
        switch (tag)
        {
          case opCountAll:
            acc = new (allocator) IntValue(1);
            break;
          case opCountNotNull:
            acc = new (allocator) IntValue(!acc->isNull());
            break;
          case opRealAvgAll:
          case opIntAvgAll:
          case opNumericAvgAll:
            if (!acc->isNull()) {
                acc = new (allocator) AvgValue(acc->realValue(), 1);
            }
            break;
          default:
            break;
        }
    }

    void GroupNode::accumulate(Tuple* dst, Tuple* src, AbstractAllocator* allocator)
    {
        Value* &acc = dst->values[columnNo];
        Value* val = src->values[columnNo];
        switch (tag)
        {
          case opCountAll:
            ((IntValue*)acc)->val += 1;
            break;
          case opCountNotNull:
            ((IntValue*)acc)->val += !val->isNull();
            break;
          case opNumericMinAll:
          case opNumericMinDis:
          case opIntMinAll:
          case opIntMinDis:
          case opStrMin:
          case opRealMinAll:
          case opRealMinDis:
            if (!val->isNull() && (acc->isNull() || acc->compare(val) > 0)) {
                acc = val->clone(allocator);
            }
            break;
          case opNumericMaxAll:
          case opNumericMaxDis:
          case opIntMaxAll:
          case opIntMaxDis:
          case opStrMax:
          case opRealMaxAll:
          case opRealMaxDis:
            if (!val->isNull() && (acc->isNull() || acc->compare(val) < 0)) {
                acc = val->clone(allocator);
            }
            break;
          case opIntSumAll:
          case opNumericSumAll:
            if (!val->isNull()) {
                if (acc->isNull()) {
                    acc = val->clone(allocator);
                } else {
                    ((IntValue*)acc)->val += ((IntValue*)val)->val;
                }
            }
            break;
          case opRealSumAll:
            if (!val->isNull()) {
                if (acc->isNull()) {
                    acc = val->clone(allocator);
                } else {
                    ((RealValue*)acc)->val += ((RealValue*)val)->val;
                }
            }
            break;
          case opRealAvgAll:
          case opIntAvgAll:
          case opNumericAvgAll:
            if (!val->isNull()) {
                if (acc->isNull()) {
                    acc = new (allocator) AvgValue(val->realValue(), 1);
                } else {
                    AvgValue* avg = (AvgValue*)acc;
                    avg->val = (avg->val*avg->count + val->realValue())/(avg->count + 1);
                    avg->count += 1;
                }
            }
            break;
          default:
            assert(false);
        }
    }



    Value* GroupNode::calculate(Runtime* runtime)
    {
        if (columnNo < 0)
        {
            MCO_THROW RuntimeError("Invalid use of GROUP function");
        }
        AbstractAllocator* allocator = runtime->allocator;
        int begin = runtime->groupBegin;
        int end = runtime->groupEnd;
        if (end == 0)
        {
            end = runtime->result->size;
        }
        runtime->result->toArray();
        Record** tuples = runtime->result->recordArray->items;

        switch (tag)
        {
            case opCountAll:
                return new (allocator) IntValue(end - begin);
            case opCountDis:
                {
                    int64_t result = 0;
                    iqsort(tuples + begin, end - begin, &compareColumn, &columnNo);
                    Value* prev = NULL;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull() && (prev == NULL || !v->equals(prev)))
                        {
                            prev = v;
                            result += 1;
                        }
                    }
                    return new (allocator) IntValue(result);
                }
            case opCountNotNull:
                {
                    int64_t result = 0;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull()) {
                            result += 1;
                        }
                    }
                    return new (allocator) IntValue(result);
                }
            case opNumericMinAll:
            case opNumericMinDis:
                {
                    int i, maxPrec = 0;
                    for (i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            if (maxPrec < ((NumericValue*)v)->precision) {
                                maxPrec = ((NumericValue*)v)->precision;
                            }
                        }
                    }
                    int64_t result = 0;
                    bool isNull = true;
                    for (i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            int64_t val = ((NumericValue*)v)->scale(maxPrec);
                            if (isNull)
                            {
                                result = val;
                                isNull = false;
                            }
                            else if (val < result)
                            {
                                result = val;
                            }
                        }
                    }
                    if (isNull)
                    {
                        return  &Null;
                    }
                    return new (allocator) NumericValue(result, maxPrec);
                }
            case opNumericMaxAll:
            case opNumericMaxDis:
                {
                    int i, maxPrec = 0;
                    for (i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            if (maxPrec < ((NumericValue*)v)->precision) {
                                maxPrec = ((NumericValue*)v)->precision;
                            }
                        }
                    }
                    int64_t result = 0;
                    bool isNull = true;
                    for (i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            int64_t val = ((NumericValue*)v)->scale(maxPrec);
                            if (isNull)
                            {
                                result = val;
                                isNull = false;
                            }
                            else if (val > result)
                            {
                                result = val;
                            }
                        }
                    }
                    if (isNull)
                    {
                        return  &Null;
                    }
                    return new (allocator) NumericValue(result, maxPrec);
                }
            case opIntMinAll:
            case opIntMinDis:
                {
                    int64_t result = 0;
                    bool isNull = true;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            if (isNull)
                            {
                                result = ((IntValue*)v)->val;
                                isNull = false;
                            }
                            else if (((IntValue*)v)->val < result)
                            {
                                result = ((IntValue*)v)->val;
                            }
                        }
                    }
                    if (isNull)
                    {
                        return  &Null;
                    }
                    return type == tpDateTime ? new (allocator) DateTime(result): new (allocator) IntValue(result);
                }
            case opIntMaxAll:
            case opIntMaxDis:
                {
                    int64_t result = 0;
                    bool isNull = true;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            if (isNull)
                            {
                                result = ((IntValue*)v)->val;
                                isNull = false;
                            }
                            else if (((IntValue*)v)->val > result)
                            {
                                result = ((IntValue*)v)->val;
                            }
                        }
                    }
                    if (isNull)
                    {
                        return  &Null;
                    }
                    return type == tpDateTime ? new (allocator) DateTime(result): new (allocator) IntValue(result);
                }
            case opStrMin:
                {
                    Value* result = &Null;
                    bool isNull = true;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            if (isNull)
                            {
                                result = v;
                                isNull = false;
                            }
                            else if (result->compare(v) > 0)
                            {
                                result = v;
                            }
                        }
                    }
                    return result;
                }
            case opStrMax:
                {
                    Value* result = &Null;
                    bool isNull = true;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            if (isNull)
                            {
                                result = v;
                                isNull = false;
                            }
                            else if (result->compare(v) < 0)
                            {
                                result = v;
                            }
                        }
                    }
                    return result;
                }
            case opIntSumDis:
                {
                    iqsort(tuples + begin, end - begin, &compareColumn, &columnNo);
                    int64_t prev = 0;
                    int64_t result = 0;
                    bool isNull = true;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            int64_t val = ((IntValue*)v)->val;
                            isNull = false;
                            if (val != prev)
                            {
                                prev = val;
                                result += prev;
                            }
                        }
                    }
                    if (isNull)
                    {
                        return  &Null;
                    }
                    return type == tpDateTime ? new (allocator) DateTime(result): new (allocator) IntValue(result);
                }
            case opIntSumAll:
                {
                    int64_t result = 0;
                    bool isNull = true;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            result += ((IntValue*)v)->val;
                            isNull = false;
                        }
                    }
                    if (isNull)
                    {
                        return  &Null;
                    }
                    return type == tpDateTime ? new (allocator) DateTime(result): new (allocator) IntValue(result);
                }
#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
            case opIntSequenceDis:
                {
                    int64_t prev = 0;
                    mco_size_t nElems = end - begin;
                    int64_t* elems = (int64_t*)runtime->allocator->allocate(nElems*sizeof(int64_t));
                    iqsort(tuples + begin, nElems, &compareColumn, &columnNo);
                    nElems = 0;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            int64_t val = ((IntValue*)v)->val;
                            if (nElems == 0 || val != prev)
                            {
                                prev = val;
                                elems[nElems++] = val;
                            }
                        }
                    }
                    return mco_seq_sql_construct_int((mco_int8 *)elems, nElems);
                }
            case opIntSequenceAll:
                {
                    mco_size_t nElems = end - begin;
                    int64_t* elems = (int64_t*)runtime->allocator->allocate(nElems*sizeof(int64_t));
                    nElems = 0;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            elems[nElems++] = ((IntValue*)v)->val;
                        }
                    }
                    return mco_seq_sql_construct_int((mco_int8 *)elems, nElems);
                }
            case opRealSequenceDis:
                {
                    double prev = 0;
                    mco_size_t nElems = end - begin;
                    double* elems = (double*)runtime->allocator->allocate(nElems*sizeof(double));
                    iqsort(tuples + begin, nElems, &compareColumn, &columnNo);
                    nElems = 0;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            double val = ((RealValue*)v)->val;
                            if (nElems == 0 || val != prev)
                            {
                                prev = val;
                                elems[nElems++] = val;
                            }
                        }
                    }
                    return mco_seq_sql_construct_double(elems, nElems);
                }
            case opRealSequenceAll:
                {
                    mco_size_t nElems = end - begin;
                    double* elems = (double*)runtime->allocator->allocate(nElems*sizeof(double));
                    nElems = 0;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            elems[nElems++] = ((RealValue*)v)->val;
                        }
                    }
                    return mco_seq_sql_construct_double(elems, nElems);
                }
#endif
            case opNumericSumDis:
                {
                    iqsort(tuples + begin, end - begin, &compareColumn, &columnNo);
                    int64_t prev = 0;
                    int64_t result = 0;
                    bool isNull = true;
                    int i, maxPrec = 0;
                    for (i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            if (maxPrec < ((NumericValue*)v)->precision) {
                                maxPrec = ((NumericValue*)v)->precision;
                            }
                        }
                    }
                    for (i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            int64_t val = ((NumericValue*)v)->scale(maxPrec);
                            isNull = false;
                            if (val != prev)
                            {
                                prev = val;
                                result += prev;
                            }
                        }
                    }
                    if (isNull)
                    {
                        return  &Null;
                    }
                    return new (allocator) NumericValue(result, maxPrec);
                }
            case opNumericSumAll:
                {
                    int64_t result = 0;
                    bool isNull = true;
                    int i, maxPrec = 0;
                    for (i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            if (maxPrec < ((NumericValue*)v)->precision) {
                                maxPrec = ((NumericValue*)v)->precision;
                            }
                        }
                    }
                    for (i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            result += ((NumericValue*)v)->scale(maxPrec);
                            isNull = false;
                        }
                    }
                    if (isNull)
                    {
                        return  &Null;
                    }
                    return new (allocator) NumericValue(result, maxPrec);
                }
            case opIntAvgDis:
                {
                    int count = 0;
                    int64_t result = 0;
                    if (begin != end)
                    {
                        iqsort(tuples + begin, end - begin, &compareColumn, &columnNo);
                        int64_t prev = 0;
                        for (int i = begin; i < end; i++)
                        {
                            Value* v = tuples[i]->get(columnNo);
                            if (!v->isNull())
                            {
                                int64_t val = ((IntValue*)v)->val;
                                if (val != prev || count == 0)
                                {
                                    prev = val;
                                    result += prev;
                                    count += 1;
                                }
                            }
                        }
                    }
                    if (count == 0)
                    {
                        return  &Null;
                    }
                    return new (allocator) AvgValue((double)result / count, count);
                }
            case opIntAvgAll:
                {
                    int count = 0;
                    int64_t result = 0;
                    if (begin != end)
                    {
                        iqsort(tuples + begin, end - begin, &compareColumn, &columnNo);
                        for (int i = begin; i < end; i++)
                        {
                            Value* v = tuples[i]->get(columnNo);
                            if (!v->isNull())
                            {
                                result += ((IntValue*)v)->val;
                                count += 1;
                            }
                        }
                    }
                    if (count == 0)
                    {
                        return  &Null;
                    }
                    return new (allocator) AvgValue((double)result / count, count);
                }
            case opNumericAvgDis:
                {
                    int count = 0;
                    double result = 0;
                    int i;
                    if (begin != end)
                    {
                        iqsort(tuples + begin, end - begin, &compareColumn, &columnNo);
                        double prev = 0;
                        for (i = begin; i < end; i++)
                        {
                            Value* v = tuples[i]->get(columnNo);
                            if (!v->isNull())
                            {
                                double val = v->realValue();
                                if (val != prev || count == 0)
                                {
                                    prev = val;
                                    result += val;
                                    count += 1;
                                }
                            }
                        }
                    }
                    if (count == 0)
                    {
                        return  &Null;
                    }
                    return new (allocator) AvgValue(result / count, count);
                }
            case opNumericAvgAll:
                {
                    int count = 0;
                    double result = 0;
                    int i;
                    if (begin != end)
                    {
                        iqsort(tuples + begin, end - begin, &compareColumn, &columnNo);
                        for (i = begin; i < end; i++)
                        {
                            Value* v = tuples[i]->get(columnNo);
                            if (!v->isNull())
                            {
                                result += v->realValue();
                                count += 1;
                            }
                        }
                    }
                    if (count == 0)
                    {
                        return  &Null;
                    }
                    return new (allocator) AvgValue(result / count, count);
                }
                case opUserAggr:
                {
                    Value* result = &Null;
                    FuncCallNode* fcall = (FuncCallNode*)expr;
                    if (fcall->fdecl->ctx != NULL) {
                        for (int i = begin; i < end; i++)
                        {
                            Value* v = tuples[i]->get(columnNo);
                            if (!v->isNull()) {
                                if (result->isNull()) {
                                    result = v;
                                } else {
                                    result = (*((Value *(*)(SqlFunctionDeclaration*, Value* , Value*))fcall->fdecl->func))(fcall->fdecl, result, v);
                                }
                            }
                        }
                    } else {
                        for (int i = begin; i < end; i++)
                        {
                            Value* v = tuples[i]->get(columnNo);
                            if (!v->isNull()) {
                                if (result->isNull()) {
                                    result = v;
                                } else {
                                    result = (*((Value *(*)(Value* , Value*))fcall->fdecl->func))(result, v);
                                }
                            }
                        }
                    }
                    return result;
                }
#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
                case opSequenceCount:
                {
                    int64_t result = 0;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull()) {
                            size_t mark = runtime->allocator->mark();
                            result += mco_seq_sql_agg_count((McoGenericSequence*)v)->intValue();
                            runtime->allocator->reset(mark);
                        }
                    }
                    return new (allocator) IntValue(result);
                }
                case opSequenceMinAll:
                {
                    double result = 0;
                    bool isNull = true;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            size_t mark = runtime->allocator->mark();
                            double seq_min = mco_seq_sql_min((McoGenericSequence*)v)->realValue();
                            runtime->allocator->reset(mark);
                            if (isNull || seq_min < result)
                            {
                                result = seq_min;
                                isNull = false;
                            }
                        }
                    }
                    return isNull ? (Value*)&Null : (Value*)new (allocator) RealValue(result);
                }
                case opSequenceMaxAll:
                {
                    double result = 0;
                    bool isNull = true;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            size_t mark = runtime->allocator->mark();
                            double seq_max = mco_seq_sql_max((McoGenericSequence*)v)->realValue();
                            runtime->allocator->reset(mark);
                            if (isNull || seq_max > result)
                            {
                                result = seq_max;
                                isNull = false;
                            }
                        }
                    }
                    return isNull ? (Value*)&Null : (Value*)new (allocator) RealValue(result);
                }
                case opSequenceAvgAll:
                {
                    double result = 0;
                    size_t count = 0;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            size_t mark = runtime->allocator->mark();
                            result += mco_seq_sql_sum((McoGenericSequence*)v)->realValue();
                            mco_seq_sql_reset((McoGenericSequence*)v);
                            count += (size_t)mco_seq_sql_agg_count((McoGenericSequence*)v)->intValue();
                            runtime->allocator->reset(mark);
                        }
                    }
                    return count == 0 ? (Value*)&Null : (Value*)new (allocator) AvgValue(result/count, count);
                }
                case opSequenceSumAll:
                {
                    double result = 0;
                    bool isNull = true;
                    for (int i = begin; i < end; i++)
                    {
                        Value* v = tuples[i]->get(columnNo);
                        if (!v->isNull())
                        {
                            size_t mark = runtime->allocator->mark();
                            result += mco_seq_sql_sum((McoGenericSequence*)v)->realValue();
                            runtime->allocator->reset(mark);
                            isNull = false;
                        }
                    }
                    return isNull ? (Value*)&Null : (Value*)new (allocator) RealValue(result);
                }
               case opSequenceCovAll:
               {                   
                   int size = end - begin;
                   Vector< ScalarArray<double> >* matrix = Vector< ScalarArray<double> >::create(size, allocator);
                   Vector<McoGenericSequence>* seq = Vector<McoGenericSequence>::create(size, allocator);
                   for (int i = 0; i < size; i++) { 
                       seq->items[i] = (McoGenericSequence*)tuples[begin + i]->get(columnNo);
                       matrix->items[i] = new (allocator) ScalarArray<double>(size, allocator);
                       matrix->items[i]->setSize(size);
                   }
                   for (int i = 0; i < size; i++) { 
                       for (int j = 0; j < i; j++) { 
                           mco_seq_sql_reset(seq->items[i]);
                           mco_seq_sql_reset(seq->items[j]);
                           double cov = mco_seq_sql_cov(seq->items[i], seq->items[j])->realValue();
                           matrix->items[i]->at(j) = cov;
                           matrix->items[j]->at(i) = cov;
                       }
                       mco_seq_sql_reset(seq->items[i]);
                       matrix->items[i]->at(i) = mco_seq_sql_var(seq->items[i])->realValue();
                   }
                   return new ArrayStub((Vector<Value>*)matrix, tpArray);
               }
               case opSequenceCorAll:
               {                   
                   int size = end - begin;
                   Vector< ScalarArray<double> >* matrix = Vector< ScalarArray<double> >::create(size, allocator);
                   Vector<McoGenericSequence>* seq = Vector<McoGenericSequence>::create(size, allocator);
                   for (int i = 0; i < size; i++) { 
                       seq->items[i] = (McoGenericSequence*)tuples[begin + i]->get(columnNo);
                       matrix->items[i] = new (allocator) ScalarArray<double>(size, allocator);
                       matrix->items[i]->setSize(size);
                   }
                   for (int i = 0; i < size; i++) { 
                       for (int j = 0; j < i; j++) { 
                           mco_seq_sql_reset(seq->items[i]);
                           mco_seq_sql_reset(seq->items[j]);
                           double cor = mco_seq_sql_corr(seq->items[i], seq->items[j])->realValue();
                           matrix->items[i]->at(j) = cor;
                           matrix->items[j]->at(i) = cor;
                       }
                       matrix->items[i]->at(i) = 1.0;
                   }
                   return new ArrayStub((Vector<Value>*)matrix, tpArray);
               }
 #endif
                #ifndef NO_FLOATING_POINT
                case opRealMinAll:
                case opRealMinDis:
                    {
                        double result = 0;
                        bool isNull = true;
                        for (int i = begin; i < end; i++)
                        {
                            Value* v = tuples[i]->get(columnNo);
                            if (!v->isNull())
                            {
                                if (isNull)
                                {
                                    result = ((RealValue*)v)->val;
                                    isNull = false;
                                }
                                else if (((RealValue*)v)->val < result)
                                {
                                    result = ((RealValue*)v)->val;
                                }
                            }
                        }
                        if (isNull)
                        {
                            return  &Null;
                        }
                        return new (allocator) RealValue(result);
                    }
                case opRealMaxAll:
                case opRealMaxDis:
                    {
                        double result = 0;
                        bool isNull = true;
                        for (int i = begin; i < end; i++)
                        {
                            Value* v = tuples[i]->get(columnNo);
                            if (!v->isNull())
                            {
                                if (isNull)
                                {
                                    result = ((RealValue*)v)->val;
                                    isNull = false;
                                }
                                else if (((RealValue*)v)->val > result)
                                {
                                    result = ((RealValue*)v)->val;
                                }
                            }
                        }
                        if (isNull)
                        {
                            return  &Null;
                        }
                        return new (allocator) RealValue(result);
                    }
                case opRealSumDis:
                    {
                        iqsort(tuples + begin, end - begin, &compareColumn, &columnNo);
                        double prev = 0;
                        double result = 0;
                        bool isNull = true;
                        for (int i = begin; i < end; i++)
                        {
                            Value* v = tuples[i]->get(columnNo);
                            if (!v->isNull())
                            {
                                double val = ((RealValue*)v)->val;
                                isNull = false;
                                if (val != prev)
                                {
                                    prev = val;
                                    result += prev;
                                }
                            }
                        }
                        if (isNull)
                        {
                            return  &Null;
                        }
                        return new (allocator) RealValue(result);
                    }
                case opRealSumAll:
                    {
                        double result = 0;
                        bool isNull = true;
                        for (int i = begin; i < end; i++)
                        {
                            Value* v = tuples[i]->get(columnNo);
                            if (!v->isNull())
                            {
                                isNull = false;
                                result += ((RealValue*)v)->val;
                            }
                        }
                        if (isNull)
                        {
                            return  &Null;
                        }
                        return new (allocator) RealValue(result);
                    }
                case opRealAvgDis:
                    {
                        int count = 0;
                        double result = 0;
                        if (begin != end)
                        {
                            iqsort(tuples + begin, end - begin, &compareColumn, &columnNo);
                            double prev = 0;
                            for (int i = begin; i < end; i++)
                            {
                                Value* v = tuples[i]->get(columnNo);
                                if (!v->isNull())
                                {
                                    double val = ((RealValue*)v)->val;
                                    if (val != prev || count == 0)
                                    {
                                        prev = val;
                                        result += prev;
                                        count += 1;
                                    }
                                }
                            }
                        }
                        if (count == 0)
                        {
                            return  &Null;
                        }
                        return new (allocator) AvgValue(result / count, count);
                    }
                case opAvgMerge:
                {
                    int count = 0;
                    double result = 0;
                    if (begin != end)
                    {
                        iqsort(tuples + begin, end - begin, &compareColumn, &columnNo);
                        for (int i = begin; i < end; i++)
                        {
                            Value* v = tuples[i]->get(columnNo);
                            if (!v->isNull())
                            {
                                AvgValue* avg = (AvgValue*)v;
                                result += avg->val*avg->count;
                                count += avg->count;
                            }
                        }
                    }
                    if (count == 0)
                    {
                        return &Null;
                    }
                    return new (allocator) AvgValue(result / count, count);
                }
                case opRealAvgAll:
                {
                    int count = 0;
                    double result = 0;
                    if (begin != end)
                    {
                        iqsort(tuples + begin, end - begin, &compareColumn, &columnNo);
                        for (int i = begin; i < end; i++)
                        {
                            Value* v = tuples[i]->get(columnNo);
                            if (!v->isNull())
                            {
                                result += ((RealValue*)v)->val;
                                count += 1;
                            }
                        }
                    }
                    if (count == 0)
                    {
                        return &Null;
                    }
                    return new (allocator) AvgValue(result / count, count);
                }
                #endif
            default:
                assert(false);
        }
        return NULL;
    }

    String* IndexNode::dump()
    {
        return String::format("(%s %d)", opMnemonic[tag], loopId);
    }

    Value* IndexNode::evaluate(Runtime* runtime)
    {
        return new (runtime->allocator) IntValue(runtime->indexVar[loopId]);
    }

    int InsertNode::executeStmt(Runtime* runtime)
    {
        Range range[MAX_KEYS];
        bool multiIndex = table->_indices != NULL && table->_indices->next != NULL;
        if (select != NULL)
        {
            bool useHash = false;
            int nColumns = table->columns->length;
            if (replaceIndex && replaceIndex->_nKeys == 1) {
                int nSequences = 0;
                for (int i = 1; i < nColumns; i++) { 
                    if (table->columns->items[i]->_type == tpSequence) { 
                        nSequences += 1;
                    }
                }
                useHash = nSequences + 1 == nColumns;
            }
            DataSource* result = select->executeQuery(runtime);
            Cursor* cursor = result->internalCursor(runtime->trans);
            size_t mark = runtime->allocator->mark();
            int nRecords = 0;

            if (useHash) { 
                DynamicHashTable hash;
                while (cursor->hasNext()) {
                    Record* src = cursor->next();
                    Value* key = src->get(0);
                    Vector<Array>* seq = (Vector<Array>*)hash.get(key);
                    if (seq == NULL) {
                        seq = Vector<Array>::create(nColumns-1, &hash.alloc);
                        for (int i = 1; i < nColumns; i++) { 
                            seq->items[i-1] = createScalarArray(&hash.alloc, table->columns->items[i]->_elemType, table->columns->items[i]->_elemSize, MCO_CFG_INSERT_BUF_SIZE);
                        }
                        hash.put(key, seq);
                    }
                    int used = seq->items[0]->size();
                    if (used == MCO_CFG_INSERT_BUF_SIZE) { 
                        range[0].lowBound = range[0].highBound = key;
                        range[0].isLowInclusive = range[0].isHighInclusive = true;
                        Cursor* selected = replaceIndex->select(runtime->trans, Index::opEQ, 1, range)->records(runtime->trans);
                        bool newRecord = false;
                        Record* dst;
                        ColumnValue* cv = values;
                        if (selected->hasNext()) {
                            dst = selected->next();
                        } else { 
                            dst = table->createRecord(runtime->trans);
                            newRecord = true;
                            cv->field->set(dst, key);
                        }
                        cv = cv->next;
                        nRecords += 1;
                        for (int i = 0; cv != NULL; i++) {
                            cv->field->set(dst, seq->items[i]);
                            cv = cv->next;
                            seq->items[i]->setSize(0);
                        }
                        table->updateRecord(runtime->trans, dst);
                        if (newRecord) {
                            table->checkpointRecord(runtime->trans, dst);
                        }
                        used = 0;
                    } 
                    ColumnValue* cv = values->next;                            
                    for (int i = 0; cv != NULL; i++) {
                        seq->items[i]->setSize(used + 1);
                        seq->items[i]->setAt(used, src->get(i+1));
                        cv = cv->next;
                    }
                    runtime->allocator->reset(mark);
                }
                DynamicHashTable::Iterator it = hash.iterator();
                while (it.hasNext()) { 
                    DynamicHashTable::Entry* entry = it.next();
                    range[0].lowBound = range[0].highBound = entry->key;
                    range[0].isLowInclusive = range[0].isHighInclusive = true;
                    Cursor* selected = replaceIndex->select(runtime->trans, Index::opEQ, 1, range)->records(runtime->trans);
                    bool newRecord = false;
                    Record* dst;
                    ColumnValue* cv = values;
                    if (selected->hasNext()) {
                        dst = selected->next();
                    } else { 
                        dst = table->createRecord(runtime->trans);
                        newRecord = true;
                        cv->field->set(dst, entry->key);
                    }
                    cv = cv->next;
                    nRecords += 1;
                    Vector<Array>* seq = (Vector<Array>*)entry->value;
                    for (int i = 0; cv != NULL; i++) {
                        cv->field->set(dst, seq->items[i]);
                        cv = cv->next;
                        seq->items[i]->setSize(0);
                    }
                    table->updateRecord(runtime->trans, dst);
                    if (newRecord) {
                        table->checkpointRecord(runtime->trans, dst);
                    }
                }                    
            } else { 
                while (cursor->hasNext()) {
                    Record* src = cursor->next();
                    ColumnValue* cv = values;
                    bool newRecord = multiIndex; // if records contains more than one index, then always do checkpoint in case of replacing existed records
                    Record* dst = NULL;
                    int i = 0;
                    if (replaceIndex) {
                        KeyDescriptor* key = replaceIndex->_keys;
                        do {
                            range[i].lowBound = range[i].highBound = src->get(i);
                            range[i].isLowInclusive = range[i].isHighInclusive = true;
                            i += 1;
                            cv = cv->next;
                            key = key->next;
                        } while (key != NULL);

                        Cursor* selected = replaceIndex->select(runtime->trans, Index::opEQ, i, range)->records(runtime->trans);
                        if (selected->hasNext()) {
                            dst = selected->next();
                        } else {
                            cv = values;
                            i = 0;
                        }
                    }
                    if (dst == NULL) {
                        dst = table->createRecord(runtime->trans);
                        newRecord = true;
                    }
                    nRecords += 1;
                    while (cv != NULL) {
                        cv->field->set(dst, src->get(i++));
                        cv = cv->next;
                    }
                    table->updateRecord(runtime->trans, dst);
                    
                    if (newRecord && replaceIndex) {
                        table->checkpointRecord(runtime->trans, dst);
                    }
                    runtime->allocator->reset(mark);
                }
            }
            runtime->allocator->reset(mark);
            cursor->release();
            return nRecords;
        }
        else if (structPtr != NULL)
        {
            Record* rec = table->createRecord(runtime->trans);
            size_t offs = 0;
            for (ColumnValue* cv = values; cv != NULL; cv = cv->next)
            {
                offs = storeField(runtime, rec, cv->field, structPtr, offs);
            }
            table->updateRecord(runtime->trans, rec);
            return 1;
        }
        else
        {
            Record* rec;
            bool newRecord;
            int nRecords;
            ColumnValue* cv = values; 
           if (bulkInsert) { 
                size_t mark = runtime->allocator->mark();
                Vector<Array>* bulk = Vector<Array>::create(nValues);                
                int bulkSize = 0;
                int i;
                for (i = 0; cv != NULL; i++, cv = cv->next) { 
                    bulk->items[i] = (Array*)cv->value->evaluate(runtime);
                    if (i == 0) { 
                        bulkSize = bulk->items[0]->size();
                    } else if (bulkSize != bulk->items[i]->size()) { 
                        MCO_THROW RuntimeError("Mismatched bulk arrays length");                        
                    }
                }
                assert(i == nValues);
                nRecords = bulkSize;
                for (int j = 0; j < bulkSize; j++) {  
                    rec = NULL;
                    newRecord = multiIndex;
                    cv = values;
                    i = 0;
                    if (replaceIndex) {
                        KeyDescriptor* key = replaceIndex->_keys; 
                        do {
                            range[i].lowBound = range[i].highBound = bulk->items[i]->getAt(j);
                            range[i].isLowInclusive = range[i].isHighInclusive = true;
                            i += 1;
                            cv = cv->next;
                            key = key->next;
                        } while (key != NULL);
                        
                        Cursor* selected = replaceIndex->select(runtime->trans, Index::opEQ, i, range)->records(runtime->trans);
                        if (selected->hasNext()) {
                            rec = selected->next();
                        } else {
                            cv = values;
                            i = 0;
                        }
                    }
                    if (rec == NULL) {
                        rec = table->createRecord(runtime->trans);
                        newRecord = true;
                    }
                    setCurrentRecord(runtime, 1, rec);
                    while (cv != NULL) {
                        cv->field->set(rec, bulk->items[i]->getAt(j));
                        cv = cv->next;                   
                        i += 1;
                    }
                    table->updateRecord(runtime->trans, rec);
                    if (newRecord && replaceIndex) {
                        table->checkpointRecord(runtime->trans, rec);
                    }
                }                    
                runtime->allocator->reset(mark);
            } else { 
                rec = NULL;
                newRecord = multiIndex;
                nRecords = 1;
                if (replaceIndex) {
                    KeyDescriptor* key = replaceIndex->_keys;
                    int i = 0;
                    do {
                        range[i].lowBound = range[i].highBound = cv->value->evaluate(runtime);
                        range[i].isLowInclusive = range[i].isHighInclusive = true;
                        i += 1;
                        cv = cv->next;
                        key = key->next;
                    } while (key != NULL);
                    
                    Cursor* selected = replaceIndex->select(runtime->trans, Index::opEQ, i, range)->records(runtime->trans);
                    if (selected->hasNext()) {
                        rec = selected->next();
                    } else {
                        cv = values;
                    }
                }
                if (rec == NULL) {
                    rec = table->createRecord(runtime->trans);
                    newRecord = true;
                }
                setCurrentRecord(runtime, 1, rec);
                while (cv != NULL) {
                    cv->field->set(rec, cv->value->evaluate(runtime));
                    cv = cv->next;
                }
                table->updateRecord(runtime->trans, rec);
                if (newRecord && replaceIndex) {
                    table->checkpointRecord(runtime->trans, rec);
                }
            }
            ExtraValue* ev = moreValues;
            while (ev != NULL) {
                cv = values;
                rec = NULL;
                newRecord = multiIndex;
                if (replaceIndex) {
                    int i = 0;
                    ExtraValue* ev0 = ev;
                    KeyDescriptor* key = replaceIndex->_keys;
                    do {
                        range[i].lowBound = range[i].highBound = ev->value->evaluate(runtime);
                        range[i].isLowInclusive = range[i].isHighInclusive = true;
                        i += 1;
                        ev = ev->next;
                        cv = cv->next;
                        key = key->next;
                    } while (key != NULL);

                    Cursor* selected = replaceIndex->select(runtime->trans, Index::opEQ, i, range)->records(runtime->trans);
                    if (selected->hasNext()) {
                        rec = selected->next();
                    } else {
                        cv = values;
                        ev = ev0;
                    }
                }
                if (rec == NULL) {
                    rec = table->createRecord(runtime->trans);
                    newRecord = true;
                }
                setCurrentRecord(runtime, 1, rec);
                while (cv != NULL) {
                    cv->field->set(rec, ev->value->evaluate(runtime));
                    cv = cv->next;
                    ev = ev->next;
                }
                table->updateRecord(runtime->trans, rec);
                if (newRecord && replaceIndex) {
                    table->checkpointRecord(runtime->trans, rec);
                }
                nRecords += 1;
            }
            return nRecords;
        }
    }
    
    size_t InsertNode::storeField(Runtime* runtime, Record* rec, Field* field, void* src, size_t offs)
    {
        Type fieldType = field->type();
        int fixedSize = 0;
        AbstractAllocator* allocator = runtime->allocator;
        if (fixedSizeStrings && (fieldType == tpString || fieldType == tpUnicode) && (fixedSize = field->fixedSize()) > 0)
        {
            if (fieldType == tpUnicode)
            {
                offs = DOALIGN(offs, sizeof(wchar_t));
            }
        }
        else
        {
            offs = DOALIGN(offs, typeAlignment[fieldType]);
        }
        char* ptr = (char*)src + offs;
        Value* value;
        switch (fieldType)
        {
            case tpInt1:
                value = new (allocator) IntValue(*ptr);
                offs += 1;
                break;
            case tpUInt1:
                value = new (allocator) IntValue(*(unsigned char*)ptr);
                offs += 1;
                break;
            case tpInt2:
                value = new (allocator) IntValue(*(short*)ptr);
                offs += 2;
                break;
            case tpUInt2:
                value = new (allocator) IntValue(*(unsigned short*)ptr);
                offs += 2;
                break;
            case tpInt4:
                value = new (allocator) IntValue(*(int*)ptr);
                offs += 4;
                break;
            case tpUInt4:
                value = new (allocator) IntValue(*(unsigned int*)ptr);
                offs += 4;
                break;
            case tpInt8:
            case tpUInt8:
                value = new (allocator) IntValue(*(int64_t*)ptr);
                offs += 8;
                break;
            #ifndef NO_FLOATING_POINT
            case tpReal4:
                value = new (allocator) RealValue(*(float*)ptr);
                offs += 4;
                break;
            case tpReal8:
                value = new (allocator) RealValue(*(double*)ptr);
                offs += 8;
                break;
            #endif
            case tpDateTime:
                value = new (allocator) DateTime(*(time_t*)ptr);
                offs += sizeof(time_t);
                break;
                #ifdef UNICODE_SUPPORT
                case tpUnicode:
                    if (fixedSize > 0)
                    {
                        value = UnicodeString::create((wchar_t*)ptr, fixedSize);
                        offs += fixedSize * sizeof(wchar_t);
                    }
                    else
                    {
                        value = *(wchar_t**)ptr == NULL ? (Value*) &Null: (Value*)UnicodeString::create(*(wchar_t**)ptr)
                                  ;
                        offs += sizeof(wchar_t*);
                    }
                    break;
                #endif
            case tpString:
                if (fixedSize > 0)
                {
                    value = String::create(ptr, fixedSize);
                    offs += fixedSize;
                }
                else
                {
                    value = *(char**)ptr == NULL ? (Value*) &Null: (Value*)String::create(*(char**)ptr);
                    offs += sizeof(char*);
                }
                break;
            case tpStruct:
                {
                    size_t alignment = field->calculateStructAlignment();
                    offs = DOALIGN(offs, alignment);
                    Iterator < Field > * fi = field->components();
                    while (fi->hasNext())
                    {
                        offs = storeField(runtime, rec, fi->next(), src, offs);
                    }
                    return offs;
                }
            default:
                value = *(Value**)ptr;
                if (value == NULL)
                {
                    value = &Null;
                }
                offs += sizeof(Value*);
                break;
        }
        field->set(rec, value);
        return offs;
    }

    Value* MarkNode::evaluate(Runtime* runtime)
    {
        return new (runtime->allocator) IntValue(runtime->frameMark);
    }

    const int MAX_LITERAL_LENGTH = 256;

    bool LiteralNode::isLiteralNode()
    {
        return true;
    }

    bool LiteralNode::isConstantExpression()
    {
        return true;
    }

    String* LiteralNode::dump()
    {
        char buf[MAX_LITERAL_LENGTH + 1];
        value->toString(buf, MAX_LITERAL_LENGTH);
        buf[MAX_LITERAL_LENGTH] = '\0';
        return String::create(buf);
    }

    bool LiteralNode::equalsNode(ExprNode* node)
    {
        return node->tag == tag && ((LiteralNode*)node)->value->equals(value);
    }

    Value* LiteralNode::evaluate(Runtime* runtime)
    {
        return value;
    }


    bool ParamNode::isLiteralNode()
    {
        return true;
    }

    String* ParamNode::dump()
    {
        char buf[MAX_LITERAL_LENGTH + 1];
        evaluate(NULL)->toString(buf, MAX_LITERAL_LENGTH);
        buf[MAX_LITERAL_LENGTH] = '\0';
        return String::create(buf);
    }

    bool ParamNode::equalsNode(ExprNode* node)
    {
        return node->tag == tag && ((ParamNode*)node)->desc == desc;
    }

    Value* ParamNode::evaluate(Runtime* runtime)
    {
        AbstractAllocator* allocator = runtime->allocator;
        void* ptr = desc->ptr;
        switch (desc->type)
        {
            case tpInt1:
                return new (allocator) IntValue(*(char*)ptr);
            case tpUInt1:
                return new (allocator) IntValue(*(unsigned char*)ptr);
            case tpInt2:
                return new (allocator) IntValue(*(short*)ptr);
            case tpUInt2:
                return new (allocator) IntValue(*(unsigned short*)ptr);
            case tpInt4:
                return new (allocator) IntValue(*(int*)ptr);
            case tpUInt4:
                return new (allocator) IntValue(*(unsigned int*)ptr);
            case tpInt8:
            case tpUInt8:
                return new (allocator) IntValue(*(int64_t*)ptr);
            #ifndef NO_FLOATING_POINT
            case tpReal4:
                return new (allocator) RealValue(*(float*)ptr);
            case tpReal8:
                return new (allocator) RealValue(*(double*)ptr);
            #endif
            case tpDateTime:
                return new (allocator) DateTime(*(time_t*)ptr);
                #ifdef UNICODE_SUPPORT
                case tpUnicode:
                    return desc->lenptr != NULL ? UnicodeString::create((wchar_t*)ptr, * desc->lenptr): UnicodeString
                        ::create((wchar_t*)ptr);
                #endif
            case tpString:
                return desc->lenptr != NULL ? String::create((char*)ptr, * desc->lenptr): String::create((char*)ptr);
            default:
              return (Value*)ptr;
              // MCO_THROW RuntimeError("Invalid parameter type");
        }
    }

    bool IndirectParamNode::isLiteralNode()
    {
        return true;
    }

    String* IndirectParamNode::dump()
    {
        char buf[MAX_LITERAL_LENGTH + 1];
        evaluate(NULL)->toString(buf, MAX_LITERAL_LENGTH);
        buf[MAX_LITERAL_LENGTH] = '\0';
        return String::create(buf);
    }

    bool IndirectParamNode::equalsNode(ExprNode* node)
    {
        return node->tag == tag && ((IndirectParamNode*)node)->ptr == ptr && node->type == type;
    }

    Value* IndirectParamNode::evaluate(Runtime* runtime)
    {
        AbstractAllocator* allocator = runtime->allocator;
        switch (ptrType)
        {
            case tpInt1:
                return new (allocator) IntValue(*(char*)ptr);
            case tpUInt1:
                return new (allocator) IntValue(*(unsigned char*)ptr);
            case tpInt2:
                return new (allocator) IntValue(*(short*)ptr);
            case tpUInt2:
                return new (allocator) IntValue(*(unsigned short*)ptr);
            case tpInt4:
                return new (allocator) IntValue(*(int*)ptr);
            case tpUInt4:
                return new (allocator) IntValue(*(unsigned int*)ptr);
            case tpInt8:
            case tpUInt8:
                return new (allocator) IntValue(*(int64_t*)ptr);
            #ifndef NO_FLOATING_POINT
            case tpReal4:
                return new (allocator) RealValue(*(float*)ptr);
            case tpReal8:
                return new (allocator) RealValue(*(double*)ptr);
            #endif
            case tpDateTime:
                return new (allocator) DateTime(*(time_t*)ptr);
            case tpReference:
                return runtime->db->createReference(*(int64_t*)ptr);
                #ifdef UNICODE_SUPPORT
                case tpUnicode:
                    return UnicodeString::create(*(wchar_t**)ptr);
                #endif
            case tpString:
                return String::create(*(char**)ptr);
            default:
                MCO_THROW RuntimeError("Invalid parameter type");
        }
    }


    LoadNode::LoadNode(FieldDescriptor* field, ExprNode* base): ExprNode(fieldValueType[field->_type], opLoad)
    {
        this->base = base;
        this->field = field;
        assert(base != NULL);
    }

    FieldDescriptor* LoadNode::getField()
    {
        return field;
    }

    bool LoadNode::isSelfLoadNode()
    {
        return base->tag == opTable;
    }

    String* LoadNode::dump()
    {
        return String::format("(%s %s.%s)", opMnemonic[tag], base->dump()->cstr(), field->name()->cstr());
    }

    bool LoadNode::equalsNode(ExprNode* node)
    {
        return node->tag == tag && field == ((LoadNode*)node)->field && ((LoadNode*)node)->base->equalsNode(base);
    }

    int LoadNode::dependsOn()
    {
        return base->dependsOn();
    }

    Value* LoadNode::evaluate(Runtime* runtime)
    {
        Record* rec = (Record*)base->evaluate(runtime);
        return rec->isNull() ? rec : field->get(rec);
    }


    String* QuantorNode::dump()
    {
        return String::format("(%s %s)", opMnemonic[tag], condition->dump()->cstr());
    }

    Value* QuantorNode::evaluate(Runtime* runtime)
    {
        DataSource* set = (DataSource*)((QueryNode*)subquery)->executeQuery(runtime);
        if (condition == NULL)
        {
            MCO_THROW RuntimeError("Quantor is used not in compare operation");
        }
        if (set->nFields() != 1)
        {
            MCO_THROW RuntimeError("Subquery should have only one column");
        }

        Cursor* iterator = set->records();
        switch (tag)
        {
            case opQuantorAll:
                while (iterator->hasNext())
                {
                    elem = iterator->next()->get(0);
                    if (!condition->evaluate(runtime)->isTrue())
                    {
                        iterator->release();
                        return  &BoolValue::False;
                    }
                }
                iterator->release();
                return  &BoolValue::True;
            case opQuantorAny:
                while (iterator->hasNext())
                {
                    elem = iterator->next()->get(0);
                    if (condition->evaluate(runtime)->isTrue())
                    {
                        iterator->release();
                        return  &BoolValue::True;
                    }
                }
                iterator->release();
                return  &BoolValue::False;
            default:
                assert(false);
        }
        iterator->release();
        return NULL;
    }

    Value* SubqueryElemNode::evaluate(Runtime* runtime)
    {
        return quantor->elem;
    }

    void TableNode::cleanup()
    {
        projection = NULL;
        rec = NULL;
    }

    Value* TableNode::evaluate(Runtime* runtime)
    {
        return rec;
    }

    String* TableNode::dump()
    {
        return String::format("(%s %s)", opMnemonic[tag], name->cstr());
    }

    int TableNode::dependsOn()
    {
        return index;
    }

    TableNode::TableNode(TableDescriptor* desc, int idx, TableNode* chain, int join): ExprNode(tpDataSource, opTable)
    {
        name = desc->_name;
        table = desc;
        index = idx;
        next = chain;
        joinType = join;
        projection = NULL;
        joinColumns = NULL;
        joinTable = NULL;
        sequentialSearch = false;
        rec = NULL;
        nSelectedRecords = 0;
        nIndexSearches = 0;
        indexSearchTime = 0;
    }


    String* TripleOpNode::dump()
    {
        if (o3 != NULL)
        {
            return String::format("(%s %s %s)", opMnemonic[tag], o1->dump()->cstr(), o2->dump()->cstr(), o3->dump()
                                  ->cstr());
        }
        else
        {
            return String::format("(%s %s)", opMnemonic[tag], o1->dump()->cstr(), o2->dump()->cstr());
        }
    }

    bool TripleOpNode::isTripleOpNode()
    {
        return true;
    }

    bool TripleOpNode::equalsNode(ExprNode* node)
    {
        return node->tag == tag && o1->equalsNode(((TripleOpNode*)node)->o1) && o2->equalsNode(((TripleOpNode*)node)
                                                  ->o2) && (o3 == ((TripleOpNode*)node)->o3 || (o3 != NULL && o3
                                                  ->equalsNode(((TripleOpNode*)node)->o3)));
    }

    int TripleOpNode::dependsOn()
    {
        int maxDep = max(o1->dependsOn(), o2->dependsOn());
        if (o3 != NULL)
        {
            maxDep = max(o3->dependsOn(), maxDep);
        }
        return maxDep;
    }

    Value* TripleOpNode::evaluate(Runtime* runtime)
    {
        switch (tag)
        {
            case opExactMatchAII:
                {
                    Array* arr = (Array*)o1->evaluate(runtime);
                    uint64_t pattern = o2->evaluate(runtime)->intValue();
                    int count = (int)o3->evaluate(runtime)->intValue();
                    int size = arr->size();
                    if (size >= count)
                    {
                        while (--count >= 0)
                        {
                            if ((uint64_t)arr->getAt(--size)->intValue() != (pattern >> count))
                            {
                                return  &BoolValue::False;
                            }
                        }
                    }
                    return  &BoolValue::True;
                }
            case opExactMatchIII:
                {
                    uint64_t value = o1->evaluate(runtime)->intValue();
                    uint64_t pattern = o2->evaluate(runtime)->intValue();
                    int count = (int)o3->evaluate(runtime)->intValue();
                    return BoolValue::create(value >> count == pattern);
                }
            case opExactMatchSS:
                {
                    String* value = o1->evaluate(runtime)->stringValue();
                    String* pattern = o2->evaluate(runtime)->stringValue();
                    return BoolValue::create(value->startsWith(pattern));
                }

            case opPrefixMatchSS:
                {
                    String* value = o1->evaluate(runtime)->stringValue();
                    String* pattern = o2->evaluate(runtime)->stringValue();
                    return BoolValue::create(pattern->startsWith(value));
                }
            case opPrefixMatchAII:
                {
                    Array* arr = (Array*)o1->evaluate(runtime);
                    uint64_t pattern = o2->evaluate(runtime)->intValue();
                    int count = (int)o3->evaluate(runtime)->intValue();
                    int size = arr->size();
                    if (size <= count)
                    {
                        while (--size >= 0)
                        {
                            if ((uint64_t)arr->getAt(--size)->intValue() != (pattern >> --count))
                            {
                                return  &BoolValue::False;
                            }
                        }
                    }
                    return  &BoolValue::True;
                }

            case opSubStr:
                {
                    Value* str = o1->evaluate(runtime);
                    Value* from = o2->evaluate(runtime);
                    if (str->isNull() || from->isNull())
                    {
                        return  &Null;
                    }
                    int fromIndex = (int)((IntValue*)from)->val;
                    int strLen = ((String*)str)->size();
                    char* s = ((String*)str)->body();
                    if (o3 != NULL)
                    {
                        Value* len = o3->evaluate(runtime);
                        if (len->isNull())
                        {
                            return  &Null;
                        }
                        int substrLen = (int)((IntValue*)len)->val;
                        if (fromIndex <= 0 || fromIndex - 1+substrLen > strLen)
                        {
                            MCO_THROW IndexOutOfBounds(fromIndex, strLen);
                        }
                        return String::create(s + fromIndex - 1, substrLen);
                    }
                    else
                    {
                        if (fromIndex <= 0 || fromIndex - 1 > strLen)
                        {
                            MCO_THROW IndexOutOfBounds(fromIndex, strLen);
                        }
                        return String::create(s + fromIndex - 1, strLen - fromIndex + 1);
                    }
                }
             case opSubStr2:
                {
                    Value* str = o1->evaluate(runtime);
                    Value* from = o2->evaluate(runtime);
                    if (str->isNull() || from->isNull())
                    {
                        return &Null;
                    }
                    int fromIndex = (int)((IntValue*)from)->val;
                    int strLen = ((String*)str)->size();
                    if (fromIndex <= 0 || fromIndex - 1 > strLen)
                    {
                        MCO_THROW IndexOutOfBounds(fromIndex, strLen);
                    }
                    char* s = ((String*)str)->body();
                    if (o3 != NULL)
                    {
                        Value* till = o3->evaluate(runtime);
                        if (!till->isNull())
                        {
                            int tillIndex = (int)((IntValue*)till)->val;
                            if (tillIndex - fromIndex + 1 < 0 || tillIndex > strLen)
                            {
                                MCO_THROW IndexOutOfBounds(tillIndex, strLen);
                            }
                            return String::create(s + fromIndex - 1, tillIndex - fromIndex + 1);
                        }
                    }
                    return String::create(s + fromIndex - 1, strLen - fromIndex + 1);
                }
            case opStrLike:
                {
                    Value* str = o1->evaluate(runtime);
                    Value* pat = o2->evaluate(runtime);
                    if (str->isNull() || pat->isNull())
                    {
                        return  &Null;
                    }
                    return BoolValue::create(matchString((String*)str, (String*)pat, '\\', runtime));
                }
            case opStrLikeEsc:
                {
                    Value* str = o1->evaluate(runtime);
                    Value* pat = o2->evaluate(runtime);
                    Value* esc = o3->evaluate(runtime);
                    if (str->isNull() || pat->isNull() || esc->isNull())
                    {
                        return  &Null;
                    }
                    if (((String*)esc)->size() != 1)
                    {
                        MCO_THROW RuntimeError("Escape string should contain one character");
                    }
                    return BoolValue::create(matchString((String*)str, (String*)pat, ((String*)esc)->body()[0], runtime));
                }
            case opStrILike:
                {
                    Value* str = o1->evaluate(runtime);
                    Value* pat = o2->evaluate(runtime);
                    if (str->isNull() || pat->isNull())
                    {
                        return  &Null;
                    }
                    return BoolValue::create(matchString(((String*)str)->toLowerCase(), ((String*)pat)->toLowerCase(), '\\', runtime));
                }
            case opStrILikeEsc:
                {
                    Value* str = o1->evaluate(runtime);
                    Value* pat = o2->evaluate(runtime);
                    Value* esc = o3->evaluate(runtime);
                    if (str->isNull() || pat->isNull() || esc->isNull())
                    {
                        return  &Null;
                    }
                    if (((String*)esc)->size() != 1)
                    {
                        MCO_THROW RuntimeError("Escape string should contain one character");
                    }
                    return BoolValue::create(matchString(((String*)str)->toLowerCase(), ((String*)pat)->toLowerCase(), ((String*)esc)->body()[0], runtime));
                }
                #ifdef UNICODE_SUPPORT
                case opExactMatchUU:
                    {
                        UnicodeString* value = o1->evaluate(runtime)->unicodeStringValue();
                        UnicodeString* pattern = o2->evaluate(runtime)->unicodeStringValue();
                        return BoolValue::create(value->startsWith(pattern));
                    }
                case opPrefixMatchUU:
                    {
                        UnicodeString* value = o1->evaluate(runtime)->unicodeStringValue();
                        UnicodeString* pattern = o2->evaluate(runtime)->unicodeStringValue();
                        return BoolValue::create(pattern->startsWith(value));
                    }

                case opUnicodeSubStr:
                    {
                        Value* str = o1->evaluate(runtime);
                        Value* from = o2->evaluate(runtime);
                        if (str->isNull() || from->isNull())
                        {
                            return  &Null;
                        }
                        int fromIndex = (int)((IntValue*)from)->val;
                        int strLen = ((UnicodeString*)str)->size();
                        wchar_t* s = ((UnicodeString*)str)->body();
                        if (o3 != NULL)
                        {
                            Value* len = o3->evaluate(runtime);
                            if (len->isNull())
                            {
                                return  &Null;
                            }
                            int substrLen = (int)((IntValue*)len)->val;
                            if (fromIndex < 0 || fromIndex + substrLen > strLen)
                            {
                                MCO_THROW IndexOutOfBounds(fromIndex, strLen);
                            }
                            return UnicodeString::create(s + fromIndex, substrLen);
                        }
                        else
                        {
                            if (fromIndex < 0 || fromIndex > strLen)
                            {
                                MCO_THROW IndexOutOfBounds(fromIndex, strLen);
                            }
                            return UnicodeString::create(s + fromIndex, strLen - fromIndex);
                        }
                    }
                case opUnicodeSubStr2:
                {
                    Value* str = o1->evaluate(runtime);
                    Value* from = o2->evaluate(runtime);
                    if (str->isNull() || from->isNull())
                    {
                        return &Null;
                    }
                    int fromIndex = (int)((IntValue*)from)->val;
                    int strLen = ((UnicodeString*)str)->size();
                    if (fromIndex <= 0 || fromIndex - 1 > strLen)
                    {
                        MCO_THROW IndexOutOfBounds(fromIndex, strLen);
                    }
                    wchar_t* s = ((UnicodeString*)str)->body();
                    if (o3 != NULL)
                    {
                        Value* till = o3->evaluate(runtime);
                        if (!till->isNull())
                        {
                            int tillIndex = (int)((IntValue*)till)->val;
                            if (tillIndex - fromIndex + 1 < 0 || tillIndex > strLen)
                            {
                                MCO_THROW IndexOutOfBounds(tillIndex, strLen);
                            }
                            return UnicodeString::create(s + fromIndex - 1, tillIndex - fromIndex + 1);
                        }
                    }
                    return UnicodeString::create(s + fromIndex - 1, strLen - fromIndex + 1);
                }
                case opUnicodeStrLike:
                    {
                        Value* str = o1->evaluate(runtime);
                        Value* pat = o2->evaluate(runtime);
                        if (str->isNull() || pat->isNull())
                        {
                            return  &Null;
                        }
                        return BoolValue::create(matchString((UnicodeString*)str, (UnicodeString*)pat, '\\'));
                    }
                case opUnicodeStrLikeEsc:
                    {
                        Value* str = o1->evaluate(runtime);
                        Value* pat = o2->evaluate(runtime);
                        Value* esc = o3->evaluate(runtime);
                        if (str->isNull() || pat->isNull() || esc->isNull())
                        {
                            return  &Null;
                        }
                        if (((String*)esc)->size() != 1)
                        {
                            MCO_THROW RuntimeError("Escape string should contain one character");
                        }
                        return BoolValue::create(matchString((UnicodeString*)str, (UnicodeString*)pat, ((String*)esc)
                                                 ->body()[0]));
                    }
                case opUnicodeStrILike:
                    {
                        Value* str = o1->evaluate(runtime);
                        Value* pat = o2->evaluate(runtime);
                        if (str->isNull() || pat->isNull())
                        {
                            return  &Null;
                        }
                        return BoolValue::create(matchString(((UnicodeString*)str)->toLowerCase(), ((UnicodeString*)pat)->toLowerCase(), '\\'));
                    }
                case opUnicodeStrILikeEsc:
                    {
                        Value* str = o1->evaluate(runtime);
                        Value* pat = o2->evaluate(runtime);
                        Value* esc = o3->evaluate(runtime);
                        if (str->isNull() || pat->isNull() || esc->isNull())
                        {
                            return  &Null;
                        }
                        if (((String*)esc)->size() != 1)
                        {
                            MCO_THROW RuntimeError("Escape string should contain one character");
                        }
                        return BoolValue::create(matchString(((UnicodeString*)str)->toLowerCase(), ((UnicodeString*)pat)->toLowerCase(),
                                                             ((String*)esc)->body()[0]));
                    }
                 #endif
            default:
                assert(false);
        }
        return NULL;
    }

    Value* BetweenNode::evaluate(Runtime* runtime)
    {
        Value* v1 = o1->evaluate(runtime);
        Value* v2 = o2->evaluate(runtime);
        Value* v3 = o3->evaluate(runtime);
        if (v1->isNull() || v2->isNull() || v3->isNull())
        {
            return  &Null;
        }
        bool result;
        switch (tag)
        {
            case opBetweenII:
                result = v1->compare(v2) >= 0 && v1->compare(v3) <= 0;
                break;
            case opBetweenIE:
                result = v1->compare(v2) >= 0 && v1->compare(v3) < 0;
                break;
            case opBetweenEI:
                result = v1->compare(v2) > 0 && v1->compare(v3) <= 0;
                break;
            case opBetweenEE:
                result = v1->compare(v2) > 0 && v1->compare(v3) < 0;
                break;
            default:
                result = false;
                assert(false);
        }
        return BoolValue::create(result);
    }

    String* UnaryOpNode::dump()
    {
        return String::format("(%s %s)", opMnemonic[tag], opd->dump()->cstr());
    }

    bool UnaryOpNode::isUnaryOpNode()
    {
        return true;
    }

    bool UnaryOpNode::equalsNode(ExprNode* node)
    {
        return node->tag == tag && opd->equalsNode(((UnaryOpNode*)node)->opd);
    }

    int UnaryOpNode::dependsOn()
    {
        return opd->dependsOn();
    }


    Value* UnaryOpNode::evaluate(Runtime* runtime)
    {
        Value* v = opd->evaluate(runtime);
        AbstractAllocator* allocator = runtime->allocator;
        if (v->isNull())
        {
            if (tag == opIsNull)
            {
                return  &BoolValue::True;
            }
            return  &Null;
        }
        switch (tag)
        {
            case opIsNull:
                return  &BoolValue::False;
            case opIsTrue:
                return BoolValue::create(v ==  &BoolValue::True);
            case opIsFalse:
                return BoolValue::create(v ==  &BoolValue::False);
            case opIntNot:
                return new (allocator) IntValue(~((IntValue*)v)->val);
            case opIntNeg:
                return new (allocator) IntValue( - ((IntValue*)v)->val);
            case opIntAbs:
                return new (allocator) IntValue(((IntValue*)v)->val < 0 ?  - ((IntValue*)v)->val: ((IntValue*)v)->val);
            case opNumericNeg:
                return new (allocator) NumericValue(-((NumericValue*)v)->val, ((NumericValue*)v)->precision);
            case opNumericAbs:
              return new (allocator) NumericValue(((NumericValue*)v)->val < 0 ?  - ((NumericValue*)v)->val : ((NumericValue*)v)->val,
                  ((NumericValue*)v)->precision);
            case opLength:
                return new (allocator) IntValue(((List*)v)->size());
            case opStrToInt:
                {
                    int64_t ival;
                    if (stringToInt64(((String*)v)->cstr(), ival))
                    {
                        return new (allocator) IntValue(ival);
                    }
                    MCO_THROW RuntimeError("Failed to convert string to integer");
                }
            case opBoolToInt:
                return new (allocator) IntValue(v->isTrue() ? 1 : 0);

                #ifndef NO_FLOATING_POINT
                case opRealToInt:
                    return new (allocator) IntValue(v->intValue());
                case opRealNeg:
                    return new (allocator) RealValue( - ((RealValue*)v)->val);
                case opRealAbs:
                    return new (allocator) RealValue(((RealValue*)v)->val < 0 ?  - ((RealValue*)v)->val: ((RealValue*)v)->val);
                case opRealSin:
                    return new (allocator) RealValue(sin(((RealValue*)v)->val));
                case opRealCos:
                    return new (allocator) RealValue(cos(((RealValue*)v)->val));
                case opRealTan:
                    return new (allocator) RealValue(tan(((RealValue*)v)->val));
                case opRealAsin:
                    return new (allocator) RealValue(asin(((RealValue*)v)->val));
                case opRealAcos:
                    return new (allocator) RealValue(acos(((RealValue*)v)->val));
                case opRealAtan:
                    return new (allocator) RealValue(atan(((RealValue*)v)->val));
                case opRealExp:
                    return new (allocator) RealValue(exp(((RealValue*)v)->val));
                case opRealLog:
                    return new (allocator) RealValue(log(((RealValue*)v)->val));
                case opRealSqrt:
                    return new (allocator) RealValue(sqrt(((RealValue*)v)->val));
                case opRealCeil:
                    return new (allocator) RealValue(ceil(((RealValue*)v)->val));
                case opRealFloor:
                    return new (allocator) RealValue(floor(((RealValue*)v)->val));
                case opIntToReal:
                    return new (allocator) RealValue((double)((IntValue*)v)->val);
                case opIntToRef:
                    return runtime->db->createReference(((IntValue*)v)->val);
                case opNumericToReal:
                    return new (allocator) RealValue((double)((NumericValue*)v)->realValue());
                case opNumericToInt:
                    return new (allocator) IntValue(((NumericValue*)v)->intValue());
                case opIntToNumeric:
                  return new (allocator) NumericValue(((IntValue*)v)->val, 0);
                case opStrToNumeric:
                  return new (allocator) NumericValue(((String*)v)->cstr());
                case opRefToInt:
                    return new (allocator) IntValue(v->intValue());
                case opStrToTime:
                    {
                        time_t t;
                        if (stringToTime(((String*)v)->cstr(), t))
                        {
                            return new (allocator) DateTime(t);
                        }
                        else
                        {
                            MCO_THROW RuntimeError("Failed to convert string to timestamp");
                        }
                    }
                case opIntToTime:
                    return new (allocator) DateTime((time_t)((IntValue*)v)->val);
                case opTimeToInt:
   		    return v;
                case opNumericToTime:
                    return new (allocator) DateTime(((NumericValue*)v)->timeValue());
                case opStrToReal:
                    {
                        double dval;
                        int n;
                        if (sscanf(((String*)v)->cstr(), "%lf%n", &dval, &n) == 1 && n == ((String*)v)->size())
                        {
                            return new (allocator) RealValue(dval);
                        }
                        MCO_THROW RuntimeError("Failed to convert string to real");
                    }
                case opBoolToReal:
                    return new (allocator) RealValue(v->isTrue() ? 1 : 0);
                case opAnyToStr:
                    return v->stringValue();
                case opRealToBool:
                    return BoolValue::create(((RealValue*)v)->val != 0);
                #endif
            case opStrUpper:
                return ((String*)v)->toUpperCase();
            case opStrLower:
                return ((String*)v)->toLowerCase();
            case opBoolNot:
                return BoolValue::create(!((BoolValue*)v)->val);
            case opIntToBool:
                return BoolValue::create(((IntValue*)v)->val != 0);
            case opStrToBool:
                return BoolValue::create(((String*)v)->compare("true") == 0);
                #ifdef UNICODE_SUPPORT
                case opUnicodeStrUpper:
                    return ((UnicodeString*)v)->toUpperCase();
                case opUnicodeStrLower:
                    return ((UnicodeString*)v)->toLowerCase();
                case opMbsToWcs:
                    return UnicodeString::create((String*)v);
                case opWcsToMbs:
                    return ((UnicodeString*)v)->stringValue();
                #endif
            case opNullIfZero:
                return (type == tpReal ? v->realValue() == 0.0 : v->intValue() == 0) ? &Null : v;
            default:
                assert(false);
        }
        return NULL;
    }

    Value* SetNotEmptyNode::evaluate(Runtime* runtime)
    {
        size_t mark = runtime->allocator->mark();
        DataSource* source = (DataSource*)((QueryNode*)opd)->executeQuery(runtime);
        bool result;
        if (source->isNumberOfRecordsKnown()) {
            result = source->nRecords(runtime->trans) != 0;
        } else {
            Cursor* cursor = source->records();
            result = cursor->hasNext();
            cursor->release();
        }
        runtime->allocator->reset(mark);
        return BoolValue::create(result);
    }


    int UpdateNode::executeStmt(Runtime* runtime)
    {
        DataSource* result = SelectNode::executeQuery(runtime);
        Cursor* iterator = result->records();
        int nUpdated = 0;
        while (iterator->hasNext())
        {
            Record* rec = iterator->next();
            setCurrentRecord(runtime, 1, rec);
            for (ColumnValue* cv = values; cv != NULL; cv = cv->next)
            {
                cv->field->set(rec, cv->value->evaluate(runtime));
            }
            table->updateRecord(runtime->trans, rec);
            nUpdated += 1;
        }
        iterator->release();
        return nUpdated;
    }

    QueryNode::QueryNode(ExprCode cop, Vector < ColumnNode > * columns): StmtNode(cop)
    {
        resultColumns = columns;
        sortNeeded = false;
        forUpdate = false;
        order = NULL;
        start = limit = NULL;
    }

    bool QueryNode::isReadOnly()
    {
        return !forUpdate;
    }

    bool QueryNode::isQueryNode()
    {
        return tag == opSelect;
    }

    DataSource* QueryNode::subset(DataSource* src, Runtime* runtime)
    {
        if (limit != NULL)
        {
            return new LimitDataSource(runtime, src, start != NULL ? (int)start->evaluate(runtime)->intValue() : 0,
                                       (int)limit->evaluate(runtime)->intValue());
        }
        return src;
    }

    int QueryNode::executeStmt(Runtime* runtime)
    {
        DataSource* result = executeQuery(runtime);
        if (result->isNumberOfRecordsKnown())
        {
            return (int)result->nRecords(runtime->trans);
        }
        return  - 1;
    }

    Value* QueryNode::evaluate(Runtime* runtime)
    {
        DataSource* result = executeQuery(runtime);
        Cursor* iterator = result->internalCursor(runtime->trans);
        if (iterator->hasNext())
        {
            Record* rec = iterator->next();
            if (iterator->hasNext())
            {
                MCO_THROW NotSingleValue("Subquery doesn't return the single value");
            }
            iterator->release();
            return rec->get(0);
        }
        else
        {
            iterator->release();
            return  &Null;
        }
    }

    int QueryNode::compareRecordAllFields(void* p, void* q, void* ptr)
    {
        Record* r1 = (Record*)p;
        Record* r2 = (Record*)q;
        int n = *(int*)ptr;
        for (int i = 0; i < n; i++)
        {
            int diff = r1->get(i)->compare(r2->get(i));
            if (diff != 0)
            {
                return diff;
            }
        }
        return 0;
    }

    int QueryNode::compareRecordOrderByFields(void* p, void* q, void* ctx)
    {
        Record* r1 = (Record*)p;
        Record* r2 = (Record*)q;
        for (OrderNode* order = (OrderNode*)ctx; order != NULL; order = order->next)
        {
            int diff = r1->get(order->columnNo)->compare(r2->get(order->columnNo));
            if (diff != 0)
            {
                return order->kind == ASCENT_ORDER ? diff :  - diff;
            }
        }
        return 0;
    }

    int QueryNode::compareRecordOrderByField(void* p, void* q, void* ctx)
    {
        Record* r1 = (Record*)p;
        Record* r2 = (Record*)q;
        OrderNode* order = (OrderNode*)ctx;
        return r1->get(order->columnNo)->compare(r2->get(order->columnNo));
    }

    bool TableOpNode::isQueryNode()
    {
        return true;
    }

    inline int TableOpNode::skipDuplicates(Record** rp, int i, int len, int* nColumns)
    {
        int j = i;
        while (++i < len && compareRecordAllFields(rp[i], rp[j], nColumns) == 0)
            ;
        return i;
    }

    DataSource* TableOpNode::executeQuery(Runtime* runtime)
    {
        int nResultColumns = resultColumns->length;
        ResultSet* result = NULL;
        DataSource* ds1 = left->executeQuery(runtime);
        DataSource* ds2 = right->executeQuery(runtime);
        switch (tag)
        {
            case opUnionAll:
                if (sortNeeded)
                {
                    Cursor* cursor;
                    result = new TemporaryResultSet(resultColumns, runtime);
                    cursor = ds1->internalCursor(runtime->trans);
                    while (cursor->hasNext())
                    {
                        result->add(cursor->next());
                    }
                    cursor->release();
                    cursor = ds2->internalCursor(runtime->trans);
                    while (cursor->hasNext())
                    {
                        result->add(cursor->next());
                    }
                    cursor->release();
                    result->toArray();
                    iqsort(result->recordArray->items, result->size, &compareRecordAllFields, &nResultColumns);
                    assert(order == NULL);
                    return subset(result, runtime);
                } else {
                    return subset(new MergeDataSource(runtime, ds1, ds2), runtime);
                }
            case opUnion:
                {
                    ResultSet* rs1 = (ResultSet*)ds1;
                    ResultSet* rs2 = (ResultSet*)ds2;
                    Record** rp1 = rs1->recordArray->items;
                    Record** rp2 = rs2->recordArray->items;
                    int len1 = rs1->size;
                    int len2 = rs2->size;
                    result = new TemporaryResultSet(resultColumns, runtime);
                    int i1, i2;
                    for (i1 = 0, i2 = 0; i1 < len1 && i2 < len2;)
                    {
                        int diff = compareRecordAllFields(rp1[i1], rp2[i2], &nResultColumns);
                        if (diff < 0)
                        {
                            result->add(rp1[i1]);
                            i1 = skipDuplicates(rp1, i1, len1, &nResultColumns);
                        }
                        else if (diff > 0)
                        {
                            result->add(rp2[i2]);
                            i2 = skipDuplicates(rp2, i2, len2, &nResultColumns);
                        }
                        else
                        {
                            result->add(rp1[i1]);
                            i1 = skipDuplicates(rp1, i1, len1, &nResultColumns);
                            i2 = skipDuplicates(rp2, i2, len2, &nResultColumns);
                        }
                    }
                    while (i1 < len1)
                    {
                        result->add(rp1[i1]);
                        i1 = skipDuplicates(rp1, i1, len1, &nResultColumns);
                    }
                    while (i2 < len2)
                    {
                        result->add(rp2[i2++]);
                        i2 = skipDuplicates(rp2, i2, len2, &nResultColumns);
                    }
                    break;
                }
            case opIntersect:
                {
                    ResultSet* rs1 = (ResultSet*)ds1;
                    ResultSet* rs2 = (ResultSet*)ds2;
                    Record** rp1 = rs1->recordArray->items;
                    Record** rp2 = rs2->recordArray->items;
                    int len1 = rs1->size;
                    int len2 = rs2->size;
                    int i1, i2, n;
                    for (i1 = 0, i2 = 0, n = 0; i1 < len1 && i2 < len2;)
                    {
                        int diff = compareRecordAllFields(rp1[i1], rp2[i2], &nResultColumns);
                        if (diff < 0)
                        {
                            i1 += 1;
                        }
                        else if (diff > 0)
                        {
                            i2 += 1;
                        }
                        else
                        {
                            rp1[n++] = rp2[i2];
                            i1 = skipDuplicates(rp1, i1, len1, &nResultColumns);
                            i2 = skipDuplicates(rp2, i2, len2, &nResultColumns);
                        }
                    }
                    rs1->size = n;
                    result = rs1;
                    break;
                }
            case opMinus:
                {
                    ResultSet* rs1 = (ResultSet*)ds1;
                    ResultSet* rs2 = (ResultSet*)ds2;
                    Record** rp1 = rs1->recordArray->items;
                    Record** rp2 = rs2->recordArray->items;
                    int len1 = rs1->size;
                    int len2 = rs2->size;
                    int i1, i2, n;
                    for (i1 = 0, i2 = 0, n = 0; i1 < len1 && i2 < len2;)
                    {
                        int diff = compareRecordAllFields(rp1[i1], rp2[i2], &nResultColumns);
                        if (diff < 0)
                        {
                            rp1[n++] = rp1[i1];
                            i1 = skipDuplicates(rp1, i1, len1, &nResultColumns);
                        }
                        else if (diff > 0)
                        {
                            i2 += 1;
                        }
                        else
                        {
                            i1 = skipDuplicates(rp1, i1, len1, &nResultColumns);
                            i2 += 1;
                        }
                    }
                    while (i1 < len1)
                    {
                        rp1[n++] = rp1[i1];
                        i1 = skipDuplicates(rp1, i1, len1, &nResultColumns);
                    }
                    rs1->size = n;
                    result = rs1;
                    break;
                }
            default:
                assert(false);
        }
        result->toArray();
        if (order != NULL)
        {
            iqsort(result->recordArray->items, result->size, &compareRecordOrderByFields, order);
        }
        return subset(result, runtime);
    }

    TableOpNode::TableOpNode(ExprCode cop, Vector < ColumnNode > * columns, QueryNode* left, QueryNode* right):
                             QueryNode(cop, columns)
    {
        this->left = left;
        this->right = right;
    }

    Type FuncCallNode::getElemType()
    {
        return fdecl->elemType;
    }

    Value* FuncCallNode::evaluate(Runtime* runtime)
    {
        Value* result = NULL;
        size_t mark = runtime->allocator->mark();
        runtime->frameMark = mark;
        if (fdecl->args != NULL) { // dynamically defined UDF
            Vector < Value > * params = Vector < Value > ::create(args->length, runtime->allocator);
            for (int i = 0; i < params->length; i++)
            {
                params->items[i] = args->items[i]->evaluate(runtime);
            }
            result = (*((Value *(*)(SqlEngine*,Vector < Value > *))fdecl->func))(runtime->engine, params);
        } else if (fdecl->ctx != NULL) {
            switch (fdecl->nArgs)
            {
              case  - 1:
              {
                  Vector < Value > * params = Vector < Value > ::create(args->length, runtime->allocator);
                  for (int i = 0; i < params->length; i++)
                  {
                      params->items[i] = args->items[i]->evaluate(runtime);
                  }
                  result = (*((Value *(*)(SqlFunctionDeclaration*,Vector < Value > *))fdecl->func))(fdecl, params);
                  break;
              }
              case 0:
                result = (*((Value *(*)(SqlFunctionDeclaration*))fdecl->func))(fdecl);
                break;
              case 1:
                result = (*((Value *(*)(SqlFunctionDeclaration*,Value*))fdecl->func))(fdecl,
                                                                    args->items[0]->evaluate(runtime));
                break;
              case 2:
                result = (*((Value *(*)(SqlFunctionDeclaration*,Value* , Value*))fdecl->func))(fdecl,
                                                                             args->items[0]->evaluate(runtime),
                                                                             args->items[1]->evaluate(runtime));
                break;
              case 3:
                result = (*((Value *(*)(SqlFunctionDeclaration*,Value* , Value* , Value*))fdecl->func))(fdecl,
                                                                                      args->items[0]->evaluate(runtime),
                                                                                      args->items[1]->evaluate(runtime),
                                                                                      args->items[2]->evaluate(runtime));
                break;
              case 4:
                result = (*((Value *(*)(SqlFunctionDeclaration*,Value* , Value* , Value* , Value*))fdecl->func))(fdecl,
                                                                                               args->items[0]->evaluate(runtime),
                                                                                               args->items[1]->evaluate(runtime),
                                                                                               args->items[2]->evaluate(runtime),
                                                                                               args->items[3]->evaluate(runtime));
                break;
              default:
                assert(false);
            }
        } else {
            switch (fdecl->nArgs)
            {
              case  - 1:
              {
                  Vector < Value > * params = Vector < Value > ::create(args->length, runtime->allocator);
                  for (int i = 0; i < params->length; i++)
                  {
                      params->items[i] = args->items[i]->evaluate(runtime);
                  }
                  result = (*((Value *(*)(Vector < Value > *))fdecl->func))(params);
                  break;
              }
              case 0:
                result = (*((Value *(*)())fdecl->func))();
                break;
              case 1:
                result = (*((Value *(*)(Value*))fdecl->func))(args->items[0]->evaluate(runtime));
                break;
              case 2:
                result = (*((Value *(*)(Value* , Value*))fdecl->func))(args->items[0]->evaluate(runtime), args
                                                                       ->items[1]->evaluate(runtime));
                break;
              case 3:
                result = (*((Value *(*)(Value* , Value* , Value*))fdecl->func))(args->items[0]->evaluate(runtime), args
                                                                                ->items[1]->evaluate(runtime), args->items[2]->evaluate(runtime));
                break;
              case 4:
                result = (*((Value *(*)(Value* , Value* , Value* , Value*))fdecl->func))(args->items[0]->evaluate
                                                                                         (runtime), args->items[1]->evaluate(runtime), args->items[2]->evaluate(runtime), args
                                                                                         ->items[3]->evaluate(runtime));
                break;
              default:
                assert(false);
            }
        }
        if (result == NULL)
        {
            result = &Null;
        }
        else
        {
            if (!(result->type() == tpNull || result->type() == type)) {
                MCO_THROW RuntimeError(String::format("Actual return type %s for function %s doesn't match with declaraed return type %s", typeMnemonic[result->type()], fdecl->name, typeMnemonic[fdecl->type])->cstr());
            }
        }
        switch (result->type()) {
          case tpInt:
          {
              int64_t val = ((IntValue*)result)->val;
              runtime->allocator->reset(mark);
              result = new (runtime->allocator) IntValue(val);
              break;
          }
          case tpReal:
          {
              double val = ((RealValue*)result)->val;
              runtime->allocator->reset(mark);
              result = new (runtime->allocator) RealValue(val);
              break;
          }
          case tpNull:
            runtime->allocator->reset(mark);
            result = &Null;
            break;
          default:
            break;
        }
        return result;
    }


    FuncCallNode::FuncCallNode(SqlFunctionDeclaration* fdecl, Vector < ExprNode > * args): ExprNode(fieldValueType[fdecl->type],
                               opFuncCall)
    {
        this->fdecl = fdecl;
        this->args = args;
    }

    int FuncCallNode::dependsOn()
    {
        int dep = 0;
        if (args != NULL) {
            for (int i = args->length; --i >= 0;)
            {
                int argDep = args->items[i]->dependsOn();
                if (argDep > dep)
                {
                    dep = argDep;
                }
            }
        }
        return dep;
    }

}
