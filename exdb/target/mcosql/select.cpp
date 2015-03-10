/*******************************************************************
 *                                                                 *
 *  select.cpp                                                     *
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
 * MODULE:    select.cpp
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#include "nodes.h"
#include "cursor.h"
#include "stub.h"

#ifndef MEASURE_INDEX_SEARCH_TIME
#define MEASURE_INDEX_SEARCH_TIME 0
#endif

namespace McoSql
{

    void SelectNode::cleanup()
    {
        preliminaryResult = NULL;
        preliminaryResultsCalculated = false;
        for (int i = tables->length; --i >= 0;)
        {
            tables->items[i]->cleanup();
        }
        for (SelectNode* inner = inners; inner != NULL; inner = inner->sibling)
        {
            inner->cleanup();
        }
    }

    static IntValue Low(0); // dummy value
    static IntValue High(1); // dummy value
    static IntValue Any( - 1); // dummy value


    IndexDescriptor* SelectNode::locateIndex(ExprNode* expr, IndexOperationKind kind)
    {
        FieldDescriptor* fd = expr->getField();
        if (fd == NULL)
        {
            return NULL;
        }
        SortOrder sort = UNSPECIFIED_ORDER;
        if (order != NULL &&  columns->items[order->columnNo]->field == fd) {
            sort = order->kind;
        }
        IndexDescriptor* index = fd->_table->findIndex(fd, sort, kind);
        if (index == NULL)
        {
            return NULL;
        }
        IndexDescriptor* baseIndex;
        if (expr->isSelfLoadNode()
            || (expr->tag == opLoad
                && ((LoadNode*)expr)->base->tag == opDeref
                && index->table()->isRIDAvailable()
                && (baseIndex = locateIndex(((DerefNode*)((LoadNode*)expr)->base)->base, IDX_EXACT_MATCH)) != NULL))
        {
            return index;
        }
        return NULL;
    }

    enum ExprCost {
        CONSTANT_EVAL_COST = 1000000,
        SEQUENTIAL_SEARCH_COST = 1000,
        EQ_BOOL_COST = 200,
        OPEN_INTERVAL_COST = 100,
        SCAN_SET_COST = 50,
        OR_COST = 10,
        AND_COST = 10,
        IS_NULL_COST = 6,
        CLOSE_INTERVAL_COST = 5,
        PREFIX_MATCH_COST = 4,
        EXACT_MATCH_COST = 3,
        PATTERN_MATCH_COST = 2,
        EQ_COST = 1
    };

    int SelectNode::calculateCost(SqlOptimizerParameters const& params, ExprNode* expr, int tableNo)
    {
        const int MAX_COST = 1000000;
        bool isUnique = false;
        if (tableNo == 0) {
            return MAX_COST;
        }
        if (!canUseIndex(NULL, expr, tableNo, 0, isUnique)) {
            return params.sequentialSearchCost;
        }
        return calculateCost(params, expr, isUnique);
    }

    int SelectNode::calculateCost(SqlOptimizerParameters const& params, ExprNode* expr, bool isUnique)
    {
        switch (expr->tag)
        {
          case opBoolAnd:
            return params.andCost + min(calculateCost(params, ((BinOpNode*)expr)->left, isUnique), calculateCost(params, ((BinOpNode*)expr)->right, isUnique));
          case opBoolOr:
            return params.orCost + calculateCost(params, ((BinOpNode*)expr)->left, isUnique) + calculateCost(params, ((BinOpNode*)expr)->right, isUnique);
          case opIsNull:
            return params.isNullCost;
          case opIsTrue:
          case opIsFalse:
            return params.eqBoolCost + params.notUniqCost;
          case opEq:
          {
              int cost = 0;
              switch (((BinOpNode*)expr)->left->type)
              {
                case tpBool:
                  cost = params.eqBoolCost;
                  break;
                case tpString:
                  cost = params.eqStringCost;
                  break;
                case tpReal4:
                case tpReal8:
                  cost = params.eqRealCost;
                  break;
                default:
                  cost = params.eqCost;
              }
              if (!isUnique) {
                  cost += params.notUniqCost;
              }
              return cost;
          }
          case opScanSet:
          case opScanArray:
          case opBinarySearch:
            return params.scanSetCost;
          case opGt:
          case opGe:
          case opLt:
          case opLe:
            return params.openIntervalCost;
          case opExactMatchIII:
          case opExactMatchAII:
          case opExactMatchSS:
          case opExactMatchUU:
            return params.exactMatchCost;
          case opPrefixMatchAII:
          case opPrefixMatchSS:
          case opPrefixMatchUU:
            return params.prefixMatchCost;
          case opBetweenII:
          case opBetweenEE:
          case opBetweenIE:
          case opBetweenEI:
            return params.closeIntervalCost;
          case opStrLikeEsc:
          case opStrLike:
          case opUnicodeStrLike:
          case opUnicodeStrLikeEsc:
          case opStrILikeEsc:
          case opStrILike:
          case opUnicodeStrILike:
          case opUnicodeStrILikeEsc:
            return params.patternMatchCost;
          default:
            return params.sequentialSearchCost;
        }
    }

    bool SelectNode::canUseIndex(Runtime* runtime, ExprNode* expr, int tableNo, int level, bool& isUnique)
    {
        if (expr->tag == opBoolAnd || expr->tag == opBoolOr)
        {
#if MCO_CFG_SQL_COMBINE_INDEXES_USING_AUTOID
            return canUseIndex(runtime, ((BinOpNode*)expr)->left, tableNo, level + 1, isUnique)
                && canUseIndex(runtime, ((BinOpNode*)expr)->right, tableNo, level + 1, isUnique);
#else // we can always union two data source using filter
            return canUseIndex(runtime, ((BinOpNode*)expr)->left, tableNo, level, isUnique)
                && canUseIndex(runtime, ((BinOpNode*)expr)->right, tableNo, level, isUnique);
#endif
        }
        Range range;
        Index* index = NULL;
        Index::SearchOperation cop = Index::opEQ;

        if (expr->isBinOpNode())
        {
            ExprNode* left = ((BinOpNode*)expr)->left;
            ExprNode* right = ((BinOpNode*)expr)->right;
            int leftDep = left->dependsOn();
            int rightDep = right->dependsOn();
            if (leftDep < rightDep)
            {
                ExprNode* tmp = left;
                left = right;
                right = tmp;
                int dep = leftDep;
                leftDep = rightDep;
                rightDep = dep;
            }
            if (!(leftDep == tableNo && rightDep <= tableNo) || rightDep == tableNo)
            {
                return false;
            }
            IndexOperationKind kind = (expr->tag == opEq || expr->tag == opScanSet || expr->tag == opScanArray)
                ? IDX_EXACT_MATCH : (expr->tag == opInString || expr->tag == opInUnicodeString) ? IDX_SUBSTRING : IDX_RANGE;
            index = locateIndex(left, kind);
            if (index == NULL)
            {
                return false;
            }
            isUnique = index->isUnique();
            switch (expr->tag)
            {
                case opEq:
                case opScanSet:
                case opScanArray:
                case opBinarySearch:
                    range.lowBound = range.highBound = &Any;
                    range.isLowInclusive = range.isHighInclusive = true;
                    break;
                case opGt:
                    range.lowBound = &Any;
                    range.isLowInclusive = range.isHighInclusive = false;
                    cop = Index::opGT;
                    break;
                case opGe:
                    range.lowBound = &Any;
                    range.isLowInclusive = range.isHighInclusive = true;
                    cop = Index::opGE;
                    break;
                case opLt:
                    range.highBound = &Any;
                    range.isLowInclusive = range.isHighInclusive = false;
                    cop = Index::opLT;
                    break;
                case opLe:
                    range.highBound = &Any;
                    range.isLowInclusive = range.isHighInclusive = true;
                    cop = Index::opLE;
                    break;
                case opInString:
                case opInUnicodeString:
                    if (index->isTrigram()) {
                        range.lowBound = &Low;
                        range.highBound = &High;
                        range.isLowInclusive = range.isHighInclusive = true;
                        cop = Index::opContains;
                        break;
                    }
                    // no break
                default:
                    return false;
            }
        }
        else if (expr->isTripleOpNode())
        {
            TripleOpNode* cmp = (TripleOpNode*)expr;

            if (cmp->o1->dependsOn() != tableNo || cmp->o2->dependsOn() >= tableNo || (cmp->o3 != NULL && cmp->o2
                ->dependsOn() >= tableNo))
            {
                return false;
            }
            index = locateIndex(cmp->o1, IDX_SUBSTRING);
            if (index == NULL)
            {
                return false;
            }
            char esc = '\\';
            switch (cmp->tag)
            {
                case opExactMatchIII:
                case opExactMatchAII:
                case opExactMatchSS:
                    cop = Index::opExactMatch;
                    break;
                case opPrefixMatchAII:
                case opPrefixMatchSS:
                    cop = Index::opPrefixMatch;
                    break;
                case opBetweenII:
                    range.lowBound = &Low;
                    range.highBound = &High;
                    range.isLowInclusive = range.isHighInclusive = true;
                    cop = Index::opBetween;
                    break;
                case opBetweenEE:
                    range.lowBound = &Low;
                    range.highBound = &High;
                    range.isLowInclusive = range.isHighInclusive = false;
                    cop = Index::opBetween;
                    break;
                case opBetweenIE:
                    range.lowBound = &Low;
                    range.highBound = &High;
                    range.isLowInclusive = true;
                    range.isHighInclusive = false;
                    cop = Index::opBetween;
                    break;
                case opBetweenEI:
                    range.lowBound = &Low;
                    range.highBound = &High;
                    range.isLowInclusive = false;
                    range.isHighInclusive = true;
                    cop = Index::opBetween;
                    break;
                case opStrLikeEsc:
                case opStrILikeEsc:
                    if (runtime != NULL)
                    {
                        String* s = (String*)cmp->o3->evaluate(runtime);
                        if (s->size() != 1)
                        {
                            MCO_THROW RuntimeError("Escape string in LIKE clause should consists of one character");
                        }
                        esc = s->body()[0];
                    }
                    // no break
                case opStrLike:
                case opStrILike:
                    if (runtime != NULL)
                    {
                        String* pattern = (String*)cmp->o2->evaluate(runtime);
                        int i = findWildcard(pattern, 0, esc);
                        if (i < 0 && !(cmp->tag == opStrILikeEsc || cmp->tag == opStrILike))
                        {
                            range.lowBound = range.highBound = &Any;
                            range.isLowInclusive = range.isHighInclusive = true;
                            if (index->isTrigram()) {
                                cop = Index::opContains;
                            }
                        }
                        else if (i > 0 && !index->isTrigram() && (char)(pattern->body()[i - 1] + 1) > pattern->body()[i - 1])
                        {
                            range.lowBound = &Low;
                            range.highBound = &High;
                            range.isLowInclusive = range.isHighInclusive = true;
                            cop = (cmp->tag == opStrILikeEsc || cmp->tag == opStrILike) ? Index::opILike : Index::opLike;
                        }
                        else
                        {
                            if (cmp->tag == opStrILike) {
                                int pos = 0;
                                if (findLongestSubstring(pattern, pos, esc) >= 3) {
                                    range.lowBound = range.highBound = &Any;
                                    range.isLowInclusive = range.isHighInclusive = true;
                                    cop = Index::opContains;
                                    break;
                                }
                            }
                            return false;
                        }
                    }
                    break;
                    #ifdef UNICODE_SUPPORT
                    case opExactMatchUU:
                        cop = Index::opExactMatch;
                        break;
                    case opPrefixMatchUU:
                        cop = Index::opPrefixMatch;
                        break;
                    case opUnicodeStrLikeEsc:
                    case opUnicodeStrILikeEsc:
                        if (runtime != NULL)
                        {
                            String* s = (String*)cmp->o3->evaluate(runtime);
                            if (s->size() != 1)
                            {
                                MCO_THROW RuntimeError("Escape string in LIKE clause should consists of one character");
                            }
                            esc = s->body()[0];
                        }
                        // no break
                    case opUnicodeStrLike:
                    case opUnicodeStrILike:
                        if (runtime != NULL)
                        {
                            UnicodeString* pattern = (UnicodeString*)cmp->o2->evaluate(runtime);
                            int i = findWildcard(pattern, 0, esc);
                            if (i < 0 && !(cmp->tag == opUnicodeStrILikeEsc || cmp->tag == opUnicodeStrILike))
                            {
                                range.lowBound = range.highBound = &Any;
                                range.isLowInclusive = range.isHighInclusive = true;
                                if (index->isTrigram()) {
                                    cop = Index::opContains;
                                }
                            }
                            else if (i > 0 && !index->isTrigram() && (wchar_t)(pattern->body()[i - 1] + 1) > pattern->body()[i - 1])
                            {
                                range.lowBound = &Low;
                                range.highBound = &High;
                                range.isLowInclusive = range.isHighInclusive = true;
                                cop = (cmp->tag == opUnicodeStrILikeEsc || cmp->tag == opUnicodeStrILike) ? Index::opILike : Index::opLike;
                            }
                            else
                            {
                                if (cmp->tag == opStrILike) {
                                    int pos = 0;
                                    if (findLongestSubstring(pattern, pos, esc) >= 3) {
                                        range.lowBound = range.highBound = &Any;
                                        range.isLowInclusive = range.isHighInclusive = true;
                                        cop = Index::opContains;
                                        break;
                                    }
                                }
                                return false;
                            }
                        }
                        break;
                    #endif
                default:
                    return false;
            }
        }
        else if (expr->isUnaryOpNode())
        {
            switch (expr->tag)
            {
                case opIsNull:
                    range.lowBound = range.highBound = &Null;
                    break;
                case opIsTrue:
                    range.lowBound = range.highBound = &BoolValue::True;
                    break;
                case opIsFalse:
                    range.lowBound = range.highBound = &BoolValue::False;
                    break;
                default:
                    return false;
            }
            index = locateIndex(((UnaryOpNode*)expr)->opd, IDX_EXACT_MATCH);
            if (index == NULL)
            {
                return false;
            }
            range.isLowInclusive = range.isHighInclusive = true;
        }
        else if (expr->tag == opBinarySearch)
        {
            index = locateIndex(((BinarySearchNode*)expr)->selector, IDX_EXACT_MATCH);
            if (index == NULL)
            {
                return false;
            }
            range.isLowInclusive = range.isHighInclusive = true;
            range.lowBound = range.highBound = &Any;
        }
        else
        {
            return false;
        }
        return (level == 0 || index->table()->isRIDAvailable()) && index->isApplicable(cop, 1, &range);
    }

    DataSource* SelectNode::applyIndex(Runtime* runtime, ExprNode* expr, ExprNode* topExpr, int tableNo, ExprNode*
                                       filter, int level)
    {
        bool isUnique;
        if (expr->tag == opBoolAnd)
        {
            if (filter != NULL)
            {
                return applyIndex(runtime, ((BinOpNode*)expr)->left, topExpr, tableNo, topExpr, level);
            }
#if MCO_CFG_SQL_COMBINE_INDEXES_USING_AUTOID
            else if (canUseIndex(runtime, expr, tableNo, 0, isUnique))
            {
                DataSource* left = applyIndex(runtime, ((BinOpNode*)expr)->left, expr, tableNo, NULL, level + 1);
                if (left == NULL)
                {
                    return NULL;
                }
                DataSource* right = applyIndex(runtime, ((BinOpNode*)expr)->right, expr, tableNo, NULL, level + 1);
                return intersectDataSources((ResultSet*)left, (ResultSet*)right);
            }
#endif
            else
            {
                return applyIndex(runtime, ((BinOpNode*)expr)->left, expr, tableNo, ((BinOpNode*)expr)->right, level);
            }
        }
        else if (expr->tag == opBoolOr)
        {
            if (canUseIndex(runtime, expr, tableNo, 0, isUnique))
            {
                ExprNode* rightFilter = new UnaryOpNode(tpBool, opBoolNot, ((BinOpNode*)expr)->left);
                if (filter != NULL) {
                    rightFilter = new LogicalOpNode(opBoolAnd, filter, rightFilter);
                }
                DataSource* left = applyIndex(runtime, ((BinOpNode*)expr)->left, topExpr, tableNo, filter, level + 1);
                DataSource* right = applyIndex(runtime, ((BinOpNode*)expr)->right, topExpr, tableNo, rightFilter, level + 1);
                if (left != NULL && right != NULL) {
                    return new MergeDataSource(runtime, left, right);
                }
            }
            return NULL;
        }
        Range keys[MAX_KEYS];
        bool searchConstant = true;
        return calculateKeyBounds(runtime, expr, topExpr, tableNo, filter, NULL, keys, 0, level, searchConstant);
    }

    inline char const* getIndexKind(Index* index) {
        return index->isSpatial() ? "Spatail"
            : index->isTrigram() ? "Trigram"
            : index->isOrdered() ? "Tree" : "Hash";
    }


    DataSource* SelectNode::calculateKeyBounds(Runtime* runtime, ExprNode* expr, ExprNode* topExpr, int tableNo,
                                               ExprNode* filter, IndexDescriptor* index, Range* ranges, int keyNo, int
                                               level, bool &searchConstant)
    {
        DataSource* set = NULL;
        Vector < Value > * list = NULL;
        Array* array = NULL;
        ExprNode* indexedExpr = NULL;
        Range &range = ranges[keyNo];
        Index::SearchOperation cop = Index::opEQ;

        if (expr->isBinOpNode())
        {
            ExprNode* left = ((BinOpNode*)expr)->left;
            ExprNode* right = ((BinOpNode*)expr)->right;
            int leftDep = left->dependsOn();
            int rightDep = right->dependsOn();
            bool swap = false;
            if (leftDep < rightDep)
            {
                swap = true;
                ExprNode* tmp = left;
                left = right;
                right = tmp;
                int dep = leftDep;
                leftDep = rightDep;
                rightDep = dep;
            }
            assert(leftDep <= tableNo && rightDep <= tableNo);
            if (rightDep == tableNo)
            {
                return NULL;
            }
            else if (rightDep != 0)
            {
                searchConstant = false;
            }
            if (index == NULL)
            {
                IndexOperationKind kind = (expr->tag == opEq || expr->tag == opScanSet || expr->tag == opScanArray)
                    ? IDX_EXACT_MATCH : (expr->tag == opInString || expr->tag == opInUnicodeString) ? IDX_SUBSTRING : IDX_RANGE;
                index = locateIndex(left, kind);
                if (index == NULL)
                {
                    return NULL;
                }
            }
            else
            {
                if (left->isSelfLoadNode())
                {
                    FieldDescriptor* fd = left->getField();
                    if (index->getKey(keyNo) != fd)
                    {
                        return NULL;
                    }
                }
                else
                {
                    return NULL;
                }
            }
            indexedExpr = left;

            switch (expr->tag)
            {
                case opEq:
                    range.lowBound = range.highBound = right->evaluate(runtime);
                    range.isLowInclusive = range.isHighInclusive = true;
                    if (index->isTrigram()) {
                        filter = filter == NULL ? expr : topExpr;
                        cop = Index::opContains;
                    }
                    break;
                case opGt:
                    range.lowBound = right->evaluate(runtime);
                    range.isLowInclusive = range.isHighInclusive = false;
                    cop = Index::opGT;
                    break;
                case opGe:
                    range.lowBound = right->evaluate(runtime);
                    range.isLowInclusive = range.isHighInclusive = true;
                    cop = Index::opGE;
                    break;
                case opLt:
                    range.highBound = right->evaluate(runtime);
                    range.isLowInclusive = range.isHighInclusive = false;
                    cop = Index::opLT;
                    break;
                case opLe:
                    range.highBound = right->evaluate(runtime);
                    range.isLowInclusive = range.isHighInclusive = true;
                    cop = Index::opLE;
                    break;
                case opScanSet:
                    range.lowBound = range.highBound = &Any;
                    set = ((SelectNode*)right)->executeQuery(runtime);
                    range.isLowInclusive = range.isHighInclusive = true;
                    break;
                case opScanArray:
                    range.lowBound = range.highBound = &Any;
                    array = (Array*)right->evaluate(runtime);
                    range.isLowInclusive = range.isHighInclusive = true;
                    break;
                case opContainsAll:
                    if (swap) {
                        return NULL;
                    }
                    range.lowBound = range.highBound = right->evaluate(runtime);
                    cop = Index::opContainsAll;
                    break;
                case opContainsAny:
                    if (swap) {
                        return NULL;
                    }
                    range.lowBound = range.highBound = right->evaluate(runtime);
                    cop = Index::opContainsAny;
                    break;
                case opContains:
                    range.lowBound = range.highBound = right->evaluate(runtime);
                    cop = swap ? Index::opBelongs : Index::opContains;
                    break;
                case opOverlaps:
                    range.lowBound = range.highBound = right->evaluate(runtime);
                    cop = Index::opOverlaps;
                    break;
                case opNear:
                    range.lowBound = range.highBound = right->evaluate(runtime);
                    cop = Index::opNear;
                    break;
                case opInString:
                case opInUnicodeString:
                    if (index->isTrigram()) {
                        range.isLowInclusive = range.isHighInclusive = true;
                        range.lowBound = range.highBound = right->evaluate(runtime);
                        if (((List*)range.lowBound)->size() >= 3) {
                            cop = Index::opContains;
                            break;
                        }
                    }
                    // no break
                default:
                    return NULL;
            }
            if (swap)
            {
                Value* temp = range.highBound;
                range.highBound = range.lowBound;
                range.lowBound = temp;
            }
        }
        else if (expr->isTripleOpNode())
        {
            TripleOpNode* cmp = (TripleOpNode*)expr;
            int dep2, dep3 = 0;
            if (cmp->o1->dependsOn() != tableNo || (dep2 = cmp->o2->dependsOn()) >= tableNo || (cmp->o3 != NULL &&
                (dep3 = cmp->o3->dependsOn()) >= tableNo))
            {
                return NULL;
            }

            if (index == NULL)
            {
                index = locateIndex(cmp->o1, IDX_SUBSTRING);
                if (index == NULL)
                {
                    return NULL;
                }
            }
            else
            {
                if (cmp->o1->isSelfLoadNode())
                {
                    FieldDescriptor* fd = cmp->o1->getField();
                    if (index->getKey(keyNo) != fd)
                    {
                        return NULL;
                    }
                }
                else
                {
                    return NULL;
                }
            }
            indexedExpr = cmp->o1;

            if (dep2 + dep3 != 0)
            {
                searchConstant = false;
            }
            char esc = '\\';
            switch (cmp->tag)
            {
                case opExactMatchIII:
                case opExactMatchAII:
                    range.lowBound = cmp->o2->evaluate(runtime);
                    range.highBound = cmp->o3->evaluate(runtime);
                    range.isLowInclusive = range.isHighInclusive = true;
                    cop = Index::opExactMatch;
                    break;
                case opExactMatchSS:
                    range.lowBound = cmp->o2->evaluate(runtime);
                    range.isLowInclusive = true;
                    cop = Index::opExactMatch;
                    break;
                case opPrefixMatchAII:
                    range.lowBound = cmp->o2->evaluate(runtime);
                    range.highBound = cmp->o3->evaluate(runtime);
                    range.isLowInclusive = range.isHighInclusive = true;
                    cop = Index::opPrefixMatch;
                    break;
                case opPrefixMatchSS:
                    range.lowBound = cmp->o2->evaluate(runtime);
                    range.isLowInclusive = true;
                    cop = Index::opPrefixMatch;
                    break;
                case opBetweenII:
                    range.lowBound = cmp->o2->evaluate(runtime);
                    range.highBound = cmp->o3->evaluate(runtime);
                    range.isLowInclusive = range.isHighInclusive = true;
                    cop = Index::opBetween;
                    break;
                case opBetweenEE:
                    range.lowBound = cmp->o2->evaluate(runtime);
                    range.highBound = cmp->o3->evaluate(runtime);
                    range.isLowInclusive = range.isHighInclusive = false;
                    cop = Index::opBetween;
                    break;
                case opBetweenIE:
                    range.lowBound = cmp->o2->evaluate(runtime);
                    range.highBound = cmp->o3->evaluate(runtime);
                    range.isLowInclusive = true;
                    range.isHighInclusive = false;
                    cop = Index::opBetween;
                    break;
                case opBetweenEI:
                    range.lowBound = cmp->o2->evaluate(runtime);
                    range.highBound = cmp->o3->evaluate(runtime);
                    range.isLowInclusive = false;
                    range.isHighInclusive = true;
                    cop = Index::opBetween;
                    break;
                case opStrLikeEsc:
                case opStrILikeEsc:
                    {
                        String* s = (String*)cmp->o3->evaluate(runtime);
                        if (s->size() != 1)
                        {
                            MCO_THROW RuntimeError("Escape string in LIKE clause should consists of one character");
                        }
                        esc = s->body()[0];
                        // no break
                    }
                case opStrLike:
                case opStrILike:
                    {
                        String* pattern = (String*)cmp->o2->evaluate(runtime);
                        int i = findWildcard(pattern, 0, esc);
                        if (i < 0 && !(cmp->tag == opStrILikeEsc || cmp->tag == opStrILike))
                        {
                            range.isLowInclusive = range.isHighInclusive = true;
                            range.lowBound = range.highBound = pattern;
                            if (index->isTrigram()) {
                                cop = Index::opContains;
                                filter = filter == NULL ? expr : topExpr;
                            }
                        }
                        else if (i > 0 && !index->isTrigram() && (char)(pattern->body()[i - 1] + 1) > pattern->body()[i - 1])
                        {
                            range.lowBound = String::create(pattern->body(), i);
                            String* h = String::create(pattern->body(), i);
                            range.highBound = h;
                            range.isLowInclusive = true;
                            range.isHighInclusive = false;
                            h->body()[i - 1] += 1;
                            filter = filter == NULL ? expr : topExpr;
                            cop = (cmp->tag == opStrILikeEsc || cmp->tag == opStrILike) ? Index::opILike : Index::opLike;
                        }
                        else
                        {
                            int pos = 0;
                            int len = findLongestSubstring(pattern, pos, esc);
                            if (len >= 3) {
                                range.lowBound = range.highBound = String::create(pattern->body() + pos, len);
				range.isLowInclusive = range.isHighInclusive = true;
                                filter = filter == NULL ? expr : topExpr;
                                cop = Index::opContains;
                            } else {
                                return NULL;
                            }
                        }
                        break;
                    }
                    #ifdef UNICODE_SUPPORT
                    case opExactMatchUU:
                        range.lowBound = cmp->o2->evaluate(runtime);
                        range.isLowInclusive = true;
                        cop = Index::opExactMatch;
                        break;
                    case opPrefixMatchUU:
                        range.lowBound = cmp->o2->evaluate(runtime);
                        range.isLowInclusive = true;
                        cop = Index::opPrefixMatch;
                        break;
                    case opUnicodeStrLikeEsc:
                    case opUnicodeStrILikeEsc:
                        {
                            String* s = (String*)cmp->o3->evaluate(runtime);
                            if (s->size() != 1)
                            {
                                MCO_THROW RuntimeError("Escape string in LIKE clause should consists of one character");
                            }
                            esc = s->body()[0];
                            // no break
                        }
                    case opUnicodeStrLike:
                    case opUnicodeStrILike:
                        {
                            UnicodeString* pattern = (UnicodeString*)cmp->o2->evaluate(runtime);
                            int i = findWildcard(pattern, 0, esc);
                            if (i < 0 && !(cmp->tag == opUnicodeStrILikeEsc || cmp->tag == opUnicodeStrILike))
                            {
                                range.isLowInclusive = range.isHighInclusive = true;
                                range.lowBound = range.highBound = pattern;
                                if (index->isTrigram()) {
                                    cop = Index::opContains;
                                    filter = filter == NULL ? expr : topExpr;
                                }
                            }
                            else if (i > 0 && !index->isTrigram() && (wchar_t)(pattern->body()[i - 1] + 1) > pattern->body()[i - 1])
                            {
                                range.lowBound = UnicodeString::create(pattern->body(), i);
                                UnicodeString* h = UnicodeString::create(pattern->body(), i);
                                range.highBound = h;
                                range.isLowInclusive = true;
                                range.isHighInclusive = false;
                                h->body()[i - 1] += 1;
                                filter = filter == NULL ? expr : topExpr;
                                cop = (cmp->tag == opUnicodeStrILikeEsc || cmp->tag == opUnicodeStrILike) ? Index::opILike : Index::opLike;
                            }
                            else
                            {
                                int pos = 0;
                                int len = findLongestSubstring(pattern, pos, esc);
                                if (len >= 3) {
                                    range.lowBound = range.highBound = UnicodeString::create(pattern->body() + pos, len);
                                    range.isLowInclusive = range.isHighInclusive = true;
                                    filter = filter == NULL ? expr : topExpr;
                                    cop = Index::opContains;
                                } else {
                                    return NULL;
                                }
                                break;
                            }
                        }
                    #endif
                default:
                    return NULL;
            }
        }
        else if (expr->isUnaryOpNode())
        {
            switch (expr->tag)
            {
                case opIsNull:
                    if (expr->type == tpReference) {
                        range.lowBound = range.highBound = &Null;
                    } else {
                        return NULL;
                    }
                    break;
                case opIsTrue:
                    range.lowBound = range.highBound = &BoolValue::True;
                    break;
                case opIsFalse:
                    range.lowBound = range.highBound = &BoolValue::False;
                    break;
                default:
                    return NULL;
            }
            indexedExpr = ((UnaryOpNode*)expr)->opd;
            if (index == NULL) {
                index = locateIndex(indexedExpr, IDX_EXACT_MATCH);
                if (index == NULL)
                {
                    return NULL;
                }
            }
            else
            {
                if (indexedExpr->isSelfLoadNode())
                {
                    FieldDescriptor* fd = indexedExpr->getField();
                    if (index->getKey(keyNo) != fd)
                    {
                        return NULL;
                    }
                }
                else
                {
                    return NULL;
                }
            }
            range.isLowInclusive = range.isHighInclusive = true;
        }
        else if (expr->tag == opBinarySearch)
        {
            BinarySearchNode* bin = (BinarySearchNode*)expr;
            indexedExpr = bin->selector;
            if (index == NULL)  {
                index = locateIndex(indexedExpr, IDX_EXACT_MATCH);
                if (index == NULL)
                {
                    return NULL;
                }
            } else {
                if (indexedExpr->isSelfLoadNode())
                {
                    FieldDescriptor* fd = indexedExpr->getField();
                    if (index->getKey(keyNo) != fd)
                    {
                        return NULL;
                    }
                }
                else
                {
                    return NULL;
                }
            }
            list = bin->list;
            range.isLowInclusive = range.isHighInclusive = true;
            range.lowBound = range.highBound = &Any;
        }
        else
        {
            return NULL;
        }
        keyNo += 1;
        if (indexedExpr->isSelfLoadNode() && index->getKey(keyNo) != NULL && filter != NULL && set == NULL && list == NULL && array == NULL)
        {
            assert(keyNo < MAX_KEYS);
            DataSource* result;
            if (filter->tag == opBoolAnd)
            {
                result = calculateKeyBounds(runtime, ((BinOpNode*)filter)->left, topExpr, tableNo,
                                            ((BinOpNode*)filter)->right, index, ranges, keyNo, level, searchConstant);
            }
            else
            {
                result = calculateKeyBounds(runtime, filter, topExpr, tableNo, NULL, index, ranges, keyNo, level, searchConstant);
            }
            if (result != NULL)
            {
                return result;
            }
        }
        if (!index->isApplicable(cop, keyNo, ranges))
        {
            return NULL;
        }
        searchTableNo = tableNo;
        if (runtime->traceEnabled && traceTableNo < tableNo)
        {
            printf("%s index search through table '%s' < ", getIndexKind(index), tables->items[tableNo - 1]->name->cstr());
            for (int i = 0; i < keyNo; i++)
            {
                FieldDescriptor* fd = index->getKey(i);
                printf("%s ", fd->name()->cstr());
            }
            printf(">\n\texpression: %s\n", expr->dump()->cstr());
            if (filter != NULL)
            {
                printf("\tfilter: %s\n", filter->dump()->cstr());
            }
            if (level == 0)
            {
                traceTableNo = tableNo;
            }
        }
        ExprNode* selfFilter = filter;
        if (!indexedExpr->isSelfLoadNode())
        {
            filter = NULL;
        }
        DataSource* result;
        if (set != NULL)
        {
            assert(set->nFields() == 1);
            if (level == 0) {
                result = new IndexMergeDataSource(this, index, tableNo, keyNo, ranges, set, filter, runtime);
            } else {
                Cursor* iterator = set->internalCursor(runtime->trans);
                TableResultSet* rs = new TableResultSet((TableDescriptor*)index->table(), runtime);
                result = rs;
                while (iterator->hasNext())
                {
                    range.lowBound = range.highBound = iterator->next()->get(0);
                    Cursor* selected = index->select(runtime->trans, cop, keyNo, ranges)->records(runtime->trans);
                    if (filter != NULL)
                    {
                        size_t mark = runtime->allocator->mark();
                        while (selected->hasNext())
                        {
                            Record* rec = selected->next();
                            setCurrentRecord(runtime, tableNo, rec);
                            if (filter->evaluate(runtime)->isTrue())
                            {
                                if (rs->add(rec)) {
                                    mark = runtime->allocator->mark();
                                    continue;
                                }
                            }
                            runtime->allocator->reset(mark);
                        }
                    }
                    else
                    {
                        size_t mark = runtime->allocator->mark();
                        while (selected->hasNext())
                        {
                            if (!rs->add(selected->next())) {
                                runtime->allocator->reset(mark);
                            } else {
                                mark = runtime->allocator->mark();
                            }
                        }
                    }
                    selected->release();
                }
                iterator->release();
            }
        }
        else if (list != NULL)
        {
            if (level == 0) {
                result = new IndexMergeDataSource(this, index, tableNo, keyNo, ranges, new ArrayStub(list), filter, runtime);
            } else {
                TableResultSet* rs = new TableResultSet((TableDescriptor*)index->table(), runtime);
                result = rs;
                Value* prev = &Null;
                for (int i = 0, n = list->length; i < n; i++)
                {
                    Value* v = list->items[i];
                    if (!v->equals(prev))
                    {
                        range.lowBound = range.highBound = v;
                        Cursor* selected = index->select(runtime->trans, cop, keyNo, ranges)->records(runtime->trans);
                        if (filter != NULL)
                        {
                            size_t mark = runtime->allocator->mark();
                            while (selected->hasNext())
                            {
                                Record* rec = selected->next();
                                setCurrentRecord(runtime, tableNo, rec);
                                if (filter->evaluate(runtime)->isTrue())
                                {
                                    if (rs->add(rec)) {
                                        mark = runtime->allocator->mark();
                                        continue;
                                    }
                                }
                                runtime->allocator->reset(mark);
                            }
                        }
                        else
                        {
                            size_t mark = runtime->allocator->mark();
                            while (selected->hasNext())
                            {
                                if (!rs->add(selected->next()))  {
                                    runtime->allocator->reset(mark);
                                } else {
                                    mark = runtime->allocator->mark();
                                }
                            }
                        }
                        selected->release();
                        prev = v;
                    }
                }
            }
        }
        else if (array != NULL)
        {
            if (level == 0) {
                result = new IndexMergeDataSource(this, index, tableNo, keyNo, ranges, array, filter, runtime);
            } else {
                TableResultSet* rs = new TableResultSet((TableDescriptor*)index->table(), runtime);
                result = rs;
                Value* v;
                while ((v = array->next()) != NULL) {
                    range.lowBound = range.highBound = v;
                    Cursor* selected = index->select(runtime->trans, cop, keyNo, ranges)->records(runtime->trans);
                    if (filter != NULL)
                    {
                        size_t mark = runtime->allocator->mark();
                        while (selected->hasNext())
                        {
                            Record* rec = selected->next();
                            setCurrentRecord(runtime, tableNo, rec);
                            if (filter->evaluate(runtime)->isTrue())
                            {
                                if (rs->add(rec)) {
                                    mark = runtime->allocator->mark();
                                    continue;
                                }
                            }
                            runtime->allocator->reset(mark);
                        }
                    }
                    else
                    {
                        size_t mark = runtime->allocator->mark();
                        while (selected->hasNext())
                        {
                            if (!rs->add(selected->next()))  {
                                runtime->allocator->reset(mark);
                            } else {
                                mark = runtime->allocator->mark();
                            }
                        }
                    }
                    selected->release();
                }
            }
        }
        else
        {
            if (level == 0)
            {
                result = new FilterDataSource(this, index->select(runtime->trans, cop, keyNo, ranges), index->_table,
                                              filter, runtime, tableNo);
            }
            else
            {
                Cursor* selected = index->select(runtime->trans, cop, keyNo, ranges)->records(runtime->trans);
                ResultSet* rs = new TableResultSet((TableDescriptor*)index->table(), runtime);
                if (filter != NULL)
                {
                    size_t mark = runtime->allocator->mark();
                    while (selected->hasNext())
                    {
                        Record* rec = selected->next();
                        setCurrentRecord(runtime, tableNo, rec);
                        if (filter->evaluate(runtime)->isTrue())
                        {
                            if (rs->add(rec)) {
                                mark = runtime->allocator->mark();
                                continue;
                            }
                        }
                        runtime->allocator->reset(mark);
                    }
                }
                else
                {
                    size_t mark = runtime->allocator->mark();
                    while (selected->hasNext())
                    {
                        if (!rs->add(selected->next())) {
                            runtime->allocator->reset(mark);
                        } else {
                            mark = runtime->allocator->mark();
                        }
                    }
                }
                result = rs;
                selected->release();
            }
        }
        if (indexedExpr->isSelfLoadNode())
        {
            if (tableNo == 1 && keyNo == 1)
            {
                usedIndex = index;
            }
            if (searchConstant && filter == NULL && level == 0)
            {
                tables->items[tableNo - 1]->projection = result;
            }
        }
        else
        {
            do
            {
                indexedExpr = ((DerefNode*)((LoadNode*)indexedExpr)->base)->base;
                Table* table = index->table();
                assert(table->isRIDAvailable());
                index = locateIndex(indexedExpr, IDX_EXACT_MATCH);
                assert(index != NULL);
                ResultSet* rs = new TableResultSet((TableDescriptor*)index->table(), runtime);
                Cursor* cursor = result->internalCursor(runtime->trans);
                while (cursor->hasNext())
                {
                    Record* rec = cursor->next();
                    Reference* ref = table->getRID(rec);
                    range.lowBound = range.highBound = ref;
                    range.isLowInclusive = range.isHighInclusive = true;
                    Cursor* selected = index->select(runtime->trans, Index::opEQ, 1, ranges)->records(runtime->trans);
                    if (selfFilter == NULL || !indexedExpr->isSelfLoadNode())
                    {
                        size_t mark = runtime->allocator->mark();
                        while (selected->hasNext())
                        {
                            if (!rs->add(selected->next())) {
                                runtime->allocator->reset(mark);
                            } else {
                                mark = runtime->allocator->mark();
                            }
                        }
                    }
                    else
                    {
                        size_t mark = runtime->allocator->mark();
                        while (selected->hasNext())
                        {
                            rec = selected->next();
                            setCurrentRecord(runtime, tableNo, rec);
                            if (selfFilter->evaluate(runtime)->isTrue())
                            {
                                if (rs->add(rec)) {
                                    mark = runtime->allocator->mark();
                                    continue;
                                }
                            }
                            runtime->allocator->reset(mark);
                        }
                    }
                    selected->release();
                }
                cursor->release();
                result = rs;
            }
            while (!indexedExpr->isSelfLoadNode())
                ;
        }
        return result;
    }


    int SelectNode::compareRid(void* p, void* q, void* ctx)
    {
        return ((DataSource*)ctx)->compareRID((Record*)p, (Record*)q);
    }

    DataSource* SelectNode::intersectDataSources(ResultSet* left, ResultSet* right)
    {
        left->toArray();
        right->toArray();
        Record** rp1 = left->recordArray->items;
        Record** rp2 = right->recordArray->items;
        int len1 = left->size;
        int len2 = right->size;
        iqsort(rp1, len1, &compareRid, left);
        iqsort(rp2, len2, &compareRid, right);

        int n = 0;
        for (int i1 = 0, i2 = 0; i1 < len1 && i2 < len2;)
        {
            int diff = left->compareRID(rp1[i1], rp2[i2]);
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
                rp1[n] = rp2[i2];
                n += 1;
                i1 += 1;
                i2 += 1;
            }
        }
        left->size = n;
        return left;
    }

    DataSource* SelectNode::unionDataSources(ResultSet* left, ResultSet* right)
    {
        left->toArray();
        right->toArray();
        Record** rp1 = left->recordArray->items;
        Record** rp2 = right->recordArray->items;
        int len1 = left->size;
        int len2 = right->size;
        iqsort(rp1, len1, &compareRid, left);
        iqsort(rp2, len2, &compareRid, right);
        ResultSet* result = left->clone();
        int i1, i2;
        for (i1 = 0, i2 = 0; i1 < len1 && i2 < len2;)
        {
            int diff = left->compareRID(rp1[i1], rp2[i2]);
            if (diff < 0)
            {
                result->add(rp1[i1++]);
            }
            else if (diff > 0)
            {
                result->add(rp2[i2++]);
            }
            else
            {
                result->add(rp1[i1]);
                i1 += 1;
                i2 += 1;
            }
        }
        while (i1 < len1)
        {
            result->add(rp1[i1++]);
        }
        while (i2 < len2)
        {
            result->add(rp2[i2++]);
        }
        return result;
    }

    inline bool SelectNode::getJoinColumn(TableDescriptor* table, int tableNo, ExprNode* condition, Value* &value, int& column, Runtime* runtime)
    {
        if (!table->impl->isResultSet()) {
            return false;
        }
        OrderNode* orderBy = ((ResultSet*)table->impl)->orderBy;
        if (orderBy == NULL || orderBy->next != NULL) {
            return false;
        }
        if (condition->tag != opEq) {
            return false;
        }
        ExprNode* left = ((BinOpNode*)condition)->left;
        ExprNode* right = ((BinOpNode*)condition)->right;
        int leftDep = left->dependsOn();
        int rightDep = right->dependsOn();
        if (leftDep < rightDep)
        {
            ExprNode* tmp = left;
            left = right;
            right = tmp;
            int dep = leftDep;
            leftDep = rightDep;
            rightDep = dep;
        }
        assert(leftDep <= tableNo && rightDep <= tableNo);
        if (leftDep != tableNo || rightDep == tableNo)
        {
            return false;
        }
        if (!left->isSelfLoadNode()) {
            return false;
        }
        FieldDescriptor* fd = left->getField();
        if (table->columns->items[orderBy->columnNo] != fd) {
            return false;
        }
        column = orderBy->columnNo;
        value = right->evaluate(runtime);
        return true;
    }

    bool SelectNode::joinTables(int tableNo, ExprNode* condition, Runtime* runtime)
    {
        bool moreResults = false;
        AbstractAllocator* allocator = runtime->allocator;
        if (tableNo == tables->length)
        {
            size_t mark = allocator->mark();
            if (condition == NULL || condition->evaluate(runtime)->isTrue())
            {
                if (selectType == SELECT_COUNT)
                {
                    allocator->reset(mark);
                    runtime->result->size += 1;
                }
                else
                {
                    Tuple* tuple = Tuple::create(columns->length, allocator);
                    for (int i = columns->length; --i >= 0;)
                    {
                        tuple->values[i] = columns->items[i]->evaluate(runtime);
                    }
                    if (runtime->result->add(tuple)) {
                        moreResults = true;
                    } else {
                        allocator->reset(mark);
                    }
                }
            }
            return moreResults;
        }

        TableNode* tnode = tables->items[tableNo];
        TableDescriptor* table = tnode->table;
        tableNo += 1;
        ExprNode* filter = NULL;
        BinOpNode* next = NULL;
        BinOpNode* prev = NULL;

        if (condition != NULL)
        {
            if (condition->tag == opBoolAnd)
            {
                next = (BinOpNode*)condition;
                BinOpNode* pprev = NULL; // previous previuos
                while (next->left->dependsOn() <= tableNo)
                {
                    if (next->right->tag == opBoolAnd)
                    {
                        pprev = prev;
                        prev = next;
                        next = (BinOpNode*)next->right;
                    }
                    else
                    {
                        if (next->right->dependsOn() <= tableNo)
                        {
                            filter = condition;
                            condition = NULL;
                        }
                        else
                        {
                            if (prev != NULL)
                            {
                                prev->right = next->left;
                                filter = condition;
                            }
                            else
                            {
                                filter = next->left;
                            }
                            condition = next->right;
                        }
                        break;
                    }
                }
                if (filter == NULL && prev != NULL)
                {
                    if (pprev != NULL)
                    {
                        pprev->right = prev->left;
                        next = prev;
                        prev = pprev;
                        filter = condition;
                        condition = next->right;
                    }
                    else
                    {
                        filter = prev->left;
                        condition = next;
                        prev = NULL;
                    }
                }
            }
            else if (condition->dependsOn() <= tableNo)
            {
                filter = condition;
                condition = NULL;
            }
            if (filter != NULL && !tnode->sequentialSearch)
            {
                size_t mark1 = allocator->mark();
                DataSource* selection;
#if MEASURE_INDEX_SEARCH_TIME
                struct timespec start, stop;
                if (runtime->traceEnabled) {
                    clock_gettime(CLOCK_MONOTONIC, &start);
                }
#endif
                selection = tnode->projection != NULL ? tnode->projection: applyIndex(runtime, filter, filter, tableNo, NULL, 0);
#if MEASURE_INDEX_SEARCH_TIME
                if (runtime->traceEnabled) {
                    clock_gettime(CLOCK_MONOTONIC, &stop);
                    tnode->indexSearchTime += (stop.tv_sec - start.tv_sec)*1000000000 + (stop.tv_nsec - start.tv_nsec);
                }
#endif
                if (selection != NULL)
                {
                    Cursor* iterator = selection->internalCursor(runtime->trans);
                    int64_t nSelected = 0;
                    while (iterator->hasNext() && (size_t)runtime->result->size < runtime->limit)
                    {
                        size_t mark2 = allocator->mark();
                        tnode->rec = iterator->next();
                        nSelected += 1;
                        for (int j = 0; j < columns->length; j++)
                        {
                            if (columns->items[j]->tableDep == tableNo)
                            {
                                columns->items[j]->initialize(runtime);
                            }
                        }
                        bool more = joinTables(tableNo, condition, runtime);
                        if (!more)
                        {
                            allocator->reset(mark2);
                        }
                        else
                        {
                            moreResults = true;
                        }
                    }
                    tnode->nIndexSearches += 1;
                    tnode->nSelectedRecords += nSelected;
                    iterator->release();

                    runtime->allocator = NULL; // prevernt destruction of runtime
                    selection->release(); 
                    runtime->allocator = allocator;

                    if (nSelected == 0 && (tnode->joinType & Compiler::OUTER) != 0)
                    {
                        if (selectType == SELECT_COUNT)
                        {
                            runtime->result->size += 1;
                        }
                        else
                        {
                            size_t mark3 = allocator->mark();
                            Tuple* tuple = Tuple::create(columns->length, allocator);
                            for (int i = columns->length; --i >= 0;)
                            {
                                tuple->values[i] = columns->items[i]->tableDep >= tableNo ? &Null: columns->items[i]->evaluate(runtime);
                            }
                            if (runtime->result->add(tuple)) {
                                moreResults = true;
                            } else {
                                allocator->reset(mark3);
                            }
                        }
                    }
                    if (prev != NULL)
                    {
                        prev->right = next;
                    }
                    if (!moreResults)
                    {
                        allocator->reset(mark1);
                    }
                    return moreResults;
                }
                tnode->sequentialSearch = true;
            }
        }
        if (runtime->traceEnabled && traceTableNo < tableNo)
        {
            printf("Sequential search through table '%s'\n", tnode->name->cstr());
            if (filter != NULL)
            {
                printf("\tfilter: %s\n", filter->dump()->cstr());
            }
            if (condition != NULL)
            {
                printf("\tcondition: %s\n", condition->dump()->cstr());
            }
            traceTableNo = tableNo;
        }
        bool emptyJoin = true;
        int joinColumn;
        Value* joinValue;
        if (filter != NULL && getJoinColumn(table, tableNo, filter, joinValue, joinColumn, runtime)) {
            Vector<Record>* arr = ((ResultSet*)table->impl)->recordArray;
            int l = 0, n = arr->length, r = n;
            while (l < r) {
                int m = (l + r) >> 1;
                Value* columnValue = arr->items[m]->get(joinColumn);
                if (columnValue->compare(joinValue) < 0) {
                    l = m + 1;
                } else {
                    r = m;
                }
            }
            while (r < n && (size_t)runtime->result->size < runtime->limit
                   && joinValue->equals(arr->items[r]->get(joinColumn)))
            {
                size_t mark = allocator->mark();
                tnode->rec = arr->items[r++];
                for (int j = 0; j < columns->length; j++)
                {
                    if (columns->items[j]->tableDep == tableNo)
                    {
                        columns->items[j]->initialize(runtime);
                    }
                }
                if (!joinTables(tableNo, condition, runtime)) {
                    allocator->reset(mark);
                } else {
                    moreResults = true;
                }
            }
        } else {
            IndexDescriptor* index;
            Cursor* iterator;
            if (groupBy != NULL
                && (index = table->findIndex(columns->items[groupBy->columnNo]->field, ASCENT_ORDER)) != NULL)
            {
                iterator = index->select(runtime->trans, Index::opNop, 0, NULL)->records(runtime->trans);
            } else {
                iterator = table->internalCursor(runtime->trans);//records(runtime->trans);
            }
            size_t mark = allocator->mark();
            while (iterator->hasNext() && (size_t)runtime->result->size < runtime->limit)
            {
                tnode->rec = iterator->next();
                for (int j = 0; j < columns->length; j++)
                {
                    if (columns->items[j]->tableDep == tableNo)
                    {
                        columns->items[j]->initialize(runtime);
                    }
                }
                bool more = false;
                if (filter == NULL || filter->evaluate(runtime)->isTrue())
                {
                    emptyJoin = false;
                    more = joinTables(tableNo, condition, runtime);
                }
                if (!more)
                {
                    allocator->reset(mark);
                }
                else
                {
                    moreResults = true;
                    mark = allocator->mark();
                }
            }
            iterator->release();
        }
        if (emptyJoin && (tnode->joinType &Compiler::OUTER) != 0)
        {
            if (selectType == SELECT_COUNT)
            {
                runtime->result->size += 1;
            }
            else
            {
                size_t mark = allocator->mark();
                Tuple* tuple = Tuple::create(columns->length, allocator);
                for (int i = columns->length; --i >= 0;)
                {
                    tuple->values[i] = columns->items[i]->tableDep >= tableNo ? &Null: columns->items[i]->evaluate(runtime);
                }
                if (runtime->result->add(tuple)) {
                    moreResults = true;
                } else {
                    allocator->reset(mark);
                }
            }
        }
        if (prev != NULL)
        {
            prev->right = next;
        }
        return moreResults;
    }

    int SelectNode::dependsOn()
    {
        return outDep;
    }

    void SelectNode::setCurrentRecord(Runtime* runtime, int tableNo, Record* rec)
    {
        tables->items[tableNo - 1]->rec = rec;
        if (columns != NULL)
        {
            for (int i = 0; i < columns->length; i++)
            {
                if (columns->items[i]->tableDep == tableNo)
                {
                    columns->items[i]->initialize(runtime);
                }
            }
        }
    }

    inline bool SelectNode::sameOrder(Runtime* runtime, IndexDescriptor* index, OrderNode* order, DataSource* ds)
    {
        if (index == NULL) {
            return false;
        }
        SortOrder requiredOrder = order->kind;
        KeyDescriptor* key;
        for (key = index->_keys; order != NULL; key = key->next, order = order->next) {
            if (key == NULL || key->_field != columns->items[order->columnNo]->field || key->_order == UNSPECIFIED_ORDER || order->kind != requiredOrder) {
                return false;
            }
        }
        return ds == NULL || ds->sortOrder() == requiredOrder || ds->invert(runtime->trans);
    }

    DataSource* SelectNode::executeQuery(Runtime* runtime)
    {
        int i, j, n;
        ResultSet* result;
        usedIndex = NULL;
        if (preliminaryResult != NULL)
        {
            return preliminaryResult;
        }
        else if (!preliminaryResultsCalculated)
        {
            preliminaryResultsCalculated = true;
            for (DynamicTableDescriptor* table = dynamicTables; table != NULL; table = table->nextTable)
            {
                table->evaluate(runtime);
            }
            for (SelectNode* inner = inners; inner != NULL; inner = inner->sibling)
            {
                if (inner->outDep == 0)
                {
                    inner->preliminaryResult = inner->executeQuery(runtime);
                }
            }
        }

        if (selectType == SELECT_DISTINCT && resultColumns->length == 1 && columns->length == 1 && groupBy == NULL && groups == NULL
            && order == NULL && condition == NULL && tables->length == 1 && columns->items[0]->field != NULL)
        {
            IndexDescriptor* index = tables->items[0]->table->findIndex(columns->items[0]->field, ASCENT_ORDER);
            if (index != NULL)
            {
                return subset(projection(new DistinctDataSource(this, index, runtime), runtime), runtime);
            }
        }

        if (selectType != SELECT_INCREMENTAL || sortNeeded)
        {
            ResultSet* saveResult = runtime->result;
            result = (selectType == SELECT_GROUP_BY)
                ? (ResultSet*)new HashResultSet(resultColumns, runtime, groups, groupBy)
                : (ResultSet*)new TemporaryResultSet(resultColumns, runtime);
            if (runtime->traceEnabled && selectType == SELECT_GROUP_BY)
            {
                printf("Hash group by <");
                for (OrderNode* gby = groupBy; gby != NULL; gby = gby->next) {
                    printf(" %s", columns->items[gby->columnNo]->name != NULL
                           ? columns->items[gby->columnNo]->name->cstr() : "?");
                }
                printf(" >\n");
            }

            runtime->result = result;
            runtime->limit = (size_t)-1;
            if (limit != NULL && groupBy == NULL && order == NULL && !sortNeeded) {
                runtime->limit = (size_t)((start != NULL ? start->evaluate(runtime)->intValue() : 0)
                                          + limit->evaluate(runtime)->intValue());
            }
            GroupNode* group;
            for (i = tables->length; --i >= 0;)
            {
                tables->items[i]->projection = NULL;
                tables->items[i]->sequentialSearch = false;
                tables->items[i]->nSelectedRecords = 0;
                tables->items[i]->nIndexSearches = 0;
                tables->items[i]->indexSearchTime = 0;
            }
            for (i = 0; i < columns->length; i++)
            {
                if (columns->items[i]->tableDep == 0) // evaluate constants
                {
                    columns->items[i]->value = columns->items[i]->evaluate(runtime);
                }
            }
            int saveGroupBeg = runtime->groupBegin;
            int saveGroupEnd = runtime->groupEnd;
            runtime->groupBegin = runtime->groupEnd = 0;
            if (groupBy != NULL)
            {
                OrderNode* gp;
                joinTables(0, condition, runtime);
                int nSelected = result->size;
                if (!result->isHash()) {
                    result->toArray();
                    result->orderBy = groupBy;
                    if (!sameOrder(runtime, usedIndex, groupBy))
                    {
                        if (runtime->traceEnabled)
                        {
                            printf("Sort by <");
                            for (OrderNode* ord = groupBy; ord != NULL; ord = ord->next) {
                                printf(" %s", columns->items[ord->columnNo]->name != NULL
                                       ? columns->items[ord->columnNo]->name->cstr() : "?");
                            }
                            printf(" >\n");
                        }
                        iqsort(result->recordArray->items, nSelected, groupBy->next == NULL && groupBy->kind == ASCENT_ORDER
                               ? &compareRecordOrderByField : &compareRecordOrderByFields, groupBy);
                    }
                    for (n = i = 0; i < nSelected;)
                    {
                        for (j = columns->length; --j >= 0;)
                        {
                            columns->items[j]->value = result->get(i, j);
                        }
                        runtime->groupBegin = i;
                        do
                        {
                            if (++i == nSelected)
                            {
                                break;
                            }
                            for (gp = groupBy; gp != NULL; gp = gp->next)
                            {
                                if (!result->get(i - 1, gp->columnNo)->equals(result->get(i, gp->columnNo)))
                                {
                                    break;
                                }
                            }
                        }
                        while (gp == NULL);

                        runtime->groupEnd = i;
                        for (group = groups; group != NULL; group = group->next)
                        {
                            group->value = group->calculate(runtime);
                        }

                        if (having == NULL || having->evaluate(runtime)->isTrue())
                        {
                            for (j = 0; j < groupColumns->length; j++)
                            {
                                if (groupColumns->items[j]->aggregate || groupColumns->items[j]->constant)
                                {
                                    groupColumns->items[j]->initialize(runtime);
                                }
                                result->set(n, j, groupColumns->items[j]->evaluate(runtime));
                            }
                            n += 1;
                        }
                    }
                } else {
                    result->toArray();
                    for (n = i = 0; i < nSelected; i++) {
                        for (j = columns->length; --j >= 0;)
                        {
                            columns->items[j]->value = result->get(i, j);
                        }
                        for (group = groups; group != NULL; group = group->next)
                        {
                            group->value = result->get(i, group->columnNo);
                        }
                        if (having == NULL || having->evaluate(runtime)->isTrue())
                        {
                            for (j = 0; j < groupColumns->length; j++)
                            {
                                if (groupColumns->items[j]->aggregate || groupColumns->items[j]->constant)
                                {
                                    groupColumns->items[j]->initialize(runtime);
                                }
                                result->set(n, j, groupColumns->items[j]->evaluate(runtime));
                            }
                            n += 1;
                        }
                    }
                }
                result->size = n;

                if (order != NULL && !sortNeeded)
                {
                    if (runtime->traceEnabled)
                    {
                        printf("Sort by <");
                        for (OrderNode* ord = order; ord != NULL; ord = ord->next) {
                            printf(" %s", columns->items[ord->columnNo]->name != NULL
                                   ? columns->items[ord->columnNo]->name->cstr() : "?");
                        }
                        printf(" >\n");
                    }
                    iqsort(result->recordArray->items, result->size, order->next == NULL && order->kind == ASCENT_ORDER
                           ? &compareRecordOrderByField : &compareRecordOrderByFields, order);
                    result->orderBy = order;
                }
            }
            else
            {
                size_t mark = runtime->allocator->mark();
                if (selectType == SELECT_COUNT && condition == NULL && tables->length == 1)
                {
                    size_t nRows = 0;
                    if (tables->items[0]->table->isNumberOfRecordsKnown())
                    {
                        nRows = tables->items[0]->table->nRecords(runtime->trans);
                    }
                    else
                    {
                        Cursor* iterator = tables->items[0]->table->internalCursor(runtime->trans);//records(runtime->trans);
                        while (iterator->hasNext())
                        {
                            nRows += 1;
                            iterator->next();
                        }
                        iterator->release();
                    }
                    runtime->allocator->reset(mark);
                    Tuple* tuple = Tuple::create(1, runtime->allocator);
                    tuple->values[0] = new IntValue(nRows);
                    result->size = 0;
                    result->add(tuple);
                }
                else
                {
                    joinTables(0, condition, runtime);
                    if (selectType == SELECT_COUNT)
                    {
                        runtime->allocator->reset(mark);
                        Tuple* tuple = Tuple::create(1, runtime->allocator);
                        tuple->values[0] = new IntValue(result->size);
                        result->size = 0;
                        result->add(tuple);
                    }
                    else if (groups != NULL)
                    {
                        runtime->groupEnd = result->size;
                        for (group = groups; group != NULL; group = group->next)
                        {
                            group->value = group->calculate(runtime);
                        }
                        if (having == NULL || having->evaluate(runtime)->isTrue())
                        {
                            Tuple* tuple = Tuple::create(groupColumns->length, runtime->allocator);
                            for (i = 0; i < groupColumns->length; i++)
                            {
                                if (groupColumns->items[i]->aggregate || groupColumns->items[i]->constant)
                                {
                                    groupColumns->items[i]->initialize(runtime);
                                    tuple->values[i] = groupColumns->items[i]->evaluate(runtime);
                                }
                                else
                                {
                                    tuple->values[i] = runtime->groupEnd > runtime->groupBegin
                                        ? runtime->result->recordArray->items[runtime->groupEnd - 1]->get(i): &Null;
                                }
                            }
                            result->reset();
                            result->add(tuple);
                        } else { 
                            result->reset();
                        }
                    }
                    else
                    {
                        if (order != NULL && !sortNeeded)
                        {
                            if (!sameOrder(runtime, usedIndex, order, result))
                            {
                                if (runtime->traceEnabled)
                                {
                                    printf("Sort by <");
                                    for (OrderNode* ord = order; ord != NULL; ord = ord->next) {
                                        printf(" %s", columns->items[ord->columnNo]->name != NULL
                                               ? columns->items[ord->columnNo]->name->cstr() : "?");
                                    }
                                    printf(" >\n");
                                }
                                result->toArray();
                                iqsort(result->recordArray->items, result->size, order->next == NULL && order->kind == ASCENT_ORDER
                                       ? &compareRecordOrderByField : &compareRecordOrderByFields, order);
                                result->orderBy = order;
                            }
                        }
                    }
                }
            }
            runtime->groupBegin = saveGroupBeg;
            runtime->groupEnd = saveGroupEnd;
            if (sortNeeded)
            {
                n = result->size;
                result->toArray();
                if (n > 1)
                {
                    if (runtime->traceEnabled)
                    {
                        printf("Eliminating duplicates\n");
                    }
                    int nResultColumns = resultColumns->length;
                    iqsort(result->recordArray->items, result->size, &compareRecordAllFields, &nResultColumns);

                    if (selectType == SELECT_DISTINCT)
                    {
                        Record** tuples = result->recordArray->items;
                        for (i = 0, j = i + 1; j < n; j++)
                        {
                            if (compareRecordAllFields(tuples[i], tuples[j], &nResultColumns) != 0)
                            {
                                tuples[++i] = tuples[j];
                            }
                        }
                        result->size = i + 1;
                    }
                    if (order != NULL)
                    {
                        if (runtime->traceEnabled)
                        {
                            printf("Sort by <");
                            for (OrderNode* ord = order; ord != NULL; ord = ord->next) {
                                printf(" %s", columns->items[ord->columnNo]->name != NULL
                                       ? columns->items[ord->columnNo]->name->cstr() : "?");
                            }
                            printf(" >\n");
                        }
                        iqsort(result->recordArray->items, result->size, order->next == NULL && order->kind == ASCENT_ORDER
                           ? &compareRecordOrderByField : &compareRecordOrderByFields, order);
                        result->orderBy = order;
                    }
                }
            }
            runtime->result = saveResult;
            if (runtime->traceEnabled)
            {
                printf("Index selectivity:\n");
                for (i = 0; i < tables->length; i++)
                {
                    if (tables->items[i]->nIndexSearches != 0) {
                        printf("\t%s: %lld searches with selectivity %le, elapsed time %lld nanoseconds\n",
                               tables->items[i]->name->cstr(),
                               tables->items[i]->nIndexSearches,
                               (double)tables->items[i]->nSelectedRecords / tables->items[i]->nIndexSearches,
                               tables->items[i]->indexSearchTime
                            );
                    }
                }
            }
#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
            if (flattened) {
                return subset(new FlattenedDataSource(resultColumns, result, this, runtime), runtime);
            }
#endif
        }
        else
        {
            // incremental traversal
            if (condition == NULL)
            {
                if (order == NULL)
                {
                    return subset(projection(new FilterDataSource(this, table, table, NULL, runtime), runtime), runtime);
                }
                else
                {
                    IndexDescriptor* index;
                    if (order != NULL
                        && sameOrder(runtime, index = table->findIndex(columns->items[order->columnNo]->field, order->kind), order))
                    {
                        FilterDataSource* ds = new FilterDataSource(this, 
                                                                    index->select(runtime->trans,
                                                                                  Index::opNop,
                                                                                  0, 
                                                                                  NULL),
                                                                    index->_table, 
                                                                    NULL, 
                                                                    runtime);
                        if (ds->sortOrder() == order->kind || ds->invert(runtime->trans)) { 
                            return subset(projection(ds, runtime), runtime);
                        }
                    }
                    result = createResultSet(runtime);
                    Cursor* iterator = table->internalCursor(runtime->trans);//records(runtime->trans);
                    while (iterator->hasNext())
                    {
                        Record* rec = iterator->next();
                        setCurrentRecord(runtime, 1, rec);
                        result->add(projectRecord(rec, runtime));
                    }
                    iterator->release();
                }
            }
            else
            {
                DataSource* source = applyIndex(runtime, condition, condition, 1, NULL, 0);
                if (source != NULL)
                {
                    if (order == NULL)
                    {
                        return subset(projection(source, runtime), runtime);
                    }
                    else
                    {
                        if (usedIndex != NULL && sameOrder(runtime, usedIndex, order, source)) 
                        { 
                            return subset(projection(source, runtime), runtime);
                        }
                        else
                        {
                            result = createResultSet(runtime);
                            Cursor* cursor = source->internalCursor(runtime->trans);
                            while (cursor->hasNext())
                            {
                                Record* rec = cursor->next();
                                setCurrentRecord(runtime, 1, rec);
                                result->add(projectRecord(rec, runtime));
                            }
                            cursor->release();
                        }
                    }
                }
                else
                {
                    if (runtime->traceEnabled)
                    {
                        printf("Perform sequential search\n");
                    }

                    if (order == NULL)
                    {
                        return subset(projection(new FilterDataSource(this, table, table, condition, runtime),
                                                 runtime),
                                      runtime);
                    }
                    else
                    {
                        IndexDescriptor* index;
                        FieldDescriptor* sortField = columns->items[order->columnNo]->field;
                        if (sortField != NULL && sameOrder(runtime, index = table->findIndex(sortField, order->kind), order))
                        {
                            FilterDataSource* ds = 
                                new FilterDataSource(this,
                                                     index->select(runtime->trans,
                                                                   Index
                                                                   ::opNop, 0, NULL), 
                                                     index->_table, 
                                                     condition, 
                                                     runtime);
                            if (ds->sortOrder() == order->kind || ds->invert(runtime->trans)) { 
                                return subset(projection(ds, runtime), runtime);
                            }
                        }
                        result = createResultSet(runtime);
                        Cursor* iterator = table->internalCursor(runtime->trans);//records(runtime->trans);
                        size_t mark = runtime->allocator->mark();
                        while (iterator->hasNext())
                        {
                            Record* r = iterator->next();
                            setCurrentRecord(runtime, 1, r);
                            if (condition->evaluate(runtime)->isTrue())
                            {
                                result->add(projectRecord(r, runtime));
                                mark = runtime->allocator->mark();
                            }
                            else
                            {
                                runtime->allocator->reset(mark);
                            }
                        }
                        iterator->release();
                    }
                }
            }
            if (order != NULL)
            {
                result->toArray();
                if (runtime->traceEnabled)
                {
                    printf("Sort by <");
                    for (OrderNode* ord = order; ord != NULL; ord = ord->next) {
                        printf(" %s", columns->items[ord->columnNo]->name != NULL
                               ? columns->items[ord->columnNo]->name->cstr() : "?");
                    }
                    printf(" >\n");
                }
                iqsort(result->recordArray->items, result->size, order->next == NULL && order->kind == ASCENT_ORDER
                       ? &compareRecordOrderByField : &compareRecordOrderByFields, order);
                result->orderBy = order;
            }
        }
        return subset(result, runtime);
    }

    ResultSet* SelectNode::createResultSet(Runtime* runtime)
    {
        return selectAllColumns ? (ResultSet*)new TableResultSet(table, runtime): (ResultSet*)new TemporaryResultSet
                                   (resultColumns, runtime);
    }

    DataSource* SelectNode::projection(DataSource* src, Runtime* runtime)
    {
        DataSource* ds = selectAllColumns ? src : new ProjectionDataSource(resultColumns, src, this, runtime);
#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
        if (flattened) {
            ds = new FlattenedDataSource(resultColumns, ds, this, runtime);
        }
#endif
        return ds;
    }

    Record* SelectNode::projectRecord(Record* rec, Runtime* runtime)
    {
        if (selectAllColumns)
        {
            return rec;
        }
        Tuple* tuple = Tuple::create(resultColumns->length, runtime->allocator);
        for (int i = 0; i < resultColumns->length; i++)
        {
            tuple->values[i] = resultColumns->items[i]->value
                ? resultColumns->items[i]->value
                : resultColumns->items[i]->field
                  ? resultColumns->items[i]->field->get(rec)
                  : resultColumns->items[i]->evaluate(runtime);
        }
        return tuple;
    }

    void SelectNode::init()
    {
        table = NULL;
        condition = NULL;
        order = NULL;
        selectType = SELECT_ALL;
        tables = NULL;
        columns = NULL;
        groupColumns = NULL;
        outDep = 0;
        outer = NULL;
        inners = NULL;
        sibling = NULL;
        preliminaryResult = NULL;
        preliminaryResultsCalculated = false;
        groupBy = NULL;
        having = NULL;
        groups = NULL;
        searchTableNo = 0;
        traceTableNo = 0;
        usedIndex = NULL;
        selectAllColumns = false;
        dynamicTables = NULL;
        flattened = false;
    }

    bool SelectNode::isProjection()
    {
        if (selectAllColumns)
        {
            return true;
        }
        for (int i = 0; i < resultColumns->length; i++)
        {
            if (resultColumns->items[i]->field == NULL)
            {
                return false;
            }
        }
        return true;
    }

    SelectNode::SelectNode(SelectNode* scope): QueryNode(opSelect)
    {
        init();
        tables = Vector < TableNode > ::create(0);
        outer = scope;
        if (scope != NULL)
        {
            sibling = scope->inners;
            scope->inners = this;
        }
    }

    SelectNode::SelectNode(ExprCode tag, TableDescriptor* desc): QueryNode(tag)
    {
        init();
        table = desc;
        tables = Vector < TableNode > ::create(1);
        tables->items[0] = new TableNode(table, 1, NULL, 0);
        forUpdate = true;
        selectType = SELECT_INCREMENTAL;
        selectAllColumns = true;
    }

}
