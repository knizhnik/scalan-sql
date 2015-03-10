/*******************************************************************
 *                                                                 *
 *  compiler.cpp                                                      *
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
 * MODULE:    compiler.cpp
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#include "compiler.h"
#include "stub.h"

namespace McoSql
{
    static char const* const sequenceCmpFunctions[] = {"seq_eq", "seq_ne", "seq_gt", "seq_ge", "seq_lt", "seq_le"};

    SymbolTable::SymbolTable(): HashTable(tknEof)
    {
        put("add", new Symbol(tknAdd));
        put("all", new Symbol(tknAll));
        put("and", new Symbol(tknAnd));
        put("any", new Symbol(tknAny));
        put("array", new Symbol(tknArray));
        put("as", new Symbol(tknAs));
        put("asc", new Symbol(tknAsc));
        put("avg", new Symbol(tknAvg));
        put("between", new Symbol(tknBetween));
        put("binary", new Symbol(tknBinary));
        put("bigint", new Symbol(tknBigint));
        put("bit", new Symbol(tknBit));
        put("blob", new Symbol(tknBlob));
        put("boolean", new Symbol(tknBoolean));
        put("by", new Symbol(tknBy));
        put("cascade", new Symbol(tknCascade));
        put("case", new Symbol(tknCase));
        put("cast", new Symbol(tknCast));
        put("char", new Symbol(tknChar));
        put("coalesce", new Symbol(tknCoalesce));
        put("commit", new Symbol(tknCommit));
        put("constraint", new Symbol(tknConstraint));
        put("contains", new Symbol(tknContains));
        put("cov", new Symbol(tknCov));
        put("cor", new Symbol(tknCor));
        put("count", new Symbol(tknCount));
        put("create", new Symbol(tknCreate));
        put("date", new Symbol(tknDate));
        put("delete", new Symbol(tknDelete));
        put("desc", new Symbol(tknDesc));
        put("distinct", new Symbol(tknDistinct));
        put("domain", new Symbol(tknDomain));
        put("drop", new Symbol(tknDrop));
        put("else", new Symbol(tknElse));
        put("end", new Symbol(tknEnd));
        put("escape", new Symbol(tknEscape));
        put("exact_match", new Symbol(tknExactMatch));
        put("except", new Symbol(tknExcept));
        put("exists", new Symbol(tknExists));
        put("false", new Symbol(tknFalse));
        put("flattened", new Symbol(tknFlattened));
        put("for", new Symbol(tknFor));
        put("foreign", new Symbol(tknForeign));
        put("from", new Symbol(tknFrom));
        put("function", new Symbol(tknFunction));
        put("group", new Symbol(tknGroup));
        put("hash", new Symbol(tknHash));
        put("having", new Symbol(tknHaving));
        put("in", new Symbol(tknIn));
        put("index", new Symbol(tknIndex));
        put("inner", new Symbol(tknInner));
        put("into", new Symbol(tknInto));
        put("intersect", new Symbol(tknIntersect));
        put("insert", new Symbol(tknInsert));
        put("is", new Symbol(tknIs));
        put("int", new Symbol(tknInt));
        put("integer", new Symbol(tknInteger));
        put("join", new Symbol(tknJoin));
        put("key", new Symbol(tknKey));
        put("left", new Symbol(tknLeft));
        put("length", new Symbol(tknLength));
        put("like", new Symbol(tknLike));
        put("ifnull", new Symbol(tknIfNull));
        put("ilike", new Symbol(tknILike));
        put("limit", new Symbol(tknLimit));
        put("local", new Symbol(tknLocal));
        put("longint", new Symbol(tknBigint));
        put("lower", new Symbol(tknLower));
        put("max", new Symbol(tknMax));
        put("min", new Symbol(tknMin));
        put("minus", new Symbol(tknMinus));
        put("natural", new Symbol(tknNatural));
        put("near", new Symbol(tknNear));
        put("not", new Symbol(tknNot));
        put("now", new Symbol(tknSysdate));
        put("null", new Symbol(tknNull));
        put("nullifzero", new Symbol(tknNullIfZero));
        put("on", new Symbol(tknOn));
        put("or", new Symbol(tknOr));
        put("order", new Symbol(tknOrder));
        put("outer", new Symbol(tknOuter));
        put("overlaps", new Symbol(tknOverlaps));
        put("prefix_match", new Symbol(tknPrefixMatch));
        put("preserve", new Symbol(tknPreserve));
        put("primary", new Symbol(tknPrimary));
        put("reference", new Symbol(tknReference));
        put("references", new Symbol(tknReferences));
        put("returns", new Symbol(tknReturns));
        put("right", new Symbol(tknRight));
        put("rollback", new Symbol(tknRollback));
        put("rows", new Symbol(tknRows));
        put("select", new Symbol(tknSelect));
        put("sequence", new Symbol(tknSequence));
        put("set", new Symbol(tknSet));
        put("smallint", new Symbol(tknSmallint));
        put("some", new Symbol(tknSome));
        put("rtree", new Symbol(tknRtree));
        put("trigram", new Symbol(tknTrigram));
        put("start", new Symbol(tknStart));
        put("string", new Symbol(tknString));
        put("sysdate", new Symbol(tknSysdate));
        put("substr", new Symbol(tknSubstr));
        put("substring", new Symbol(tknSubstr));
        put("sum", new Symbol(tknSum));
        put("table", new Symbol(tknTable));
        put("temporary", new Symbol(tknTemporary));
        put("then", new Symbol(tknThen));
        put("time", new Symbol(tknTime));
        put("timestamp", new Symbol(tknTimestamp));
        put("tinyint", new Symbol(tknTinyint));
        put("to", new Symbol(tknTo));
        put("transaction", new Symbol(tknTransaction));
        put("true", new Symbol(tknTrue));
        put("values", new Symbol(tknValues));
        put("varbinary", new Symbol(tknVarbinary));
        put("varchar", new Symbol(tknVarchar));
        put("longvarbinary", new Symbol(tknLongvarbinary));
        put("longvarchar", new Symbol(tknLongvarchar));
        put("union", new Symbol(tknUnion));
        put("unique", new Symbol(tknUnique));
        put("unsigned", new Symbol(tknUnsigned));
        put("update", new Symbol(tknUpdate));
        put("upper", new Symbol(tknUpper));
        put("using", new Symbol(tknUsing));
        put("view", new Symbol(tknView));
        put("where", new Symbol(tknWhere));
        put("when", new Symbol(tknWhen));
        put("with", new Symbol(tknWith));
        #ifdef UNICODE_SUPPORT
            put("unicode", new Symbol(tknUnicode));
        #endif
        #ifndef NO_FLOATING_POINT
            put("abs", new Symbol(tknAbs));
            put("acos", new Symbol(tknAcos));
            put("asin", new Symbol(tknAsin));
            put("atan", new Symbol(tknAtan));
            put("ceil", new Symbol(tknCeil));
            put("cos", new Symbol(tknCos));
            put("decimal", new Symbol(tknDecimal));
            put("double", new Symbol(tknDouble));
            put("exp", new Symbol(tknExp));
            put("float", new Symbol(tknFloat));
            put("floor", new Symbol(tknFloor));
            put("log", new Symbol(tknLog));
            put("numeric", new Symbol(tknNumeric));
            put("real", new Symbol(tknReal));
            put("sin", new Symbol(tknSin));
            put("sqrt", new Symbol(tknSqrt));
            put("tan", new Symbol(tknTan));
        #endif
    }

    Compiler::Compiler(SqlEngine* engine, char const* sql, va_list* list, Value** array, ParamDesc* descs): ExprNode
                       (tpNull, opNop)
    {
        this->engine = engine;
        query = (char*)sql;
        length = (int)STRLEN(sql);
        params = list;
        paramArr = array;
        paramDescs = descs;
        paramCount = 0;
        pos = 0;
        select = NULL;
        ident = NULL;
        func = NULL;
        str = NULL;
        bindings = NULL;
        refTableName = NULL;
        table = NULL;
        value = NULL;
        structPtr = NULL;
        context = CTX_UNKNOWN;
        callCtx = NULL;
        columnIsAggregate = false;
        vars = 0;
        columnList = NULL;
        withContexts = NULL;
        with = NULL;
        if (engine->traceEnabled) {
            printf("Execute statement: %s\n", sql);
        }
    }

    void Compiler::initializeSymbolTable(SqlEngine::CompilerContext& ctx)
    {
        ctx.symtab = new SymbolTable();
        ctx.domainHash = new HashTable(INIT_HASH_SIZE);
        ctx.loadingFunction = false;
    }

    Token Compiler::scan(bool skip)
    {
        int p = pos;
        int eol = length;
        unsigned char* buf = (unsigned char*)query;
        int ch = 0;
        int i, j;

        while (true)
        {
            while (p < eol && isspace(ch = buf[p]))
            {
                p += 1;
            }
            if (p == eol)
            {
                pos = p;
                return tknEof;
            }
            pos = ++p;
            switch (ch)
            {
                case ';':
                    return tknEof;
                case '+':
                    return tknAdd;
                case '-':
                    if (buf[p] == '-')
                    {
                        while (++p < eol && buf[p] != '\n')
                            ;
                        if (p == eol)
                        {
                            return tknEof;
                        }
                        continue;
                    }
                    return tknSub;
                case '*':
                    return tknMul;
                case '/':
                    return tknDiv;
                case ',':
                    return tknComma;
                case '(':
                    return tknLpar;
                case ')':
                    if (with != NULL && pos == with->endPos) {
                        pos = with->usePos;
                        with->usePos = 0;
                        with = with->outer;
                    }
                    return tknRpar;
                case '@':
                    return tknAt;
                case '[':
                    return tknLbr;
                case ']':
                    return tknRbr;
                case ':':
                    if (buf[p] == ':') {
                        pos += 1;
                        return tknCol2;
                    }
                    return tknCol;
                case '^':
                    return tknPower;
                case '?':
                    if (paramDescs == NULL)
                    {
                        throwCompileError("Parameters bindings were not provided", p - 1);
                    }
                    if (!skip && paramDescs[paramCount++].type == tpNull)
                    {
                        throwCompileError("Too many parameters", p - 1);
                    }
                    return tknParam;
                case '%':
                    pos += 1;
                    if (skip) {
                        if (buf[p] == '*') {
                            pos += 1;
                        }
                        return tknParam;
                    }
                    switch (buf[p])
                    {
                    case '*':
                        {
                            pos += 1;
                            Type paramType = tpNull;
                            switch (buf[++p])
                            {
                            case 'b':
                                paramType = tpBool;
                                break;
                            case 'i':
                                paramType = tpInt4;
                                break;
                            case 'u':
                                paramType = tpUInt4;
                                break;
                            case 'l':
                                paramType = tpInt8;
                                break;
                            case 'p':
                                paramType = tpReference;
                                break;
                                #ifndef NO_FLOATING_POINT
                                case 'f':
                                    paramType = tpReal8;
                                    break;
                                #endif
                            case 't':
                                paramType = tpDateTime;
                                break;
                                #ifdef UNICODE_SUPPORT
                                case 'w':
                                    paramType = tpUnicode;
                                    break;
                                #endif
                            case 's':
                                paramType = tpString;
                                break;
                            case 'r':
                                if (!skip) {
                                    structPtr = va_arg(*params, void*);
                                }
                                return tknStruct;
                            case 'R':
                                if (!skip) {
                                    structPtr = va_arg(*params, void*);
                                }
                                return tknStructFS;
                            default:
                                throwCompileError("Invalid parameter type", p - 1);
                            }
                            param = new IndirectParamNode(paramType, va_arg(*params, void*));
                            return tknIndirectParam;
                        }
                    case 'b':
                        return (Token)(va_arg(*params, int) ? (int)tknTrue: (int)tknFalse);
                    case 'i':
                        value = new IntValue(va_arg(*params, int));
                        return tknIconst;
                    case 'u':
                        value = new IntValue(va_arg(*params, unsigned));
                        return tknIconst;
                    case 'l':
                        value = new IntValue(va_arg(*params, int64_t));
                        return tknIconst;
                    case 'p':
                        value = engine->db->createReference(*va_arg(*params, int64_t*));
                        return tknValue;
                        #ifndef NO_FLOATING_POINT
                        case 'f':
                            value = new RealValue(va_arg(*params, double));
                            return tknFconst;
                        #endif
                        #ifdef UNICODE_SUPPORT
                        case 'w':
                            value = UnicodeString::create(va_arg(*params, wchar_t*));
                            return tknUSconst;
                        #endif
                    case 't':
                        value = new DateTime(va_arg(*params, time_t));
                        return tknIconst;
                    case 's':
                        value = String::create(va_arg(*params, char*));
                        return tknSconst;
                    case 'r':
                        structPtr = va_arg(*params, void*);
                        return tknStruct;
                    case 'R':
                        structPtr = va_arg(*params, void*);
                        return tknStructFS;
                    case 'v':
                        value = skip ? &Null : paramArr != NULL ? paramArr[paramCount++]: (Value*)va_arg(*params, Value*);
                        if (value == NULL)
                        {
                            value = &Null;
                        }
                        return tknValue;
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    {
                        int param_no;
                        int n;
                        if (sscanf((char*)&buf[p], "%d%n", &param_no, &n) != 1) {
                            throwCompileError("Invalid parameter number", p);
                        }
                        value = paramArr[param_no];
                        pos += n - 1;
                        return tknValue;
                    }
                    default:
                        throwCompileError("Invalid parameter type", p - 1);
                    }
                case '<':
                    if (buf[p] == '=')
                    {
                        pos += 1;
                        return tknLe;
                    }
                    if (buf[p] == '>')
                    {
                        pos += 1;
                        return tknNe;
                    }
                    return tknLt;
                case '>':
                    if (buf[p] == '=')
                    {
                        pos += 1;
                        return tknGe;
                    }
                    return tknGt;
                case '=':
                    return tknEq;
                case '!':
                    if (buf[p] != '=')
                    {
                        return tknExcl;
                        throwCompileError("Invalid token '!'", p - 1);
                    }
                    pos += 1;
                    return tknNe;
                case '&':
                    return tknAnd;
                case '~':
                    return tknNot;
                case '|':
                    if (buf[p] == '|')
                    {
                        pos += 1;
                        return tknConcat;
                    }
                    return tknOr;
                case '"':
                    while (true)
                    {
                        if (p == eol)
                        {
                            throwCompileError("Unexpected end of quoted identifier", p);
                        }
                        if (buf[p] == '"')
                        {
                            ident = String::create((char*)buf + pos, p - pos);
                            pos = p + 1;
                            return tknIdent;
                        }
                        p += 1;
                    }
                case '\'':
                    i = 0;
                    while (true)
                    {
                        if (p == eol)
                        {
                            throwCompileError("Unexpected end of string constant", p);
                        }
                        if (buf[p] == '\'')
                        {
                            if (++p == eol || buf[p] != '\'')
                            {
                                if (!skip) {
                                    value = String::create(str, i);
                                }
                                pos = p;
                                return tknSconst;
                            }
                        }
                        str[i++] = (char)buf[p++];
                    }
                case '.':
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    i = p -= 1;
                    while (p < eol && isdigit(ch = buf[p]))
                    {
                        p += 1;
                    }
                    if (ch == '.' || ch == 'e' || ch == 'E')
                    {
                        #ifdef NO_FLOATING_POINT
                            throwCompileError("Floating point is disabled", i);
                        #else
                            int hasDigits = false;
                            while (++p < eol && (isdigit(buf[p]) || buf[p] == 'e' || buf[p] == 'E' || buf[p] == '.' ||
                                   ((buf[p - 1] == 'e' || buf[p - 1] == 'E') && (buf[p] == '-' || buf[p] == '+'))))
                            {
                                hasDigits |= isdigit(buf[p]);
                            }
                            double d;
                            int n;
                            if (!hasDigits)
                            {
                                return tknDot;
                            }
                            pos = p;
                            if (sscanf((char*) &buf[i], "%lf%n", &d, &n) == 1 && n == p - i)
                            {
                                if (!skip) {
                                    value = new RealValue(d);
                                }
                                return tknFconst;
                            }
                            throwCompileError("Bad floating point constant", i);
                        #endif
                    }
                    else
                    {
                        pos = p;
                        int64_t val = 0;
                        while (i < p)
                        {
                            val = val * 10+buf[i++] - '0';
                        }
                        value = new IntValue(val);
                        return tknIconst;
                    }
                default:
                    if (isalpha(ch) || ch == '$' || ch == '_')
                    {
                        i = p - 1;
                        j = 0;
                        if (ch != '_')
                        {
                            ch |= (char)('a' - 'A');
                        }
                        str[j++] = ch;
                        while (p < eol && ((ch = buf[p]) == '_' || isalnum(ch) || ch == '$'))
                        {
                            if (ch != '_')
                            {
                                ch |= (char)('a' - 'A');
                            }
                            str[j++] = ch;
                            p += 1;
                        }
                        pos = p;
                        str[j] = '\0';
                        Symbol* s = engine->compilerCtx.symtab->get(str);
                        if (s == NULL)
                        {
                            if (skip || engine->compilerCtx.loadingFunction) { 
                                func = engine->findFunction(str);
                            } else { 
                                engine->compilerCtx.loadingFunction = true;
                                func = engine->findFunction(str, true);
                                engine->compilerCtx.loadingFunction = false;
                            }
                            if (func != NULL)
                            {
                                return tknFunc;
                            }
                            ident = String::create((char*)buf + i, p - i);
                            return tknIdent;
                        }
                        return s->tkn;
                    }
                    else
                    {
                        throwCompileError(String::format("Invalid symbol: %c", ch), p - 1);
                    }
            }
        }
    }
    
    String* Compiler::getTableName()
    {
        str = (char*)MemoryManager::allocator->allocate(length + 1);
        while ((lex = scan(true)) != tknFrom) { 
            if (lex == tknEof) {
                throwCompileError("SELECT ... FROM expected", pos);            
            }
        }
        if (scan(true) != tknIdent) {
            throwCompileError("Table name expected", pos);            
        }
        return ident;
    }
    
    ResultMetadata* Compiler::getResultMetadata(DataSource* ds)
    {
        int nFields = ds->nFields();
        int nParen = 0;
        int i;
        Iterator<Field>* fields = ds->fields();
        ResultMetadata* result = new ResultMetadata();
        result->columns =  Vector<ColumnNode>::create(nFields);
        GroupNode** agp = &result->aggregate;
        OrderNode** gbp = &result->groupBy;
        OrderNode** obp = &result->orderBy;
        Token tkn;
        bool isFlattened = false;
        result->limit = 0;

        for (i = 0; i < nFields; i++) {
            Field* field = fields->next();
            result->columns->items[i] = new ColumnNode(i + 1, field->type(), field->name());
        }
        str = (char*)MemoryManager::allocator->allocate(length + 1);

        while ((lex = scan(true)) != tknSelect || nParen != 0) {
            if (lex == tknEof) {
                 throwCompileError("Expect SELECT statement", pos);
            } else if (lex == tknLpar) {
                nParen += 1;
            } else if (lex == tknRpar) {
                nParen -= 1;
            }
        }
        i = 0;
        while (true) {
            GroupNode* agg = NULL;
            Type type = result->columns->items[i]->type;
            switch (tkn = scan(true)) {
              case tknLpar:
                nParen += 1;
                break;
              case tknFlattened:
                isFlattened = true;
                continue;
              case tknCount:
                agg = new GroupNode(tpInt, opIntSumAll);
                break;
              case tknIfNull:
                expect(tknLpar, "(");
                nParen += 1;
                continue;
              case tknMin:
              case tknMax:
              case tknSum:
                agg = new GroupNode(result->columns->items[i]->type,
                                    ExprCode((int)(type == tpInt ? opIntMinAll : type == tpNumeric ? opNumericMinAll : opRealMinAll) + (int)tkn - (int)tknMin));
                break;
              case tknAvg:
                agg = new GroupNode(tpReal, opAvgMerge);
                break;
              case tknIdent:
                if (ident->compare("seq_sum") == 0 || (isFlattened && ident->compare("seq_hash_agg_sum") == 0)) {
                    agg = (type == tpInt) ? new GroupNode(tpInt, opIntSumAll) : new GroupNode(tpReal, opRealSumAll);
                    break;
                } else if (ident->compare("seq_min") == 0 || (isFlattened && ident->compare("seq_hash_agg_min") == 0)) {
                    agg = (type == tpInt) ? new GroupNode(tpInt, opIntMinAll) : new GroupNode(tpReal, opRealMinAll);
                    break;
                } else if (ident->compare("seq_max") == 0 || (isFlattened && ident->compare("seq_hash_agg_max") == 0)) {
                    agg = (type == tpInt) ? new GroupNode(tpInt, opIntMaxAll) : new GroupNode(tpReal, opRealMaxAll);
                    break;
                } else if (ident->compare("seq_avg") == 0 || (isFlattened && ident->compare("seq_hash_agg_avg") == 0)) {
                    // It will not actually work correctly
                    agg = new GroupNode(tpReal, opRealAvgAll);
                    break;
                } else if (ident->compare("seq_count") == 0 || (isFlattened && ident->compare("seq_hash_agg_count") == 0)) {
                    agg = new GroupNode(tpInt, opIntSumAll);
                    break;
                }
                break;
              default:
                break;
            }
            if (agg != NULL) {
                *agp = agg;
                agp = &agg->next;
                agg->columnNo = i;
                result->columns->items[i]->aggregate = true;
            }
            while ((tkn = scan(true)) != tknEof && ((tkn != tknComma && tkn != tknFrom) || nParen != 0)) {
                if (tkn == tknLpar || tkn == tknLbr) {
                    nParen += 1;
                } else if (tkn == tknRpar || tkn == tknRbr) {
                    nParen -= 1;
                }
            }
            if (tkn != tknComma) {
                break;
            }
            i += 1;
        }

        while (tkn != tknEof) {
            if (tkn == tknOrder) {
                expect(tknBy, "BY");
                do {
                    int columnNo = 0;
                    SortOrder order = ASCENT_ORDER;
                    if ((tkn = scan(true)) == tknIconst) {
                        columnNo = (int)value->intValue();
                    } else if (tkn == tknIdent) {
                        for (i = 0; i < nFields; i++) {
                            if (ident->compare(result->columns->items[i]->name) == 0) {
                                columnNo = i + 1;
                                break;
                            }
                        }
                    }
                    if (columnNo == 0) {
                        throwCompileError("Such sort expression is not supported for distributed query", pos);
                    }
                    if ((tkn = scan(true)) == tknAsc) {
                         tkn = scan(true);
                    } else if (tkn == tknDesc) {
                         order = DESCENT_ORDER;
                         tkn = scan(true);
                    }
                    *obp = new OrderNode(columnNo-1, order);
                    obp = &(*obp)->next;
                } while (tkn == tknComma);
             } else if (tkn == tknGroup) {
                expect(tknBy, "BY");
                do {
                    int columnNo = 0;
                    if ((tkn = scan(true)) == tknIconst) {
                        columnNo = (int)value->intValue();
                    } else if (tkn == tknIdent) {
                        for (i = 0; i < nFields; i++) {
                            if (ident->compare(result->columns->items[i]->name) == 0) {
                                columnNo = i + 1;
                                break;
                            }
                        }
                    }
                    if (columnNo == 0) {
                        throwCompileError("Such group-by expression is not supported for distributed query", pos);
                    }
                    *gbp = new OrderNode(columnNo-1, ASCENT_ORDER);
                    gbp = &(*gbp)->next;
                } while ((tkn = scan(true)) == tknComma);
            } else if (tkn == tknLimit) {
                if ((tkn = scan(true)) == tknIconst) {
                    result->limit = (int)value->intValue();
                    tkn = scan(true);
                } else {
                    throwCompileError("Such limit expression is not supported for distributed query", pos);
                }
                if (tkn == tknComma) {
                    throwCompileError("It is not possible to specify start position for LIMIT clause in distributed query", pos);
                }
            } else {
                tkn = scan(true);
            }
        }
        *gbp = NULL;
        *obp = NULL;
        *agp = NULL;
        if (result->aggregate != NULL && result->groupBy == NULL) {
            for (i = 0; i < nFields; i++) {
                if (!result->columns->items[i]->aggregate) {
                    *gbp = new OrderNode(i, ASCENT_ORDER);
                    gbp = &(*gbp)->next;
                }
            }
            *gbp = NULL;
        }
        return result;
    }

    FieldDescriptor* Compiler::getColumn()
    {
        int p = pos;
        expect(tknIdent, "column name");
        FieldDescriptor* fd = (FieldDescriptor*)table->findField(ident);
        if (fd == NULL)
        {
            throwCompileError(String::format("No such column %s in table %s", ident->cstr(), table->_name->cstr()), p);
        }
        while ((lex = scan()) == tknDot)
        {
            expect(tknIdent, "column name");
            FieldDescriptor* comp = (FieldDescriptor*)fd->findComponent(ident);
            if (comp == NULL)
            {
                throwCompileError(String::format("No such component %s in structure %s", ident->cstr(), fd->_name
                                   ->cstr()), p);
            }
            fd = comp;
        }
        return fd;
    }

    OrderNode* Compiler::orderBy()
    {
        int p = pos;
        int i =  - 1;
        lex = scan();
        if (lex == tknIconst)
        {
            i = (int)((IntValue*)value)->val - 1;
            lex = scan();
        }
        else
        {
            pos = p; // unget token
            ExprNode* expr = disjunction();
            if (expr->tag != opColumn)
            {
                if (select->selectType == SelectNode::SELECT_INCREMENTAL)
                {
                    select->selectType = SelectNode::SELECT_ALL;
                }
                i = select->columns->length;
                Vector < ColumnNode > * columns = Vector < ColumnNode > ::create(i + 1);
                columns->copy(select->columns);
                columns->items[i] = new ColumnNode(i + 1, expr, NULL, NULL);
                select->columns = columns;
            }
            else
            {
                i = ((ColumnNode*)expr)->index - 1;
            }
        }
        if ((unsigned)i >= (unsigned)select->columns->length)
        {
            throwCompileError(String::format("Invalid column specifier in %s by part", (context == CTX_ORDER_BY) ?
                               "order" : "group"), p);
        }
        SortOrder order = ASCENT_ORDER;
        switch (lex)
        {
            case tknAsc:
                lex = scan();
                break;
            case tknDesc:
                order = DESCENT_ORDER;
                lex = scan();
            default:
                ;
        }
        select->columns->items[i]->sorted = true;
        return new OrderNode(i, order);
    }

    KeyDescriptor* Compiler::indexKey()
    {
        FieldDescriptor* fd = getColumn();
        SortOrder order = UNSPECIFIED_ORDER;
        switch (lex)
        {
            case tknAsc:
                order = ASCENT_ORDER;
                lex = scan();
                break;
            case tknDesc:
                order = DESCENT_ORDER;
                lex = scan();
            default:
                ;
        }
        return new KeyDescriptor(fd, order);
    }



    TableDescriptor* Compiler::getTable()
    {
        expect(tknIdent, "table name");
        if (ident->compare(MCO_DATABASE_NAME) == 0) {
            expect(tknDot, ".");
            expect(tknIdent, "table name");
        }
        TableDescriptor* desc = lookupTable(ident);
        lex = scan();
        return desc;
    }

    TableDescriptor* Compiler::lookupTable(String* tableName)
    {
        TableDescriptor* table = (TableDescriptor*)engine->db->findTable(tableName);
        if (table != NULL)
        {
            return table;
        }
        throwCompileError(String::format("Unknown table %s", tableName->cstr()), pos - 1);
    }

    ExprNode* Compiler::condition()
    {
        int p = pos;
        ExprNode* node = disjunction();
        if (node == NULL || (node->type != tpBool && node->type != tpSequence))
        {
            throwCompileError("Boolean expression expected", p);
        }
        if (node->type == tpSequence) {
            node = invokeSequenceUnaryFunction("seq_empty", invokeSequenceBinaryFunction("seq_filter_first_pos", node, new LiteralNode(tpInt, opIntConst, new IntValue(1))));
        }
        return node;
    }

    ColumnNode* Compiler::addColumns(TableNode* tabref, int nColumns, ColumnNode* list, bool join)
    {
        if (join && tabref->joinTable != NULL)
        {
            list = addColumns(tabref->joinTable, nColumns, list, true);
            if (list != NULL)
            {
                nColumns = list->index;
            }
        }
        TableDescriptor* table = tabref->table;
        for (int i = 0, n = table->columns->length; i < n; i++)
        {
            FieldDescriptor* fd = table->columns->items[i];
            JoinColumn* jc;
            for (jc = tabref->joinColumns; jc != NULL && jc->right != fd; jc = jc->next)
                ;
            if (jc == NULL)
            {
                list = new ColumnNode(++nColumns, new LoadNode(fd, tabref), NULL, list);
            }
        }
        return list;
    }

    ExprNode* Compiler::addJoinCondition(TableNode* leftTable, FieldDescriptor* leftColumn, TableNode* rightTable,
                                         FieldDescriptor* rightColumn, ExprNode* joinCondition)
    {
        ExprNode* left = new LoadNode(leftColumn, leftTable);
        ExprNode* right = new LoadNode(rightColumn, rightTable);
        if (!isComparableType(left->type))
        {
            throwCompileError("Invalid type for JOIN column", pos - 1);
        }
        ExprNode* condition = new CompareNode(opEq, left, right);
        if (joinCondition != NULL)
        {
            if (joinCondition->tag == opBoolAnd)
            {
                ((BinOpNode*)joinCondition)->right = condition = new LogicalOpNode(opBoolAnd, ((BinOpNode*)joinCondition)->right, condition);
            }
            else
            {
                condition = new LogicalOpNode(opBoolAnd, joinCondition, condition);
            }
        }
        return condition;
    }

    QueryNode* Compiler::selectStatement(int selectAttr)
    {
        QueryNode* left = unionTables();
        int p = pos;
        if ((selectAttr & SA_SUBQUERY) != 0)
        {
            if (left->resultColumns->length == 1)
            {
                left->type = left->resultColumns->items[0]->type;
            }
            else
            {
                throwCompileError("Subquery should return the single column", p);
            }
        }
        #if 0 // incremental select seems to be more efficient than count all
            if ((selectAttr & SA_EXISTS) != 0)
            {
                if (left->tag == opSelect)
                {
                    SelectNode* select = (SelectNode*)left;
                    if (select->selectType == SelectNode::SELECT_ALL && select->groupBy == NULL)
                    {
                        select->selectType = SelectNode::SELECT_COUNT;
                        select->resultColumns = Vector < ColumnNode > ::create(1);
                        select->resultColumns->items[0] = new ColumnNode(1, new GroupNode(tpInt4, opCountAll, new
                            LiteralNode(tpInt, opIntConst, new IntValue(1))), NULL, NULL);
                        return left;
                    }
                }
            }
        #endif
        if (lex == tknOrder)
        {
            SelectNode* saveSelect = select;
            if (left->tag == opSelect)
            {
                select = (SelectNode*)left;
            }
            expect(tknBy, "BY");
            context = CTX_ORDER_BY;
            OrderNode* last = left->order = orderBy();
            while (lex == tknComma)
            {
                last = last->next = orderBy();
            }
            select = saveSelect;
        }
        if (lex == tknLimit)
        {
            ExprNode* expr = disjunction();
            if (lex == tknComma)
            {
                left->start = expr;
                left->limit = disjunction();
            }
            else
            {
                left->limit = expr;
            }
        }
        if (lex == tknFor)
        {
            expect(tknUpdate, "UPDATE");
            left->forUpdate = true;
            lex = scan();
        }
        return left;
    }

    QueryNode* Compiler::intersectTables()
    {
        QueryNode* left = tablePrimary();
        while (lex == tknIntersect)
        {
            lex = scan();
            QueryNode* right = tablePrimary();
            left = setOperation(opIntersect, "INTERSECT", left, right);
        }
        return left;
    }

    QueryNode* Compiler::unionTables()
    {
        QueryNode* left = intersectTables();
        while (lex == tknUnion || lex == tknMinus || lex == tknExcept)
        {
            ExprCode cop;
            char const* opName;
            if (lex == tknUnion)
            {
                cop = opUnion;
                opName = "UNION";
                lex = scan();
                if (lex == tknAll)
                {
                    cop = opUnionAll;
                    lex = scan();
                }
            }
            else
            {
                opName = "MINUS";
                cop = opMinus;
                lex = scan();
            }
            QueryNode* right = intersectTables();
            left = setOperation(cop, opName, left, right);
        }
        return left;
    }

    QueryNode* Compiler::tablePrimary()
    {
        QueryNode* stmt;
        if (lex == tknLpar)
        {
            lex = scan();
            if (lex == tknSelect) {
                stmt = selectStatement(0);
            } else {
                expected(tknLpar, "(");
                stmt = unionTables();
            }
            expected(tknRpar, ")");
            lex = scan();
        }
        else if (lex == tknSelect)
        {
            stmt = joinTables();
        }
        else
        {
            throwCompileError("SELECT expected", pos - 1);
        }
        return stmt;
    }

    char* Compiler::findView(String* viewName)
    {
        if (engine->db->findTable(String::create("Views")))
        {
            QueryResult result(engine->executeQuery("select body from Views where name=%v", viewName));
            Cursor* cursor = result->records();
            if (cursor->hasNext()) {
                String* str = cursor->next()->get(0)->stringValue();
                char* viewBody = new char[str->size()+1];
                memcpy(viewBody, str->cstr(), str->size()+1);
                return viewBody;
            }
        }
        return NULL;
    }

    static int getKeyPosition(ExprNode* expr, int optimalKeyPos)
    {
        int minKeyPos = MAX_KEY_POS-2;
        if (expr->isBinOpNode()) {
            FieldDescriptor* fd = ((BinOpNode*)expr)->left->getField();
            if (fd == NULL) {
                fd = ((BinOpNode*)expr)->right->getField();
            }
            if (fd != NULL) {
                for (IndexDescriptor* index = fd->_table->_indices; index != NULL; index = index->next) {
                    int keyPos;
                    KeyDescriptor* key;
                    for (key = index->_keys, keyPos = 0; key != NULL && key->_field != fd; keyPos++, key = key->next);
                    if (key != NULL) {
                        if (keyPos == optimalKeyPos) {
                            return optimalKeyPos+1;
                        }
                        if (keyPos < minKeyPos) {
                            minKeyPos = keyPos;
                        }
                    }
                }
            }
        }
        return minKeyPos+1;
    }

    Array* Compiler::arrayLiteral()
    {
        Vector<Value>* vec = Vector<Value>::create(64);
        Type elemType = tpNull;
        int size = 0;
        do {
            int vp = pos;
            if (size == vec->length) {
                Vector<Value>* vec2 = Vector<Value>::create(size*2);
                vec2->copy(vec);
                vec = vec2;
            }
            switch (scan()) {
            case tknIconst:
                if (elemType != tpNull && elemType != tpInt) {
                    throwCompileError("Elements of array should have same type", vp);
                }
                elemType = tpInt;
                break;
            case tknSconst:
                if (elemType != tpNull && elemType != tpString) {
                    throwCompileError("Elements of array should have same type", vp);
                }
                elemType = tpString;
                break;
            case tknFconst:
                if (elemType != tpNull && elemType != tpReal) {
                    throwCompileError("Elements of array should have same type", vp);
                }
                elemType = tpReal;
                break;
            case tknValue:
                if (elemType != tpNull && elemType != value->type()) {
                    throwCompileError("Elements of array should have same type", vp);
                }
                elemType = value->type();
                break;
            case tknLbr:
                if (elemType != tpNull && elemType != tpArray) {
                    throwCompileError("Elements of array should have same type", vp);
                }
                elemType = tpArray;
                value = arrayLiteral();
                break;
            default:
                throwCompileError("Constant array element expected", vp);
            }
            vec->items[size++] = value;
            lex = scan();
        } while (lex == tknComma);

        expected(tknRbr, "]");
        Vector<Value>* finalVec = Vector<Value>::create(size);
        finalVec->copy(vec);
        return new ArrayStub(finalVec, elemType);
    }

    inline bool isIgnored(ExprNode* expr) 
    {
        return expr->tag == ExprNode::opFuncCall &&
            (STRCMP(((FuncCallNode*)expr)->fdecl->getName(), "seq_ignore") == 0
             || STRCMP(((FuncCallNode*)expr)->fdecl->getName(), "seq_internal_ignore") == 0);
    }

    SelectNode* Compiler::joinTables()
    {
        int i;
        SelectNode* outer = select;
        int p = pos;
        lex = scan(true);
        select = new SelectNode(outer);
        select->selectType = SelectNode::SELECT_ALL;
        if (lex == tknAll)
        {
            p = pos;
            lex = scan(true);
        }
        else if (lex == tknDistinct)
        {
            select->sortNeeded = true;
            select->selectType = SelectNode::SELECT_DISTINCT;
            p = pos;
            lex = scan(true);
        }
        else if (lex == tknFlattened)
        {
            select->flattened = true;
            p = pos;
            lex = scan(true);
        }
        if (lex == tknMul)
        {
            select->selectAllColumns = true;
            p = pos;
            lex = scan(true);
        }
        int paren = 0;
        while (lex != tknEof && (lex != tknFrom || paren != 0))
        {
            if (lex == tknLpar || lex == tknLbr) {
                paren += 1;
            } else if (lex == tknRpar || lex == tknRbr) {
                if (paren == 0) { 
                    break;
                }
                paren -= 1;
            }
            lex = scan(true);
        }
        if (paren != 0) {
            throwCompileError("Unbalanced parentheses", pos - 1);
        }
        int nTables = 0;
        TableNode* tableList = NULL;
        int join = 0;
        ExprNode* joinCondition = NULL;
        ExprNode* lastJoinCondition = NULL;
        int nJoinConditions = 0;
        bool containsOuterJoins = false;
        int nParentheses = 0;

        if (lex != tknFrom) {
            TemporaryResultSet* dummyRows = new TemporaryResultSet(Vector<ColumnNode>::create(0), NULL);
            dummyRows->add(0, Tuple::create(0));
            table = new SyntheticTableDescriptor(engine->db, String::create("Dummy"),
                                                 Vector<FieldDescriptor>::create(0),
                                                 dummyRows);
            tableList = new TableNode(table, ++nTables, tableList, join);
        } else {
            while (true)
            {
                TableDescriptor* table;
                lex = scan();
                if (lex == tknLpar) {
                    int unwind = pos;
                    int depth = 0;
                    do {
                        depth += 1;
                    } while ((lex = scan()) == tknLpar);

                    if (lex == tknSelect) {
                        pos = unwind;
                        lex = tknLpar;
                    } else {
                        nParentheses += depth;
                    }
                }
                if (lex == tknSelect)
                {
                    QueryNode* stmt = selectStatement(0);
                    DynamicTableDescriptor* dyntab = new DynamicTableDescriptor(engine->db, stmt);
                    dyntab->nextTable = select->dynamicTables;
                    select->dynamicTables = dyntab;
                    table = dyntab;
                }
                else if (lex == tknLpar)
                {
                    QueryNode* stmt = tablePrimary();
                    DynamicTableDescriptor* dyntab = new DynamicTableDescriptor(engine->db, stmt);
                    dyntab->nextTable = select->dynamicTables;
                    select->dynamicTables = dyntab;
                    table = dyntab;
                }
#if MCO_CFG_CSV_IMPORT_SUPPORT
                else if (lex == tknSconst)
                {
                    String* file = (String*)value;
                    expect(tknAs, "AS");
                    expect(tknIdent, "table name");
                    table = (TableDescriptor*)engine->db->findTable(ident);
                    if (table == NULL)
                    {
                        throwCompileError(String::format("Unknown table %s", ident->cstr()), pos - 1);
                    }
                    table = new CsvTableDescriptor(engine->db, file, table);
                    lex = scan();
                }
                else if (lex == tknForeign)
                {
                    expect(tknTable, "table");
                    expect(tknLpar, "(");
                    String* path = NULL;
                    String* delimiter = NULL;
                    int skip = 0;
                    do {
                        expect(tknIdent, "Foreign table paramter name");
                        String* paramName = ident;
                        expect(tknEq, "=");
                        if (paramName->compare("path") == 0) {
                            expect(tknSconst, "file path");
                            path = (String*)value;
                        } else if (paramName->compare("delimiter") == 0) {
                            expect(tknSconst, "delimiter");
                            delimiter = (String*)value;
                        } else if (paramName->compare("skip") == 0) {
                            expect(tknIconst, "number of lines");
                            skip = (int)value->intValue();
                        } else { 
                            throwCompileError(String::format("Unknown CSV file parameter %s", paramName->cstr()), pos - 1);
                        }                            
                    } while ((lex = scan()) == tknComma);
                    expected(tknRpar, ")");
                    if (path == NULL) {
                        throwCompileError("CSV file path was not specified", pos - 1);
                    }
                    expect(tknAs, "AS");
                    expect(tknIdent, "table name");
                    table = (TableDescriptor*)engine->db->findTable(ident);
                    if (table == NULL)
                    {
                        throwCompileError(String::format("Unknown table %s", ident->cstr()), pos - 1);
                    }
                    table = new CsvTableDescriptor(engine->db, path, table, delimiter, skip);
                    lex = scan();
                }
#endif
                else if (lex == tknLbr)
                {
                    Array* arr = arrayLiteral();
                    Type elemType = arr->getElemType();
                    Type subarrayElemType = tpNull;
                    if (elemType == tpArray) {
                        for (int i = 0, n = arr->size(); i < n; i++) {
                            Type t = ((Array*)arr->getAt(i))->getElemType();
                            if (subarrayElemType != tpNull && t != subarrayElemType) {
                                throwCompileError("Elements of array should have same type", p);
                            }
                            subarrayElemType = t;
                        }
                    }
                    table = new ArrayDataSource(engine->db, arr, elemType, subarrayElemType);
                    lex = scan();
                }
                else if (lex == tknValue)
                {
                    if (value->type() != tpArray) {
                        throwCompileError("Array value expected", pos-1);
                    }
                    Array* arr = (Array*)value;
                    int size = arr->size();
                    Type elemType = arr->getElemType();
                    if (elemType == tpRaw) {
                        for (int i = 0; i < size; i++) {
                            Value* v = arr->getAt(i);
                            if (v != NULL && !v->isNull()) {
                                if (elemType != tpRaw && elemType != v->type()) {
                                    throwCompileError("Elements of array should have same type", pos-1);
                                }
                                elemType = v->type();
                            }
                        }
                        if (elemType == tpRaw) {
                            throwCompileError("Type of array elements can not be concluded", pos-1);
                        }
                    }
                    Type subarrayElemType = tpNull;
                    if (elemType == tpArray) {
                        for (int i = 0; i < size; i++) {
                            Array* subarr = (Array*)arr->getAt(i);
                            if (subarr != NULL && !subarr->isNull()) {
                                Type et = subarr->getElemType();
                                if (et == tpRaw) {
                                    int ss = subarr->size();
                                    for (int j = 0; j < ss; j++) {
                                        Value* v = subarr->getAt(j);
                                        if (v != NULL && !v->isNull()) {
                                            if (subarrayElemType != tpNull && subarrayElemType != v->type()) {
                                                throwCompileError("Elements of array should have same type", pos-1);
                                            }
                                            subarrayElemType = v->type();
                                        }
                                    }
                                } else {
                                    if (subarrayElemType != tpNull && subarrayElemType != et) {
                                        throwCompileError("Elements of array should have same type", pos-1);
                                    }
                                    subarrayElemType = et;
                                }
                            }
                        }
                    }
                    table = new ArrayDataSource(engine->db, arr, elemType, subarrayElemType);
                    lex = scan();
                }
                else
                {
                    WithContext* ctx;
                    expected(tknIdent, "table name");                    
                    for (ctx = withContexts; ctx != NULL; ctx = ctx->next) {
                        if (ident->compare(ctx->name) == 0) {
                            if (ctx->usePos != 0) {
                                throwCompileError(String::format("Recusive use of with context %s is not supported", ident->cstr()), pos - 1);
                            }
                            ctx->usePos = pos;
                            ctx->outer = with;
                            with = ctx;
                            pos = ctx->defPos;
                            break;
                        }
                    }
                    if (ctx != NULL) {
                            continue;
                    }
                    if (ident->compare(MCO_DATABASE_NAME) == 0) {
                        expect(tknDot, ".");
                        expect(tknIdent, "table name");
                    }
                    table = (TableDescriptor*)engine->db->findTable(ident);
                    if (table == NULL)
                    {
                        char* view = findView(ident);
                        if (view != NULL) {
                            size_t viewLen = STRLEN(view);
                            size_t newLength = length + viewLen + 2 - ident->size();
                            char* subst = String::create(newLength)->body();
                            size_t prefix = pos - ident->size();
                            size_t newPos = prefix;
                            memcpy(subst, query, prefix);
                            subst[prefix++] = '(';
                            memcpy(subst+prefix, view, viewLen);
                            prefix += viewLen;
                            subst[prefix++] = ')';
                            memcpy(subst + prefix, query+pos, length - pos);
                            pos = newPos;
                            length = newLength;
                            query = subst;
                            delete[] view;
                            continue;
                        }
                        throwCompileError(String::format("Unknown table %s", ident->cstr()), pos - 1);
                    }                
                    lex = scan();
                }
                while (lex == tknRpar && nParentheses != 0)
                {
                    nParentheses -= 1;
                    lex = scan();
                }
                tableList = new TableNode(table, ++nTables, tableList, join);
                if (lex == tknAs)
                {
                    expect(tknIdent, "table alias name");
                    tableList->name = ident;
                    lex = scan();
                }
                else if (lex == tknIdent)
                {
                    tableList->name = ident;
                    lex = scan();
                }
                if ((join & NATURAL) != 0)
                {
                    JoinColumn* joinColumns = NULL;
                    TableNode* joinList = tableList->next;
                    do
                    {
                        TableDescriptor* leftTable = joinList->table;
                        for (i = table->columns->length; --i >= 0;)
                        {
                            FieldDescriptor* rightColumn = table->columns->items[i];
                            if (!rightColumn->isAutoGenerated())
                            {
                                FieldDescriptor* leftColumn = (FieldDescriptor*)leftTable->findField(rightColumn->_name);
                                if (leftColumn != NULL)
                                {
                                    if (leftColumn->_type != rightColumn->_type)
                                    {
                                        throwCompileError("Types of columns for NATURAL JOIN do not match", pos - 1);
                                    }
                                    lastJoinCondition = addJoinCondition(tableList->next, leftColumn, tableList,
                                                                         rightColumn, lastJoinCondition);
                                    if (++nJoinConditions == 2)
                                    {
                                        joinCondition = lastJoinCondition;
                                    }
                                    joinColumns = new JoinColumn(leftColumn, rightColumn, joinColumns);
                                }
                            }
                        }
                    }
                    while ((joinList = joinList->joinTable) != NULL);
                    if (joinColumns == NULL)
                    {
                        throwCompileError("NATURAL JOIN not possible", pos - 1);
                    }
                    tableList->joinColumns = joinColumns;
                    tableList->joinTable = tableList->next;
                }
                else if (lex == tknUsing)
                {
                    JoinColumn* joinColumns = NULL;
                    do
                    {
                        expect(tknIdent, "column name");
                        FieldDescriptor* rightColumn = (FieldDescriptor*)table->findField(ident);
                        TableNode* joinList = tableList->next;
                        FieldDescriptor* leftColumn = NULL;
                        while (joinList != NULL && (leftColumn = (FieldDescriptor*)joinList->table->findField(ident)) ==
                               NULL)
                        {
                            joinList = joinList->joinTable;
                        }
                        if (leftColumn == NULL || rightColumn == NULL || leftColumn->_type != rightColumn->_type)
                        {
                            throwCompileError(String::format(
                                                  "JOIN column '%s' doesn't present in both tables or types mismatch", ident
                                                  ->cstr()), pos - 1);
                        }
                        lastJoinCondition = addJoinCondition(tableList->next, leftColumn, tableList, rightColumn,
                                                             lastJoinCondition);
                        if (++nJoinConditions == 2)
                        {
                            joinCondition = lastJoinCondition;
                        }
                        joinColumns = new JoinColumn(leftColumn, rightColumn, joinColumns);
                    }
                    while ((lex = scan()) == tknComma)
                        ;

                    tableList->joinTable = tableList->next;
                    tableList->joinColumns = joinColumns;
                } else if (lex == tknOn) {
                    select->tables = Vector < TableNode > ::create(nTables);
                    TableNode* tn = tableList;
                    for (int i = nTables; --i >= 0; ) {
                        select->tables->items[i] = tn;
                        tn = tn->next;
                    }
                    ExprNode* onCond = condition();
                    if (lastJoinCondition != NULL)
                    {
                        if (lastJoinCondition->tag == opBoolAnd)
                        {
                            lastJoinCondition = ((BinOpNode*)lastJoinCondition)->right =
                                new LogicalOpNode(opBoolAnd,
                                                  ((BinOpNode*)lastJoinCondition)->right, onCond);
                        }
                        else
                        {
                            lastJoinCondition = new LogicalOpNode(opBoolAnd, lastJoinCondition, onCond);
                        }
                    } else {
                        lastJoinCondition = onCond;
                    }
                    if (++nJoinConditions == 2)
                    {
                        joinCondition = lastJoinCondition;
                    }
                }
                while (lex == tknRpar && nParentheses != 0)
                {
                    nParentheses -= 1;
                    lex = scan();
                    if (lex == tknAs)
                    {
                        expect(tknIdent, "table alias name");
                        tableList->name = ident;
                        lex = scan();
                    }
                    else if (lex == tknIdent)
                    {
                        tableList->name = ident;
                        lex = scan();
                    }
                }
                join = 0;
                while (true)
                {
                    if (lex == tknNatural)
                    {
                        join |= NATURAL;
                    }
                    else if (lex == tknLeft)
                    {
                        join |= OUTER|LEFT;
                    }
                    else if (lex == tknRight)
                    {
                        join |= OUTER|RIGHT;
                    }
                    else if (lex == tknInner)
                    {
                        join |= INNER;
                    }
                    else if (lex == tknOuter)
                    {
                        join |= OUTER;
                        containsOuterJoins = true;
                    }
                    else
                    {
                        break;
                    }
                    lex = scan();
                }
                if (join != 0)
                {
                    expected(tknJoin, "JOIN");
                    if ((join &(RIGHT | OUTER)) == (RIGHT | OUTER))
                    {
                        throwCompileError("RIGHT OUTER JOIN is not supported", pos - 1);
                    }
                    if (((join &(LEFT | RIGHT)) ^ (LEFT | RIGHT)) == 0)
                    {
                        throwCompileError("Invalid join type: join can be either LEFT either RIGHT", pos - 1);
                    }
                    if (((join &(INNER | OUTER)) ^ (INNER | OUTER)) == 0)
                    {
                        throwCompileError("Invalid join type: join can be either INNER either OUTER", pos - 1);
                    }
                    join |= JOIN;
                }
                else if (lex == tknJoin)
                {
                    join = JOIN;
                }
                else if (lex != tknComma)
                {
                    break;
                }
            }
        }
        if (nParentheses != 0)
        {
            throwCompileError("')' expected", pos - 1);
        }
        select->tables = Vector < TableNode > ::create(nTables);
        i = nTables;
        do
        {
            select->tables->items[--i] = tableList;
        }
        while ((tableList = tableList->next) != NULL);

        int savePos = pos;
        Token saveLex = lex;
        int nColumns = 0;
        columnList = NULL;

        pos = p; // returns back to the fields list
        context = CTX_COLUMNS;
        if (select->selectAllColumns)
        {
            for (i = 0; i < nTables; i++)
            {
                columnList = addColumns(select->tables->items[i], nColumns, columnList, false);
                if (columnList != NULL)
                {
                    nColumns = columnList->index;
                }
            }
            lex = scan();
        }
        else
        {
            do
            {
                p = pos;
                if (scan(true) == tknIdent)
                {
                    // handle T.*
                    String* tableName = ident;
                    if (scan(true) == tknDot)
                    {
                        if ((lex = scan(true)) == tknMul)
                        {
                            for (i = 0; i < nTables; i++)
                            {
                                if (tableName->equals(select->tables->items[i]->name))
                                {
                                    columnList = addColumns(select->tables->items[i], nColumns, columnList, true);
                                    if (columnList != NULL)
                                    {
                                        nColumns = columnList->index;
                                    }
                                    lex = scan(true);
                                    break;
                                }
                            }
                            if (i < nTables)
                            {
                                continue;
                            }
                            throwCompileError(String::format("Invalid table reference: %s", tableName->cstr()), p);
                        }
                    }
                }
                pos = p;
                columnIsAggregate = false;
                ExprNode* expr = disjunction();
                String* alias = NULL;
                bool materialized = false;
                if (lex == tknAs)
                {
                    expect(tknIdent, "alias name");
                    alias = ident;
                    lex = scan();
                }
                else if (lex == tknCol)
                {
                    expect(tknIdent, "alias name");
                    alias = ident;
                    expr = invokeSequenceTernaryFunction("seq_internal_materialize", expr,
                                                        new LiteralNode(tpRaw, opConst, new ScalarArray<char>(MCO_CFG_PREALLOCATED_MATERIALIZE_BUFFER_SIZE, MCO_CFG_PREALLOCATED_MATERIALIZE_BUFFER_SIZE)),
                                                        new MarkNode());
                    lex = scan();
                    materialized = true;
                }
                else if (lex == tknIdent)
                {
                    alias = ident;
                    lex = scan();
                }
                columnList = new ColumnNode(++nColumns, expr, alias, columnList);
                columnList->materialized = materialized;
                columnList->aggregate = columnIsAggregate;
            }
            while (lex == tknComma);
        }
        if (lex != tknFrom && lex != tknEof && lex != tknRpar) {
            throwCompileError("FROM expected", pos-1);
        }

        select->groupColumns = select->resultColumns = select->columns = Vector < ColumnNode > ::create(nColumns);
        i = nColumns;
        while (columnList != NULL) {
            select->columns->items[--i] = columnList;
            columnList = columnList->next;
        }

        assert(i == 0);
        if (select->groups != NULL)
        {
            select->columns = Vector < ColumnNode > ::create(nColumns);
            select->columns->copy(select->groupColumns);
            for (i = 0; i < nColumns; i++)
            {
                ColumnNode* column = select->columns->items[i];
                if (column->expr != NULL && column->expr->isGroupNode())
                {
                    if (((GroupNode*)column->expr)->tag == opUserAggr) {
                        select->columns->items[i] = new ColumnNode(column->index,
                                                                   ((FuncCallNode*)((GroupNode*)column->expr)->expr)->args->items[0], NULL, NULL);
                        column->aggregate = true;
                    } else {
                        select->columns->items[i] = new ColumnNode(column->index, ((GroupNode*)column->expr)->expr, NULL, NULL);
                    }
                    ((GroupNode*)column->expr)->columnNo = i;
                }
                else if (column->aggregate)
                {
                    select->columns->items[i] = new ColumnNode(column->index, new ConstantNode(tpNull, opNull),
                        NULL, NULL);
                }
            }
            for (GroupNode* group = select->groups; group != NULL; group = group->next)
            {
                if (group->columnNo < 0)
                {
                    int n = select->columns->length;
                    Vector < ColumnNode > * columns = Vector < ColumnNode > ::create(n + 1);
                    columns->copy(select->columns);
                    group->expr = columns->items[n] = new ColumnNode(n + 1, group->expr, NULL, NULL);
                    group->columnNo = n;
                    select->columns = columns;
                }
            }
        }
        lex = saveLex;
        pos = savePos;
        if (lex == tknWhere)
        {
            context = CTX_WHERE;
            select->condition = condition();
        }
        if (nJoinConditions > 0)
        {
            if (select->condition == NULL)
            {
                select->condition = nJoinConditions == 1 ? lastJoinCondition : joinCondition;
            }
            else if (nJoinConditions == 1)
            {
                select->condition = new LogicalOpNode(opBoolAnd, lastJoinCondition, select->condition);
            }
            else
            {
                ((BinOpNode*)lastJoinCondition)->right =
                    new LogicalOpNode(opBoolAnd, ((BinOpNode*)lastJoinCondition)->right, select->condition);
                select->condition = joinCondition;
            }
        }
        if (select->condition != NULL)
        {
            ExprNode* andCond;
            int nConjuncts = 1;
            for (andCond = select->condition; andCond->tag == opBoolAnd; andCond = ((BinOpNode*)andCond)->right)
            {
                nConjuncts += 1;
            }
            if (nConjuncts > 1)
            {
                assert(nConjuncts <= MAX_CONJUNCTS);
                assert(nTables <= MAX_TABLE_JOINS);
                ExprNode* originalConjuncts[MAX_CONJUNCTS];
                ExprNode* conjuncts[MAX_CONJUNCTS];
                int weight[MAX_CONJUNCTS];
                int position[(MAX_TABLE_JOINS + 1)*MAX_KEY_POS];
                memset(position, 0, sizeof(position));
                i = 0;
                andCond = select->condition;
                int firstKeyPos = -1;
                do
                {
                    ExprNode* conjunct;
                    if (andCond->tag == opBoolAnd)
                    {
                        conjunct = ((BinOpNode*)andCond)->left;
                        andCond = ((BinOpNode*)andCond)->right;
                    }
                    else
                    {
                        conjunct = andCond;
                        andCond = NULL;
                    }
                    originalConjuncts[i] = conjunct;
                    int tableNo = conjunct->dependsOn();
                    if (engine->optimizerParams.enableCostBasedOptimization) {
                        weight[i] = select->calculateCost(engine->optimizerParams, conjunct, tableNo);
                    } else {
                        bool isUnique;
                        int conjunctWeight = tableNo*MAX_KEY_POS;
                        int keyPos = 0;
                        if (firstKeyPos >= 0 && (keyPos = getKeyPosition(conjunct, i - firstKeyPos)) == i - firstKeyPos + 1) {
                            if (keyPos >= MAX_KEY_POS) {
                                keyPos = MAX_KEY_POS - 1;
                            }
                            conjunctWeight += keyPos;
                        } else if (tableNo > 0 && select->canUseIndex(NULL, conjunct, tableNo, 0, isUnique)) {
                            firstKeyPos = i;
                        } else {
                            if (keyPos == 0) {
                                keyPos = getKeyPosition(conjunct, 0);
                            }
                            conjunctWeight += keyPos;
                        }
                        weight[i] = conjunctWeight;
                        position[conjunctWeight] += 1;
                    }
                    i += 1;
                }
                while (andCond != NULL);

                if (engine->optimizerParams.enableCostBasedOptimization) {
                    // insertion sort
                    int first = 1;
                    int middle = 1;
                    int last = nConjuncts - 1;
                    int proceeded = 0;
                    int tempWeight;
                    ExprNode* tempConjunct;

                    while (first != last) {
                        first += 1;
                        if (weight[middle] > weight[first]) {
                            middle = first;
                        }
                    }
                    if (weight[proceeded] > weight[middle]) {
                        tempWeight = weight[proceeded];
                        weight[proceeded] = weight[middle];
                        weight[middle] = tempWeight;

                        tempConjunct = conjuncts[proceeded];
                        conjuncts[proceeded] = conjuncts[middle];
                        conjuncts[middle] = tempConjunct;
                    }
                    proceeded += 1;
                    while (proceeded != last) {
                        first = proceeded++;
                        if (weight[first] > weight[proceeded]) {
                            middle = proceeded;
                            tempWeight = weight[middle];
                            tempConjunct = conjuncts[middle];
                            do
                            {
                                weight[middle] = weight[first];
                                conjuncts[middle--] = conjuncts[first--];
                            } while (weight[first] > tempWeight);
                            weight[middle] = tempWeight;
                            conjuncts[middle] = tempConjunct;
                        }
                    }
                } else {
                    int offs = 0;
                    for (i = 0; i < (nTables + 1)*MAX_KEY_POS; i++)
                    {
                        int n = position[i];
                        position[i] = offs;
                        offs += n;
                    }
                    assert(offs = nConjuncts);
                    for (i = 0; i < nConjuncts; i++)
                    {
                        conjuncts[position[weight[i]]++] = originalConjuncts[i];
                    }
                }
                andCond = new LogicalOpNode(opBoolAnd, conjuncts[i - 2], conjuncts[i - 1]);
                i -= 2;
                while (--i >= 0)
                {
                    andCond = new LogicalOpNode(opBoolAnd, conjuncts[i], andCond);
                }
                select->condition = andCond;
            }
        }
        if (lex == tknGroup || lex == tknHaving)
        {
            if (lex == tknGroup) { 
                expect(tknBy, "BY");
                context = CTX_GROUP_BY;
                OrderNode* last = select->groupBy = orderBy();
                while (lex == tknComma)
                {
                    last = last->next = orderBy();
                }
            }
            if (lex == tknHaving)
            {
                context = CTX_HAVING;
                select->having = condition();
            }
            GroupNode* group = select->groups;
            if (group != NULL && select->groupBy != NULL && outer == NULL && select->selectType != SelectNode::SELECT_DISTINCT) {
                OrderNode* gby;
                do {
                    switch (group->tag) {
                      case opCountAll:
                      case opCountNotNull:
                      case opNumericMinAll:
                      case opNumericMinDis:
                      case opIntMinAll:
                      case opIntMinDis:
                      case opStrMin:
                      case opRealMinAll:
                      case opRealMinDis:
                      case opNumericMaxAll:
                      case opNumericMaxDis:
                      case opIntMaxAll:
                      case opIntMaxDis:
                      case opStrMax:
                      case opRealMaxAll:
                      case opRealMaxDis:
                      case opIntSumAll:
                      case opRealSumAll:
                      case opNumericSumAll:
                      case opRealAvgAll:
                      case opIntAvgAll:
                      case opNumericAvgAll:
                        if (group->columnNo < 0) { 
                            continue;
                        }
                        for (gby = select->groupBy; gby != NULL && gby->columnNo != group->columnNo; gby = gby->next);
                        if (gby == NULL) { 
                            continue;
                        }
                        // no break
                      default:
                        break;
                    }
                    break;
                } while ((group = group->next) != NULL);

                if (group == NULL) {
                    select->selectType = SelectNode::SELECT_GROUP_BY;
                }
            }
        }
        else if (select->groupColumns->length == 1 && select->groupColumns->items[0]->expr != NULL && select->groupColumns->items[0]->expr->tag == opCountAll)
        {
            select->selectType = SelectNode::SELECT_COUNT;
        }
        if (!containsOuterJoins)
        {
            for (i = 0; i < nColumns; i++)
            {
                ColumnNode* column = select->columns->items[i];
                if (!column->used)
                {
                    // lazy calculation of not used columns
                    column->tableDep = nTables;
                }
            }
        }
        SelectNode* stmt = select;
        select = outer;
        if (stmt->selectType == SelectNode::SELECT_ALL && nTables == 1 && /*stmt->isProjection() && */
            stmt->resultColumns->length == stmt->columns->length && stmt->groupBy == NULL && stmt->groups == NULL
            /*&& outer == NULL*/)
        {
            stmt->selectType = SelectNode::SELECT_INCREMENTAL;
            stmt->table = stmt->tables->items[0]->table;
        }
        if (stmt->flattened) {
            int nSequences = 0;
            for (i = 0; i < nColumns; i++) {
                if (stmt->resultColumns->items[i]->type == tpSequence) {
                    stmt->resultColumns->items[i]->type = tpReal;
                    stmt->resultColumns->items[i]->flattened = true;
                    nSequences += 1;
                }
            }
            if (nSequences == 0) {
                stmt->flattened = false;
            }
        }
        return stmt;
    }

    QueryNode* Compiler::setOperation(ExprCode cop, char const* opName, QueryNode* left, QueryNode* right)
    {
        if (left->resultColumns->length != right->resultColumns->length)
        {
            throwCompileError(String::format("Operands of %s operator should have the same number of columns", opName),
                               pos - 1);
        }
        Vector < ColumnNode > * columns = Vector < ColumnNode > ::create(left->resultColumns->length);
        for (int i = 0; i < left->resultColumns->length; i++)
        {
            if (left->resultColumns->items[i]->type != right->resultColumns->items[i]->type && left->resultColumns
                ->items[i]->type != tpNull && right->resultColumns->items[i]->type != tpNull)
            {
                throwCompileError(String::format("Column %d of left operand of %s has type %s and right operand - %s",
                                   i + 1, opName, typeMnemonic[left->resultColumns->items[i]->type], typeMnemonic[right
                                   ->resultColumns->items[i]->type]), pos - 1);
            }
            if (left->resultColumns->items[i]->field == right->resultColumns->items[i]->field && (left->resultColumns
                ->items[i]->name == right->resultColumns->items[i]->name || (left->resultColumns->items[i]->name !=
                NULL && left->resultColumns->items[i]->name->equals(right->resultColumns->items[i]->name))))
            {
                columns->items[i] = left->resultColumns->items[i];
            }
#if 0
            else if (left->resultColumns->items[i]->name != NULL && left->resultColumns->items[i]->name->equals(right
                     ->resultColumns->items[i]->name))
            {
                columns->items[i] = new ColumnNode(i + 1, left->resultColumns->items[i]->type, left->resultColumns
                                                   ->items[i]->name);
            }
            else
            {
                columns->items[i] = new ColumnNode(i + 1, left->resultColumns->items[i]->type, NULL);
            }
#endif
            columns->items[i] = new ColumnNode(i + 1, left->resultColumns->items[i]->type,
                                               left->resultColumns->items[i]->name);
        }
        if (cop != opUnionAll)
        {
            left->sortNeeded = true;
            right->sortNeeded = true;
        }
        return new TableOpNode(cop, columns, left, right);
    }



    String* Compiler::compoundName(char const* what)
    {
        expect(tknIdent, what);
        String* name = ident;
        while ((lex = scan()) == tknDot)
        {
            expect(tknIdent, what);
            name = String::concat(name, ident);
        }
        return name;
    }

    StmtNode* Compiler::statement()
    {
        int p;
        StmtNode* stmt;
        bool unique = false;

        str = (char*)MemoryManager::allocator->allocate(length + 1);
        table = NULL;
        with = NULL;
        withContexts = NULL;

        switch (lex = scan())
        {
            case tknCreate:
            {
                p = pos;
                McoSql::Token l;
                bool temporary = false;
                while (true) { 
                    l = scan(); 
                    if (l == tknTemporary) { 
                        temporary = true;
                    } else if (l != tknLocal) {
                        break;
                    }
                }
                switch (l)
                {
                case tknTable:
                {
                    stmt = createTable(temporary);
                    break;
                }

                case tknFunction:
                    stmt = createFunction();
                    break;                

                case tknView:
                {
                    expect(tknIdent, "View name");
                    String* viewName = ident;
                    expect(tknAs, "AS");
                    return new CreateViewNode(viewName, String::create(&query[pos]));
                }
                case tknUnique:
                    expect(tknIndex, "INDEX");
                    unique = true;
                    // nobreak
                case tknIndex:
                    {
                        String* indexName = compoundName("index name");
                        expected(tknOn, "ON");
                        table = getTable();
                        p = pos;
                        expected(tknLpar, "(");
                        KeyDescriptor* keys;
                        KeyDescriptor** kpp = &keys;
                        do
                        {
                            KeyDescriptor* key = indexKey();
                            *kpp = key;
                            kpp = &key->next;
                        }
                        while (lex == tknComma);
                        *kpp = NULL;
                        expected(tknRpar, ")");
                        lex = scan();
                        bool ordered = true;
                        bool spatial = false;
                        bool trigram = false;
                        if (lex == tknUsing) {
                            p = pos;
                            lex = scan();
                            if (lex == tknHash) {
                                ordered = false;
                            } else if (lex == tknRtree) {
                                spatial = true;
                            } else if (lex == tknTrigram) {
                                trigram = true;
                            } else {
                                throwCompileError("HASH, RTREE or TRIGRAM expected", p);
                            }
                            lex = scan();
                        }
                        stmt = new AddIndexNode(indexName, table, keys, unique, ordered, spatial, trigram);
                        break;
                    }

                case tknDomain:
                    {
                        p = pos;
                        lex = scan();
                        if (lex != tknIdent)
                        {
                            throwCompileError("Domain name expected", p);
                        }
                        FieldDescriptor* domain = new FieldDescriptor(table, ident, tpNull, tpNull);
                        setColumnType(domain);
                        stmt = new CreateDomainNode(domain);
                        break;
                    }

                default:
                    throwCompileError("CREATE [TABLE, INDEX or DOMAIN] expected", p);
                }
                break;
            }

            case tknWith:
                {
                    do {
                        expect(tknIdent, "Identifier");
                        WithContext* ctx = new WithContext();
                        ctx->name = ident;
                        expect(tknAs, "AS");
                        ctx->defPos = pos;
                        ctx->next = withContexts;
                        withContexts = ctx;
                        ctx->usePos = 0;
                        expect(tknLpar, "(");
                        int nParen = 1;
                        do {
                            lex = scan();
                            if (lex == tknLpar) {
                                nParen += 1;
                            } else if (lex == tknRpar) {
                                nParen -= 1;
                            }
                        } while (nParen != 0);
                        ctx->endPos = pos;
                    } while ((lex = scan()) == tknComma);

                    expected(tknSelect, "SELECT");
                }
                // no break

            case tknSelect:
                p = pos;
                stmt = selectStatement(0);
                break;

            case tknDelete:
                {
                    expect(tknFrom, "FROM");
                    table = getTable();
                    DeleteNode* deleteStmt = new DeleteNode(table);
                    stmt = select = deleteStmt;
                    if (lex == tknWhere)
                    {
                        context = CTX_WHERE;
                        deleteStmt->condition = condition();
                    }
				    break;
                }

            case tknSet:
                {
                    p = pos;
                    expect(tknIdent, "Property name");
                    String* param = ident->toLowerCase();
                    if (param->compare("default_isolation_level") == 0 || param->compare("defaultisolationlevel") == 0) {
                        TransactionNode* trans = new TransactionNode(opSetDefaultIsolationLevel);
                        p = pos;
                        expect(tknIdent, "isolation level");
                        param = ident->toLowerCase();
                        if (param->compare("read_committed") == 0 || param->compare("readcommitted") == 0) {
                            trans->isolationLevel = Transaction::ReadCommitted;
                        } else if (param->compare("repeatable_read") == 0 || param->compare("repeatableread") == 0) {
                            trans->isolationLevel = Transaction::RepeatableRead;
                        } else if (param->compare("serializable") == 0) {
                            trans->isolationLevel = Transaction::Serializable;
                        } else {
                            throwCompileError("Unsupported isolation level", p);
                        }
                        stmt = trans;
                    } else if (param->compare("default_priority") == 0 || param->compare("defaultpriority") == 0) {
                        expect(tknIconst, "priority");
                        TransactionNode* trans = new TransactionNode(opSetDefaultPriority);
                        trans->priority = (int)((IntValue*)value)->val;
                        stmt = trans;
                    } else {
                        throwCompileError("Unsupported property name", p);
                    }
                    lex = scan();
                    break;
                }
            case tknStart:
                {
                    expect(tknTransaction, "TRANSACTION");
                    p = pos;
                    TransactionNode* trans = new TransactionNode(opStartTransaction);
                    stmt = trans;
                    while ((lex = scan()) == tknIdent || lex == tknIconst) {
                        if (lex == tknIdent) {
                            String* param = ident->toLowerCase();
                            if (param->compare("read_only") == 0 || param->compare("readonly") == 0) {
                                trans->mode = Transaction::ReadOnly;
                            } else if (param->compare("read_write") == 0 || param->compare("readwrite") == 0) {
                                trans->mode = Transaction::ReadWrite;
                            } else if (param->compare("update") == 0) {
                                trans->mode = Transaction::Update;
                            } else if (param->compare("read_committed") == 0 || param->compare("readcommitted") == 0) {
                                trans->isolationLevel = Transaction::ReadCommitted;
                            } else if (param->compare("repeatable_read") == 0 || param->compare("repeatableread") == 0) {
                                trans->isolationLevel = Transaction::RepeatableRead;
                            } else if (param->compare("serializable") == 0) {
                                trans->isolationLevel = Transaction::Serializable;
                            } else {
                                throwCompileError("Invalid parameter of START TRANSACTION", p);
                            }
                        } else {
                            trans->priority = (int)((IntValue*)value)->val;
                        }
                    }
                }
                break;
            case tknCommit:
                expect(tknTransaction, "TRANSACTION");
                lex = scan();
                stmt = new TransactionNode(opCommitTransaction);
                break;
            case tknRollback:
                expect(tknTransaction, "TRANSACTION");
                lex = scan();
                stmt = new TransactionNode(opRollbackTransaction);
                break;

            case tknInsert:
                {
                    bool replace = false;
                    lex = scan();
                    if (lex == tknOr) {
                        expect(tknUpdate, "UPDATE");
                        replace = true;
                        lex = scan();
                    }
                    expected(tknInto, "INTO");
                    table = getTable();
                    ColumnValue* first = NULL;
                    int nColumns = 0;
                    if (lex == tknLpar)
                    {
                        first = new ColumnValue(getColumn());
                        ColumnValue* last = first;
                        nColumns = 1;
                        while (lex == tknComma)
                        {
                            last = last->next = new ColumnValue(getColumn());
                            nColumns += 1;
                        }
                        expected(tknRpar, ")");
                        lex = scan();
                    }
                    else
                    {
                        nColumns = table->columns->length;
                        for (int i = nColumns; --i >= 0;)
                        {
                            if (!table->columns->items[i]->isAutoGenerated()) {
                                ColumnValue* cv = new ColumnValue(table->columns->items[i]);
                                cv->next = first;
                                first = cv;
                            } else {
                                nColumns -= 1;
                            }
                        }
                    }
                    InsertNode* insertStmt = new InsertNode(table, first, nColumns);
                    if (replace) {
                        IndexDescriptor* index = table->findIndex(first->field, UNSPECIFIED_ORDER, IDX_UNIQUE_MATCH);
                        if (index != NULL) {
                            KeyDescriptor* key = index->_keys;
                            ColumnValue* cv = first;
                            int i = 0;
                            while ((key = key->next) != NULL) {
                                cv = cv->next;
                                i += 1;
                                if (cv == NULL || key->_field != cv->field) {
                                    throwCompileError(String::format("Field %s should be specified instead of %s at position %d of VALUES list as part of %s index key",
                                                                      key->_field->_name->cstr(), cv->field->_name->cstr(), i, index->_name->cstr()), pos);
                                }
                            }
                            insertStmt->replaceIndex = index;
                        } else {
                            throwCompileError(String::format("No unique index on field %s", table->columns->items[0]->_name->cstr()), pos);
                        }
                    }
                    stmt = insertStmt;
                    if (lex == tknValues)
                    {
                        context = CTX_INSERT;
                        stmt = select = insertStmt;
                        expect(tknLpar, "(");
                        p = pos;
                        ColumnValue* column = first;
                        do
                        {
                            if (column == NULL)
                            {
                                throwCompileError("Too many values", p);
                            }
                            p = pos;
                            column->value = disjunction();
                            if (column->value->type == tpArray) { 
                                if ((column->field->_type != tpArray && column == first) || insertStmt->bulkInsert) { 
                                    if (column->value->getElemType() != tpNull && fieldValueType[column->value->getElemType()] != fieldValueType[column->field->_type]) { 
                                        throwCompileError(String::format(
                                                              "Column %s has type %s but is assigned value of type %s", column->field->_name->cstr(), typeMnemonic[column->field->_type], typeMnemonic[column->value->getElemType()]), p);
                                    }
                                    insertStmt->bulkInsert = true;
                                } 
                            } else if (insertStmt->bulkInsert) { 
                                throwCompileError(String::format("Incorrect use of bulk insert: column %s should have array type", 
                                                                 column->field->_name)->cstr(), p);
                            }                                         
                            column = column->next;
                        }
                        while (lex == tknComma);

                        expected(tknRpar, ")");
                        if (column != NULL)
                        {
                            throwCompileError("Too few values", p);
                        }
                        lex = scan();
                        ExtraValue** evp = &insertStmt->moreValues;
                        while (lex == tknLpar) {
                            p = pos;
                            column = first;
                            do
                            {
                                if (column == NULL)
                                {
                                    throwCompileError("Too many values", p);
                                }
                                p = pos;
                                ExtraValue* ev = new ExtraValue();
                                ev->value = disjunction();
                                *evp = ev;
                                evp = &ev->next;
                                column = column->next;
                            }
                            while (lex == tknComma);

                            expected(tknRpar, ")");
                            if (column != NULL)
                            {
                                throwCompileError("Too few values", p);
                            }
                            lex = scan();
                        }
                        *evp = NULL;
                    }
                    else if (lex == tknSelect)
                    {
                        p = pos;
                        QueryNode* selectStmt = selectStatement(0);
                        if (selectStmt->resultColumns->length != nColumns)
                        {
                            throwCompileError("Number of selected columns doesn't match with number of inserted columns", p);
                        }
                        ColumnValue* column = first;
                        for (int i = 0; i < nColumns; i++)
                        {
                            if (selectStmt->resultColumns->items[i]->type != fieldValueType[column->field->_type]
                                && selectStmt->resultColumns->items[i]->type != tpNull
                                && column->field->_type != tpSequence)
                            {
                                throwCompileError(String::format(
                                                   "Column %d of selected data source has type %s and in inserted table - %s", i + 1, typeMnemonic[selectStmt->resultColumns->items[i]->type], typeMnemonic[column->field->_type]), p);
                            }
                            column = column->next;
                        }
                        insertStmt->select = selectStmt;
                    }
                    else if (lex == tknStruct || lex == tknStructFS)
                    {
                        insertStmt->structPtr = structPtr;
                        insertStmt->fixedSizeStrings = (lex == tknStructFS);
                        lex = scan();
                    }
                    else
                    {
                        throwCompileError("VALUES or SELECT expected", pos - 1);
                    }
                    break;
                }
            case tknUpdate:
                {
                    table = getTable();
                    context = CTX_WHERE;
                    expected(tknSet, "SET");
                    UpdateNode* updateStmt = new UpdateNode(table);
                    stmt = select = updateStmt;
                    ColumnValue** cpp = &updateStmt->values;
                    do
                    {
                        ColumnValue* column = new ColumnValue(getColumn());
                        expected(tknEq, "=");
                        p = pos;
                        column->value = disjunction();
                        *cpp = column;
                        cpp = &column->next;
                    }
                    while (lex == tknComma);
                    *cpp = NULL;

                    if (lex == tknWhere)
                    {
                        updateStmt->condition = condition();
                    }
                    break;
                }
            case tknDrop:
                p = pos;
                switch (scan())
                {
                  case tknTable:
                    stmt = new DropTableNode(getTable());
                    break;

                  case tknIndex:
                    stmt = new DropIndexNode(compoundName("index name"));
                    break;

                  case tknView:
                  {
                      expect(tknIdent, "View name");
                      stmt = new DropViewNode(ident);
                      lex = scan();
                      break;
                  }
                  case tknFunction:
                  {
                      expect(tknFunc, "Function name");
                      stmt = new DropFunctionNode(func->name);
                      lex = scan();
                      break;
                  }
                  default:
                    throwCompileError("DROP [TABLE or INDEX] expected", p);
                }
                break;

            default:
                throwCompileError("Invalid SQL statement", 0);
        }
        if (lex != tknEof)
        {
            throwCompileError("SQL syntax error", pos - 1);
        }
        return stmt;
    }

    void Compiler::addConstraint(Constraint::ConstraintType type, Vector < Field > * fields, String* name)
    {
        table->_constraints = new ConstraintDescriptor(table, name, type, fields, table->_constraints, false);
    }

    void Compiler::addForeignKeyConstraint(Vector < Field > * fk, String* name)
    {
        expect(tknIdent, "referenced table name");
        String* buf[MAX_KEYS];
        String* pkTable = ident;
        Vector < String > * pk;
        bool odc = false;

        if (lex == tknLpar)
        {
            int p = pos;
            lex = scan();
            if (lex == tknIdent)
            {
                int i;
                buf[0] = ident;
                for (i = 1; (lex = scan()) == tknComma; i++)
                {
                    expect(tknIdent, "key name");
                    if (i == MAX_KEYS)
                    {
                        throwCompileError("Too many keys", p);
                    }
                    buf[i] = ident;
                    p = pos;
                }
                pk = Vector < String > ::create(i, buf);
            }
            else
            {
                throwCompileError("Column name expected ", p);
            }
            if (lex != tknRpar)
            {
                throwCompileError("')' expected ", p);
            }
            lex = scan();
        }
        else
        {
            pk = Vector < String > ::create(fk->length);
            for (int i = 0; i < fk->length; i++)
            {
                pk->items[i] = fk->items[i]->name();
            }
        }
        if (lex == tknOn)
        {
            expect(tknDelete, "DELETE");
            expect(tknCascade, "CASCADE");
            lex = scan();
            odc = true;
        }
        table->_constraints = new ConstraintDescriptor(table, name, Constraint::FOREIGN_KEY, fk, table->_constraints, odc,
                                                       pkTable, pk);
    }

    void Compiler::setColumnType(FieldDescriptor* fd)
    {
        Type type;
        int p = pos;
        switch (scan())
        {
            case tknBit:
            case tknBoolean:
                type = tpBool;
                lex = scan();
                break;
            case tknChar:
            case tknVarchar:
            case tknLongvarchar:
            case tknString:
                type = tpString;
                if ((lex = scan()) == tknLpar)
                {
                    // skip fields length
                    expect(tknIconst, "field width");
                    fd->_fixedSize = (int)((IntValue*)value)->val;
                    expect(tknRpar, ")");
                    lex = scan();
                }
                break;
            case tknUnicode:
                type = tpUnicode;
                lex = scan();
                if (lex == tknChar || lex == tknVarchar || lex == tknLongvarchar)
                {
                    lex = scan();
                }
                if (lex == tknLpar)
                {
                    // skip fields length
                    expect(tknIconst, "field width");
                    fd->_fixedSize = (int)((IntValue*)value)->val;
                    expect(tknRpar, ")");
                    lex = scan();
                }
                break;
            case tknTinyint:
                type = tpInt1;
                lex = scan();
                break;
            case tknSmallint:
                type = tpInt2;
                lex = scan();
                break;
            case tknInteger:
            case tknInt:
                if ((lex = scan()) == tknLpar)
                {
                    expect(tknIconst, "field width");
                    switch ((int)((IntValue*)value)->val)
                    {
                    case 1:
                        type = tpInt1;
                        break;
                    case 2:
                        type = tpInt2;
                        break;
                    case 4:
                        type = tpInt4;
                        break;
                    case 8:
                        type = tpInt8;
                        break;
                    default:
                        throwCompileError("Invalid precision", p);
                    }
                    expect(tknRpar, ")");
                    lex = scan();
                }
                else
                {
                    type = tpInt4;
                }
                break;
            case tknUnsigned:
                if ((lex = scan()) == tknLpar)
                {
                    expect(tknIconst, "field width");
                    switch ((int)((IntValue*)value)->val)
                    {
                    case 1:
                        type = tpUInt1;
                        break;
                    case 2:
                        type = tpUInt2;
                        break;
                    case 4:
                        type = tpUInt4;
                        break;
                    case 8:
                        type = tpUInt8;
                        break;
                    default:
                        throwCompileError("Invalid precision", p);
                    }
                    expect(tknRpar, ")");
                    lex = scan();
                }
                else
                {
                    type = tpUInt4;
                }
                break;
            case tknBigint:
                type = tpInt8;
                lex = scan();
                break;
            case tknFloat:
                type = tpReal4;
                lex = scan();
                break;
            case tknReal:
            case tknDouble:
                type = tpReal8;
                lex = scan();
                break;
            case tknNumeric:
            case tknDecimal:
                type = tpNumeric;
                if ((lex = scan()) == tknLpar)
                {
                    // skip fields length
                    p = pos;
                    expect(tknIconst, "field width");
                    fd->_width = (int)((IntValue*)value)->val;
                    if ((lex = scan()) == tknComma)
                    {
                        expect(tknIconst, "field precision");
                        fd->_precision = (int)((IntValue*)value)->val;
                        lex = scan();
                    }
                    if (fd->_width > 19) {
                        throwCompileError("Decimal field width should not be larger than 19", p);
                    }
                    if (fd->_width < fd->_precision) {
                        throwCompileError("Decimal precision should not exceed width", p);
                    }
                    expected(tknRpar, ")");
                    lex = scan();
                }
                break;
            case tknDate:
            case tknTime:
            case tknTimestamp:
                type = tpDateTime;
                lex = scan();
                break;
            case tknBinary:
            case tknVarbinary:
            case tknLongvarbinary:
                type = tpArray;
                if ((lex = scan()) == tknLpar)
                {
                    // skip fields length
                    expect(tknIconst, "field width");
                    fd->_fixedSize = (int)((IntValue*)value)->val;
                    expect(tknRpar, ")");
                    lex = scan();
                }
                break;
            case tknReference:
                type = tpReference;
                if ((lex = scan()) == tknTo)
                {
                    expect(tknIdent, "referenced table name");
                    fd->_referencedTableName = ident;
                    lex = scan();
                }
                break;
            case tknBlob:
                type = tpBlob;
                lex = scan();
                break;
            case tknArray:
                type = tpArray;
                expect(tknLpar, "(");
                p = pos;
                setColumnType(fd);
                if (fd->_fixedSize != 0) { 
                    fd->_components = new FieldDescriptor(table, NULL, fd->_type, tpNull);
                    fd->_components->_fixedSize = fd->_fixedSize;
                    fd->_fixedSize = 0;
                }
                if (lex == tknComma) {
                    expect(tknIconst, "array length");
                    fd->_fixedSize = (int)value->intValue();
                    lex = scan();
                }
                expected(tknRpar, ")");
                lex = scan();
                fd->_elemType = fd->_type;
                break;
            case tknSequence:
                type = tpSequence;
                expect(tknLpar, "(");
                p = pos;
                setColumnType(fd);
                if (lex == tknAsc) {
                    fd->_order = ASCENT_ORDER;
                    lex = scan();
                } else if (lex == tknDesc) {
                    fd->_order = DESCENT_ORDER;
                    lex = scan();
                }
                expected(tknRpar, ")");
                if (fd->_type != tpString && (fd->_type < tpBool || fd->_type > tpDateTime) ) {
                    throwCompileError("scalar type int1-8, uint1-8, float, double, date, time or string expected", p);
                }
                lex = scan();
                fd->_elemType = fd->_type;
                fd->_elemSize = fd->_fixedSize;
                break;
            case tknIdent:
                {
                    FieldDescriptor* domain = (FieldDescriptor*)engine->compilerCtx.domainHash->get(ident->cstr());
                    if (domain != NULL)
                    {
                        type = domain->_type;
                        fd->_referencedTableName = domain->_referencedTableName;
                        lex = scan();
                        break;
                    }
                    // no break
                }
            default:
                throwCompileError("column type expected", p);
        }
        fd->_type = type;
        Field* key[1];
        key[0] = fd;
        while (true)
        {
            switch (lex)
            {
                case tknPrimary:
                    expect(tknKey, "KEY");
                    addConstraint(Constraint::PRIMARY_KEY, Vector < Field > ::create(1, key));
                    if ((lex = scan()) == tknIdent)
                    {
                        lex = scan();
                    }
                    break;
                case tknUnique:
                    addConstraint(Constraint::UNIQUE, Vector < Field > ::create(1, key));
                    lex = scan();
                    break;
                case tknUsing:
                    lex = scan();
                    if (lex == tknHash) {
                        expect(tknIndex, "INDEX");
                        addConstraint(Constraint::USING_HASH_INDEX, Vector < Field > ::create(1, key));
                    } else if (lex == tknRtree) {
                        expect(tknIndex, "INDEX");
                        addConstraint(Constraint::USING_RTREE_INDEX, Vector < Field > ::create(1, key));
                    } else if (lex == tknTrigram) {
                        expect(tknIndex, "INDEX");
                        addConstraint(Constraint::USING_TRIGRAM_INDEX, Vector < Field > ::create(1, key));
                    } else {
                        expected(tknIndex, "INDEX");
                        addConstraint(Constraint::USING_INDEX, Vector < Field > ::create(1, key));
                    }
                    lex = scan();
                    break;
                case tknForeign:
                    expect(tknKey, "KEY");
                    expect(tknReferences, "REFERENCES");
                    // no break
                case tknReferences:
                    addForeignKeyConstraint(Vector < Field > ::create(1, key));
                    break;
                case tknNot:
                    expect(tknNull, "NULL");
                    addConstraint(Constraint::NOT_NULL, Vector < Field > ::create(1, key));
                    lex = scan();
                    break;
                case tknNull:
                    fd->_nullable = true;
                    addConstraint(Constraint::MAY_BE_NULL, Vector < Field > ::create(1, key));
                    lex = scan();
                    break;
                default:
                    return ;
            }
        }
    }

    Vector < Field > * Compiler::getConstraintFields(FieldDescriptor* columns)
    {
        Field* fields[MAX_KEYS];
        expect(tknLpar, "(");
        int i = 0;
        do
        {
            int p = pos;
            expect(tknIdent, "column name");
            FieldDescriptor* fd;
            for (fd = columns; fd != NULL && !ident->equals(fd->name()); fd = fd->next);
            if (fd == NULL)  {
                throwCompileError(String::format("No such column %s in table %s", ident->cstr(), table->_name->cstr()), p);
            }
            if (i == MAX_KEYS)
            {
                throwCompileError("Too much constaints", p);
            }
            fields[i++] = fd;
            lex = scan();
        }
        while (lex == tknComma);

        if (lex != tknRpar)
        {
            throwCompileError("')' expected", pos - 1);
        }
        return Vector < Field > ::create(i, fields);
    }


    StmtNode* Compiler::createFunction()
    {
        lex = scan();
        if (lex == tknFunc) { 
            throwCompileError(String::format("Function %s is already defined", func->name)->cstr(), pos-1);
        } else {  
            expected(tknIdent, "Function name expected");
        }
        String* sqlFuncName = ident;
        String* cFuncName = ident;
        int nArgs = 0;
        FieldDescriptor* args[MAX_FUNC_VARGS];
        expect(tknLpar, "(");
        lex = scan();
        if (lex != tknRpar) { 
            while (true) {
                expected(tknIdent, "Column name");
                if (nArgs == MAX_FUNC_VARGS) { 
                    throwCompileError(String::format("Function %s has too many arguments (maximal %d supported)", sqlFuncName->cstr(),
                                                     MAX_FUNC_ARGS), pos - 1);
                }
                args[nArgs] = new FieldDescriptor(NULL, ident, tpNull, tpNull);
                setColumnType(args[nArgs++]);               
                if (lex == tknRpar) { 
                    break;
                }
                expected(tknComma, ",");
                lex = scan();
            }                
        }
        expect(tknReturns, "Returns");
        FieldDescriptor returns(NULL, ident, tpNull, tpNull);        
        setColumnType(&returns);  
        expected(tknAs, "AS");
        expect(tknSconst, "Library path");
        String* library = value->stringValue();
        lex = scan();
        if (lex == tknComma) { 
            expect(tknSconst, "C function name");
            cFuncName = value->stringValue();
            lex = scan();
        }          
        return new CreateFunctionNode(returns.type(), returns.elementType(), library, sqlFuncName, cFuncName, Vector<Field>::create(nArgs, (Field**)args), String::create(query), !engine->compilerCtx.loadingFunction);
    }
                

    StmtNode* Compiler::createTable(bool temporary)
    {
        expect(tknIdent, "table name");
        table = new TableDescriptor(engine->db, ident, temporary);
        expect(tknLpar, "(");
        FieldDescriptor* fields;
        FieldDescriptor* fp;
        FieldDescriptor** fpp = &fields;
        int nFields = 0;
        do
        {
            int p = pos;
            lex = scan();
            String* constraintName = NULL;
            if (lex == tknConstraint)
            {
                expect(tknIdent, "Constraint name");
                constraintName = ident;
                p = pos;
                lex = scan();
                if (lex == tknIdent)
                {
                    throwCompileError("Constraint specification expected", p);
                }
            }
            switch (lex)
            {
                case tknIdent:
                    fp = new FieldDescriptor(table, ident, tpNull, tpNull);
                    *fpp = fp;
                    fpp = &fp->next;
                    nFields += 1;
                    setColumnType(fp);
                    break;
                case tknPrimary:
                    expect(tknKey, "KEY");
                    addConstraint(Constraint::PRIMARY_KEY, getConstraintFields(fields), constraintName);
                    lex = scan();
                    break;
                case tknForeign:
                    {
                        expect(tknKey, "KEY");
                        Vector < Field > * fk = getConstraintFields(fields);
                        expect(tknReferences, "REFERENCES");
                        addForeignKeyConstraint(fk, constraintName);
                        break;
                    }
                case tknUnique:
                    addConstraint(Constraint::UNIQUE, getConstraintFields(fields), constraintName);
                    lex = scan();
                    break;
                default:
                    throwCompileError("Column name or constraint expected", p);
            }
        }
        while (lex == tknComma);

        table->columns = Vector < FieldDescriptor > ::create(nFields);
        fp = fields;
        fpp = table->columns->items;
        for (int i = 0; i < nFields; i++, fp = fp->next)
        {
            fpp[i] = fp;
        }

        expected(tknRpar, ")");
        do { lex = scan(); } while ( lex == tknOn || lex == tknCommit || lex == tknPreserve || lex == tknRows );
        return new CreateTableNode(table);
    }


    ExprNode* Compiler::disjunction()
    {
        ExprNode* left = conjunction();
        if (left->type == tpList) { 
            throwCompileError("List is not expected here: remove extra braces", pos-1);
        }
        if (lex == tknOr)
        {
            int p = pos;
            ExprNode* right = disjunction();
            if (left->type == tpSequence && right->type == tpSequence)
            {
                left = invokeSequenceBinaryFunction("seq_or", left, right);
            }
            else if (left->type == tpInt && right->type == tpInt)
            {
                left = new BinOpNode(tpInt, opIntOr, left, right);
            }
            else if (left->type == tpBool && right->type == tpBool)
            {
                left = new LogicalOpNode(opBoolOr, left, right);
            }
            else
            {
                throwCompileError("Bad operands for OR operator", p);
            }
        }
        return left;
    }

    ExprNode* Compiler::conjunction()
    {
        ExprNode* left = comparison();
        if (lex == tknAnd)
        {
            int p = pos;
            ExprNode* right = conjunction();
            if (left->type == tpSequence && right->type == tpSequence)
            {
                left = invokeSequenceBinaryFunction("seq_and", left, right);
            }
            else if (left->type == tpInt && right->type == tpInt)
            {
                left = new BinOpNode(tpInt, opIntAnd, left, right);
            }
            else if (left->type == tpBool && right->type == tpBool)
            {
                ExprNode* between;
                if ((between = replaceWithBetween(left, right)) != NULL || (between = replaceWithBetween(right, left))
                    != NULL)
                {
                    left = between;
                }
                else
                {
                    left = new LogicalOpNode(opBoolAnd, left, right);
                }
            }
            else
            {
                throwCompileError("Bad operands for AND operator", p);
            }
        }
        return left;
    }

    ExprNode* Compiler::replaceWithBetween(ExprNode* left, ExprNode* right)
    {
        if (left->isBinOpNode() && right->isBinOpNode() && ((BinOpNode*)left)->left->equalsNode(((BinOpNode*)right)
            ->left))
        {
            ExprCode cop;
            switch (left->tag)
            {
                case opGe:
                    switch (right->tag)
                    {
                    case opLt:
                        cop = opBetweenIE;
                        break;
                    case opLe:
                        cop = opBetweenII;
                        break;
                    default:
                        return NULL;
                    }
                    break;
                case opGt:
                    switch (right->tag)
                    {
                    case opLt:
                        cop = opBetweenEE;
                        break;
                    case opLe:
                        cop = opBetweenEI;
                        break;
                    default:
                        return NULL;
                    }
                    break;
                default:
                    return NULL;
            }
            return new BetweenNode(cop, ((BinOpNode*)left)->left, ((BinOpNode*)left)->right, ((BinOpNode*)right)->right)
                                   ;
        }
        return NULL;
    }

    ExprNode* Compiler::int2numeric(ExprNode* expr)
    {
        if (expr->tag == opIntConst)
        {
            return new LiteralNode(tpNumeric, opConst, new NumericValue(((IntValue*)((LiteralNode*)expr)->value)->val, 0));
        }
        return new UnaryOpNode(tpNumeric, opIntToNumeric, expr);
    }

    ExprNode* Compiler::int2real(ExprNode* expr)
    {
        #ifndef NO_FLOATING_POINT
            if (expr->tag == opIntConst)
            {
                return new LiteralNode(tpReal, opRealConst, new RealValue((double)((IntValue*)((LiteralNode*)expr)
                                       ->value)->val));
            }
        #endif
        return new UnaryOpNode(tpReal, opIntToReal, expr);
    }

    ExprNode* Compiler::int2ref(ExprNode* expr)
    {
        if (expr->tag == opIntConst)
        {
            return new LiteralNode(tpReference, opConst, engine->db->createReference(((IntValue*)((LiteralNode*)expr)
                                   ->value)->val));
        }
        return new UnaryOpNode(tpReference, opIntToRef, expr);
    }

    ExprNode* Compiler::int2str(ExprNode* expr)
    {
        if (expr->tag == opIntConst)
        {
            char buf[32];
            int64_t val = ((IntValue*)((LiteralNode*)expr)->value)->val;
            return new LiteralNode(tpString, opStrConst, String::create(int64ToString(val, buf, sizeof(buf))));
        }
        return new UnaryOpNode(tpString, opAnyToStr, expr);
    }


    ExprNode* Compiler::str2int(ExprNode* expr, int p)
    {
        if (expr->tag == opStrConst)
        {
            int64_t val;
            char* str = ((LiteralNode*)expr)->value->stringValue()->cstr();
            if (stringToInt64(str, val))
            {
                return new LiteralNode(tpInt, opIntConst, new IntValue(val));
            }
            else
            {
                throwCompileError("String can not be converted to integer constant", p);
            }
        }
        return new UnaryOpNode(tpInt, opStrToInt, expr);
    }

    ExprNode* Compiler::numeric2str(ExprNode* expr)
    {
        return new UnaryOpNode(tpString, opAnyToStr, expr);
    }

    ExprNode* Compiler::numeric2time(ExprNode* expr)
    {
        return new UnaryOpNode(tpDateTime, opNumericToTime, expr);
    }

    ExprNode* Compiler::numeric2real(ExprNode* expr)
    {
        return new UnaryOpNode(tpReal, opNumericToReal, expr);
    }

    ExprNode* Compiler::numeric2int(ExprNode* expr)
    {
        return new UnaryOpNode(tpInt, opNumericToInt, expr);
    }

    ExprNode* Compiler::str2numeric(ExprNode* expr)
    {
        if (expr->tag == opStrConst)
        {
            char* str = ((LiteralNode*)expr)->value->stringValue()->cstr();
            return new LiteralNode(tpNumeric, opConst, new NumericValue(str));
        }
        return new UnaryOpNode(tpNumeric, opStrToNumeric, expr);
    }

    ExprNode* Compiler::real2str(ExprNode* expr)
    {
        #ifndef NO_FLOATING_POINT
            if (expr->tag == opRealConst)
            {
                return new LiteralNode(tpString, opStrConst, ((LiteralNode*)expr)->value->stringValue());
            }
        #endif
        return new UnaryOpNode(tpString, opAnyToStr, expr);
    }

    ExprNode* Compiler::str2real(ExprNode* expr, int p)
    {
        if (expr->tag == opStrConst)
        {
            #ifndef NO_FLOATING_POINT
            double val;
            int n;
            char* str = ((LiteralNode*)expr)->value->stringValue()->cstr();
            if (sscanf(str, "%lf%n", &val, &n) == 1 && str[n] == '\0')
            {
                return new LiteralNode(tpReal, opRealConst, new RealValue(val));
            }
            else
            #endif
            {
                throwCompileError("String can not be converted to real constant", p);
            }
        }
        return new UnaryOpNode(tpReal, opStrToReal, expr);
    }

    ExprNode* Compiler::int2time(ExprNode* expr)
    {
        if (expr->tag == opIntConst)
        {
            return new LiteralNode(tpDateTime, opConst, new DateTime(((IntValue*)((LiteralNode*)expr)->value)->val));
        }
        return new UnaryOpNode(tpDateTime, opIntToTime, expr);
    }

    ExprNode* Compiler::str2time(ExprNode* expr, int p)
    {
        if (expr->tag == opStrConst)
        {
            time_t val;
            char* str = ((LiteralNode*)expr)->value->stringValue()->cstr();
            if (stringToTime(str, val))
            {
                return new LiteralNode(tpDateTime, opConst, new DateTime(val));
            }
            else
            {
                throwCompileError("String can not be converted to timestamp", p);
            }
        }
        else
        {
            return new UnaryOpNode(tpDateTime, opStrToTime, expr);
        }
    }

    ExprNode* Compiler::mbs2wcs(ExprNode* expr)
    {
        #ifdef UNICODE_SUPPORT
            if (expr->tag == opStrConst)
            {
                return new LiteralNode(tpUnicode, opUnicodeStrConst, UnicodeString::create((String*)((LiteralNode*)expr)
                                       ->value));
            }
        #endif
        return new UnaryOpNode(tpUnicode, opMbsToWcs, expr);
    }

    ExprNode* Compiler::wcs2mbs(ExprNode* expr)
    {
        #ifdef UNICODE_SUPPORT
            if (expr->tag == opUnicodeStrConst)
            {
                return new LiteralNode(tpString, opStrConst, ((UnicodeString*)((LiteralNode*)expr)->value)->stringValue
                                       ());
            }
        #endif
        return new UnaryOpNode(tpString, opWcsToMbs, expr);
    }

    int Compiler::getConstantListLength(BinOpNode* list, Type selectorType)
    {
        int n = 0;
        do
        {
            if (!list->left->isLiteralNode() || list->left->type != selectorType)
            {
                return  - 1;
            }
            n += 1;
        }
        while ((list = (BinOpNode*)list->right) != NULL);
        return n;
    }

    void Compiler::extractListItems(BinOpNode* list, Vector < Value > * v)
    {
        int n = 0;
        do
        {
            v->items[n++] = ((LiteralNode*)list->left)->value;
        }
        while ((list = (BinOpNode*)list->right) != NULL);
    }

    static int compareListItems(void* a, void* b, void*)
    {
        return ((Value*)a)->compare((Value*)b);
    }

    ExprNode* Compiler::compare(ExprNode* selector, BinOpNode* list)
    {
        if (!isComparableType(selector->type))
        {
            throwCompileError("Left operand of IN has invalid type", pos);
        }
        assert(list != NULL);
        int n = getConstantListLength(list, selector->type);
        if (n >= 1)
        {
            Vector<Value>* v = Vector<Value>::create(n);
            int i, j;
            extractListItems(list, v);
            iqsort(v->items, n, &compareListItems, NULL);
            for (i = 0, j = 1; j < n; j++) {
                if (v->items[i]->compare(v->items[j]) != 0) {
                    v->items[++i] = v->items[j];
                }
            }
            if (++i != n) {
                Vector<Value>* uniq = Vector<Value>::create(i);
                uniq->copy(v);
                v = uniq;
            }
            return new BinarySearchNode(selector, v);
        }
        return compare(selector, list, 0);
    }

    ExprNode* Compiler::compare(ExprNode* selector, BinOpNode* list, int n)
    {
        ExprNode* expr = selector;
        ExprNode* elem = list->left;
        BinOpNode* tail = (BinOpNode*)list->right;
        n += 1;
        if (expr->type != elem->type)
        {
            if (expr->type == tpInt)
            {
                if (elem->type == tpReal)
                {
                    expr = int2real(expr);
                }
                else if (elem->type == tpReference)
                {
                    expr = int2ref(expr);
                }
                else if (elem->type == tpNumeric)
                {
                    expr = int2numeric(expr);
                }
                else if (isStringType(elem->type))
                {
                    expr = int2str(expr);
                }
            }
            if (expr->type == tpNumeric)
            {
                if (elem->type == tpInt)
                {
                    elem = int2numeric(elem);
                }
                else if (isStringType(elem->type))
                {
                    expr = numeric2str(expr);
                }
            }
            else if (expr->type == tpNull)
            {
                if (elem->type == tpReference)
                {
                    expr->type = tpReference;
                }
            }
            else if (expr->type == tpReal)
            {
                if (elem->type == tpInt)
                {
                    elem = int2real(elem);
                }
                else if (isStringType(elem->type))
                {
                    expr = real2str(expr);
                }
            }
            else if (expr->type == tpString)
            {
                if (elem->type == tpInt)
                {
                    elem = int2str(elem);
                }
                if (elem->type == tpNumeric)
                {
                    elem = numeric2str(elem);
                }
                else if (elem->type == tpReal)
                {
                    elem = real2str(elem);
                }
                else if (elem->type == tpUnicode)
                {
                    expr = mbs2wcs(expr);
                }
            }
            else if (elem->type == tpString && expr->type == tpUnicode)
            {
                elem = mbs2wcs(elem);
            }
            else if (expr->type == tpReference)
            {
                if (elem->type == tpInt)
                {
                    elem = int2ref(elem);
                }
            }
            else if (expr->type == tpNull)
            {
                if (elem->type == tpReference)
                {
                    expr->type = tpReference;
                }
            }
            if (expr->type != elem->type)
            {
                throwCompileError(String::format("Expression %d in right part of IN operator has incompatible type", n)
                                   , pos);
            }
        }
        if (tail != NULL)
        {
            return new LogicalOpNode(opBoolOr, new CompareNode(opEq, expr, elem), compare(selector, tail, n));
        }
        else
        {
            return new CompareNode(opEq, expr, elem);
        }
    }


    ExprNode* Compiler::convertToString(ExprNode* node, int p)
    {
        switch (node->type)
        {
            case tpInt:
                return int2str(node);
            case tpNumeric:
                return numeric2str(node);
            case tpReal:
                return real2str(node);
            case tpString:
                return node;
            case tpUnicode:
                return wcs2mbs(node);
            case tpBool:
            case tpDateTime:
                return new UnaryOpNode(tpString, opAnyToStr, node);
            default:
                throwCompileError("Operand of numeric or string type expected", p);
        }
    }

    ExprNode* Compiler::convertToUnicodeString(ExprNode* node, int p)
    {
        switch (node->type)
        {
            case tpNumeric:
                return mbs2wcs(numeric2str(node));
            case tpInt:
                return mbs2wcs(int2str(node));
            case tpReal:
                return mbs2wcs(real2str(node));
            case tpString:
                return mbs2wcs(node);
            case tpUnicode:
                return node;
            case tpBool:
            case tpDateTime:
                return new UnaryOpNode(tpUnicode, opMbsToWcs, new UnaryOpNode(tpString, opAnyToStr, node));
            default:
                throwCompileError("Operand of numeric or string type expected", p);
        }
    }

    ExprNode* Compiler::convertToReal(ExprNode* node, int p)
    {
        switch (node->type)
        {
            case tpInt:
                return int2real(node);
            case tpNumeric:
                return numeric2real(node);
            case tpReal:
                return node;
            case tpString:
                return str2real(node, p);
            default:
                throwCompileError("Operand of numeric or string type expected", p);
        }
    }

    ExprNode* Compiler::convertToInt(ExprNode* node, int p)
    {
        switch (node->type)
        {
            case tpInt:
                return node;
            case tpNumeric:
                return numeric2int(node);
            case tpString:
                return str2int(node, p);
            default:
                throwCompileError("Operand of numeric or string type expected", p);
        }
    }

    ExprNode* Compiler::convertToNumeric(ExprNode* node, int p)
    {
        switch (node->type)
        {
            case tpNumeric:
                return node;
            case tpInt:
                return int2numeric(node);
            case tpString:
                return str2numeric(node);
            default:
                throwCompileError("Operand of numeric or string type expected", p);
        }
    }

    ExprNode* Compiler::convertToRef(ExprNode* node, int p)
    {
        switch (node->type)
        {
            case tpNull:
                node->type = tpReference;
                // no break
            case tpReference:
                return node;
            case tpInt:
                return int2ref(node);
            default:
                throwCompileError("Operand of integer or reference type expected", p);
        }
    }

    ExprNode* Compiler::convertToTime(ExprNode* node, int p)
    {
        switch (node->type)
        {
            case tpInt:
                return int2time(node);
            case tpNumeric:
                return numeric2time(node);
            case tpDateTime:
                return node;
            case tpString:
                return str2time(node, p);
            default:
                throwCompileError("Operand of integer or string type expected", p);
        }
    }

    ExprNode* Compiler::parseArray(ExprNode* expr, char* p, int pos)
    {
        FieldDescriptor* fld = expr->getField();
        if (fld == NULL) {
            throwCompileError("Failed to parse array because element type is not known", pos);
        }
        int errPos;
        Value* arr = parseStringAsArray(fld->_elemType, p, errPos);
        if (arr == NULL) {
            if (errPos < 0) {
                throwCompileError("Unsupported array type", pos);
            } else {
                throwCompileError("Failed to parse array", pos + errPos);
            }
        }
        return new LiteralNode(tpArray, opConst, arr);
    }

    ExprNode* Compiler::comparison()
    {
        int leftPos = pos;
        ExprNode* left;
        ExprNode* right;
        left = addition();

        Token tkn = lex;
        if (tkn == tknEq || tkn == tknNe || tkn == tknGt || tkn == tknGe || tkn == tknLe || tkn == tknLt || tkn == tknBetween
            || tkn == tknLike || tkn == tknILike || tkn == tknNot || tkn == tknIs || tkn == tknIn || tkn == tknExactMatch || tkn == tknPrefixMatch || tkn == tknOverlaps || tkn == tknContains || tkn == tknNear)
        {
            int rightPos = pos;
            bool inverse = false;
            if (tkn == tknNot)
            {
                inverse = true;
                tkn = scan();
                if (tkn != tknLike && tkn != tknILike && tkn != tknBetween && tkn != tknIn)
                {
                    throwCompileError("LIKE, BETWEEN or IN expected", rightPos);
                }
                rightPos = pos;
            }
            else if (tkn == tknIs)
            {
                bool isNot = false;
                if ((tkn = scan()) == tknNot)
                {
                    isNot = true;
                    tkn = scan();
                }
                switch (tkn)
                {
                    case tknNull:
                        left = new UnaryOpNode(tpBool, opIsNull, left);
                        break;
                    case tknTrue:
                        left = new UnaryOpNode(tpBool, opIsTrue, left);
                        break;
                    case tknFalse:
                        left = new UnaryOpNode(tpBool, opIsFalse, left);
                        break;
                    default:
                        throwCompileError("TRUE, FALSE or NULL expected", rightPos);
                }
                if (isNot)
                {
                    left = new UnaryOpNode(tpBool, opBoolNot, left);
                }
                lex = scan();
                return left;
            } else if (tkn == tknContains) {
                tkn = scan(true);
                if (tkn == tknAll) {
                    tkn = tknContainsAll;
                } else if (tkn == tknAny) {
                    tkn = tknContainsAny;
                } else {
                    // unget
                    tkn = tknContains;
                    pos = rightPos;
                }
            }
            right = addition();
            if (tkn == tknIn)
            {
                if (right->isQueryNode())
                {
                    if (left->type != right->type)
                    {
                        throwCompileError("Incompatible types of IN operator operands", rightPos);
                    }
                    if (!isComparableType(right->type))
                    {
                        throwCompileError("Invalid operand type for IN operator", rightPos);
                    }
                    left = new ScanSetNode(left, right);
                }
                else
                {
                    switch (right->type)
                    {
                        case tpArray:
                            {
                                FieldDescriptor* fd = right->getField();
                                if (fd != NULL) {
                                    assert(fd->_type == tpArray);
                                    if (left->type != fieldValueType[fd->_components->_type])
                                    {
                                        switch (fd->_components->_type)
                                        {
                                          case tpReal4:
                                          case tpReal8:
                                            if (left->type == tpInt)
                                            {
                                                left = int2real(left);
                                            }
                                            break;
                                          case tpString:
                                            if (left->type == tpInt)
                                            {
                                                left = int2str(left);
                                            }
                                            else if (left->type == tpReal)
                                            {
                                                left = real2str(left);
                                            }
                                            else if (left->type == tpNumeric)
                                            {
                                                left = numeric2str(left);
                                            }
                                            else if (left->type == tpUnicode)
                                            {
                                                left = wcs2mbs(left);
                                            }
                                            break;
                                          case tpUnicode:
                                            if (left->type == tpInt)
                                            {
                                                left = mbs2wcs(int2str(left));
                                            }
                                            else if (left->type == tpReal)
                                            {
                                                left = mbs2wcs(real2str(left));
                                            }
                                            else if (left->type == tpString)
                                            {
                                                left = mbs2wcs(left);
                                            }
                                            break;
                                          default:
                                            ;
                                        }
                                        if (left->type != fieldValueType[fd->_components->_type])
                                        {
                                            throwCompileError("Incompatible types of IN operator operands", rightPos);
                                        }
                                    }
                                }
                                left = new BinOpNode(tpBool, opScanArray, left, right);
                                break;
                            }
                        case tpString:
                            if (left->type == tpUnicode)
                            {
                                left = new BinOpNode(tpBool, opInUnicodeString, left, mbs2wcs(right));
                            }
                            else if (left->type == tpString)
                            {
                                left = new BinOpNode(tpBool, opInString, left, right);
                            }
                            else
                            {
                                throwCompileError("Left operand of IN expression hasn't string type", leftPos);
                            }
                            break;
                        case tpUnicode:
                            if (left->type == tpUnicode)
                            {
                                left = new BinOpNode(tpBool, opInUnicodeString, left, right);
                            }
                            else if (left->type == tpString)
                            {
                                left = new BinOpNode(tpBool, opInUnicodeString, mbs2wcs(left), right);
                            }
                            else
                            {
                                throwCompileError("Left operand of IN expression hasn't string type", leftPos);
                            }
                            break;
                        case tpList:
                            left = compare(left, (BinOpNode*)right);
                            break;
                        default:
                            throwCompileError("List of expressions or array expected", rightPos);
                    }
                }
            }
            else if (tkn == tknBetween)
            {
                int andPos = pos;
                expected(tknAnd, "AND");
                ExprNode* right2 = addition();
                if (left->type == tpDateTime || right->type == tpDateTime || right2->type == tpDateTime)
                {
                    left = convertToTime(left, leftPos);
                    right = convertToTime(right, rightPos);
                    right2 = convertToTime(right2, andPos);
                }
                else if (left->type == tpReal || right->type == tpReal || right2->type == tpReal)
                {
                    left = convertToReal(left, leftPos);
                    right = convertToReal(right, rightPos);
                    right2 = convertToReal(right2, andPos);
                }
                else if (left->type == tpNumeric || right->type == tpNumeric || right2->type == tpNumeric)
                {
                    left = convertToNumeric(left, leftPos);
                    right = convertToNumeric(right, rightPos);
                    right2 = convertToNumeric(right2, andPos);
                }
                else if (left->type == tpInt || right->type == tpInt || right2->type == tpInt)
                {
                    left = convertToInt(left, leftPos);
                    right = convertToInt(right, rightPos);
                    right2 = convertToInt(right2, andPos);
                }
                else if (left->type == tpUnicode || right->type == tpUnicode || right2->type == tpUnicode)
                {
                    left = convertToUnicodeString(left, leftPos);
                    right = convertToUnicodeString(right, rightPos);
                    right2 = convertToUnicodeString(right2, andPos);
                }
                else
                {
                    left = convertToString(left, leftPos);
                    right = convertToString(right, rightPos);
                    right2 = convertToString(right2, andPos);
                }
                left = new BetweenNode(opBetweenII, left, right, right2);
            }
            else if (tkn == tknExactMatch || tkn == tknPrefixMatch)
            {
                if (left->type == tpArray)
                {
                    Type elemType = left->getElemType();
                    if (elemType != tpBool)
                    {
                        throwCompileError("Left operand of MATCH operator should be of integer, string or array of boolean type", rightPos);
                    }
                    if (right->type != tpInt)
                    {
                        throwCompileError("Incompatible type of MATCH operator operands", rightPos);
                    }
                }
                else if (left->type != right->type)
                {
                    throwCompileError("Incompatible type of MATCH operator operands", rightPos);
                }
                if (right->type == tpInt)
                {
                    expected(tknCount, "COUNT");
                    if (left->type != tpArray && tkn == tknPrefixMatch)
                    {
                        throwCompileError("Prefix match can be done only for strings and array of bit", rightPos);
                    }
                    ExprNode* count = addition();
                    if (count->type != tpInt)
                    {
                        throwCompileError("Bit counter in MATCH operator should have the integer type", rightPos);
                    }
                    if (left->type == tpArray)
                    {
                        left = new TripleOpNode(tpBool, (ExprCode)((int)tkn - (int)tknExactMatch + (int)opExactMatchAII)
                                                , left, right, count);
                    }
                    else
                    {
                        left = new TripleOpNode(tpBool, opExactMatchIII, left, right, count);
                    }
                }
                else if (left->type == tpString)
                {
                    left = new TripleOpNode(tpBool, (ExprCode)((int)tkn - (int)tknExactMatch + (int)opExactMatchSS),
                                            left, right);
                }
                else if (left->type == tpUnicode)
                {
                    left = new TripleOpNode(tpBool, (ExprCode)((int)tkn - (int)tknExactMatch + (int)opExactMatchUU),
                                            left, right);
                }
                else
                {
                    throwCompileError("Operands of MATCH operator should have integer or string type", rightPos);
                }
            }
            else if (tkn == tknLike || tkn == tknILike)
            {
                if (left->type == tpUnicode || right->type == tpUnicode)
                {
                    left = convertToUnicodeString(left, leftPos);
                    right = convertToUnicodeString(right, rightPos);
                }
                else
                {
                    left = convertToString(left, leftPos);
                    right = convertToString(right, rightPos);
                }
                if (lex == tknEscape)
                {
                    rightPos = pos;
                    if (scan() != tknSconst)
                    {
                        throwCompileError("String literal espected after ESCAPE", rightPos);
                    }
                    left = new TripleOpNode(tpBool, (ExprCode)((int)(left->type == tpString ? opStrLikeEsc : opUnicodeStrLikeEsc) + (int)tkn - (int)tknLike),
                                            left, right, new LiteralNode(tpString, opStrConst, value));
                    lex = scan();
                }
                else
                {
                    if (right->tag == opStrConst && tkn == tknLike)
                    {
                        String* str = (String*)((LiteralNode*)right)->value;
                        int i = findWildcard(str, 0, '\'');
                        if (i < 0)
                        {
                            left = new CompareNode(opEq, left, right);
                        }
                        else if (i == 0 && str->body()[0] == '%' && (i = findWildcard(str, 1, '\'')) == str->size() - 1
                                 && str->body()[i] == '%')
                        {
                            left = new BinOpNode(tpBool, opInString, new LiteralNode(tpString, opStrConst, str->substr
                                                 (1, str->size() - 2)), left);
                        }
                        else
                        {
                            left = new TripleOpNode(tpBool, opStrLike, left, right, NULL);
                        }
                        #ifdef UNICODE_SUPPORT
                        }
                        else if (right->tag == opUnicodeStrConst)
                        {
                            UnicodeString* str = (UnicodeString*)((LiteralNode*)right)->value;
                            int i = findWildcard(str, 0, '\'');
                            if (i < 0)
                            {
                                left = new CompareNode(opEq, left, right);
                            }
                            else if (i == 0 && str->body()[0] == '%' && (i = findWildcard(str, 1, '\'')) == str->size()
                                     - 1 && str->body()[i] == '%')
                            {
                                left = new BinOpNode(tpBool, opInUnicodeString, new LiteralNode(tpUnicode,
                                                     opUnicodeStrConst, str->substr(1, str->size() - 2)), left);
                            }
                            else
                            {
                                left = new TripleOpNode(tpBool, opUnicodeStrLike, left, right, NULL);
                            }
                        #endif
                    }
                    else
                    {
                        left = new TripleOpNode(tpBool, (ExprCode)((int)(left->type == tpString ? opStrLike : opUnicodeStrLike) + (int)tkn - (int)tknLike),
                                                left, right, NULL);
                    }
                }
            }
            else
            {
                if (left->tag == opStrConst)
                {
                    char* str = ((LiteralNode*)left)->value->stringValue()->cstr();
                    if (right->type == tpInt) {
                        int64_t val;
                        if (stringToInt64(str, val)) {
                            left = new LiteralNode(tpInt, opIntConst, new IntValue(val));
                        }
                    }
                    #ifndef NO_FLOATING_POINT
                    else if (right->type == tpReal) {
                        double val;
                        int n;
                        if (sscanf(str, "%lf%n", &val, &n) == 1 && str[n] == '\0')
                        {
                            left = new LiteralNode(tpReal, opRealConst, new RealValue(val));
                        }
                    }
                    #endif
                    else if (right->type == tpNumeric)
                    {
                        NumericValue* val = new NumericValue((int64_t)0, 0);
                        if (val->parse(str)) {
                            left = new LiteralNode(tpNumeric, opConst, val);
                        }
                    }
                    else if (right->type == tpDateTime)
                    {
                        time_t val;
                        if (stringToTime(str, val)) {
                            left = new LiteralNode(tpDateTime, opConst, new DateTime(val));
                        }
                    }
                    else if (right->type == tpArray)
                    {
                        left = parseArray(right, str, leftPos);
                    }
                 }
                else if (right->tag == opStrConst)
                {
                    char* str = ((LiteralNode*)right)->value->stringValue()->cstr();
                    if (left->type == tpInt) {
                        int64_t val;
                        if (stringToInt64(str, val)) {
                            right = new LiteralNode(tpInt, opIntConst, new IntValue(val));
                        }
                    }
                    #ifndef NO_FLOATING_POINT
                    else if (left->type == tpReal) {
                        double val;
                        int n;
                        if (sscanf(str, "%lf%n", &val, &n) == 1 && str[n] == '\0')
                        {
                            right = new LiteralNode(tpReal, opRealConst, new RealValue(val));
                        }
                    }
                    #endif
                    else if (left->type == tpNumeric)
                    {
                        NumericValue* val = new NumericValue((int64_t)0, 0);
                        if (val->parse(str)) {
                            right = new LiteralNode(tpNumeric, opConst, val);
                        }
                    }
                    else if (left->type == tpDateTime)
                    {
                        time_t val;
                        if (stringToTime(str, val)) {
                            right = new LiteralNode(tpDateTime, opConst, new DateTime(val));
                        }
                    }
                    else if (left->type == tpArray)
                    {
                        right = parseArray(left, str, rightPos);
                    }
                }
                if (left->type == tpArray && right->type == tpArray) {
                    if (left->tag == opNewArray) {
                        FieldDescriptor* fd = right->getField();
                        if (fd == NULL) {
                            throwCompileError("Failed to parse array because element type is not known", pos);
                        }
                        left = new CastArrayNode(fd->_elemType, left);
                    } else if (right->tag == opNewArray) {
                        FieldDescriptor* fd = left->getField();
                        if (fd == NULL) {
                            throwCompileError("Failed to parse array because element type is not known", pos);
                        }
                        right = new CastArrayNode(fd->_elemType, right);
                    }
                }
                if ((unsigned)tkn - (unsigned)tknEq <= (unsigned)tknLe - (unsigned)tknEq
                    && (left->type == tpSequence || right->type == tpSequence))
                {
                    return invokeSequenceBinaryFunction(sequenceCmpFunctions[(int)tkn - (int)tknEq], left, right);
                }
                else if (left->type == tpUnicode || right->type == tpUnicode)
                {
                    left = convertToUnicodeString(left, leftPos);
                    right = convertToUnicodeString(right, rightPos);
                }
                else if (left->type == tpString || right->type == tpString)
                {
                    left = convertToString(left, leftPos);
                    right = convertToString(right, rightPos);
                }
                else if (left->type == tpReference || right->type == tpReference)
                {
                    left = convertToRef(left, leftPos);
                    right = convertToRef(right, rightPos);
                }
                else if (left->type == tpReal || right->type == tpReal)
                {
                    left = convertToReal(left, leftPos);
                    right = convertToReal(right, rightPos);
                }
                else if (left->type == tpNumeric || right->type == tpNumeric)
                {
                    left = convertToNumeric(left, leftPos);
                    right = convertToNumeric(right, rightPos);
                }
                else if (left->type == tpInt || right->type == tpInt)
                {
                    left = convertToInt(left, leftPos);
                    right = convertToInt(right, rightPos);
                }
                else if (left->type == tpDateTime || right->type == tpDateTime)
                {
                    left = convertToTime(left, leftPos);
                    right = convertToTime(right, rightPos);
                }
                else if (left->type != right->type)
                {
                    throwCompileError("incompatible operand types", leftPos);
                }
                else if (left->type == tpArray && (tkn == tknContainsAll || tkn == tknContainsAny))
                {
                    return new BinOpNode(tpBool, tkn == tknContainsAll ? opContainsAll : opContainsAny, left, right);
                }
                else if (left->type == tpArray && (tkn == tknContains || tkn == tknOverlaps || tkn == tknNear))
                {
                    return new BinOpNode(tpBool, (ExprCode)((int)tkn - (int)tknOverlaps + (int)opOverlaps), left, right);
                }
                else if (!isComparableType(left->type))
                {
                    throwCompileError("operands of relation operator should be of intger, real, string or reference type", leftPos);
                }
                if (!left->isSelfLoadNode() && right->isSelfLoadNode())
                {
                    // swap operands
                    ExprNode* tmp = left;
                    left = right;
                    right = tmp;
                    if (tkn == tknGt || tkn == tknGe)
                    {
                        tkn = (Token)((int)tkn + 2); // replace Gt with Lt or Ge with Le
                    }
                    else if (tkn == tknLt || tkn == tknLe)
                    {
                        tkn = (Token)((int)tkn - 2); // replace Lt with Gt or Le with Ge
                    }
                }
                left = new CompareNode((ExprCode)((int)opEq + (int)tkn - (int)tknEq), left, right);
                if (right->tag == opSubqueryElem)
                {
                    QuantorNode* quantor = ((SubqueryElemNode*)right)->quantor;
                    quantor->condition = left;
                    left = quantor;
                }
            }
            if (inverse)
            {
                left = new UnaryOpNode(tpBool, opBoolNot, left);
            }
        }
        return left;
    }

    ExprNode* Compiler::invokeSequenceBinaryFunction(char const* name, ExprNode* left, ExprNode* right)
    {
        SqlFunctionDeclaration* fdecl = engine->findFunction(name);
        assert(fdecl != NULL && fdecl->nArgs == 2);
        Vector < ExprNode >* args = Vector < ExprNode > ::create(2);
        args->items[0] = left;
        args->items[1] = right;
        return new FuncCallNode(fdecl, args);
    }

    ExprNode* Compiler::invokeSequenceTernaryFunction(char const* name, ExprNode* op1, ExprNode* op2, ExprNode* op3)
    {
        SqlFunctionDeclaration* fdecl = engine->findFunction(name);
        assert(fdecl != NULL && fdecl->nArgs == 3);
        Vector < ExprNode >* args = Vector < ExprNode > ::create(3);
        args->items[0] = op1;
        args->items[1] = op2;
        args->items[2] = op3;
        return new FuncCallNode(fdecl, args);
    }

    ExprNode* Compiler::invokeSequenceUnaryFunction(char const* name, ExprNode* opd)
    {
        SqlFunctionDeclaration* fdecl = engine->findFunction(name);
        assert(fdecl != NULL && fdecl->nArgs == 1);
        Vector < ExprNode >* args = Vector < ExprNode > ::create(1);
        args->items[0] = opd;
        return new FuncCallNode(fdecl, args);
    }

    ExprNode* Compiler::addition()
    {
        int leftPos = pos;
        ExprNode* left = multiplication();
        while (lex == tknAdd || lex == tknConcat || lex == tknSub)
        {
            Token tkn = lex;
            int rightPos = pos;
            ExprNode* right = multiplication();
            if (left->type == tpSequence || right->type == tpSequence)
            {
                left = invokeSequenceBinaryFunction(tkn == tknAdd ? "seq_add" : tkn == tknConcat
                                                    ? (left->type == tpSequence && right->type == tpSequence) ? "seq_cat" : "seq_concat"
                                                    : "seq_sub", left, right);
            }
            else if (left->type == tpUnicode || right->type == tpUnicode)
            {
                if (tkn == tknAdd || tkn == tknConcat)
                {
                    left = new BinOpNode(tpUnicode, opUnicodeStrConcat, convertToUnicodeString(left, leftPos),
                                         convertToUnicodeString(right, rightPos));
                }
                else
                {
                    throwCompileError("Operation - is not defined for strings", rightPos);
                }
            }
            else if (left->type == tpString || right->type == tpString || tkn == tknConcat)
            {
                if (tkn == tknAdd || tkn == tknConcat)
                {
                    left = new BinOpNode(tpString, opStrConcat, convertToString(left, leftPos), convertToString(right, rightPos));
                }
                else
                {
                    throwCompileError("Operation - is not defined for strings", rightPos);
                }
            }
            else
            {
                if (tkn == tknConcat) {
                    throwCompileError("Operation || can be applied only to string or sequence types", rightPos);
                }
                if (left->type == tpReal || right->type == tpReal)
                {
                    left = new BinOpNode(tpReal, tkn == tknAdd ? opRealAdd : opRealSub, convertToReal(left, leftPos),
                                         convertToReal(right, rightPos));
                }
                else if (left->type == tpNumeric || right->type == tpNumeric)
                {
                    left = new BinOpNode(tpNumeric, tkn == tknAdd ? opNumericAdd : opNumericSub, convertToNumeric(left, leftPos),
                                         convertToNumeric(right, rightPos));
                }
                else if (left->type == tpInt && right->type == tpInt)
                {
                    left = new BinOpNode(tpInt, tkn == tknAdd ? opIntAdd : opIntSub, left, right);
                }
                else if (left->type == tpDateTime && right->type == tpInt)
                {
                    left = new BinOpNode(tpDateTime, tkn == tknAdd ? opTimeAdd : opTimeSub, left, right);
                }
                else
                {
                    throwCompileError("operands of arithmentic operator should be of integer or real type", rightPos);
                }
            }
            leftPos = rightPos;
        }
        return left;
    }


    ExprNode* Compiler::multiplication()
    {
        int leftPos = pos;
        ExprNode* left = power();
        while (lex == tknMul || lex == tknDiv)
        {
            Token tkn = lex;
            int rightPos = pos;
            ExprNode* right = power();
            if (left->type == tpSequence || right->type == tpSequence)
            {
                left = invokeSequenceBinaryFunction(tkn == tknMul ? "seq_mul" : "seq_div", left, right);
            }
            else if (left->type == tpReal || right->type == tpReal)
            {
                left = new BinOpNode(tpReal, tkn == tknMul ? opRealMul : opRealDiv, convertToReal(left, leftPos),
                                     convertToReal(right, rightPos));
            }
            else if (left->type == tpNumeric || right->type == tpNumeric)
            {
                left = new BinOpNode(tpNumeric, tkn == tknMul ? opNumericMul : opNumericDiv, convertToNumeric(left, leftPos),
                                     convertToNumeric(right, rightPos));
            }
            else if (left->type == tpInt && right->type == tpInt)
            {
                left = new BinOpNode(tpInt, tkn == tknMul ? opIntMul : opIntDiv, left, right);
            }
            else
            {
                throwCompileError("operands of arithmentic operator should be of integer or real type", rightPos);
            }
            leftPos = rightPos;
        }
        return left;
    }


    ExprNode* Compiler::power()
    {
        int leftPos = pos;
        ExprNode* left = term();
        if (lex == tknPower)
        {
            int rightPos = pos;
            ExprNode* right = power();
            if (left->type == tpReal || right->type == tpReal)
            {
                left = new BinOpNode(tpReal, opRealPow, convertToReal(left, leftPos), convertToReal(right, rightPos));
            }
            else if (left->type == tpInt && right->type == tpInt)
            {
                left = new BinOpNode(tpInt, opIntPow, left, right);
            }
            else
            {
                throwCompileError("operands of arithmentic operator should be of integer or real type", rightPos);
            }
        }
        return left;
    }


    ExprNode* Compiler::cast(ExprNode* expr)
    {
        int p = pos-1;
        switch (lex)
        {
        case tknChar:
        case tknVarchar:
        case tknLongvarchar:
        case tknString:
            switch (expr->type)
            {
            case tpNull:
                expr->type = tpString;
                break;
            case tpInt:
                expr = int2str(expr);
                break;
            case tpNumeric:
                expr = numeric2str(expr);
                break;
            case tpReal:
                expr = real2str(expr);
                break;
            case tpString:
                break;
            case tpUnicode:
                expr = wcs2mbs(expr);
                break;
            case tpDateTime:
            case tpBool:
                expr = new UnaryOpNode(tpString, opAnyToStr, expr);
                break;
            default:
                throwCompileError("Conversion can be performed only between string,numeric and boolean types", p);
            }
            break;

        case tknUnicode:
            switch (expr->type)
            {
            case tpNull:
                expr->type = tpUnicode;
                break;
            case tpInt:
                expr = mbs2wcs(int2str(expr));
                break;
            case tpNumeric:
                expr = mbs2wcs(numeric2str(expr));
                break;
            case tpReal:
                expr = mbs2wcs(real2str(expr));
                break;
            case tpString:
                expr = mbs2wcs(expr);
                break;
            case tpUnicode:
                break;
            case tpBool:
            case tpDateTime:
                expr = new UnaryOpNode(tpUnicode, opMbsToWcs, new UnaryOpNode(tpString, opAnyToStr, expr));
                break;
            default:
                throwCompileError("Conversion can be performed only between string,numeric and boolean types",
                                  p);
            }
            break;

        case tknDate:
        case tknTime:
        case tknTimestamp:
            switch (expr->type)
            {
            case tpNull:
                expr->type = tpDateTime;
                break;
            case tpInt:
                expr = int2time(expr);
                break;
            case tpNumeric:
                expr = numeric2time(expr);
                break;
            case tpString:
                expr = str2time(expr, p);
                break;
            case tpUnicode:
                expr = str2time(wcs2mbs(expr), p);
                break;
            default:
                throwCompileError("Conversion to date can be performed only from string and integer types", p);
            }
            break;


        case tknReference:
            switch (expr->type)
            {
            case tpNull:
                expr->type = tpReference;
                break;
            case tpInt:
                expr = int2ref(expr);
                break;
            default:
                throwCompileError("Conversion to reference can be performed only from integer type", p);
            }
            break;

        case tknInt:
        case tknInteger:
        case tknBigint:
        case tknSmallint:
        case tknTinyint:
            switch (expr->type)
            {
            case tpNull:
                expr->type = tpInt;
                break;
            case tpDateTime:
                expr = new UnaryOpNode(tpInt, opTimeToInt, expr);
                break;
            case tpInt:
                break;
            case tpReference:
                expr = new UnaryOpNode(tpInt, opRefToInt, expr);
                break;
            case tpNumeric:
                expr = new UnaryOpNode(tpInt, opNumericToInt, expr);
                break;
            case tpReal:
                expr = new UnaryOpNode(tpInt, opRealToInt, expr);
                break;
            case tpString:
                expr = new UnaryOpNode(tpInt, opStrToInt, expr);
                break;
            case tpUnicode:
                expr = new UnaryOpNode(tpInt, opStrToInt, wcs2mbs(expr));
                break;
            case tpBool:
                expr = new UnaryOpNode(tpInt, opBoolToInt, expr);
                break;
            default:
                throwCompileError("Conversion can be performed only between string,numeric and boolean types",
                                  p);
            }
            break;

        case tknNumeric:
        case tknDecimal:
            switch (expr->type)
            {
            case tpNull:
                expr->type = tpNumeric;
                break;
            case tpInt:
                expr = new UnaryOpNode(tpNumeric, opIntToNumeric, expr);
                break;
            case tpString:
                expr = new UnaryOpNode(tpNumeric, opStrToNumeric, expr);
                break;
            case tpUnicode:
                expr = new UnaryOpNode(tpNumeric, opStrToNumeric, wcs2mbs(expr));
                break;
            case tpNumeric:
                break;
            default:
                throwCompileError("Conversion can be performed only between string,numeric and boolean types", p);
            }
            if ((lex = scan()) == tknLpar) {
                expect(tknIconst, "width");
                if ((lex = scan()) == tknComma) {
                    ExprNode* precision = disjunction();
                    expr = new BinOpNode(tpNumeric, opScaleNumeric, expr, precision);
                }
                expected(tknRpar, ")");
                lex = scan();
            }
            return expr;

        case tknReal:
        case tknFloat:
        case tknDouble:
            switch (expr->type)
            {
            case tpNull:
                expr->type = tpReal;
                break;
            case tpInt:
                expr = new UnaryOpNode(tpReal, opIntToReal, expr);
                break;
            case tpReal:
                break;
            case tpString:
                expr = new UnaryOpNode(tpReal, opStrToReal, expr);
                break;
            case tpUnicode:
                expr = new UnaryOpNode(tpReal, opStrToReal, wcs2mbs(expr));
                break;
            case tpNumeric:
                expr = new UnaryOpNode(tpReal, opNumericToReal, expr);
                break;
            case tpBool:
                expr = new UnaryOpNode(tpReal, opBoolToReal, expr);
                break;
            default:
                throwCompileError("Conversion can be performed only between string,numeric and boolean types",
                                  p);
            }
            break;

        case tknBoolean:
        case tknBit:
            switch (expr->type)
            {
            case tpNull:
                expr->type = tpBool;
                break;
            case tpInt:
                expr = new UnaryOpNode(tpBool, opIntToBool, expr);
                break;
            case tpReal:
                expr = new UnaryOpNode(tpBool, opRealToBool, expr);
                break;
            case tpString:
                expr = new UnaryOpNode(tpBool, opStrToBool, expr);
                break;
            case tpUnicode:
                expr = new UnaryOpNode(tpBool, opStrToBool, wcs2mbs(expr));
                break;
            case tpBool:
                break;
            default:
                throwCompileError("Conversion can be performed only between string,numeric and boolean types",
                                  p);
            }
            break;

        case tknArray:
            {
                expect(tknLpar, "(");
                Type type;
                switch (scan()) {
                case tknBit:
                case tknBoolean:
                    type = tpBool;
                    lex = scan();
                    break;
                case tknChar:
                case tknVarchar:
                case tknLongvarchar:
                case tknString:
                    type = tpString;
                    if ((lex = scan()) == tknLpar)
                    {
                        // skip fields length
                        expect(tknIconst, "field width");
                        expect(tknRpar, ")");
                        lex = scan();
                    }
                    break;
                case tknUnicode:
                    type = tpUnicode;
                    if ((lex = scan()) == tknLpar)
                    {
                        // skip fields length
                        expect(tknIconst, "field width");
                        expect(tknRpar, ")");
                        lex = scan();
                    }
                    break;
                case tknTinyint:
                    type = tpInt1;
                    lex = scan();
                    break;
                case tknSmallint:
                    type = tpInt2;
                    lex = scan();
                    break;
                case tknInteger:
                case tknInt:
                    if ((lex = scan()) == tknLpar)
                    {
                        expect(tknIconst, "field width");
                        switch ((int)((IntValue*)value)->val)
                        {
                        case 1:
                            type = tpInt1;
                            break;
                        case 2:
                            type = tpInt2;
                            break;
                        case 4:
                            type = tpInt4;
                            break;
                        case 8:
                            type = tpInt8;
                            break;
                        default:
                            throwCompileError("Invalid precision", p);
                        }
                        expect(tknRpar, ")");
                    }
                    else
                    {
                        type = tpInt4;
                    }
                    break;
                case tknUnsigned:
                    if ((lex = scan()) == tknLpar)
                    {
                        expect(tknIconst, "field width");
                        switch ((int)((IntValue*)value)->val)
                        {
                        case 1:
                            type = tpUInt1;
                            break;
                        case 2:
                            type = tpUInt2;
                            break;
                        case 4:
                            type = tpUInt4;
                            break;
                        case 8:
                            type = tpUInt8;
                            break;
                        default:
                            throwCompileError("Invalid precision", p);
                        }
                        expect(tknRpar, ")");
                        lex = scan();
                    }
                    else
                    {
                        type = tpUInt4;
                    }
                    break;
                case tknBigint:
                    type = tpInt8;
                    lex = scan();
                    break;
                case tknFloat:
                    type = tpReal4;
                    lex = scan();
                    break;
                case tknReal:
                case tknDouble:
                    type = tpReal8;
                    lex = scan();
                    break;
                case tknNumeric:
                case tknDecimal:
                    type = tpNumeric;
                    if ((lex = scan()) == tknLpar)
                    {
                        // skip fields length
                        expect(tknIconst, "field width");
                        if ((lex = scan()) == tknComma)
                        {
                            expect(tknIconst, "field precision");
                            lex = scan();
                        }
                        expected(tknRpar, ")");
                        lex = scan();
                    }
                    break;
                case tknDate:
                case tknTime:
                case tknTimestamp:
                    type = tpDateTime;
                    lex = scan();
                    break;
                case tknReference:
                    type = tpReference;
                    lex = scan();
                    break;
                default:
                    throwCompileError("Unsupported array type", p);
                }
                expected(tknRpar, ")");
                if (expr->type == tpNull) { 
                    expr->type = tpArray;
                } else if (expr->type != tpArray) {
                    throwCompileError("Array type expected", p);
                }
                expr = new CastArrayNode(type, expr);
                break;
            }
        default:
            throwCompileError("Unsupported conversion", p);
        }
        if ((lex = scan()) == tknLpar) {
            // skip fields length
            expect(tknIconst, "field width");
            expect(tknRpar, ")");
            lex = scan();
        }
        return expr;
    }

    ExprNode* Compiler::arrayElement(ExprNode* expr)
    {
        while (true)
        {
            int p = pos;
            FieldDescriptor* field;

            switch (lex)
            {
                case tknCol2:
                    lex = scan();
                    expr = cast(expr);
                    break;
                case tknAt:
                {
                    expect(tknIdent, "column name");
                    SqlFunctionDeclaration* fdecl = engine->findFunction("seq_at");
                    if (fdecl == NULL || fdecl->nArgs != 2) {
                        throwCompileError("Binary function mco_seq_at is not defined", pos);
                    }
                    Vector < ExprNode >* args = Vector < ExprNode > ::create(2);
                    SqlFunctionDeclaration* saveCtx = callCtx;
                    callCtx = fdecl;
                    args->items[0] = expr;
                    args->items[1] = objectField();
                    callCtx = saveCtx;
                    expr = new FuncCallNode(fdecl, args);
                    break;
                }
                case tknDot:
                    expect(tknIdent, "field name");
                    expr = objectField(expr);
                    break;
                case tknLbr:
                    {
                        p = pos;
                        ExprNode* index = disjunction();
                        if (index->type != tpInt)
                        {
                            throwCompileError("Index should have integer type", p);
                        }
                        if (lex == tknCol) {
                            p = pos;
                            lex = scan();
                            if (lex == tknRbr) {
                                if (isStringType(expr->type)) {
                                    expr = expr->type == tpString ? new TripleOpNode(tpString, opSubStr, expr, index, NULL)
                                        : new TripleOpNode(tpUnicode, opUnicodeSubStr, expr, index, NULL);
                                } else if (expr->type == tpSequence) {
                                    expr = invokeSequenceTernaryFunction("seq_subseq", expr, index, new ConstantNode(tpNull, opNull));
                                } else {
                                    throwCompileError("Subrange operator can be applied only to string or sequence types", p);
                                }
                            } else {
                                pos = p; // unwind
                                ExprNode* till = disjunction();
                                if (till->type != tpInt)
                                {
                                    throwCompileError("Index should have integer type", p);
                                }
                                expected(tknRbr, "]");
                                if (isStringType(expr->type)) {
                                    expr = expr->type == tpString ? new TripleOpNode(tpString, opSubStr2, expr, index, till)
                                        : new TripleOpNode(tpUnicode, opUnicodeSubStr2, expr, index, till);
                                } else if (expr->type == tpSequence) {
                                    expr = invokeSequenceTernaryFunction("seq_subseq", expr, index, till);
                                } else {
                                    throwCompileError("Subrange operator can be applied only to string or sequence types", p);
                                }
                            }
                        } else {
                            expected(tknRbr, "]");
                            if (isStringType(expr->type)) {
                                expr = new GetAtNode(tpInt, expr, index, NULL);
                            } else if (expr->type == tpSequence) {
                                 expr = invokeSequenceTernaryFunction("seq_subseq", expr, index, index);
                            } else if (expr->type == tpArray) {
                                if ((field = expr->getField()) != NULL) {
                                    if (field->_type != tpArray) {
                                        throwCompileError("Array type expected", p);
                                    }
                                    field = field->_components;
                                }
                                Type elemType = expr->getElemType();
                                if (elemType == tpNull) {
                                    throwCompileError("Type of array element is not known", p);
                                }
                                expr = new GetAtNode(elemType, expr, index, field);
                            } else {
                                throwCompileError("Index can be applied only to arrays, sequences or strings", p);
                            }
                        }
                        lex = scan();
                        break;
                    }
                default:
                    return expr;
            }
        }
    }

    ExprNode* Compiler::structComponent(FieldDescriptor* fd, ExprNode* base)
    {
        assert(fd != NULL);

        if (fd->_type == tpStruct)
        {
            while ((lex = scan()) == tknDot)
            {
                int p = pos;
                expect(tknIdent, "field name");
                fd = (FieldDescriptor*)fd->findComponent(ident);
                if (fd == NULL)
                {
                    throwCompileError(String::format("No such component '%s'", ident->cstr()), p);
                }
            }
            if (select->columns != NULL)
            {
                ColumnNode** columns = select->columns->items;
                for (int j = select->columns->length; --j >= 0;)
                {
                    if (columns[j]->field == fd)
                    {
                        columns[j]->used = true;
                        return columns[j];
                    }
                }
            }
        }
        else
        {
            lex = scan();
        }
        if (context == CTX_HAVING)
        {
            throwCompileError("HAVING clause can reference only columns mentioned in GROUP BY clause", pos);
        }
        return new LoadNode(fd, base);
    }

    inline bool isSearchResult(ExprNode* expr) 
    { 
        return isIgnored(expr) ? isSearchResult(((FuncCallNode*)expr)->args->items[0]) : expr->tag == ExprNode::opFuncCall && STRCMP(((FuncCallNode*)expr)->fdecl->getName(), "seq_search") == 0;
    }

    ExprNode* Compiler::structComponent(ColumnNode* column)
    {
        column->used = true;
        if (column->type == tpSequence && column->expr != NULL 
            && !(callCtx != NULL && STRCMP(callCtx->name, "seq_order_by") != 0 && STRCMP(callCtx->name, "seq_hash_group_by") != 0)
            && ((callCtx == NULL || STRCMP(callCtx->name, "seq_at") != 0) || !isSearchResult(column->expr)))
        { 
            lex = scan();
            if (isIgnored(column->expr)) { 
                column->expr = invokeSequenceUnaryFunction("seq_ignore", ((FuncCallNode*)column->expr)->args->items[0]); // replace seq_interla-ignore with seq_ignore
            }
            return column->materialized || isSearchResult(column->expr) ? invokeSequenceUnaryFunction("seq_dup", column) : column->expr;
        }
        /*
        if (column->materialized) {
            lex = scan();
            return invokeSequenceUnaryFunction("seq_dup", column);
        }
        if (column->type == tpSequence && isIgnored(column) && context != CTX_PROJECTION) { 
            throwCompileError("Ignored column can not be used in expression", pos);
        }
        */
        if (column->type == tpStruct)
        {
            return structComponent(column->field, column->expr);
        }
        else
        {
            lex = scan();
            if (context == CTX_HAVING && column->aggregate)
            {
                return column->expr;
            }
            return column;
        }
    }

    ExprNode* Compiler::structComponent(ExternColumnRefNode* columnRef)
    {
        columnRef->column->used = true;
        if (columnRef->type == tpStruct)
        {
            return structComponent(columnRef->column->field, columnRef->column->expr);
        }
        else
        {
            lex = scan();
            return columnRef;
        }
    }

    ExprNode* Compiler::objectField(ExprNode* base)
    {
        int p = pos - 1;
        String* column = ident;
        FieldDescriptor* fd = base->getField();

        if (base->type == tpReference)
        {
            if (fd == NULL)
            {
                throwCompileError("Type of referenced table is unknown", p);
            }
            if (fd->_referencedTableName == NULL)
            {
                throwCompileError(String::format("Abstract reference field '%s' can not be dereferenced", fd->_name->cstr()), p);
            }
            base = new DerefNode(base, fd);
            TableDescriptor* td = fd->referencedTable;
            if (td == NULL)
            {
                td = lookupTable(fd->_referencedTableName);
                fd->referencedTable = td;
            }
            fd = (FieldDescriptor*)td->findField(column);
        }
        else if (base->type == tpStruct)
        {
            assert(fd != NULL && fd->_type == tpStruct);
            fd = (FieldDescriptor*)fd->findComponent(column);
        }
        else
        {
            throwCompileError("Left operand of '.' should be structure or reference", p);
        }
        if (fd == NULL)
        {
            throwCompileError(String::format("Field %s is not found", column->cstr()), p);
        }
        return structComponent(fd, base);
    }

    ExprNode* Compiler::objectField()
    {
        FieldDescriptor* fd = NULL;
        int p = pos - 1;
        String* column = ident;
        SelectNode* curr;
        SelectNode* inner = NULL;
        for (curr = select; curr != NULL; inner = curr, curr = curr->outer)
        {
            int i;
            if (curr->columns != NULL)
            {
                for (i = curr->columns->length; --i >= 0;)
                {
                    if (column->equals(curr->columns->items[i]->name))
                    {
                        if (inner != NULL)
                        {
                            if (inner->outDep < curr->columns->items[i]->tableDep)
                            {
                                inner->outDep = curr->columns->items[i]->tableDep;
                            }
                            return structComponent(new ExternColumnRefNode(curr->columns->items[i]));
                        }
                        return structComponent(curr->columns->items[i]);
                    }
                }
                if (curr->groupColumns != curr->columns)
                {
                    for (i = curr->groupColumns->length; --i >= 0;)
                    {
                        if (column->equals(curr->groupColumns->items[i]->name))
                        {
                            if (inner != NULL)
                            {
                                if (inner->outDep < curr->groupColumns->items[i]->tableDep)
                                {
                                    inner->outDep = curr->groupColumns->items[i]->tableDep;
                                }
                                return structComponent(new ExternColumnRefNode(curr->groupColumns->items[i]));
                            }
                            return structComponent(curr->groupColumns->items[i]);
                        }
                    }
                }
            }
            for (i = curr->tables->length; --i >= 0;)
            {
                TableNode* tnode = curr->tables->items[i];
                Vector<ColumnNode>* currColumns = curr->columns;
                int unwind = pos;
                if (column->equals(tnode->name) && scan() == tknDot) {
                    expect(tknIdent, "field name");
                    while ((fd = (FieldDescriptor*)tnode->table->findField(ident)) == NULL && (tnode = tnode->joinTable)
                           != NULL)
                        ;
                    if (fd == NULL)
                    {
                        throwCompileError(String::format("No such column '%s' in table '%s'", ident->cstr(), column
                                           ->cstr()), p);
                    }
                    else if (currColumns != NULL)
                    {
                        for (int j = currColumns->length; --j >= 0;)
                        {
                            if (currColumns->items[j]->expr->isSelfLoadNode() && currColumns->items[j]->getField() == fd
                                && column->equals(((TableNode*)((LoadNode*)currColumns->items[j]->expr)->base)->name))
                            {
                                if (inner != NULL)
                                {
                                    if (inner->outDep <= i)
                                    {
                                        inner->outDep = i + 1;
                                    }
                                    return structComponent(new ExternColumnRefNode(currColumns->items[j]));
                                }
                                return structComponent(currColumns->items[j]);
                            }
                        }
                        currColumns = NULL; // do not search once again in column list
                    }
                }
                else
                {
                    pos = unwind;
                    fd = (FieldDescriptor*)tnode->table->findField(column);
                }
                if (fd != NULL)
                {
                    if (inner != NULL)
                    {
                        if (inner->outDep <= i)
                        {
                            inner->outDep = i + 1;
                        }
                        return new ExternalFieldNode(structComponent(fd, tnode));
                    }
					if (currColumns != NULL) {
                        for (int j = currColumns->length; --j >= 0;)
                        {
                            if (currColumns->items[j]->expr->isSelfLoadNode() && currColumns->items[j]->getField() == fd) {
                                return structComponent(currColumns->items[j]);
                            }
                        }
					}
                    return structComponent(fd, tnode);
                }
            }
        }
        for (ColumnNode* cn = columnList; cn != NULL; cn = cn->next) {
            if (column->equals(cn->name)) {
                return structComponent(cn);
            }
        }
        throwCompileError(String::format("Failed to locate column '%s'", column->cstr()), p);
    }

    ExprNode* Compiler::buildList()
    {
        BinOpNode *list = NULL, *leaf = NULL;
        do {
            ExprNode* expr = disjunction();
            if (list == NULL) {
                leaf = list = new BinOpNode(tpList, opNop, expr, NULL);
            } else {
                BinOpNode* node = new BinOpNode(tpList, opNop, expr, NULL);
                leaf->right = node;
                leaf = node;
            }
        } while (lex == tknComma);

        return list;
    }

    ExprNode* Compiler::groupFunction(ExprCode tag, bool wildcat)
    {
        int p = pos;
        if (context != CTX_COLUMNS && context != CTX_HAVING)
        {
            throwCompileError("Aggregate functions can be used only in select columns and HAVING clause", p);
        }
        ExprNode* expr;
        if (wildcat)
        {
            expr = new LiteralNode(tpInt, opIntConst, new IntValue(1));
            lex = scan();
        }
        else
        {
            Context saveContext = context;
            context = CTX_WHERE;
            expr = disjunction();
            context = saveContext;
        }
        Type type;
        if (tag != opCountAll && tag != opCountDis)
        {
            type = expr->type;
            if (tag == opCountNotNull)
            {
                if (type == tpSequence) {
                    tag = opSequenceCount;
                }
                type = tpInt;
            }
            else if (type == tpReal)
            {
                tag = (ExprCode)((int)tag + (int)opRealMinAll - (int)opIntMinAll);
            }
            else if (tag == opIntAvgAll || tag == opIntAvgDis)
            {
                type = tpReal;
            }
            else if (type == tpNumeric)
            {
                tag = (ExprCode)((int)tag + (int)opNumericMinAll - (int)opIntMinAll);
                if (tag == opNumericSequenceAll || tag == opNumericSequenceDis) {
                    throwCompileError("Sequence of numeric are not supported", p);
                }
                type = (tag == opNumericAvgAll || tag == opNumericAvgDis) ? tpReal : tpNumeric;
            }
            else if (type == tpString && (tag == opIntMinAll || tag == opIntMinDis))
            {
                tag = opStrMin;
            }
            else if (type == tpString && (tag == opIntMaxAll || tag == opIntMaxDis))
            {
                tag = opStrMax;
            }
            else if (type == tpSequence && tag < opIntMinDis && tag != opIntSequenceAll)
            {
                tag = (ExprCode)((int)tag + (int)opSequenceMinAll - (int)opIntMinAll);
                type = tag >= opSequenceCorAll ? tpArray : tpReal;
            }
            else if (type != tpReference && type != tpInt && type != tpDateTime)
            {
                throwCompileError("MIN, MAX, AVG and SUM functions can be applied only to integer or real expressions",
                                   p);
            }
        }
        else
        {
            type = tpInt;
        }
        GroupNode* group = new GroupNode(type, tag, expr);
        GroupNode* groups = select->groups;
        columnIsAggregate = true;
        while (groups != NULL)
        {
            if (groups->equalsNode(group))
            {
                return groups;
            }
            groups = groups->next;
        }
        if (context == CTX_HAVING && expr->tag != opColumn)
        {
            int n = select->columns->length;
            Vector < ColumnNode > * columns = Vector < ColumnNode > ::create(n + 1);
            columns->copy(select->columns);
            group->expr = columns->items[n] = new ColumnNode(n + 1, expr, NULL, NULL);
            group->columnNo = n;
            select->columns = columns;
        }
        group->next = select->groups;
        select->groups = group;
        return group;
    }

    ExprNode* Compiler::userFunction()
    {
        SqlFunctionDeclaration* fdecl = func;
        Vector < ExprNode > * args = NULL;
        if (fdecl->nArgs > MAX_FUNC_ARGS)
        {
            throwCompileError(String::format("Function %s has too many arguments (maximal %d supported)", fdecl->name,
                               MAX_FUNC_ARGS), pos - 1);
        }
        if (fdecl->nArgs == SqlFunctionDeclaration::FUNC_VARARG)
        {
            ExprNode* vargs[MAX_FUNC_VARGS];
            expect(tknLpar, "(");
            int i = 0;
            SqlFunctionDeclaration* saveCtx = callCtx;
            callCtx = fdecl;
            do
            {
                if (i == MAX_FUNC_VARGS)
                {
                    throwCompileError(String::format("Too many arguments for function %s", fdecl->name), pos);
                }
                vargs[i] = disjunction();
                if (fdecl->args != NULL) {
                    if (i == fdecl->args->length) { 
                        throwCompileError(String::format("Too many arguments for function %s", fdecl->name), pos);
                    }
                    if (fieldValueType[fdecl->args->items[i]->type()] != vargs[i]->type) {
                        throwCompileError(String::format("Wrong type of parameter %d of function %s: %s instead of expected %s", 
                                                         i, fdecl->name, typeMnemonic[vargs[i]->type], typeMnemonic[fdecl->args->items[i]->type()]), pos);
                    }
                }
                i += 1;
            }
            while (lex == tknComma);
            callCtx = saveCtx;

            if (fdecl->args != NULL && i != fdecl->args->length) {
                throwCompileError(String::format("Too few arguments for function %s", fdecl->name), pos);
            }
            expected(tknRpar, ")");
            args = Vector < ExprNode > ::create(i, vargs);
        }
        else if (fdecl->nArgs != 0)
        {
            args = Vector < ExprNode > ::create(fdecl->nArgs == SqlFunctionDeclaration::FUNC_AGGREGATE ? 2 : fdecl->nArgs);
            expect(tknLpar, "(");
            int i = 0;
            SqlFunctionDeclaration* saveCtx = callCtx;
            callCtx = fdecl;
            do
            {
                if (i == args->length)
                {
                    throwCompileError(String::format("Too many arguments for function %s", fdecl->name), pos);
                }                
                args->items[i] = disjunction();
                i += 1;
            }            
            while (lex == tknComma);
            callCtx = saveCtx;
 
            expected(tknRpar, ")");
            if (i < args->length)
            {
                if (i == 1 && fdecl->nArgs == SqlFunctionDeclaration::FUNC_AGGREGATE && fdecl->type == args->items[0]->type) {
                    lex = scan();
                    args->items[1] = args->items[0];
                    GroupNode* group = new GroupNode(fdecl->type, opUserAggr, new FuncCallNode(fdecl, args));
                    group->next = select->groups;
                    select->groups = group;
                    return group;
                }
                throwCompileError(String::format("Too few arguments for function %s", fdecl->name), pos);
            }
            if (i == 1 && fdecl->ctx == NULL && args->items[0]->isConstantExpression()) {
                Value* result = (*((Value *(*)(Value*))fdecl->func))(args->items[0]->evaluate(NULL));
                lex = scan();
                switch (result->type()) { 
                case tpInt:
                    return new LiteralNode(tpInt, opIntConst, result);
                    break;
                case tpReal:
                    return new LiteralNode(tpReal, opRealConst, result);
                    break;
                case tpString:
                    return new LiteralNode(tpString, opStrConst, result);
                    break;
                case tpUnicode:
                    return new LiteralNode(tpUnicode, opUnicodeStrConst, result);
                    break;
                default:
                    return new LiteralNode(result->type(), opConst, result);
                }
            }
        }
        lex = scan();
        return new FuncCallNode(fdecl, args);
    }

    ExprNode* Compiler::term()
    {
        Token prevLex = lex;
        Token cop = scan();
        int p = pos;
        ExprNode* expr;
        Binding* bp;
        lex = cop;

        switch (cop)
        {
#if 0
            case tknEof:
            case tknOrder:
                return NULL;
#endif
            case tknFunc:
                return arrayElement(userFunction());
            case tknIdent:
                for (bp = bindings; bp != NULL; bp = bp->next)
                {
                    if (bp->name->equals(ident))
                    {
                        lex = scan();
                        bp->used = true;
                        return new IndexNode(bp->loopId);
                    }
                }
                return arrayElement(objectField());

          case tknCoalesce:
             {
                 IfNullNode* last = NULL;
                 expect(tknLpar, "(");
                 expr = disjunction();
                 Type type = expr->type;
                 while (lex == tknComma) {
                     p = pos;
                     ExprNode* other = disjunction();
                     if (type == tpReal && other->type == tpInt) {
                         other = int2real(other);
                     } else if (type == tpNumeric && other->type == tpInt) {
                         other = int2numeric(other);
                     } else if (type == tpReference && other->type == tpInt) {
                         other = int2ref(other);
                     } else if (type == tpUnicode && other->type == tpString) {
                         other = mbs2wcs(other);
                     }
                     if (type != other->type) {
                         throwCompileError("Type of operands of IFNULL operator are not compatible", pos);
                     }
                     if (last == NULL) {
                         expr = last = new IfNullNode(expr, other);
                     } else {
                         last = (IfNullNode*)(last->right = new IfNullNode(last->right, other));
                     }
                 }
                 expected(tknRpar, ")");
                 break;
             }
          case tknNullIfZero:
             {
                expect(tknLpar, "(");
                expr = disjunction();
                expr = new UnaryOpNode(expr->type, opNullIfZero, expr);
                expected(tknRpar, ")");
                break;
             }
          case tknIfNull:
             {
                expect(tknLpar, "(");
                ExprNode* left = disjunction();
                expected(tknComma, ",");
                ExprNode* right = disjunction();
                expected(tknRpar, ")");
                if (left->type == tpReal && right->type == tpInt) {
                    right = int2real(right);
                } else if (left->type == tpNumeric && right->type == tpInt) {
                    right = int2numeric(right);
                } else if (left->type == tpReference && right->type == tpInt) {
                    right = int2ref(right);
                } else if (left->type == tpUnicode && right->type == tpString) {
                    right = mbs2wcs(right);
                }
                if (left->type != right->type) {
                    throwCompileError("Type of operands of IFNULL operator are not compatible", p);
                }
                expr = new IfNullNode(left, right);
                break;
             }
             case tknCase:
             {
                 CaseNode* caseExpr = NULL;
                 expect(tknWhen, "WHEN");
                 do {
                     ExprNode* whenExpr = condition();
                     expected(tknThen, "THEN");
                     ExprNode* thenExpr = disjunction();
                     if (caseExpr != NULL) {
                         if (caseExpr->then->type != thenExpr->type) {
                             throwCompileError("Types of CASE expressions do not match", p);
                         }
                         CaseNode* newBranch = new CaseNode(whenExpr, thenExpr, NULL);
                         caseExpr->otherwise = newBranch;
                         caseExpr = newBranch;
                     } else {
                         expr = caseExpr = new CaseNode(whenExpr, thenExpr, NULL);
                     }
                 } while (lex == tknWhen);

                 if (lex == tknElse) {
                     ExprNode* elseExpr = disjunction();
                     if (caseExpr->then->type != elseExpr->type) {
                         throwCompileError("Types of CASE expressions do not match", p);
                     }
                     caseExpr->otherwise = elseExpr;
                 }
                 expected(tknEnd, "END");
                 break;
             }

             case tknCast:
                expect(tknLpar, "(");
                expr = disjunction();
                if (lex == tknAs || lex == tknComma)
                {
                    lex = scan();
                }
                expr = cast(expr);
                expected(tknRpar, ")");
                lex = scan();
                return arrayElement(expr);

            case tknSubstr:
                {
                    expect(tknLpar, "(");
                    expr = disjunction();
                    if (!isStringType(expr->type))
                    {
                        throwCompileError("First operand of SUBSTR function should have STRING type", p);
                    }
                    expected(tknComma, ",");
                    p = pos;
                    ExprNode* from = disjunction();
                    if (from->type != tpInt)
                    {
                        throwCompileError("Second operand of SUBSTR function should have INTEGER type", p);
                    }
                    ExprNode* length = NULL;
                    if (lex == tknComma)
                    {
                        p = pos;
                        length = disjunction();
                        if (length->type != tpInt)
                        {
                            throwCompileError("Third operand of SUBSTR function should have INTEGER type", p);
                        }
                    }
                    expr = expr->type == tpString ? new TripleOpNode(tpString, opSubStr, expr, from, length): new
                        TripleOpNode(tpUnicode, opUnicodeSubStr, expr, from, length);
                    expected(tknRpar, ")");
                    lex = scan();
                    return arrayElement(expr);
                }
            case tknAny:
            case tknSome:
            case tknAll:
                expr = term();
                if (!expr->isQueryNode())
                {
                    throwCompileError("Subquery expected", p);
                }
                return new SubqueryElemNode(cop == tknAll ? opQuantorAll : opQuantorAny, expr);

            case tknExists:
                lex = scan();
                if (lex == tknIdent)
                {
                    bindings = bp = new Binding(ident, vars++, bindings);
                    if (vars >= MAX_INDEX_VARS)
                    {
                        throwCompileError("Too many nested EXISTS clauses", p);
                    }
                    p = pos;
                    expect(tknCol, ":");
                    p = pos;
                    expr = term();
                    if (expr->type != tpBool)
                    {
                        throwCompileError("Expresion in EXISTS clause should be of boolean type", p);
                    }
                    if (bp->used)
                    {
                        expr = new ExistsNode(expr, vars - 1);
                    }
                    vars -= 1;
                    bindings = bp->next;
                }
                else if (lex == tknLpar)
                {
                    lex = scan();
                    expr = new SetNotEmptyNode(selectStatement(SA_EXISTS));
                    expected(tknRpar, ")");
                    lex = scan();
                }
                else
                {
                    throwCompileError("Subquery or free variable name expected", p);
                }
                return expr;
            case tknSysdate:
                lex = scan();
                return new ConstantNode(tpInt, opSysdate);
            case tknFalse:
                expr = new ConstantNode(tpBool, opFalse);
                break;
            case tknTrue:
                expr = new ConstantNode(tpBool, opTrue);
                break;
            case tknNull:
                expr = new ConstantNode(tpNull, opNull);
                break;
            case tknIconst:
                expr = new LiteralNode(tpInt, opIntConst, value);
                break;
            case tknFconst:
                expr = new LiteralNode(tpReal, opRealConst, value);
                break;
            case tknValue:
                expr = new LiteralNode(value->type(), opConst, value);
                break;
            case tknSconst:
                expr = new LiteralNode(tpString, opStrConst, value);
                lex = scan();
                return arrayElement(expr);
            case tknUSconst:
                expr = new LiteralNode(tpUnicode, opUnicodeStrConst, value);
                lex = scan();
                return arrayElement(expr);
            case tknIndirectParam:
                expr = param;
                lex = scan();
                return arrayElement(expr);
            case tknParam:
                expr = new ParamNode(&paramDescs[paramCount - 1]);
                lex = scan();
                return arrayElement(expr);
            case tknSin:
            case tknCos:
            case tknTan:
            case tknAsin:
            case tknAcos:
            case tknAtan:
            case tknExp:
            case tknLog:
            case tknSqrt:
            case tknCeil:
            case tknFloor:
                expr = term();
                return new UnaryOpNode(tpReal, (ExprCode)((int)cop + (int)opRealSin - (int)tknSin), convertToReal(expr, p));
            case tknCount:
                expect(tknLpar, "(");
                p = pos;
                lex = scan();
                if (lex == tknMul)
                {
                    expr = groupFunction(opCountAll, true);
                }
                else
                {
                    ExprCode op = opCountDis;
                    if (lex != tknDistinct)
                    {
                        pos = p; // restore position of pointer
                        op = opCountNotNull;
                    }
                    expr = groupFunction(op);
                }
                expected(tknRpar, ")");
                lex = scan();
                return expr;
            case tknSequence:
            case tknMin:
            case tknMax:
            case tknAvg:
            case tknSum:
            case tknCor:
            case tknCov:
                {
                    bool distinct = false;
                    expect(tknLpar, "(");
                    p = pos;
                    lex = scan();
                    if (lex == tknDistinct)
                    {
                        distinct = true;
                    }
                    else if (lex != tknAll)
                    {
                        pos = p; // restore position of pointer
                    }
                    expr = groupFunction((ExprCode)((int)cop - (int)tknMin + (int)(distinct ? opIntMinDis : opIntMinAll)));
                    expected(tknRpar, ")");
                    lex = scan();
                    return expr;
                }
            case tknAbs:
                expr = term();
                if (expr->type == tpInt)
                {
                    return new UnaryOpNode(tpInt, opIntAbs, expr);
                }
                else if (expr->type == tpReal)
                {
                    return new UnaryOpNode(tpReal, opRealAbs, expr);
                }
                else if (expr->type == tpNumeric)
                {
                    return new UnaryOpNode(tpNumeric, opNumericAbs, expr);
                }
                else if (expr->type == tpSequence)
                {
                    return invokeSequenceUnaryFunction("seq_abs", expr);
                }
                else
                {
                    throwCompileError("ABS function can be applied only to integer or real expression", p);
                }
            case tknLength:
                expr = term();
                if (isStringType(expr->type) || expr->type == tpArray)
                {
                    return new UnaryOpNode(tpInt, opLength, expr);
                }
                else
                {
                    throwCompileError("LENGTH function is defined only for arrays and strings", p);
                }
            case tknLower:
                expr = term();
                if (expr->type == tpString)
                {
                    return arrayElement(new UnaryOpNode(tpString, opStrLower, expr));
                }
                else if (expr->type == tpUnicode)
                {
                    return arrayElement(new UnaryOpNode(tpUnicode, opUnicodeStrLower, expr));
                }
                throwCompileError("LOWER function can be applied only to string argument", p);
            case tknUpper:
                expr = term();
                if (expr->type == tpString)
                {
                    return arrayElement(new UnaryOpNode(tpString, opStrUpper, expr));
                }
                else if (expr->type == tpUnicode)
                {
                    return arrayElement(new UnaryOpNode(tpUnicode, opUnicodeStrUpper, expr));
                }
                throwCompileError("UPPER function can be applied only to string argument", p);
            case tknInt:
            case tknInteger:
                expr = term();
                switch (expr->type)
                {
                case tpDateTime:
                    return new UnaryOpNode(tpInt, opTimeToInt, expr);
                case tpReference:
                    return new UnaryOpNode(tpInt, opRefToInt, expr);
                case tpNumeric:
                    return new UnaryOpNode(tpInt, opNumericToInt, expr);
                case tpReal:
                    return new UnaryOpNode(tpInt, opRealToInt, expr);
                case tpString:
                    return new UnaryOpNode(tpInt, opStrToInt, expr);
                case tpUnicode:
                    return new UnaryOpNode(tpInt, opStrToInt, wcs2mbs(expr));
                case tpBool:
                    return new UnaryOpNode(tpInt, opBoolToInt, expr);
                case tpInt:
                    return expr;
                default:
                    throwCompileError(
                                       "INTEGER function can be applied only to expression of real,numeric,string or boolean type", p);
                }
            case tknReal:
                expr = term();
                switch (expr->type)
                {
                case tpNumeric:
                    return new UnaryOpNode(tpReal, opNumericToReal, expr);
                case tpInt:
                    return int2real(expr);
                case tpString:
                    return new UnaryOpNode(tpReal, opStrToReal, expr);
                case tpUnicode:
                    return new UnaryOpNode(tpReal, opStrToReal, wcs2mbs(expr));
                case tpBool:
                    return new UnaryOpNode(tpReal, opBoolToReal, expr);
                case tpReal:
                    return expr;
                default:
                    throwCompileError(
                                       "REAL function can be applied only to expression of integer, srting or boolean type", p);
                }
            case tknString:
                expr = term();
                switch (expr->type)
                {
                case tpInt:
                    return arrayElement(int2str(expr));
                case tpNumeric:
                    return arrayElement(numeric2str(expr));
                case tpReal:
                    return arrayElement(real2str(expr));
                case tpBool:
                case tpDateTime:
                    return arrayElement(new UnaryOpNode(tpString, opAnyToStr, expr));
                case tpString:
                    return arrayElement(expr);
                case tpUnicode:
                    return arrayElement(wcs2mbs(expr));
                default:
                    throwCompileError("STRING function can be applied only to numeric or boolean expression", p);
                }
            case tknUnicode:
                expr = term();
                switch (expr->type)
                {
                case tpInt:
                    return arrayElement(mbs2wcs(int2str(expr)));
                case tpNumeric:
                    return arrayElement(mbs2wcs(numeric2str(expr)));
                case tpReal:
                    return arrayElement(mbs2wcs(real2str(expr)));
                case tpBool:
                case tpDateTime:
                    return arrayElement(mbs2wcs(new UnaryOpNode(tpString, opAnyToStr, expr)));
                case tpUnicode:
                    return arrayElement(expr);
                case tpString:
                    return arrayElement(mbs2wcs(expr));
                default:
                    throwCompileError("UNICODE function can be applied only to numeric or boolean expression", p);
                }
            case tknLbr:
            {
                Vector<ExprNode>* v = NULL;
                int size = 0;
                p = pos;
                if (scan(true) != tknRbr) {
                    v = Vector<ExprNode>::create(64);
                    pos = p; // unget
                    do {
                        if (size == v->length) {
                            Vector<ExprNode>* v2 = Vector<ExprNode>::create(size*2);
                            v2->copy(v);
                            v = v2;
                        }
                        v->items[size++] = disjunction();
                    } while (lex == tknComma);
                    expected(tknRbr, "]");
                }
                expr = new NewArrayNode(size, v);
                break;
            }
            case tknLpar:
                expr = disjunction();
                if (lex == tknComma)
                {
                    expr = new BinOpNode(tpList, opNop, expr, buildList());
                }
                else if (prevLex == tknIn && !expr->isQueryNode())
                {
                    expr = new BinOpNode(tpList, opNop, expr, NULL);
                }
                expected(tknRpar, ")");
                break;
            case tknNot:
                expr = comparison();
                if (expr->type == tpInt)
                {
                    if (expr->tag == opIntConst)
                    {
                        LiteralNode* literal = (LiteralNode*)expr;
                        literal->value = new IntValue(~((IntValue*)literal->value)->val);
                    }
                    else
                    {
                        expr = new UnaryOpNode(tpInt, opIntNot, expr);
                    }
                    return expr;
                }
                else if (expr->type == tpBool)
                {
                    return new UnaryOpNode(tpBool, opBoolNot, expr);
                }
                else if (expr->type == tpSequence)
                {
                    return invokeSequenceUnaryFunction("seq_not", expr);
                }
                else
                {
                    throwCompileError("NOT operator can be applied only to integer or boolean expressions", p);
                }
            case tknExcl:
                expr = term();
                if (expr->type == tpSequence) {
                    return invokeSequenceBinaryFunction("seq_internal_ignore", expr, new MarkNode());
                } else {
                    throwCompileError("! operator (seq_ignore) can be applied only to sequences", p);
                }
            case tknAdd:
                throwCompileError("Using of unary plus operator has no sense", p);
            case tknSub:
                expr = term();
                if (expr->type == tpInt)
                {
                    if (expr->tag == opIntConst)
                    {
                        LiteralNode* literal = (LiteralNode*)expr;
                        literal->value = new IntValue( - ((IntValue*)literal->value)->val);
                    }
                    else
                    {
                        expr = new UnaryOpNode(tpInt, opIntNeg, expr);
                    }
                }
                else if (expr->type == tpReal)
                {
                    #ifndef NO_FLOATING_POINT
                        if (expr->tag == opRealConst)
                        {
                            LiteralNode* literal = (LiteralNode*)expr;
                            literal->value = new RealValue( - ((RealValue*)literal->value)->val);
                        }
                        else
                    #endif
                    {
                        expr = new UnaryOpNode(tpReal, opRealNeg, expr);
                    }
                }
                else if (expr->type == tpNumeric)
                {
                    expr = new UnaryOpNode(tpNumeric, opNumericNeg, expr);
                }
                else if (expr->type == tpSequence)
                {
                    return invokeSequenceUnaryFunction("seq_neg", expr);
                }
                else
                {
                    throwCompileError("Unary minus can be applied only to numeric expressions", p);
                }
                return expr;
            case tknSelect:
                // subquery
                return selectStatement(SA_SUBQUERY);
            default:
                throwCompileError("operand expected", p);
        }
        lex = scan();
        return arrayElement(expr);
    }

    void Compiler::throwCompileError(char const* msg, int pos)
    {
        MCO_THROW CompileError(msg, query, pos);
    }

    void Compiler::throwCompileError(String* msg, int pos)
    {
        MCO_THROW CompileError(msg->cstr(), query, pos);
    }

    Value* Compiler::evaluate(Runtime* runtime)
    {
        assert(false);
        return NULL;
    }

}
