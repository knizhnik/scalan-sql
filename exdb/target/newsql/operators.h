class CrossJoin : public DataSource 
{
    class CursorImpl : public Cursor 
    {
        Runtime* const runtime;
        CrossJoin* const ds;
        Cursor* outerCursor;
        Cursor* innerCursor;
        Record* outerTuple;
        int outerColumns;
        int innerColumns;
      public:
        Record* next();
        CursorImpl(Runtime* runtime, CrossJoin* ds);
        ~CursorImpl();
    };

  public:
    DataSource* outer;
    DataSource* inner;

    Cursor* cursor(Runtime* runtime);
    CrossJoin(DataSource* outer, DataSource* inner);
    ~CrossJoin();
};

class IndexedJoin : public DataSource 
{
    class CursorImpl : public Cursor 
    {
        Runtime* const runtime;
        IndexedJoin* const ds;
        Cursor* outerCursor;
        Cursor* innerCursor;
        DataSource* innerDataSource;
        Record* outerTuple;
        int outerColumns;
        int innerColumns;
      public:
        Record* next();
        CursorImpl(Runtime* runtime, CrossJoin* ds);
        ~CursorImpl();
    };

  public:
    DataSource* outer;
    Index* index;
    Vector<ExprNode>* outerKeys;
    bool isOuterJoin;

    Cursor* cursor(Runtime* runtime);
    IndexedJoin(DataSource* outer, Index* index, Vector<Field>* outerKeys, bool isOuterJoin);
    ~IndexedJoin();
};

class HashJoin : public DataSource 
{
    class CursorImpl : public Cursor 
    {
        Runtime* const runtime;
        IndexedJoin* const ds;
        Cursor* outerCursor;
        Cursor* innerCursor;
        Record* outerTuple;
        int outerColumns;
        int innerColumns;        
        MultiHashTable* hash;
        Vector<Value>* outerKeyValues;

      public:
        Record* next();
        CursorImpl(Runtime* runtime, CrossJoin* ds);
        ~CursorImpl();
    };

  public:
    DataSource* outer;
    DataSource* inner;
    Vector<ExprNode>* outerKeys;
    Vector<ExprNode>* innerKeys;
    bool isOuterJoin;

    Cursor* cursor(Runtime* runtime);
    HashJoin(DataSource* outer, DataSource* inner, Vector<ExprNode>* outerKeys, Vector<ExprNode>* innerKeys, bool isOuterJoin);
};

class Filter : public DataSource
{
  public:
    DataSource* input;
    ExprNode* predicate;

    Cursor* cursor(Runtime* runtime);
    Filter(DataSource* input, ExprNode* predicate);
};

class Project : public DataSource
{
  public:
    DataSource* input;
    Vector<Field>* columns;

    Cursor* cursor(Runtime* runtime);
    Filter(DataSource* input, Vector<Field>* columns);
};

class HashAggregate : public DataSource
{
  public:
    DataSource* input;
    Vector<ExprNode>* groupBy;
    Vector<ExprNode>* aggregates;
    
    Cursor* cursor(Runtime* runtime);
    HashAggregate(DataSource* input, Vector<ExprNode>* groupBy, Vector<ExprNode>* aggregates);
};

class GrandAggregate : public DataSource
{
  public:
    DataSource* input;
    Vector<ExprNode>* aggregates;
    
    Cursor* cursor(Runtime* runtime);
    GrandAggregate(DataSource* input, Vector<ExprNode>* aggregates);
};

class SortBy : public DataSource
{
  public:
    DataSource* input;
    Vector<OrderNode>* orderBy;
    
    Cursor* cursor(Runtime* runtime);
    SortBy(DataSource* input, Vector<OrderNode>* orderBy);
};

class Intersect : public DataSource
{
  public:
    DataSource* left;
    DataSource* right;

    Cursor* cursor(Runtime* runtime);
    Intersect(DataSource* left, DataSource* right);
};

class Union : public DataSource
{
  public:
    DataSource* left;
    DataSource* right;

    Cursor* cursor(Runtime* runtime);
    Union(DataSource* left, DataSource* right);
};

class UnionAll : public DataSource
{
  public:
    DataSource* left;
    DataSource* right;

    Cursor* cursor(Runtime* runtime);
    UnionAll(DataSource* left, DataSource* right);
};


class Except : public DataSource
{
  public:
    DataSource* left;
    DataSource* right;

    Cursor* cursor(Runtime* runtime);
    Except(DataSource* left, DataSource* right);
};

class Limit : public DataSource
{
  public:
    DataSource* input;
    ExprNode* skip;
    ExprNode* limit;
    
    Cursor* cursor(Runtime* runtime);
    Limit(DataSource* input, ExprNode* skip, ExprNode* limit);
};

    
class IndexSearch : public DataSource
{
  public:
    Index* index;
    Vector<ExprNode>* keys;
    Index::SearchOperation op;
    bool reverse;

    Cursor* cursor(Runtime* runtime);
    IndexSearch(Index* index, Vector<ExprNode>* keys, Index::SearchOperation op, bool reverse);
};


class IndexScan : public DataSource
{
  public:
    Index* index;
    bool reverse;

    Cursor* cursor(Runtime* runtime);
    IndexSearch(Index* index, bool reverse);
};


