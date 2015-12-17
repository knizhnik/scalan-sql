#include <stdio.h>
#include <time.h>
#include <sys/time.h>

#define N_ELEMS 1LL*1024*1024*1024

template<class T>
class Iterator {
public:
  virtual bool next(T& val) = 0;
};

#define virtual

template<class T, class I, bool(*predicate)(T const&)>
class Filter : public Iterator<T>
{
  I* in;
public:
  Filter(I* input) : in(input) {}
  virtual bool next(T& val) {
    while(in->next(val)) {
      if (predicate(val)) {
	return true;
      }
    }
    return false;
  }
};

template<class T, class I, class A, void (*fold)(A&,T const&)>
class Aggregate : public Iterator<A>
{
  I* in;
  bool first;
public:
  Aggregate(I* input) : in(input), first(true) {}

  virtual bool next(A& val) {
    if (!first) {
      return false;
    }
    val = 0;
    T x; 
    while (in->next(x)) {
      fold(val, x);
    }
    first = false;
    return true;
  }
};

template<class T>
class Cache : public Iterator<T>
{
  T* data;
  size_t size;
  size_t curr;
public:
  Cache(T* ptr, size_t len) : data(ptr), size(len), curr(0) {}

  virtual bool next(T& val) {
    if (curr < size) {
      val = data[curr++];
      return true;
    }
    return false;
  }
};

template<class T, bool (*predicate)(T const&), class I>
inline Filter<T, I, predicate>* filter(I* in)
{
  return new Filter<T, I, predicate>(in);
}

template<class T, class A, void (*fold)(A& dst, T const&), class I>
inline Aggregate<T, I, A, fold>* aggregate(I* in)
{
  return new Aggregate<T, I, A, fold>(in);
}

typedef unsigned date_t;
typedef char name_t[25];
typedef char priority_t[15];
typedef char shipmode_t[10];

struct Lineitem
{
    long   l_orderkey;
    int    l_partkey;
    int    l_suppkey;
    int    l_linenumber;
    double l_quantity;
    double l_extendedprice;
    double l_discount;
    double l_tax;
    char   l_returnflag;
    char   l_linestatus;
    date_t l_shipdate;
    date_t l_commitdate;
    date_t l_receiptdate;
    char   l_shipinstruct[25];
    shipmode_t l_shipmode;
    char   l_comment[44];
};



bool predicate(Lineitem const& l) {
  return l.l_shipdate >= N_ELEMS/3;
}

void sum(double& dst, Lineitem const& lineitem)
{
  dst += lineitem.l_extendedprice*(1-lineitem.l_discount)*(1+lineitem.l_tax);;
}

static time_t getCurrentTime()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec*1000 + tv.tv_usec/1000;
}




int main()
{
  Lineitem* arr = new Lineitem[N_ELEMS];
  for (size_t i = 0; i < N_ELEMS; i++) {
    arr[i].l_shipdate = i;
  }
  if (1) {
    time_t start = getCurrentTime();
    double sum = 0;
    for (size_t i = 0; i < N_ELEMS; i++) {
      Lineitem& lineitem = arr[i];
      if (lineitem.l_shipdate >= N_ELEMS/3) { 
	sum += lineitem.l_extendedprice*(1-lineitem.l_discount)*(1+lineitem.l_tax);
      }
    }
    printf("Elapsed time: %ld, sum=%lf\n", getCurrentTime() - start, sum);
  }
  {
    time_t start = getCurrentTime();
    //    auto i = new Aggregate<double, Filter<double, Cache<double>, predicate>, double, sum>(new Filter<double, Cache<double>, predicate>(new Cache<double>(arr, N_ELEMS)));
    auto i = aggregate<Lineitem,double,sum>(filter<Lineitem,predicate>(new Cache<Lineitem>(arr, N_ELEMS)));
    double result;
    if (i->next(result)) { 
      printf("Elapsed time: %ld, sum=%lf\n", getCurrentTime() - start, result);
    }
  }
  return 0;
}
  
  
