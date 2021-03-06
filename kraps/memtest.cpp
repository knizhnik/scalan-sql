#include <stdio.h>
#include <time.h>
#include <sys/time.h>

#define N_ELEMS 1LL*1024*1024*1024

#define TILE_SIZE 16

template<class T>
class Iterator {
public:
  //  virtual bool next(T& val) = 0;
};


template<class T, class I, bool(*predicate)(T const&)>
class Filter : public Iterator<T>
{
  I* in;
public:
  Filter(I* input) : in(input) {}
  bool next(T& val) {
    while (in->next(val)) {
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

  bool next(A& val) {
    if (!first) {
      return false;
    }
    double agg = 0;
    T x; 
    while (in->next(x)) {
      fold(agg, x);
    }
    val = agg;
    first = false;
    return true;
  }
};

template<class T, class C, size_t(*fetch)(T&dst, C const& src, size_t i, size_t n)>
class Cache : public Iterator<T>
{
  C const& data;
  size_t size;
  size_t curr;
public:
  Cache(C const& ptr, size_t len) : data(ptr), size(len), curr(0) {}

  bool next(T& val) {
    if (curr < size) {
      curr = fetch(val, data, curr, size);
      return true;
    }
    return false;
  }
};

template<class T, bool (*predicate)(T const& tile), class I>
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
    double* l_quantity;
    double* l_extendedprice;
    double* l_discount;
    double* l_tax;
    date_t* l_shipdate;
};


struct LineitemTile {
  Lineitem const* data;
  size_t pos;
};

bool predicate(LineitemTile const& tile) {
    return tile.data->l_shipdate[tile.pos] >= N_ELEMS/3;
}

void sum(double& dst, LineitemTile const& tile)
{
  double total = 0;
  Lineitem const* l = tile.data;
  size_t i = tile.pos;
  dst += l->l_quantity[i]*l->l_extendedprice[i]*(1-l->l_discount[i])*(1+l->l_tax[i]);
}

size_t fetch(LineitemTile& tile, Lineitem const& l, size_t i, size_t n)
{
  tile.data = &l;
  tile.pos = i;
  return i + 1;
}

static time_t getCurrentTime()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec*1000 + tv.tv_usec/1000;
}




int main()
{
  Lineitem lineitem;
  lineitem.l_extendedprice = new double[N_ELEMS];
  lineitem.l_discount = new double[N_ELEMS];
  lineitem.l_tax = new double[N_ELEMS];
  lineitem.l_quantity = new double[N_ELEMS];
  lineitem.l_shipdate = new date_t[N_ELEMS];
  for (size_t i = 0; i < N_ELEMS; i++) {
    lineitem.l_extendedprice[i] = i;
    lineitem.l_discount[i] = i;
    lineitem.l_tax[i] = i;
    lineitem.l_quantity[i] = i;
    lineitem.l_shipdate[i] = i;
  }
  if (1) {
    time_t start = getCurrentTime();
    double sum = 0;
    for (size_t i = 0; i < N_ELEMS; i++) {
      if (lineitem.l_shipdate[i] >= N_ELEMS/3) { 
	sum += lineitem.l_quantity[i]*lineitem.l_extendedprice[i]*(1-lineitem.l_discount[i])*(1+lineitem.l_tax[i]);
      }
    }
    printf("Elapsed time: %ld, sum=%lf\n", getCurrentTime() - start, sum);
  }
  {
    time_t start = getCurrentTime();
    //    auto i = new Aggregate<double, Filter<double, Cache<double>, predicate>, double, sum>(new Filter<double, Cache<double>, predicate>(new Cache<double>(arr, N_ELEMS)));
    auto i = aggregate<LineitemTile,double,sum>(filter<LineitemTile,predicate>(new Cache<LineitemTile,Lineitem,fetch>(lineitem, N_ELEMS)));
    double result;
    if (i->next(result)) { 
      printf("Elapsed time: %ld, sum=%lf\n", getCurrentTime() - start, result);
    }
  }
  return 0;
}
  
  
