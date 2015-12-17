#include <stdio.h>
#include <time.h>
#include <sys/time.h>

#define N_ELEMS 1LL*1024*1024*1024

#define TILE_SIZE 16

template<class T>
class Iterator {
public:
  virtual bool next(T& val) = 0;
};


template<class T, class I, void(*predicate)(T& dst)>
class Filter : public Iterator<T>
{
  I* in;
public:
  Filter(I* input) : in(input) {}
  bool next(T& val) {
    while (in->next(val)) {
      predicate(val);
      if (val.size != 0) {
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
    val = 0;
    T x; 
    while (in->next(x)) {
      fold(val, x);
    }
    first = false;
    return true;
  }
};

template<class T, class C, void(*fetch)(T&dst, C const& src, size_t i, size_t n)>
class Cache : public Iterator<T>
{
  C const& data;
  size_t size;
  size_t curr;
public:
  Cache(C const& ptr, size_t len) : data(ptr), size(len), curr(0) {}

  bool next(T& val) {
    if (curr < size) {
      size_t n = curr + TILE_SIZE > size ? size - curr : TILE_SIZE;
      fetch(val, data, curr, n);
      val.size = n;
      curr += n;
      return true;
    }
    return false;
  }
};

template<class T, void (*predicate)(T& tile), class I>
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
    double l_quantity[TILE_SIZE];
    double l_extendedprice[TILE_SIZE];
    double l_discount[TILE_SIZE];
    double l_tax[TILE_SIZE];
    date_t l_shipdate[TILE_SIZE];
    size_t size;
} __attribute__((aligned(16)));

void predicate(LineitemTile& tile) {
    bool   bitmap[TILE_SIZE];
    size_t n = tile.size;
    for (size_t i = 0; i < n; i++) {
      bitmap[i] = tile.l_shipdate[i] >= N_ELEMS/3;
    }
    n = 0;
    for (size_t i = 0; i < tile.size; i++) {
      if (bitmap[i]) {
	tile.l_quantity[n] = tile.l_quantity[i];
	tile.l_extendedprice[n] = tile.l_extendedprice[i];
	tile.l_discount[n] = tile.l_discount[i];
	tile.l_tax[n] = tile.l_tax[i];
	n++;
      }
    }
    tile.size = n;
}

void sum(double& dst, LineitemTile const& tile)
{
  double total = 0;
  for (size_t i = 0, n = tile.size; i < n; i++) { 
    total += tile.l_quantity[i]*tile.l_extendedprice[i]*(1-tile.l_discount[i])*(1+tile.l_tax[i]);
  }
  dst += total;
}

void fetch(LineitemTile& tile, Lineitem const& l, size_t i, size_t n)
{
  for (size_t j = 0; j < n; j++) {
    tile.l_quantity[j] = l.l_quantity[i+j];
    tile.l_extendedprice[j] = l.l_extendedprice[i+j];
    tile.l_discount[j] = l.l_discount[i+j];
    tile.l_tax[j] = l.l_tax[i+j];
    tile.l_shipdate[j] = l.l_shipdate[i+j];
  }
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
  
  
