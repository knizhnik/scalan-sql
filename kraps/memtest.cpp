#include <stdio.h>
#include <time.h>
#include <sys/time.h>

#define N_ELEMS 1LL*1024*1024*1024

#define elemof(x) decltype(x->elem)

template<class T>
class RDD
{
  public:
    static T elem;
    virtual bool getNext(T& record) = 0;
};

template<class T, bool(*predicate)(T const&), class I = RDD<T> >
class Filter : public RDD<T>
{
  I* in;
public:
  Filter(I* input) : in(input) {}
  bool next(T& val) {
    while(in->next(val)) {
      if (predicate(val)) {
	return true;
      }
    }
    return false;
  } 
    bool getNext(T& val){ 
        return next(val);
    }
};

template<class T, class I, class A, void (*fold)(A&,T const&)>
class Aggregate  : public RDD<A>
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
    bool getNext(A& val){ 
        return next(val);
    }
};

template<class T>
class Cache : public RDD<T>
{
  T* data;
  size_t size;
  size_t curr;
public:
  Cache(T* ptr, size_t len) : data(ptr), size(len), curr(0) {}

  bool next(T& val) {
    if (curr < size) {
      val = data[curr++];
      return true;
    }
    return false;
  }
    bool getNext(T& val){ 
        return next(val);
    }
};

template<class T, bool (*predicate)(T const&), class I>
inline auto filter(I* in)
{
    return new Filter<T, predicate, I>(in);
}

template<class T, class A, void (*fold)(A& dst, T const&), class I>
inline Aggregate<T, I, A, fold>* aggregate(I* in)
{
  return new Aggregate<T, I, A, fold>(in);
}


template<class T>
bool predicate(T const& val) {
  return val >= N_ELEMS/3;
}

void sum(double& dst, double const& src)
{
  dst += src;
}

static time_t getCurrentTime()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec*1000 + tv.tv_usec/1000;
}

auto query(double* arr) 
{
    auto s1 = new Cache<double>(arr, N_ELEMS);
    auto s2 = filter<elemof(s1), predicate>(s1);
    auto s3 = aggregate<double,double,sum>(s2);
    return s3;
}
int main()
{
  double* arr = new double[N_ELEMS];
  for (size_t i = 0; i < N_ELEMS; i++) {
    arr[i] = i;
  }
  {
    time_t start = getCurrentTime();
    double sum = 0;
    for (size_t i = 0; i < N_ELEMS; i++) {
      double val = arr[i];
      if (val >= N_ELEMS/3) { 
	sum += arr[i];
      }
    }
    printf("Elapsed time: %ld, sum=%lf\n", getCurrentTime() - start, sum);
  }
  {
    time_t start = getCurrentTime();
    //    auto i = new Aggregate<double, Filter<double, Cache<double>, predicate>, double, sum>(new Filter<double, Cache<double>, predicate>(new Cache<double>(arr, N_ELEMS)));
    auto i = query(arr);
    double result;
    if (i->next(result)) { 
      printf("Elapsed time: %ld, sum=%lf\n", getCurrentTime() - start, result);
    }
  }
  return 0;
}
  
  