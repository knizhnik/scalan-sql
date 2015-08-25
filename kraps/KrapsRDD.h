#include "rdd.h"
#include "pack.h"

class KrapsIterator
{
  public:
    static ThreadLocal<JNIEnv> env;

    virtual void* next() = 0;
    virual ~KrapsIterator() = 0;
};

    
