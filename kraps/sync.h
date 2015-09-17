#ifndef __SYNC_H__
#define __SYNC_H__

#include <pthread.h>

class Event;
class Thread;
class Cluster;

#define SMP_SUPPORT 1

class Mutex
{
    friend class Event;
public:
    Mutex()
    {
        pthread_mutex_init(&cs, NULL);
    }
    ~Mutex()
    {
        pthread_mutex_destroy(&cs);
    }
    void lock()
    {
        pthread_mutex_lock(&cs);
    }
    void unlock()
    {
        pthread_mutex_unlock(&cs);
    }
private:
    pthread_mutex_t cs;
};

class CriticalSection
{
public:
    CriticalSection(Mutex& cs) : mutex(cs) {
        mutex.lock();
    }
    ~CriticalSection() {
        mutex.unlock();
    }
private:
    Mutex& mutex;
};

class Event
{
public:
    Event() { 
        pthread_cond_init(&cond, NULL);
    }
    ~Event() { 
        pthread_cond_destroy(&cond);
    }
    void wait(Mutex& mutex) { 
         pthread_cond_wait(&cond, &mutex.cs);
    }
    void broadcast() { 
        pthread_cond_broadcast(&cond);
    }
    void signal() { 
        pthread_cond_signal(&cond);
    }
private:
    pthread_cond_t cond;
};   

class Semaphore 
{ 
public:
    void wait(Mutex& mutex, size_t n) { 
        CriticalSection cs(mutex);
        while (n != count) { 
            event.wait(mutex);
        }
        count = 0;
    }
    void signal(Mutex& mutex) {
        CriticalSection cs(mutex);
        count += 1;
        event.signal();
    }

    Semaphore() : count(0) {}
private:
    Event event;
    size_t count;
};


class Job {
protected:
    friend class Thread;
    Cluster* cluster;
public:
    Job();
    virtual void run() = 0;
    virtual~Job() {}
};
    
#define SAME_CORE ((size_t)-1)

class Thread
{
public:    
    Thread(Job* job, size_t core = SAME_CORE) {
#ifdef SET_THREAD_AFFINITY
        pthread_attr_t attr;
        cpu_set_t cpuset;
        pthread_attr_init(&attr);   
        if (core == SAME_CORE) { 
            pthread_getaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
        } else { 
            CPU_ZERO(&cpuset);
            CPU_SET(core, &cpuset);
        }
        pthread_attr_setaffinity_np(&attr, sizeof(cpuset), &cpuset);
        pthread_create(&thread, &attr, trampoline, job);
#else
        pthread_create(&thread, NULL, trampoline, job);
#endif
    }
    ~Thread() { 
        pthread_join(thread, NULL);
    }

private:
    pthread_t thread;

    static void* trampoline(void* arg); 
};

#if SMP_SUPPORT
template<class T>
class ThreadLocal
{
    pthread_key_t key;
  public:
    T* get() {
        return (T*)pthread_getspecific(key);
    }
    void set(T* val) {
        pthread_setspecific(key, val);
    }

    void operator=(T* val) { set(val); }
    T* operator->() { return get(); }
    operator T*() { return get(); }

    ThreadLocal() {
        pthread_key_create(&key, NULL);
    }
    ~ThreadLocal() {
        pthread_key_delete(key);
    }
};
#else
template<class T>
class ThreadLocal
{
    T* data;
    T* get() { return data; }
    void set(T* val) { data = val; }

    void operator=(T* val) { set(val); }
    T* operator->() { return get(); }
    operator T*() { return get(); }

    ThreadLocal() : data(NULL) {}
};
#endif


template<class T>
class BlockAllocator
{
    enum { BlockSize=1024 };
    struct Block {
        Block* next;
        T data[BlockSize];

        Block(Block* chain) : next(chain) {}
    };
    size_t used;
    Block* chain;
    Block* free;
    
  public:
    void reset() {
        free = chain;
        chain = NULL;
        used = 0;
    }
    T* alloc() {
        if (used == BlockSize) {
            if (free != NULL) {
                chain = free;
                free = chain->next;
            } else { 
                chain = new Block(chain);
            }
            used = 0;
        }
        return &chain->data[used++];
    }
    BlockAllocator() : used(BlockSize), chain(NULL), free(NULL) {}
    ~BlockAllocator() {
        Block *curr, *next;
        for (curr = chain; curr != NULL; curr = next) {
            next = curr->next;
            delete curr;
        }
        for (curr = free; curr != NULL; curr = next) {
            next = curr->next;
            delete curr;
        }
    }
};

#endif

