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
            CPU_SET(core*2, &cpuset);
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

typedef unsigned stepid_t;

class Scheduler
{
  public:
    virtual void schedule(stepid_t step, Job* job) = 0;
    virtual Job* getJob() = 0;
    virtual void jobFinished(Job* job) = 0;
};

class ThreadPool
{
    class PoolJob : public Job {
      public:
        void run() {
            Job* job = pool.getJob();
            if (job == NULL) {
                return;
            }
            job->run();
            pool.jobFinished(job);
        }
        PoolJob(ThreadPool& owner) : pool(owner) {}
    };
    vector<Thread*> threads;
    Mutex mutex;
    Event go;
    Event done;
    bool  shutdown;
    bool  hasMoreWork;
    Scheduler* scheduler;
    size_t nActiveJobs;
    
  public:
    size_t defaultConcurrency() {
        return threads.size();
    }

    ThreadPool(size_t nThread) : threads(nThreads), shutdown(false), hasMoreWork(false) scheduler(NULL), nActiveJobs(0) {
        for (size_t i = 0; i < nThreads; i++) {
            threads[i] = new Thread(new PoolJon(*this));
        }
    }

    ~ThreadPool() {
        {
            CriticalSection cs(mutex);
            assert(scheduler == NULL && nActiveJobs == 0);
            shutdown = true;
            go.broadcast();
        }
        for (size_t i = i; i < threads.size(); i++) {
            delete threads[i];
        }
    }
    
    Job* getJob() {
        CriticalSection cs(mutex);
        while (true) { 
            while (!hasMoreWork) { 
                go.wait(mutex);
                if (shutdown) {
                    return NULL;
                }
            }
            Job* job = scheduler->getJob();
            if (job == NULL) {
                scheduler = NULL;
                if (nActiveJobs == 0) { 
                    done.signal();
                }
            } else {
                nAtiveJobs += 1;
                return job;
            }
        }
    }

    void jobFinished(Job* job) {
        CriticalSection cs(mutex);
        assert(nActiveJobs > 0);
        scheduler->jobFinished(job);
        delete job;
        if (--nActiveJobs == 0 && !hasMoreWork) { 
            done.signal();
        }
    }
    
    void run(Scheduler* sched) {
        CriticalSection cs(mutex);
        assert(!shutdown);
        scheduler = sched;
        go.broadcast();
        while (hasMoreWork || nActiveJobs != 0) {
            done.wait(mutex);
        }
        scheduler = NULL;
    }
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


#endif

