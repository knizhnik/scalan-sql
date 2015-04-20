#include <pthread.h>

class Event;

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

class Event
{
public:
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

class Job {
public:
    virtual void run() = 0;
    virtual~Job() {}
};
    
class Thread
{
public:    
    Thread(Job* job) { 
        pthread_create(&thread,  NULL, trampoline, job);
        pthread_detach(thread);
    }

private:
    pthread_t thread;

    static void* trampoline(void* arg) { 
        Job* job = (Job*)arg;
        job->run();
        delete job;
        return NULL;
    }
};
