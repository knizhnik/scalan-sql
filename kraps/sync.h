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
public:
    virtual void run() = 0;
    virtual~Job() {}
};
    
class Thread
{
public:    
    Thread(Job* job) { 
        pthread_create(&thread,  NULL, trampoline, job);
    }
    ~Thread() { 
        pthread_join(thread, NULL);
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
