#ifndef __SYNC_H__
#define __SYNC_H__

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#elif defined( _INTEGRITY )
#include <INTEGRITY.h>
#define MCO_CFG_NATIVE_TLS 0
#define INTEGRITY_THREAD_STACK_SIZE 1024*512
#elif defined( _ECOS )
#include <cyg/kernel/kapi.h>
#define MCO_CFG_NATIVE_TLS 0
#define ECOS_THREAD_STACK_SIZE 1024*512
#else
#include <unistd.h>
#include <pthread.h>
#ifdef _VXWORKS
#define MCO_CFG_NATIVE_TLS 0
/*#ifndef _VX_TOOL_FAMILY=gnu
  #include <taskVarLib.h>
  #endif*/
#endif
#endif

#if MCO_CFG_USE_EXCEPTIONS
#include <new>
#endif

#ifndef MCO_CFG_NATIVE_TLS
#ifdef _RTP
#define MCO_CFG_NATIVE_TLS 0
#else
#define MCO_CFG_NATIVE_TLS 1
#endif
#endif

/* TLS_OUT_OF_INDEXES not defined on WinCE */
#ifndef TLS_OUT_OF_INDEXES
#define TLS_OUT_OF_INDEXES 0xffffffff
#endif

namespace McoSql
{

    #if MCO_CFG_USE_EXCEPTIONS
    #define MCO_SYS_CHECK(call) do { int _rc = call; if (_rc != 0) throw SystemError(_rc, __FILE__, __LINE__); } while (0)
    #elif defined(NDEBUG)
    #define MCO_SYS_CHECK(call) call
    #else 
    #define MCO_SYS_CHECK(call) do { int _rc = call; assert(_rc == 0); } while (0)
    #endif
    

    #ifdef _WIN32

    #if MCO_CFG_NATIVE_TLS
    class ThreadContext
    {
        typedef void(*destructor_t)(void*);
        unsigned int index;

      public:
        void* get()
        {
            return TlsGetValue(index);
        }

        void set(void* value)
        {
            TlsSetValue(index, value);
        }

        void attach() {}
        void detach() {}

        ThreadContext(destructor_t)
        {
            index = TlsAlloc();
            assert(index != TLS_OUT_OF_INDEXES);
        }

        ~ThreadContext()
        {
            TlsFree(index);
        }
    };
    #endif

    class Mutex
    {
        CRITICAL_SECTION cs;
      public:
        Mutex()
        {
            InitializeCriticalSection(&cs);
        }
        ~Mutex()
        {
            DeleteCriticalSection(&cs);
        }
        void lock()
        {
            EnterCriticalSection(&cs);
        }
        void unlock()
        {
            LeaveCriticalSection(&cs);
        }
    };

    class Thread
    {
        HANDLE h;
      public:
        typedef void(*thread_proc_t)(void*);

        static size_t getThreadId() {
            return (long)GetCurrentThreadId();
        }

        static int getNumberOfProcessors()
        {
            SYSTEM_INFO sysinfo;
            GetSystemInfo(&sysinfo);
            return sysinfo.dwNumberOfProcessors;
        }

        void create(thread_proc_t f, void* arg)
        {
            DWORD threadid;
            h = CreateThread(NULL, 0, LPTHREAD_START_ROUTINE(f), arg, 0, &threadid);
        }

        void detach()
        {
            if (h != NULL)
            {
                CloseHandle(h);
                h = NULL;
            }
        }

        void join()
        {
            WaitForSingleObject(h, INFINITE);
            CloseHandle(h);
            h = NULL;
        }

        Thread()
        {
            h = NULL;
        }

        ~Thread()
        {
            if (h != NULL)
            {
                CloseHandle(h);
            }
        }
    };

    class Semaphore
    {
        HANDLE s;

        enum
        {
            MaxSemValue = 1000000
        };

      public:
        void wait(Mutex &mutex)
        {
            mutex.unlock();
            int rc = WaitForSingleObject(s, INFINITE);
            assert(rc == WAIT_OBJECT_0 || rc == WAIT_TIMEOUT);
            mutex.lock();
        }
        void signal(unsigned inc = 1)
        {
            if (inc != 0)
            {
                ReleaseSemaphore(s, inc, NULL);
            }
        }
        Semaphore(unsigned initValue = 0)
        {
            s = CreateSemaphore(NULL, initValue, MaxSemValue, NULL);
            assert(s != NULL);
        }
        ~Semaphore()
        {
            CloseHandle(s);
        }
    };

    #elif defined( _INTEGRITY )

    class Thread
    {
        Task t;
      public:
        typedef void(*thread_proc_t)(void);

        static size_t getThreadId() {
            ::Value v;
            GetTaskUniqueId( CurrentTask(), &v );
            return (size_t)v;
        }

        static int getNumberOfProcessors()
        {
            return 1;
        }

        void create(thread_proc_t f, void* arg)
        {
            ::Value P,W;

            GetPriorityAndWeight(CurrentTask(),&P,&W);
            CommonCreateTask( P, (Address)f, INTEGRITY_THREAD_STACK_SIZE, "T", &t );
            SetTaskIdentification( t,(Address)arg );
            SetPriorityAndWeight( t, P, W, true);
            RunTask( t );
        }

        void detach()
        {
        }

        void join()
        {
            for (;;) {
                Error E;
                ::Value TS;
                Address A1, A2;

                E = GetTaskStatus( t, &TS, &A1, &A2);

                assert( E == Success );

                if ( TS == StatNoProcess ) {
                    break;
                }

                Yield();
            }
        }

        Thread()
        {
        }

        ~Thread()
        {
        }
    };

    class Mutex
    {
        ::Semaphore s;
      public:

        Mutex()
        {
            Error e;
            e = CreateBinarySemaphore( &s );
            assert ( Success == e );
        }

        ~Mutex()
        {
            CloseSemaphore( s );
        }

        void lock()
        {
            Error e;
            e = WaitForSemaphore( s );
            assert ( Success == e );
        }

        void unlock()
        {
            Error e;
            e = ReleaseSemaphore( s );
            assert ( Success == e );
        }
    };

    class Semaphore
    {
        ::Semaphore s; /* system sync */
      public:
        void wait(Mutex &mutex)
        {
            Error e;

            mutex.unlock();
            e = WaitForSemaphore( s );
            assert ( Success == e );
            mutex.lock();
        }

        void signal(unsigned inc = 1)
        {
            unsigned i;

            for ( i=0; i<inc; i++ ) {
                Error e;
                e = ReleaseSemaphore( s );
                assert ( Success == e );
            }
        }

        Semaphore(unsigned initValue = 0)
        {
            Error e;
            e = CreateSemaphore(initValue, &s );
            assert ( Success == e );
        }

        ~Semaphore()
        {
            CloseSemaphore( s );
        }
    };

    #elif defined( _THREADX )
    #include <th_api.h>

    class Thread
    {
      public:
        static size_t getThreadId() {
            return (size_t)tx_thread_identify();
        }
    };

    class Mutex
    {
        TX_MUTEX cs;
      public:
        Mutex() {
            char name[32];
            sprintf(name, "Mutex-%p", this);
            UINT status = tx_mutex_create(&cs, name, TX_NO_INHERIT);
            assert(status == TX_SUCCESS);
        }

        ~Mutex() {
            UINT status = tx_mutex_delete(&cs);
            assert(status == TX_SUCCESS);
        }

        void lock() {
            UINT status = tx_mutex_get(&cs, TX_WAIT_FOREVER);
            assert(status == TX_SUCCESS);

        }
        void unlock() {
            UINT status = tx_mutex_put(&cs);
            assert(status == TX_SUCCESS);
        }
    };

    #undef MCO_CFG_NATIVE_TLS
    #define MCO_CFG_NATIVE_TLS 0

    #elif defined( _ECOS )
    #include <cyg/kernel/kapi.h>

    class Thread
    {
      public:
        typedef void(*thread_proc_t)(void*);

      private:
        cyg_sem_t      w;
        cyg_handle_t   h;
        cyg_thread     t;
        void         * a;
        thread_proc_t  f;
        char           stack[ECOS_THREAD_STACK_SIZE];

      public:
        static size_t getThreadId() {
            return (size_t)cyg_thread_get_id(cyg_thread_self());
        }

        static int getNumberOfProcessors()
        {
            return 1;
        }

        static void start_ecos_thread( cyg_addrword_t _t ) {
            Thread * t = (Thread*)_t;

            t->f(t->a);
            cyg_semaphore_post( &t->w );
        }

        void create(thread_proc_t f, void* arg)
        {
            this->f = f;
            a = arg;
            cyg_semaphore_init( &w, 0);
            cyg_thread_create( cyg_thread_get_priority(cyg_thread_self()), start_ecos_thread, (cyg_addrword_t)this, "exdbsqlt", stack, sizeof(stack), &h, &t );
            cyg_thread_resume( h );
        }

        void detach()
        {
        }

        void join()
        {
            cyg_semaphore_wait(&w);
        }

        Thread()
        {
            a = 0;
            f = 0;
        }

        ~Thread()
        {
            cyg_thread_delete( h );
            cyg_semaphore_destroy( &w );
        }
    };

    class Mutex
    {
        cyg_sem_t      w;
      public:

        Mutex()
        {
            cyg_semaphore_init( &w, 1);
        }

        ~Mutex()
        {
            cyg_semaphore_destroy( &w );
        }

        void lock()
        {
            cyg_semaphore_wait(&w);
        }

        void unlock()
        {
            cyg_semaphore_post( &w );
        }
    };

    class Semaphore
    {
        cyg_sem_t      w;
      public:
        void wait(Mutex &mutex)
        {
            mutex.unlock();
            cyg_semaphore_wait(&w);
            mutex.lock();
        }

        void signal(unsigned inc = 1)
        {
            unsigned i;

            for ( i=0; i<inc; i++ ) {
                cyg_semaphore_post( &w );
            }
        }

        Semaphore(unsigned initValue = 0)
        {
            cyg_semaphore_init( &w, initValue);
        }

        ~Semaphore()
        {
            cyg_semaphore_destroy( &w );
        }
    };
    #else /* Generic Unix */

    #if MCO_CFG_NATIVE_TLS
    class ThreadContext
    {
        #ifdef _VXWORKS
        typedef void(*destructor_t)(void*);
        pthread_key_t key;
        volatile void* value;

      public:
        ThreadContext(){};

        void* get()
        {
            return ((void*)value);
        }

        void set(void* val)
        {
            value = val;
        }

        void attach()
        {
            value = NULL;
            MCO_SYS_CHECK(taskVarAdd(0, (int *)&value));
        }

        void detach()
        {
            MCO_SYS_CHECK(taskVarDelete(0, (int *)&value));
        }


        ThreadContext(destructor_t _destructor)
        {
            attach();
        }

        ~ThreadContext()
        {
        }
        #else
        typedef void(*destructor_t)(void*);
        pthread_key_t key;

      public:
        ThreadContext(){};

        void* get()
        {
            return pthread_getspecific(key);
        }

        void set(void* value)
        {
            MCO_SYS_CHECK(pthread_setspecific(key, value));
        }


        void attach() {}
        void detach() {}

        ThreadContext(destructor_t _destructor)
        {
            MCO_SYS_CHECK(pthread_key_create(&key, _destructor));
        }

        ~ThreadContext()
        {
            pthread_key_delete(key);
        }
        #endif
    };
    #endif

    #ifdef _VXWORKS
    #include <sysLib.h>
    #include <taskLib.h>
    #include <ioLib.h>
    #endif
    #if defined(_VXWORKS)
    class Mutex
    {
      public:
        SEM_ID cs;
        Mutex()
        {
            cs = semCCreate(SEM_Q_PRIORITY, 1);
        }
        ~Mutex()
        {
            semDelete(cs);
        }
        void lock()
        {
            semTake(cs, WAIT_FOREVER);
        }
        void unlock()
        {
            semGive(cs);
        }
    };
    #else
    class Mutex
    {
      public:
        pthread_mutex_t cs;
        Mutex()
        {
            MCO_SYS_CHECK(pthread_mutex_init(&cs, NULL));
        }
        ~Mutex()
        {
            pthread_mutex_destroy(&cs);
        }
        void lock()
        {
            MCO_SYS_CHECK(pthread_mutex_lock(&cs));
        }
        void unlock()
        {
            MCO_SYS_CHECK(pthread_mutex_unlock(&cs));
        }
    };
    #endif
    class Thread
    {
        pthread_t thread;
      public:
        typedef void(*thread_proc_t)(void*);

        static int getNumberOfProcessors();

        #ifdef _VXWORKS

        static size_t getThreadId() {
            return (long)taskIdSelf();
        }

        void create(thread_proc_t f, void* arg)
        {
            thread = taskSpawn ( NULL,100,0,32*1024,(FUNCPTR)f, (int)arg,
                                 0,0,0,0,0,0,0,0,0);
        }

        void detach()
        {
        }

        void join()
        {
            while (taskIdVerify(thread)==OK) {taskDelay( 100 * sysClkRateGet() / 1000);};
        }

        #else
        static size_t getThreadId() {
            return (long)pthread_self();
        }

        void create(thread_proc_t f, void* arg)
        {
            pthread_attr_t attr;
            pthread_attr_init(&attr);
            #if defined(_AIX41)
            /* At AIX 4.1, 4.2 threads are by default created detached*/
            pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_UNDETACHED);
            #endif
            MCO_SYS_CHECK(pthread_create(&thread, &attr, (void *(*)(void*))f, arg));
            pthread_attr_destroy(&attr);
        }

        void detach()
        {
            pthread_detach(thread);
        }

        void join()
        {
            void* result;
            MCO_SYS_CHECK(pthread_join(thread, &result));
        }
        #endif
    };

    #if defined(_SC_NPROCESSORS_ONLN)
    inline int Thread::getNumberOfProcessors() {
        return sysconf(_SC_NPROCESSORS_ONLN);
    }
    #elif defined(__linux__)
    #include <linux/smp.h>
    inline int Thread::getNumberOfProcessors() { return smp_num_cpus; }
    #elif defined(__FreeBSD__) || defined(__bsdi__) || defined(__OpenBSD__) || defined(__NetBSD__)
    #if defined(__bsdi__) || defined(__OpenBSD__)
    #include <sys/param.h>
    #endif
    #include <sys/sysctl.h>
    inline int Thread::getNumberOfProcessors() {
        int mib[2],ncpus=0;
        size_t len=sizeof(ncpus);
        mib[0]= CTL_HW;
        mib[1]= HW_NCPU;
        sysctl(mib,2,&ncpus,&len,NULL,0);
        return ncpus;
    }
    #else
    inline int Thread::getNumberOfProcessors() {
        return 1;
    }
    #endif

    #if defined(_VXWORKS)
    class Semaphore
    {
        SEM_ID sem;
      public:
        void wait(Mutex &mutex)
        {
            semTake(sem,WAIT_FOREVER);
        }

        void signal(unsigned inc = 1)
        {
            while(inc--) {semGive(sem);}
        }

        Semaphore(unsigned initValue = 0)
        {
            sem = semCCreate(SEM_Q_PRIORITY, initValue);
        }

        ~Semaphore()
        {
            semDelete(sem);
        }
    };

    #else
    class Semaphore
    {
        pthread_cond_t cond;
        int count;
      public:
        void wait(Mutex &mutex)
        {
            while (count == 0)
            {
                pthread_cond_wait(&cond, &mutex.cs);
            }
            count -= 1;
        }

        void signal(unsigned inc = 1)
        {
            count += inc;
            if (inc > 1)
            {
                MCO_SYS_CHECK(pthread_cond_broadcast(&cond));
            }
            else if (inc == 1)
            {
                MCO_SYS_CHECK(pthread_cond_signal(&cond));
            }
        }

        Semaphore(unsigned initValue = 0)
        {
            MCO_SYS_CHECK(pthread_cond_init(&cond, NULL));
            count = initValue;
        }

        ~Semaphore()
        {
            pthread_cond_destroy(&cond);
        }
    };
    #endif /* Generic Unix*/
    #endif /* System specific part*/

    #if !MCO_CFG_NATIVE_TLS

    #ifndef MCO_CFG_TLS_TABLE_SIZE
    #define MCO_CFG_TLS_TABLE_SIZE 1009
    #endif

    class ThreadContext
    {
        typedef void(*destructor_t)(void*);
        struct Binding {
            size_t threadId;
            void*  data;
        };
        Mutex mutex;
        Binding bindings[MCO_CFG_TLS_TABLE_SIZE];

      public:
        void* get()
        {
            size_t id = Thread::getThreadId();
            size_t i = id;
            while (true) {
                size_t h = i++ % MCO_CFG_TLS_TABLE_SIZE;
                if (bindings[h].threadId == id) {
                    return bindings[h].data;
                }
            }
        }

        void set(void* value)
        {
            size_t id = Thread::getThreadId();
            size_t i = id;
            while (true) {
                size_t h = i++ % MCO_CFG_TLS_TABLE_SIZE;
                if (bindings[h].threadId == id) {
                    bindings[h].data = value;
                    break;
                }
            }
        }

        void attach()
        {
            mutex.lock();
            size_t id = Thread::getThreadId();
            size_t i = id;
            while (true) {
                size_t h = i++ % MCO_CFG_TLS_TABLE_SIZE;
                if (bindings[h].threadId == id) {
                    break;
                }
                if (bindings[h].threadId == 0) {
                    bindings[h].threadId = id;
                    bindings[h].data = NULL;
                    break;
                }
            }
            mutex.unlock();
        }

        void detach()
        {
            mutex.lock();
            size_t id = Thread::getThreadId();
            size_t i = id;
            while (true) {
                size_t h = i++ % MCO_CFG_TLS_TABLE_SIZE;
                if (bindings[h].threadId == id ) {
                    bindings[h].threadId = 0;
                    bindings[h].data = NULL;
                    break;
                }
            }
            mutex.unlock();
        }

        ThreadContext(destructor_t)
        {
            memset(bindings, 0, sizeof(bindings));
            attach();
        }
    };
    #endif

    class CriticalSection
    {
        Mutex& mutex;
      public:
        CriticalSection(Mutex& cs) : mutex(cs) {
            mutex.lock();
        }
        ~CriticalSection() {
            mutex.unlock();
        }
    };

    typedef void (*job_t)(int threadId, int nThreads, void* arg);

    class ThreadPool
    {
        int nWorkers;
        int nProcessors;
        Mutex mutex;
        Mutex sync;
        Semaphore start;
        Semaphore finish;
        bool stop;
        int  workId;
        McoSqlException::ErrorCode errorCode;

        struct Worker {
            Thread thread;
            ThreadPool* pool;
        };
        Worker* workers;
        void* arg;
        job_t job;

#ifndef _INTEGRITY
        static void executor(void* arg)
#else
        static void executor(void)
#endif
        {
            Worker* worker;
            ThreadPool* pool;
#ifdef _INTEGRITY
            Address arg;
            Error   e;
            Task    task = CurrentTask();

            e = GetTaskIdentification( task, &arg );
            assert ( e == Success );
#endif
            worker = (Worker*)arg;
            pool = worker->pool;
            int workId;

            while (true) {
                {
                    CriticalSection cs(pool->sync);
                    pool->start.wait(pool->sync);
                    if (pool->stop) {
                        pool->finish.signal();
                        break;
                    }
                    workId = pool->workId++;
                }
#if MCO_CFG_USE_EXCEPTIONS
                try {
                    pool->job(workId, pool->nWorkers, pool->arg);
                } catch (McoSqlException const &x) {
                    pool->errorCode = x.code;
                    fprintf(stderr, "Exception %s in thread %d\n", x.getMessage()->cstr(), workId);
                } catch (std::bad_alloc const&) {
                    pool->errorCode = McoSqlException::NOT_ENOUGH_MEMORY;
                    fprintf(stderr, "Not enough memory in thread %d\n", workId);
                }
#else
                pool->job(workId, pool->nWorkers, pool->arg);
#endif
                {
                    CriticalSection cs(pool->sync);
                    pool->finish.signal();
                }
            }
        }

        void wait() {
            workId = 0;
            start.signal(nWorkers);
            for (int i = 0; i < nWorkers; i++) {
                finish.wait(sync);
            }
        }

        void init() {
            if (nWorkers == 0) {
                nWorkers = nProcessors == 0 ? Thread::getNumberOfProcessors() : nProcessors;
                workers = new Worker[nWorkers];
                stop = false;
                for (int i = 0; i < nWorkers; i++) {
                    workers[i].pool = this;
                    workers[i].thread.create(executor, &workers[i]);
                }
            }
        }

      public:
        int nThreads() {
            CriticalSection cs(mutex);
            init();
            return nWorkers;
        }

        ThreadPool(int nCores = 0) {
            nProcessors = nCores;
            nWorkers = 0;
            workers = NULL;
        }

        ~ThreadPool() {
            if (nWorkers != 0) {
                {
                    CriticalSection cs(sync);
                    stop = true;
#ifdef _WIN32
                    start.signal(nWorkers);
                }
                for (int i = 0; i < nWorkers; i++) {
                    workers[i].thread.join();
                }
#else
                    wait();
                }
#endif
                delete[] workers;
            }
        }

        void execute(job_t job, void* arg) {
            CriticalSection cs1(mutex);
            init();
            CriticalSection cs2(sync);
            this->job = job;
            this->arg = arg;
            errorCode = McoSqlException::NO_SQL_ERROR;
            wait();
            if (errorCode != McoSqlException::NO_SQL_ERROR) {
                MCO_THROW McoSqlException(errorCode);
            }
        }
    };
}
#endif
