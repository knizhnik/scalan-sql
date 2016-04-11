#ifndef __DEBUG_H__
#define __DEBUG_H__

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <inttypes.h>
#include <sys/time.h>
#include <signal.h>
#include <time.h>
#include "sync.h"
#include "cluster.h"

inline unsigned long long elapsedmillis(struct timeval *from)
{
    struct timeval now;

    gettimeofday(&now, 0);

    if (now.tv_usec >= from->tv_usec)
        return((now.tv_sec - from->tv_sec) * 1000 +
               (now.tv_usec - from->tv_usec) / 1000);
    else
        return((now.tv_sec - 1 - from->tv_sec) * 1000 +
               (now.tv_usec + 1000000 - from->tv_usec) / 1000);
}

#define TIC(lastTime) (gettimeofday(&lastTime, 0))
#define TOC(lastTime) (elapsedmillis(&lastTime))

extern timeval app_start_time;
inline void force_print_debug(const char *fmt, ...)
{
    Cluster* cluster = Cluster::instance.get();
    char msg[50];

    va_list args;
    va_start(args, fmt);
    vsprintf(msg, fmt, args);
    va_end(args);

    printf("[%lu] %s (%0.3f)\n", cluster->nodeId, msg, TOC(app_start_time)/1000.0);
    fflush(stdout);
}

#ifdef _DEBUG_
#define print_debug(...) force_print_debug(__VA_ARGS__)
#else
#define print_debug(...)
#endif

#ifdef _FLINT_PROFILE_

#define ENABLE_FLINT_PROFILER(nodeId, tslice_ms, size, filename) FlintProfiler *debug_profiler = new FlintProfiler((nodeId), (tslice_ms), (size), (filename));
#define DISABLE_FLINT_PROFILER()               delete debug_profiler;
#define FLINT_PROFILE(id, size) FLINT_PROFILE_(id, size, __LINE__)
#define FLINT_PROFILE_(id, size, line) FLINT_PROFILE__(id, size, line)
#define FLINT_PROFILE__(id, size, line) ProfilerInjector injector ## line((id), (size));

#define PROFILER_ID(base, did)         ((Profilee)((base) * DID_BASE_STEP + ((did) % DID_BASE_STEP)))

#define GATHER_RDD_ID(did)          PROFILER_ID(GATHER_RDD_ID_BASE, did)
#define FILE_RDD_ID(did)            PROFILER_ID(FILE_RDD_ID_BASE, did)
#define DIR_RDD_ID(did)             PROFILER_ID(DIR_RDD_ID_BASE, did)
#define FILTER_RDD_ID(did)          PROFILER_ID(FILTER_RDD_ID_BASE, did)
#define REDUCE_RDD_ID(did)          PROFILER_ID(REDUCE_RDD_ID_BASE, did)
#define PROJECT_RDD_ID(did)         PROFILER_ID(PROJECT_RDD_ID_BASE, did)
#define MAP_REDUCE_RDD_ID(did)      PROFILER_ID(MAP_REDUCE_RDD_ID_BASE, did)
#define SORT_RDD_ID(did)            PROFILER_ID(SORT_RDD_ID_BASE, did)
#define TOP_RDD_ID(did)             PROFILER_ID(TOP_RDD_ID_BASE, did)
#define HASH_JOIN_RDD_ID(did)       PROFILER_ID(HASH_JOIN_RDD_ID_BASE, did)
#define SHUFFLE_JOIN_RDD_ID(did)    PROFILER_ID(SHUFFLE_JOIN_RDD_ID_BASE, did)
#define CACHED_RDD_ID(did)          PROFILER_ID(CACHED_RDD_ID_BASE, did)
#define RX_READ_ID(did)             PROFILER_ID(RX_READ_ID_BASE, did)

#define FORK_RDD_ID(did)            PROFILER_ID(FORK_RDD_ID_BASE, did)
#define FORK_CHANNEL_ID(did)        PROFILER_ID(FORK_CHANNEL_ID_BASE, did)
#define UNPACK_RDD_ID(did)          PROFILER_ID(UNPACK_RDD_ID_BASE, did)
#define LOCAL_SORT_RDD_ID(did)      PROFILER_ID(LOCAL_SORT_RDD_ID_BASE, did)
#define ORDERED_REPL_RDD_ID(did)    PROFILER_ID(ORDERED_REPL_RDD_ID_BASE, did)
#define LOCAL_CARTESIAN_RDD_ID(did) PROFILER_ID(LOCAL_CARTESIAN_RDD_ID_BASE, did)
#define A_BLOCK_MULT_ID(did)        PROFILER_ID(A_BLOCK_MULT_ID_BASE, did)
#define B_BLOCK_MULT_ID(did)        PROFILER_ID(B_BLOCK_MULT_ID_BASE, did)

#define DID_BASE_STEP               2

enum Profilee
{
    GATHER_RDD_ID_BASE,
    FILE_RDD_ID_BASE,
    DIR_RDD_ID_BASE,
    FILTER_RDD_ID_BASE,
    REDUCE_RDD_ID_BASE,
    PROJECT_RDD_ID_BASE,
    MAP_REDUCE_RDD_ID_BASE,
    SORT_RDD_ID_BASE,
    TOP_RDD_ID_BASE,
    HASH_JOIN_RDD_ID_BASE,
    SHUFFLE_JOIN_RDD_ID_BASE,
    CACHED_RDD_ID_BASE,
    RX_READ_ID_BASE,

    FORK_RDD_ID_BASE,
    FORK_CHANNEL_ID_BASE,
    UNPACK_RDD_ID_BASE,
    LOCAL_SORT_RDD_ID_BASE,
    ORDERED_REPL_RDD_ID_BASE,
    LOCAL_CARTESIAN_RDD_ID_BASE,
    A_BLOCK_MULT_ID_BASE,
    B_BLOCK_MULT_ID_BASE,

    PROFILERS_COUNT
};

typedef struct _ProfileRecord
{
    uint64_t    timeTotal;
    uint64_t    callsCount;
    uint64_t    dataSize;
} ProfileRecord_t;

typedef ProfileRecord_t LogRecord_t[PROFILERS_COUNT * DID_BASE_STEP];

class FlintProfiler
{
  public:
    FlintProfiler(int node, uint64_t timeSlice_ms, uint32_t size, const char *name);
    ~FlintProfiler();

    void put(Profilee id, uint64_t time, uint64_t size);

    static FlintProfiler *instance;

  private:
    char* filename;

    LogRecord_t current;

    LogRecord_t *log;
    uint32_t logSize;
    uint32_t logPointer;
    uint64_t tslice;
    timer_t timer;

    static void handler_wrapper(sigval_t val);
    void initTimer(uint64_t period_ms);
    void stopTimer(void);

    void printRecord(FILE* results, ProfileRecord_t *rec);
    void printLogRecord(FILE* results, ProfileRecord_t *recs);

    Mutex mutex;
};

class ProfilerInjector
{
public:
    ProfilerInjector(Profilee id, size_t outputSize)
    {
        // gettimeofday(&timestamp, 0);
        profileeId = id;
        size = outputSize;
    }

    ~ProfilerInjector()
    {
        if (nullptr != FlintProfiler::instance)
            FlintProfiler::instance->put(profileeId, 0 /* elapsedmillis(&timestamp) */, size);
    }

private:
    // struct timeval timestamp;
    Profilee profileeId;
    size_t size;
};

extern FlintProfiler debug_profiler;


#else // #ifdef _FLINT_PROFILE_

#define ENABLE_FLINT_PROFILER(nodeId, tslice_ms, size, filename)
#define DISABLE_FLINT_PROFILER()
#define FLINT_PROFILE(id, size)

#endif // #ifdef _FLINT_PROFILE_

#endif
