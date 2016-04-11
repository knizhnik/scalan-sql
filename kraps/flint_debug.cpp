#include <unistd.h>
#include <errno.h>
#include "flint_debug.h"


static timeval get_tic() { timeval t; gettimeofday(&t, 0); return t;}
timeval app_start_time = get_tic();

#ifdef _FLINT_PROFILE_

#define STAT_TIMER_MS_TO_NS(msec) ((msec)*1000000ULL)

FlintProfiler* FlintProfiler::instance = nullptr;

void FlintProfiler::handler_wrapper(sigval_t val)
{
    FlintProfiler *profiler = FlintProfiler::instance;

    if (profiler == nullptr)
        return;

    CriticalSection cs(profiler->mutex);

    if (profiler->logPointer == profiler->logSize)
        return;

    memcpy(profiler->log[profiler->logPointer], profiler->current, sizeof(LogRecord_t));
    memset(profiler->current, 0, sizeof(profiler->current));

    profiler->logPointer++;
}

void FlintProfiler::initTimer(uint64_t period)
{
    struct itimerspec its;
    struct sigevent sigev;

    memset (&sigev, 0, sizeof (struct sigevent));
    sigev.sigev_notify = SIGEV_THREAD;
    sigev.sigev_notify_attributes = NULL;
    sigev.sigev_value.sival_ptr = (void*)this;
    sigev.sigev_notify_function = &FlintProfiler::handler_wrapper;

    if (timer_create(CLOCK_REALTIME, &sigev, &timer)) {
        perror("timer_create()");
    }

    its.it_value.tv_sec = period / 1000000000;
    its.it_value.tv_nsec = period % 1000000000;
    its.it_interval.tv_sec = its.it_value.tv_sec;
    its.it_interval.tv_nsec = its.it_value.tv_nsec;

    if (timer_settime(timer, 0, &its, NULL) == -1) {
        perror("timer_settime");
    }
}

void FlintProfiler::stopTimer(void)
{
    timer_delete(timer);
}

FlintProfiler::FlintProfiler(int node, uint64_t timeSlice_ms, uint32_t size, const char *name)
{
    instance = this;
    logSize = size;
    logPointer = 0;
    log = new LogRecord_t[logSize];
    tslice = timeSlice_ms;
    memset(current, 0, sizeof(current));

    initTimer(STAT_TIMER_MS_TO_NS(timeSlice_ms));

    filename = new char[strlen(name) + 10];
    sprintf(filename, "%s_%d.out", name, node);
}

void FlintProfiler::printRecord(FILE* results, ProfileRecord_t *rec)
{
    // double callsPerSlice = rec->callsCount*1000/tslice;
    double sizePerSlice = rec->dataSize*1000.0/tslice/1024/1024; // MB/s
    fprintf(results, "%0.3f", sizePerSlice);
}

void FlintProfiler::printLogRecord(FILE* results, ProfileRecord_t *recs)
{
    printRecord(results, recs++);

    for (uint32_t i = 1; i < PROFILERS_COUNT * DID_BASE_STEP; i++)
    {
        fprintf(results, "\t");
        printRecord(results, recs++);
    }
}

FlintProfiler::~FlintProfiler()
{
    const uint32_t logEnd = logPointer;
    const char *labels[PROFILERS_COUNT] = {"GATHER",
                                            "FILE",
                                            "DIR",
                                            "FILTER",
                                            "REDUCE",
                                            "PROJECT",
                                            "MAP_REDUCE",
                                            "SORT",
                                            "TOP",
                                            "HASH_JOIN",
                                            "SHUFFLE_JOIN",
                                            "CACHED",
                                            "RX",
                                            "FORK_RDD",
                                            "FORK_CHANNEL",
                                            "UNPACK",
                                            "LOCAL_SORT",
                                            "ORDERED_REPL",
                                            "LOCAL_CART",
                                            "A_BLOCK_MUL",
                                            "B_BLOCK_MUL"};
    stopTimer();

    CriticalSection cs(mutex);
    instance = nullptr;

    FILE* results = fopen(filename, "w");

    if (NULL != results)
    {
        fprintf(results, "time");
        for (uint32_t i = 0; i < PROFILERS_COUNT; i++)
            for (uint8_t j = 0; j < DID_BASE_STEP; j++)
                fprintf(results, "\t%s%u", labels[i], j);
        fprintf(results, "\n");

        for (uint32_t i = 0; i < logEnd; i++)
        {
            fprintf(results, "%u\t", i);
            printLogRecord(results, log[i]);
            fprintf(results, "\n");
        }
        fclose(results);
    }
    else
        printf("Profiler: cannot create file %s: %d\n", filename, errno);
    delete[] log;
    delete[] filename;
}

void FlintProfiler::put(Profilee id, uint64_t time, uint64_t size)
{
    CriticalSection cs(mutex);

    current[id].callsCount++;
    current[id].timeTotal += time;
    current[id].dataSize += size;
}

#endif
