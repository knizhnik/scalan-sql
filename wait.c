#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>


int main() {
    struct timespec abs_ts;
    struct timeval cur_tv;
    time_t start;
    int rc;
    pthread_cond_t cond;
    pthread_mutex_t mutex;

    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&cond, NULL);
    
    gettimeofday(&cur_tv, NULL);
    abs_ts.tv_sec = cur_tv.tv_sec + 1;
    abs_ts.tv_nsec = cur_tv.tv_usec * 1000;
    start = cur_tv.tv_sec*1000000 + cur_tv.tv_usec;
    rc = pthread_cond_timedwait(&cond, &mutex, &abs_ts);
    gettimeofday(&cur_tv, NULL);
    printf("rc=%d, delta=%ld\n", rc, (cur_tv.tv_sec*1000000 + cur_tv.tv_usec) - start);
    return 0;
}
    
