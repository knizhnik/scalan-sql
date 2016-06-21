#pragma once

// compile all system lua code into binary, making it self-contained
#define COMPILED_LUA
// include some debug facilities(like .print_tree system command)
#define JIT_DEBUG

#include <stdint.h>

#define PATCHER 0
#define KERNEL 0
#define LEARNER 0
#define READER 0
#define TIMING 0

#define LOG_MSG(hdr, msg, ...) fprintf(stderr, hdr " " msg "\n", ##__VA_ARGS__)

#define JLOG(sys, msg, ...) if(sys) { LOG_MSG("[" #sys "]", msg, ##__VA_ARGS__); }
#define JERR(sys, msg, ...) if(sys) { LOG_MSG("[" #sys "]", "Error: " msg, ##__VA_ARGS__); }

typedef int bool;
#define true 1
#define false 0

#define MEASURE(x) u64 x = rte_rdtsc()
#define REPORT(t0, msg) JLOG(TIMING, msg " %lu ticks", rte_rdtsc() - t0)

static inline uint64_t
rte_rdtsc(void)
{
	union {
		uint64_t tsc_64;
		struct {
			uint32_t lo_32;
			uint32_t hi_32;
		};
	} tsc;

#ifdef RTE_LIBRTE_EAL_VMWARE_TSC_MAP_SUPPORT
	if (unlikely(rte_cycles_vmware_tsc_map)) {
		/* ecx = 0x10000 corresponds to the physical TSC for VMware */
		asm volatile("rdpmc" :
		             "=a" (tsc.lo_32),
		             "=d" (tsc.hi_32) :
		             "c"(0x10000));
		return tsc.tsc_64;
	}
#endif

	asm volatile("rdtsc" :
		     "=a" (tsc.lo_32),
		     "=d" (tsc.hi_32));
	return tsc.tsc_64;
}
