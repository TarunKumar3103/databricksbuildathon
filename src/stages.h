// Probe builds only (-DSTAGES): time stamp counter totals per stage and per thread, so that a bench on the
// scorer can say where the cycles go. Without STAGES every macro below is empty and nothing here is compiled.
#pragma once
#include <cstdint>

enum StgId : int {
  // the event parse: scanner stage 1 (masks, separator list) and stage 2 (columns), the batches
  STG_E_S1, STG_E_S2, STG_E_PE,
  STG_PE_FETCH, STG_PE_IDS, STG_PE_LISTS, STG_PE_TS, STG_PE_REC, STG_PE_MASK, STG_PE_SCAT, STG_PE_PUR,
  // the click parse
  STG_C_S1, STG_C_S2, STG_C_PC,
  STG_PC_FETCH, STG_PC_IDS, STG_PC_LISTS, STG_PC_TS, STG_PC_CAMP, STG_PC_REC, STG_PC_SCAT,
  // parse tasks as a whole, dropping parsed pages, link tasks
  STG_TASK_E, STG_TASK_C, STG_DROP, STG_TASK_L,
  // the scan: a segment as a whole, then its parts
  STG_SEG, STG_PLACE, STG_SLOTS, STG_CLOSE, STG_LCLK, STG_SORT, STG_MEMSET, STG_AHEAD,
  // time inside tasks, by phase (g_phase 0..7)
  STG_BUSY0, STG_BUSY7 = STG_BUSY0 + 7,
  // perf counters inside tasks, by phase: cycles, instructions, branch misses, cache misses
  STG_PF0, STG_PF_END = STG_PF0 + 32,
  STG_N
};

#ifdef STAGES
#include <x86intrin.h>
struct alignas(64) StgBlock {
  uint64_t c[STG_N];
};
inline StgBlock g_stg_all[257];
inline thread_local uint64_t* tl_stgp = g_stg_all[256].c;  // (threads that run no tasks count into the spare)
#define STG_T(v) uint64_t v = __rdtsc()
#define STG_ACC(k, v)                   \
  do {                                  \
    const uint64_t n_ = __rdtsc();      \
    tl_stgp[(k)] += n_ - (v);           \
    (v) = n_;                           \
  } while (0)
#define STG_ADD(k, x) (tl_stgp[(k)] += (x))
#define STG_ONLY(...) __VA_ARGS__
#else
#define STG_T(v)
#define STG_ACC(k, v)
#define STG_ADD(k, x)
#define STG_ONLY(...)
#endif
