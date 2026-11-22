#!/usr/bin/env python3
"""knobs.py DIR : the bench 10 knobs on top of v4 286f27c. Not an entry.
  SS_PF  0 / 1 / 2   blocks asked for ahead in place() and load_clicks (default 1, as the entry)
  SS_PFH 0..3        the prefetch hint for those blocks: T0 (default), T1, T2, NTA
  SS_ST  n           the scan's segments on n threads, pinned to a core each if there are that many cores (default: all)
  SS_PW  1           the staging line of a record is asked for (for writing) when its partition is known
  SS_LW  1           load_clicks asks for the lines its keys go to, eight clicks ahead
  -DSS_KE=13         event partitions of 2^13 indexes; -DSS_CAP=32: parse batches of 32 lines"""
import sys
d = sys.argv[1]
def patch(fn, reps):
    p = d + '/' + fn; s = open(p).read()
    for old, new in reps:
        assert s.count(old) == 1, (fn, s.count(old), old[:80])
        s = s.replace(old, new)
    open(p, 'w').write(s)

patch('main.cpp', [
("""static std::atomic<PoolJob*> g_job{nullptr};""",
 """static std::atomic<PoolJob*> g_job{nullptr};
// bench knobs (see knobs.py)
static int g_k_pf = 1, g_k_pfh = 0, g_k_st = 0, g_k_pw = 0, g_k_lw = 0;
static std::atomic<int> g_pool_limit{1 << 30};  // workers from this number on leave the job alone
static int g_core_cpu[64];                      // one CPU of each core
static int g_ncore = 0;
static void find_cores() {
  cpu_set_t cs;
  if (sched_getaffinity(0, sizeof cs, &cs) != 0) return;
  for (int c = 0; c < CPU_SETSIZE && g_ncore < 64; c++) {
    if (!CPU_ISSET(c, &cs)) continue;
    char path[128];
    snprintf(path, sizeof path, "/sys/devices/system/cpu/cpu%d/topology/thread_siblings_list", c);
    FILE* f = fopen(path, "r");
    int first = c;
    if (f) {
      if (fscanf(f, "%d", &first) != 1) first = c;
      fclose(f);
    }
    if (first == c) g_core_cpu[g_ncore++] = c;
  }
}
static thread_local bool tl_pinned = false;
static inline void pin_to_core(int tid) {
  if (tl_pinned || g_ncore == 0) return;
  tl_pinned = true;
  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(g_core_cpu[tid % g_ncore], &cs);
  sched_setaffinity(0, sizeof cs, &cs);
}
static FORCE_INLINE void pf_block(const char* p, u32 bytes) {
  switch (g_k_pfh) {
    case 0: for (u32 o = 0; o < bytes; o += 64) _mm_prefetch(p + o, _MM_HINT_T0); break;
    case 1: for (u32 o = 0; o < bytes; o += 64) _mm_prefetch(p + o, _MM_HINT_T1); break;
    case 2: for (u32 o = 0; o < bytes; o += 64) _mm_prefetch(p + o, _MM_HINT_T2); break;
    default: for (u32 o = 0; o < bytes; o += 64) _mm_prefetch(p + o, _MM_HINT_NTA); break;
  }
}"""),
("""static void pool_take(PoolJob* j, int tid) {
  u64 u0 = 0, s0 = 0, u1, s1;""",
 """static void pool_take(PoolJob* j, int tid) {
  if (tid >= g_pool_limit.load(std::memory_order_relaxed)) return;
  u64 u0 = 0, s0 = 0, u1, s1;"""),
("""  if (const char* e = getenv("SS_NODROP")) g_drop_parsed = !(*e && *e != '0');""",
 """  if (const char* e = getenv("SS_NODROP")) g_drop_parsed = !(*e && *e != '0');
  if (const char* e = getenv("SS_PF")) g_k_pf = std::max(0, std::min(2, atoi(e)));
  if (const char* e = getenv("SS_PFH")) g_k_pfh = std::max(0, std::min(3, atoi(e)));
  if (const char* e = getenv("SS_ST")) g_k_st = std::max(0, atoi(e));
  if (const char* e = getenv("SS_PW")) g_k_pw = atoi(e);
  if (const char* e = getenv("SS_LW")) g_k_lw = atoi(e);
  if (g_k_st) find_cores();"""),
])

patch('dense.h', [
("""static constexpr u32 KE = 14, KC = 8;  // event indexes per event partition, person indexes per click partition""",
 """#ifndef SS_KE
#define SS_KE 14
#endif
#ifndef SS_CAP
#define SS_CAP 64
#endif
static constexpr u32 KE = SS_KE, KC = 8;  // event indexes per event partition, person indexes per click partition"""),
("""struct EvB : Cols<7, 64> {""", """struct EvB : Cols<7, SS_CAP> {"""),
("""struct CkB : Cols<4, 64> {""", """struct CkB : Cols<4, SS_CAP> {"""),
# pfx is read 64 bytes at a time
("  alignas(32) u8 pfx[CAP + 8];               // nonzero: a prefix byte is not the expected one\n  alignas(32) u8 cpx[CAP + 8];               // nonzero: campaign_id does not start with \"c_\"\n",
 "  alignas(32) u8 pfx[72];                    // nonzero: a prefix byte is not the expected one\n  alignas(32) u8 cpx[72];                    // nonzero: campaign_id does not start with \"c_\"\n"),
("""  alignas(32) u8 pfx[CAP + 8];               // nonzero: a prefix byte is not the expected one
  alignas(32) u64 idx[CAP + 8];    // event index""",
 """  alignas(32) u8 pfx[72];                    // nonzero: a prefix byte is not the expected one
  alignas(32) u64 idx[CAP + 8];    // event index"""),
# the staging line of an event record, when its partition is known
("""      _mm_store_si128((__m128i*)(B.part + i), low32x4(_mm256_srli_epi64(idx, KE)));
""", """      _mm_store_si128((__m128i*)(B.part + i), low32x4(_mm256_srli_epi64(idx, KE)));
      if (g_k_pw) {
        const u32 P = cur_out.sc->P;
        for (u32 k = 0; k < 4; k++) {
          const u32 p = B.part[i + k];
          if (p < P) __builtin_prefetch(cur_out.sp[p], 1, 3);
        }
      }
"""),
("""      _mm_store_si128((__m128i*)(B.part + i), _mm_srli_epi32(n4, 9 + KC));
""", """      _mm_store_si128((__m128i*)(B.part + i), _mm_srli_epi32(n4, 9 + KC));
      if (g_k_pw) {
        const u32 P = cur_out.sc->P;
        for (u32 k = 0; k < 4; k++) {
          const u32 p = B.part[i + k];
          if (p < P) __builtin_prefetch(cur_out.sp[p], 1, 3);
        }
      }
"""),
# place(): the blocks ahead
("""  S.blk.push_back(S.blk[0]);  // (something harmless to prefetch after the last block)
  for (size_t bi = 0; bi < nblk; bi++) {
    // (the blocks were written long ago: the whole next one is asked for while this one is placed)
    const char* const nx = (const char*)S.blk[bi + 1].first;
    const u32 nxb = S.blk[bi + 1].second * (u32)sizeof(D16);
    for (u32 o = 0; o < nxb; o += 64) _mm_prefetch(nx + o, _MM_HINT_T0);""",
 """  S.blk.push_back(S.blk[0]);  // (something harmless to prefetch after the last block)
  S.blk.push_back(S.blk[0]);
  for (size_t bi = 0; bi < nblk; bi++) {
    // (the blocks were written long ago: the whole next one is asked for while this one is placed)
    if (g_k_pf) {
      pf_block((const char*)S.blk[bi + (size_t)g_k_pf].first, S.blk[bi + (size_t)g_k_pf].second * (u32)sizeof(D16));
      if (g_k_pf == 2 && bi == 0) pf_block((const char*)S.blk[1].first, S.blk[1].second * (u32)sizeof(D16));
    }"""),
# load_clicks: the blocks ahead
("""  if (nblk) S.cblk.push_back(S.cblk[0]);  // (something harmless to prefetch after the last block)""",
 """  if (nblk) {
    S.cblk.push_back(S.cblk[0]);  // (something harmless to prefetch after the last block)
    S.cblk.push_back(S.cblk[0]);
  }"""),
("""    // the blocks were written long ago: the whole next one is asked for while this one is counted
    const char* const nx = (const char*)S.cblk[bi + 1].first;
    const u32 nxb = S.cblk[bi + 1].second * (u32)sizeof(C16);
    for (u32 o = 0; o < nxb; o += 64) _mm_prefetch(nx + o, _MM_HINT_T0);""",
 """    // the blocks were written long ago: the whole next one is asked for while this one is counted
    if (g_k_pf) {
      pf_block((const char*)S.cblk[bi + (size_t)g_k_pf].first, S.cblk[bi + (size_t)g_k_pf].second * (u32)sizeof(C16));
      if (g_k_pf == 2 && bi == 0) pf_block((const char*)S.cblk[1].first, S.cblk[1].second * (u32)sizeof(C16));
    }"""),
# load_clicks: where the keys go
("""    for (u32 j = 0; j < m; j++) {
      const u64 w0 = r[j].w0, w1 = r[j].w1;
      const u32 un = (u32)(w0 >> 32), g = (un >> 9) & (G - 1);
      const u32 dense = (un & 511) < cntv[g];""",
 """    for (u32 j = 0; j < m; j++) {
      if (g_k_lw && j + 8 < m) {
        const u32 q8 = cpos[((u32)(r[j + 8].w0 >> 41) & (G - 1))];
        __builtin_prefetch(ckey + q8, 1, 3);
        __builtin_prefetch(cclk + q8, 1, 3);
      }
      const u64 w0 = r[j].w0, w1 = r[j].w1;
      const u32 un = (u32)(w0 >> 32), g = (un >> 9) & (G - 1);
      const u32 dense = (un & 511) < cntv[g];"""),
# the scan on fewer threads
("""  parallel_for((int)nseg, [&](int s, int tid) {
    if (sb[s] < sb[s + 1]) scan_segment<false>(sb[s], sb[s + 1], tid, scr[tid], st[s], ends.data(), (u32)s, nseg);
  });""",
 """  if (g_k_st) g_pool_limit.store(g_k_st, std::memory_order_relaxed);
  parallel_for((int)nseg, [&](int s, int tid) {
    if (g_k_st && g_k_st <= g_ncore) pin_to_core(tid);  // (more threads than cores: the scheduler places them)
    if (sb[s] < sb[s + 1]) scan_segment<false>(sb[s], sb[s + 1], tid, scr[tid], st[s], ends.data(), (u32)s, nseg);
  });"""),
("""  parallel_for((int)nseg, [&](int s, int tid) {
    if (ends[s].has_tail) scan_segment<true>(sb[s], sb[s + 1], tid, scr[tid], st_ends[s], ends.data(), (u32)s, nseg);
  });""",
 """  parallel_for((int)nseg, [&](int s, int tid) {
    if (ends[s].has_tail) scan_segment<true>(sb[s], sb[s + 1], tid, scr[tid], st_ends[s], ends.data(), (u32)s, nseg);
  });
  g_pool_limit.store(1 << 30, std::memory_order_relaxed);"""),
])
print("knobs in", d)
