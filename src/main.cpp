// Shopping Sessions: native pipeline (C++17, x86-64 with BMI2/AVX2).
//
// Phases:
//   1. lookups: manifest, products, campaigns
//   2. links: all threads insert the IDs into one hash table H and join them with a lock-free union-find
//      kept in the same table; then H maps id -> person
//   3. events + clicks: parse in parallel chunks.
//        events -> R1 records, partitioned by hash(event_id)                    (for dedup)
//        clicks -> person lookup -> R2C records, partitioned by hash(person)    (for attribution)
//   4. dedup per event_id partition -> person lookup -> R2 records, partitioned by hash(person)
//   5. per person partition: group, sort, sessionize, attribute, aggregate
//   6. write Parquet
#include <dirent.h>
#include <fcntl.h>
#include <immintrin.h>
#include <linux/futex.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "parquet.h"
#include "scan.h"

using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;
using i32 = int32_t;
using i64 = int64_t;

#define FORCE_INLINE inline __attribute__((always_inline))
#define NOINLINE __attribute__((noinline))

static constexpr i64 DAY = 86400000LL;
static constexpr i64 MIN30 = 1800000LL;
static constexpr i64 WEEK = 7 * DAY;
static constexpr u32 TS_INVALID = 0xFFFFFFFFu;

// ------------------------------------------------------------------------------------------
// utilities

static double g_t0;
static bool g_log = false;
static double now_s() {
  return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
#define LOG(...)                                         \
  do {                                                   \
    if (g_log) {                                         \
      fprintf(stderr, "[%7.3f] ", now_s() - g_t0);       \
      fprintf(stderr, __VA_ARGS__);                      \
      fputc('\n', stderr);                               \
    }                                                    \
  } while (0)

[[noreturn]] static void die(const char* msg) {
  fprintf(stderr, "fatal: %s\n", msg);
  exit(2);
}

static int NT = 8;

static thread_local int tl_serial_tid = -1;  // >= 0: parallel_for runs inline on this thread
// per-phase CPU accounting (sum of task time over threads), reported with SS_LOG
static std::atomic<u64> g_phase_cycles[16];  // user microseconds
static std::atomic<u64> g_phase_sys[16];     // system microseconds
static inline void thread_times(u64& user_us, u64& sys_us) {
  struct rusage ru;
  getrusage(RUSAGE_THREAD, &ru);
  user_us = (u64)ru.ru_utime.tv_sec * 1000000 + ru.ru_utime.tv_usec;
  sys_us = (u64)ru.ru_stime.tv_sec * 1000000 + ru.ru_stime.tv_usec;
}
static int g_phase = 0;
static const char* g_phase_names[16] = {"init", "links", "parse", "dedup", "person", "output"};

// ---- the worker pool. The NT - 1 workers are started at the first parallel section and kept to the end: a
// section then costs a wake-up, not NT - 1 thread starts (there are about twenty sections, some of them a
// fraction of a millisecond long). A worker that finds nothing to do spins for a moment before it sleeps,
// as the next section is usually close. Sections are posted by the main thread only, one at a time.
struct PoolJob {
  void (*run)(void* ctx, int t, int tid);
  void* ctx;
  int ntasks;
  int phase;
  alignas(64) std::atomic<int> next{0};
  alignas(64) std::atomic<int> finished{0};
};
static std::atomic<PoolJob*> g_job{nullptr};
static std::atomic<u32> g_job_seq{0};  // bumped for every job; the workers sleep on it
static bool g_pool_up = false;

static inline void futex_wait(std::atomic<u32>* a, u32 seen) {
  syscall(SYS_futex, (u32*)a, FUTEX_WAIT_PRIVATE, seen, nullptr, nullptr, 0);
}
static inline void futex_wake_all(std::atomic<u32>* a) { syscall(SYS_futex, (u32*)a, FUTEX_WAKE_PRIVATE, INT_MAX, nullptr, nullptr, 0); }

// Tasks of job j until none is left.
static void pool_take(PoolJob* j, int tid) {
  u64 u0 = 0, s0 = 0, u1, s1;
  bool any = false;
  for (;;) {
    if (j->next.load(std::memory_order_relaxed) >= j->ntasks) break;
    const int t = j->next.fetch_add(1, std::memory_order_relaxed);
    if (t >= j->ntasks) break;
    if (!any && g_log) thread_times(u0, s0);
    any = true;
    j->run(j->ctx, t, tid);
    if (g_log) {  // (task by task, so that the times are in before the job counts as done)
      thread_times(u1, s1);
      g_phase_cycles[j->phase] += u1 - u0;
      g_phase_sys[j->phase] += s1 - s0;
      u0 = u1;
      s0 = s1;
    }
    j->finished.fetch_add(1, std::memory_order_release);
  }
}
static void pool_worker(int tid) {
  u32 seen = 0;
  for (;;) {
    u32 s;
    for (u32 spins = 0; (s = g_job_seq.load(std::memory_order_acquire)) == seen; spins++) {
      if (spins < 20000)
        _mm_pause();
      else
        futex_wait(&g_job_seq, seen);
    }
    seen = s;
    pool_take(g_job.load(std::memory_order_acquire), tid);
  }
}

template <class F>
static void parallel_for(int ntasks, F&& f) {
  if (ntasks <= 0) return;
  if (tl_serial_tid >= 0) {
    for (int t = 0; t < ntasks; t++) f(t, tl_serial_tid);
    return;
  }
  // (a job is never freed: a worker may still look at one that is over, to find it has nothing left)
  PoolJob* const j = new PoolJob;
  typedef typename std::remove_reference<F>::type Fn;
  j->run = [](void* ctx, int t, int tid) { (*(Fn*)ctx)(t, tid); };
  j->ctx = (void*)&f;
  j->ntasks = ntasks;
  j->phase = g_phase;
  if (NT > 1 && ntasks > 1) {
    if (!g_pool_up) {
      g_pool_up = true;
      for (int i = 1; i < NT; i++) std::thread(pool_worker, i).detach();
    }
    g_job.store(j, std::memory_order_release);
    g_job_seq.fetch_add(1, std::memory_order_release);
    futex_wake_all(&g_job_seq);
  }
  pool_take(j, 0);
  while (j->finished.load(std::memory_order_acquire) < ntasks) _mm_pause();  // the tasks other threads still have
}

// Zero-filled memory for the big tables and the arenas, on huge pages where the kernel grants them: from
// 1 MB on, the size is rounded up to whole 2 MB pages and the start is aligned to one.
static void* vm_alloc(size_t bytes) {
  constexpr size_t HP = (size_t)2 << 20;
  if (bytes == 0) bytes = 64;
  const bool huge = bytes >= ((size_t)1 << 20);
  if (huge) bytes = (bytes + HP - 1) & ~(HP - 1);
  u8* p = (u8*)mmap(nullptr, bytes + (huge ? HP : 0), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (p == MAP_FAILED) die("mmap anonymous");
  if (huge) p = (u8*)(((uintptr_t)p + HP - 1) & ~(uintptr_t)(HP - 1));  // (the unused ends are never touched)
  madvise(p, bytes, MADV_HUGEPAGE);
  return p;
}

// Records are kept in blocks of one page. Every thread has an open block per partition, on average half
// full at the end, and all of that memory has to be zeroed by the kernel first: small blocks keep it small.
// Not smaller than this, though: with 2 KB blocks the passes that read the records back measured 3% more
// CPU time (twice as many block boundaries to cross), for only 30 MB less memory.
static constexpr size_t BLOCK_BYTES = 4096;

struct Arena {
  u8* cur = nullptr;
  u8* end = nullptr;
  std::vector<void*> free_blocks;  // recycled BLOCK_BYTES blocks
  std::vector<std::pair<void*, size_t>> slabs;
  void* alloc(size_t n) {
    n = (n + 63) & ~(size_t)63;
    if ((size_t)(end - cur) < n) {
      size_t sz = std::max(n, (size_t)256 << 20);
      cur = (u8*)vm_alloc(sz);
      end = cur + sz;
      slabs.push_back({cur, sz});
    }
    void* p = cur;
    cur += n;
    return p;
  }
  void* block() {
    if (!free_blocks.empty()) {
      void* b = free_blocks.back();
      free_blocks.pop_back();
      return b;
    }
    return alloc(BLOCK_BYTES);
  }
};

template <class T>
struct Stream {
  static constexpr u32 BLK = (u32)(BLOCK_BYTES / sizeof(T));
  T* cur = nullptr;
  T* lim = nullptr;
  T* beg = nullptr;
  std::vector<std::pair<T*, u32>> blocks;
  inline void push(const T& x, Arena& a) {
    if (__builtin_expect(cur == lim, 0)) grow(a);
    *cur++ = x;
  }
  inline T* slot(Arena& a) {
    if (__builtin_expect(cur == lim, 0)) grow(a);
    return cur++;
  }
  // Room for k more items (k <= BLK) at the returned position; the caller advances `cur` itself.
  inline T* reserve(u32 k, Arena& a) {
    if (__builtin_expect((size_t)(lim - cur) < k, 0)) grow(a);
    return cur;
  }
  // n items that need not fit a normal block: they get a block of their own, already counted as full.
  T* big(size_t n, Arena& a) {
    if (beg) blocks.push_back({beg, (u32)(cur - beg)});
    T* p = (T*)a.alloc(n * sizeof(T));
    blocks.push_back({p, (u32)n});
    beg = cur = lim = nullptr;
    return p;
  }
  void grow(Arena& a) {
    if (beg) blocks.push_back({beg, (u32)(cur - beg)});
    beg = cur = (T*)a.block();
    lim = beg + BLK;
  }
  void finish() {
    if (beg && cur > beg) blocks.push_back({beg, (u32)(cur - beg)});
    else if (beg) blocks.push_back({beg, 0});
    beg = cur = lim = nullptr;
  }
  size_t size() const {
    size_t n = 0;
    for (auto& b : blocks) n += b.second;
    return n;
  }
  void release(Arena& a) {
    for (auto& b : blocks) a.free_blocks.push_back(b.first);
    std::vector<std::pair<T*, u32>>().swap(blocks);
  }
};

// Hash-partitioned output with software write-combining: the records of a partition are collected in a
// 64-byte staging line, and full lines go to the partition's block with non-temporal stores.
//
// "Is this line full now?" is a coin flip for a random partition, so a branch on it mispredicts on a large
// share of pushes, and copying the line somewhere on every push costs several vector loads and stores.
// Instead a push is one store through the partition's cursor into its staging line. When that fills the
// line, the line itself joins a queue and the partition takes a spare line, all without a branch: the queue
// entry and the partition's cursor are written either way, and only whether the queue advances depends on
// the outcome. Every QCAP full lines the queue is drained in a tight loop and its lines become the spares.
template <class T>
struct WCScatter {
  static_assert(sizeof(T) == 16 || sizeof(T) == 32, "record size");
  static constexpr u32 PER = 64 / sizeof(T);
  static constexpr u32 BLK = (u32)(BLOCK_BYTES / sizeof(T));
  static constexpr u32 QCAP = 32;
  struct Hot {
    T* cur;
    T* lim;
  };
  struct QEnt {
    T* nxt;     // end of a staging line that is full (if the entry is in use)
    u8* spare;  // a free line for the partition whose line this entry takes
    u32 p;      // the partition
  };
  u32 P = 0;
  Hot* hot = nullptr;
  u8* lines = nullptr;  // P + QCAP staging lines
  T** sp = nullptr;     // per partition: where its next record goes, inside its staging line
  QEnt* q = nullptr;    // QCAP entries; those before qe hold full lines
  QEnt* qe = nullptr;
  T** beg = nullptr;
  std::vector<std::vector<std::pair<T*, u32>>> blocks;
  // (The working state is set up by the side thread that also reads the lookups. It comes from the heap, not
  // from the thread's arena: touching the arena would fault in a 2 MB page per thread on that one side thread,
  // which is slower than the links phase it runs beside when memory is cold.)
  void init(u32 parts) {
    P = parts;
    hot = (Hot*)calloc(P, sizeof(Hot));
    lines = (u8*)aligned_alloc(64, (size_t)64 * (P + QCAP));
    sp = (T**)malloc((size_t)P * sizeof(T*));
    for (u32 p = 0; p < P; p++) sp[p] = (T*)(lines + (size_t)64 * p);
    q = (QEnt*)calloc(QCAP, sizeof(QEnt));
    for (u32 i = 0; i < QCAP; i++) q[i].spare = lines + (size_t)64 * (P + i);
    qe = q;
    beg = (T**)calloc(P, sizeof(T*));
    blocks.assign(P, {});
  }
  void grow(u32 p, Arena& a) {
    if (beg[p]) blocks[p].push_back({beg[p], (u32)(hot[p].cur - beg[p])});
    beg[p] = hot[p].cur = (T*)a.block();
    hot[p].lim = beg[p] + BLK;
  }
  NOINLINE void drain(Arena& a) {
    QEnt* const end = qe;
    qe = q;
    for (QEnt* e = q; e != end; e++) {
      u8* const line = (u8*)e->nxt - 64;
      Hot& h = hot[e->p];
      if (__builtin_expect(h.cur == h.lim, 0)) grow(e->p, a);
      const __m256i* src = (const __m256i*)line;
      _mm256_stream_si256((__m256i*)h.cur, _mm256_load_si256(src));
      _mm256_stream_si256((__m256i*)h.cur + 1, _mm256_load_si256(src + 1));
      h.cur += PER;
      e->spare = line;  // the entry's old spare went to the partition; the entries from `end` on kept theirs
    }
  }
  // A copy of the fields a run of pushes needs, kept in a local so they stay in registers.
  struct Cursor {
    T** sp;
    QEnt* qe;
    QEnt* qend;
    WCScatter* sc;
    Arena* arena;
  };
  Cursor cursor(Arena& a) { return Cursor{sp, qe, q + QCAP, this, &a}; }
  void close(const Cursor& c) { qe = c.qe; }
  // The record at `cur` has just been written for partition p.
  static FORCE_INLINE void advance(Cursor& c, u32 p, T* cur) {
    T* const nxt = cur + 1;
    QEnt* const e = c.qe;
    e->nxt = nxt;
    e->p = p;
    T* const fresh = (T*)e->spare;
    QEnt* const e1 = e + 1;
    // if the line is full now (nxt is at a 64-byte boundary): the partition continues in the spare line and
    // the queue keeps the entry. Two conditional moves, spelled out because the compiler prefers a branch
    // here, and this one is as unpredictable as branches get.
    T* nsp = nxt;
    QEnt* nqe = e;
    __asm__("testb $63, %b[s]\n\tcmove %[f], %[s]\n\tcmove %[e1], %[q]"
            : [s] "+r"(nsp), [q] "+r"(nqe)
            : [f] "r"(fresh), [e1] "r"(e1)
            : "cc");
    c.sp[p] = nsp;
    c.qe = nqe;
    if (__builtin_expect(c.qe == c.qend, 0)) {
      c.sc->qe = c.qe;
      c.sc->drain(*c.arena);
      c.qe = c.sc->q;
    }
  }
  static FORCE_INLINE void push32(Cursor& c, u32 p, __m256i x) {
    static_assert(sizeof(T) == 32, "push32");
    T* const cur = c.sp[p];
    _mm256_store_si256((__m256i*)cur, x);
    advance(c, p, cur);
  }
  static FORCE_INLINE void push16(Cursor& c, u32 p, __m128i x) {
    static_assert(sizeof(T) == 16, "push16");
    T* const cur = c.sp[p];
    _mm_store_si128((__m128i*)cur, x);
    advance(c, p, cur);
  }
  // Ends this thread's run of pushes: full lines are written out (partial lines stay staged).
  void flush(Arena& a) {
    drain(a);
    _mm_sfence();
  }
  void finish(Arena& a) {
    drain(a);
    for (u32 p = 0; p < P; p++) {
      T* const end = sp[p];
      T* const start = (T*)((uintptr_t)end & ~(uintptr_t)63);
      for (const T* r = start; r < end; r++) {
        if (hot[p].cur == hot[p].lim) grow(p, a);
        memcpy(hot[p].cur, r, sizeof(T));
        hot[p].cur++;
      }
      sp[p] = start;
      if (beg[p]) blocks[p].push_back({beg[p], (u32)(hot[p].cur - beg[p])});
      beg[p] = nullptr;
      hot[p].cur = hot[p].lim = nullptr;
    }
    _mm_sfence();
  }
  size_t size(u32 p) const {
    size_t n = 0;
    for (auto& b : blocks[p]) n += b.second;
    return n;
  }
  void release(u32 p, Arena& a) {
    for (auto& b : blocks[p]) a.free_blocks.push_back(b.first);
    std::vector<std::pair<T*, u32>>().swap(blocks[p]);
  }
};

// IDs are hashed with one multiplication by an odd constant. That is a bijection on 64-bit keys, so a hash
// can stand in for its key (the inverse multiplication gives the key back), and its HIGH bits are well mixed;
// the low bits are not. Every partition and table index is therefore cut from the top of the hash.
static constexpr u64 HASH_K = 0x9E3779B97F4A7C15ULL;
static constexpr u64 inv_odd(u64 a) {
  u64 x = a;  // correct to 3 bits; each step doubles that
  for (int i = 0; i < 5; i++) x *= 2 - a * x;
  return x;
}
static constexpr u64 HASH_KI = inv_odd(HASH_K);
static_assert(HASH_K * HASH_KI == 1, "inverse of the hash multiplier");
static inline u64 hash_id(u64 key) { return key * HASH_K; }
static inline u64 unhash_id(u64 h) { return h * HASH_KI; }

static inline u32 ceil_log2(u64 x) {
  u32 b = 0;
  while ((1ULL << b) < x) b++;
  return b;
}

// ------------------------------------------------------------------------------------------
// IDs as 64-bit keys.
//
// Fast keys: strings of 1..9 bytes whose first and last bytes are not NUL and whose bytes after the first
// are all below 0x80.
//   key = first byte << 56 | the other bytes' low 7 bits packed big-endian (zero padded).
//   The padding cannot be confused with the string's own bytes because the last byte is not NUL.
//   Integer order of fast keys equals byte-wise order of the strings.
// Slow keys: any other string, interned; key = index + 1 (< 2^56, so the top byte is 0).
// Both functions need n >= 1 and 9 readable bytes at s.

static constexpr u64 KEY_HIGH = 0x8080808080808080ULL, KEY_M7 = 0x7F7F7F7F7F7F7F7FULL;

static inline u64 key_fast(const u8* s, u32 n) {
  const u32 m = n - 1;
  u64 w;
  memcpy(&w, s + 1, 8);
  w = _bzhi_u64(w, 8 * m);  // bytes 1..n-1 (all eight for m >= 8)
  if (m > 8 || (w & KEY_HIGH) != 0 || s[0] == 0 || s[m] == 0) return 0;
  return ((u64)s[0] << 56) | _pext_u64(__builtin_bswap64(w), KEY_M7);
}

struct Interner {
  std::mutex mu;
  std::unordered_map<std::string, u64> map;
  std::vector<std::string> strs;
  u64 get(const u8* s, u32 n) {
    std::lock_guard<std::mutex> lk(mu);
    std::string str((const char*)s, n);
    auto it = map.find(str);
    if (it != map.end()) return it->second;
    u64 k = strs.size() + 1;
    strs.push_back(str);
    map.emplace(std::move(str), k);
    return k;
  }
};
static Interner g_intern;

static FORCE_INLINE u64 make_key(const u8* s, u32 n) {
  const u32 m = n - 1;
  u64 w;
  memcpy(&w, s + 1, 8);
  w = _bzhi_u64(w, 8 * m);
  if (__builtin_expect(m > 8 || (w & KEY_HIGH) != 0 || s[0] == 0 || s[m] == 0, 0)) return g_intern.get(s, n);
  return ((u64)s[0] << 56) | _pext_u64(__builtin_bswap64(w), KEY_M7);
}
static inline bool is_slow(u64 k) { return (k >> 56) == 0; }

// The string of a fast key: writes up to 9 bytes at `out` and returns the length.
static inline u32 fast_key_bytes(u64 k, u8* out) {
  const u64 w = __builtin_bswap64(_pdep_u64(k, KEY_M7));  // bytes 1..8, zero padded
  out[0] = (u8)(k >> 56);
  memcpy(out + 1, &w, 8);
  return w ? 9 - ((u32)__builtin_clzll(w) >> 3) : 1;
}

static void key_str(u64 k, std::string& out) {
  if (is_slow(k)) {
    std::lock_guard<std::mutex> lk(g_intern.mu);
    out = g_intern.strs[k - 1];
    return;
  }
  u8 b[9];
  const u32 n = fast_key_bytes(k, b);
  out.assign((const char*)b, n);
}

static inline int key_cmp(u64 a, u64 b) {
  if (!is_slow(a) && !is_slow(b)) return a < b ? -1 : (a > b ? 1 : 0);
  std::string sa, sb;
  key_str(a, sa);
  key_str(b, sb);
  int c = sa.compare(sb);
  return c < 0 ? -1 : (c > 0 ? 1 : 0);
}

// ------------------------------------------------------------------------------------------
// timestamps

static inline bool is_dig(u8 c) { return (u8)(c - '0') < 10; }

static inline i64 days_from_civil(i64 y, i64 m, i64 d) {
  y -= m <= 2;
  const i64 era = (y >= 0 ? y : y - 399) / 400;
  const i64 yoe = y - era * 400;
  const i64 doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const i64 doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}

static const u8 DIM[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static const u16 CUM[13] = {0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
static i32 g_ydays[400];  // days from the epoch to Jan 1 of year 1900 + i
static u8 g_yleap[400];

static inline bool is_leap(u32 y) { return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0); }

static void init_doy();
static void init_years() {
  init_doy();
  for (int i = 0; i < 400; i++) {
    g_ydays[i] = (i32)days_from_civil(1900 + i, 1, 1);
    g_yleap[i] = is_leap(1900 + i);
  }
}

// Scalar reference for "YYYY-MM-DDTHH:MM:SS" (19 bytes).
static inline bool civil19_scalar(const u8* s, i64& out) {
  if (s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':') return false;
  static const int pos[14] = {0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18};
  u32 c[14];
  for (int i = 0; i < 14; i++) {
    c[i] = (u32)s[pos[i]] - '0';
    if (c[i] > 9) return false;
  }
  u32 y = c[0] * 1000 + c[1] * 100 + c[2] * 10 + c[3];
  u32 mo = c[4] * 10 + c[5], d = c[6] * 10 + c[7], h = c[8] * 10 + c[9], mi = c[10] * 10 + c[11],
      se = c[12] * 10 + c[13];
  if (mo < 1 || mo > 12 || d < 1) return false;
  u32 dim = DIM[mo] + ((mo == 2 && is_leap(y)) ? 1 : 0);
  if (d > dim || h > 23 || mi > 59 || se > 59) return false;
  out = ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60000LL + (i64)se * 1000LL;
  return true;
}

// day of year (0-based) for [leap][month * 32 + day], 0xFFFF if the date does not exist
static u16 g_doy[2][13 * 32];
static void init_doy() {
  for (int lp = 0; lp < 2; lp++)
    for (int m = 0; m < 13; m++)
      for (int d = 0; d < 32; d++) {
        u16 v = 0xFFFF;
        if (m >= 1 && d >= 1) {
          int dim = DIM[m] + ((m == 2 && lp) ? 1 : 0);
          if (d <= dim) v = (u16)(CUM[m] + ((m > 2 && lp) ? 1 : 0) + d - 1);
        }
        g_doy[lp][m * 32 + d] = v;
      }
}


// Finish a parsed civil time: y, mo*32+dd, hh*60+mi already range checked (mo<=12, dd<=31, hh<=23, mi<=59)
static FORCE_INLINE bool civil_finish(u32 y, u32 md, u32 hm, u32 se, i64& out) {
  u32 yi = y - 1900;
  i64 days;
  if (__builtin_expect(yi < 400, 1)) {
    u32 doy = g_doy[g_yleap[yi]][md];
    if (doy == 0xFFFF) return false;
    days = (i64)g_ydays[yi] + doy;
  } else {
    u32 mo = md >> 5, dd = md & 31;
    if (mo < 1 || dd < 1 || dd > DIM[mo] + ((mo == 2 && is_leap(y)) ? 1u : 0u)) return false;
    days = days_from_civil(y, mo, dd);
  }
  out = (days * 1440 + hm) * 60000LL + (i64)se * 1000LL;
  return true;
}

static FORCE_INLINE __m128i civil_lane_ok(__m128i v, int& okmask) {
  const __m128i d = _mm_sub_epi8(v, _mm_set1_epi8('0'));
  const __m128i isdig = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
  const __m128i sepval = _mm_setr_epi8(0, 0, 0, 0, '-', 0, 0, '-', 0, 0, 'T', 0, 0, ':', 0, 0);
  const __m128i sepm = _mm_setr_epi8(0, 0, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0);
  const __m128i ok = _mm_blendv_epi8(isdig, _mm_cmpeq_epi8(v, sepval), sepm);
  okmask = _mm_movemask_epi8(ok);
  const __m128i g = _mm_shuffle_epi8(d, _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1));
  return _mm_maddubs_epi16(g, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0));
}

// SIMD version; needs 16 readable bytes at s.
static FORCE_INLINE bool civil19(const u8* s, i64& out) {
  int okm;
  const __m128i p = civil_lane_ok(_mm_loadu_si128((const __m128i*)s), okm);
  u32 s17 = (u32)s[17] - '0', s18 = (u32)s[18] - '0';
  const __m128i bad = _mm_or_si128(_mm_cmpgt_epi16(p, _mm_setr_epi16(99, 99, 12, 31, 23, 59, 0, 0)),
                                   _mm_cmpgt_epi16(_mm_setr_epi16(0, 0, 1, 1, 0, 0, 0, 0), p));
  if ((okm != 0xFFFF) | (s[16] != ':') | (s17 > 5) | (s18 > 9) | !_mm_testz_si128(bad, bad)) return false;
  const __m128i q = _mm_madd_epi16(p, _mm_setr_epi16(100, 1, 32, 1, 60, 1, 0, 0));
  u32 y = (u32)_mm_cvtsi128_si32(q), md = (u32)_mm_extract_epi32(q, 1), hm = (u32)_mm_extract_epi32(q, 2);
  return civil_finish(y, md, hm, s17 * 10 + s18, out);
}

// Two at once (AVX2): returns bit 0 if a parsed, bit 1 if b parsed.
static FORCE_INLINE u32 civil19x2(const u8* sa, const u8* sb, i64& oa, i64& ob) {
  const __m256i v = _mm256_inserti128_si256(_mm256_castsi128_si256(_mm_loadu_si128((const __m128i*)sa)),
                                            _mm_loadu_si128((const __m128i*)sb), 1);
  const __m256i d = _mm256_sub_epi8(v, _mm256_set1_epi8('0'));
  const __m256i isdig = _mm256_cmpeq_epi8(_mm256_min_epu8(d, _mm256_set1_epi8(9)), d);
  const __m256i sepval = _mm256_setr_epi8(0, 0, 0, 0, '-', 0, 0, '-', 0, 0, 'T', 0, 0, ':', 0, 0, 0, 0, 0, 0, '-', 0,
                                          0, '-', 0, 0, 'T', 0, 0, ':', 0, 0);
  const __m256i sepm = _mm256_setr_epi8(0, 0, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, 0, 0, 0, 0, -1, 0, 0, -1,
                                        0, 0, -1, 0, 0, -1, 0, 0);
  const __m256i ok = _mm256_blendv_epi8(isdig, _mm256_cmpeq_epi8(v, sepval), sepm);
  u32 okm = (u32)_mm256_movemask_epi8(ok);
  const __m256i g = _mm256_shuffle_epi8(d, _mm256_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1, 0,
                                                            1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1));
  const __m256i p = _mm256_maddubs_epi16(
      g, _mm256_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10,
                          1, 0, 0, 0, 0));
  const __m256i hi = _mm256_setr_epi16(99, 99, 12, 31, 23, 59, 0, 0, 99, 99, 12, 31, 23, 59, 0, 0);
  const __m256i lo = _mm256_setr_epi16(0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0);
  const __m256i bad = _mm256_or_si256(_mm256_cmpgt_epi16(p, hi), _mm256_cmpgt_epi16(lo, p));
  u32 badm = (u32)_mm256_movemask_epi8(bad);
  const __m256i q = _mm256_madd_epi16(p, _mm256_setr_epi16(100, 1, 32, 1, 60, 1, 0, 0, 100, 1, 32, 1, 60, 1, 0, 0));
  alignas(32) u32 Lbuf[8];
  _mm256_store_si256((__m256i*)Lbuf, q);
  const u32* L = Lbuf;
  __asm__("" : "+r"(L));  // read the values back with plain loads instead of vector extracts
  u32 a17 = (u32)sa[17] - '0', a18 = (u32)sa[18] - '0', b17 = (u32)sb[17] - '0', b18 = (u32)sb[18] - '0';
  u32 r = 0;
  if (((okm & 0xFFFF) == 0xFFFF) & ((badm & 0xFFFF) == 0) & (sa[16] == ':') & (a17 <= 5) & (a18 <= 9))
    r |= civil_finish(L[0], L[1], L[2], a17 * 10 + a18, oa) ? 1u : 0u;
  if (((okm >> 16) == 0xFFFF) & ((badm >> 16) == 0) & (sb[16] == ':') & (b17 <= 5) & (b18 <= 9))
    r |= civil_finish(L[4], L[5], L[6], b17 * 10 + b18, ob) ? 2u : 0u;
  return r;
}

// 13 ASCII digits at s (16 readable bytes) -> value
static FORCE_INLINE bool digits13(const u8* s, i64& out) {
  __m128i v = _mm_loadu_si128((const __m128i*)s);
  __m128i d = _mm_sub_epi8(v, _mm_set1_epi8('0'));
  __m128i isdig = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
  if ((_mm_movemask_epi8(isdig) & 0x1FFF) != 0x1FFF) return false;
  d = _mm_slli_si128(d, 3);
  __m128i t1 = _mm_maddubs_epi16(d, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1));
  __m128i t2 = _mm_madd_epi16(t1, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1));
  __m128i t3 = _mm_packus_epi32(t2, t2);
  __m128i t4 = _mm_madd_epi16(t3, _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1));
  u64 hi = (u32)_mm_cvtsi128_si32(t4), lo = (u32)_mm_extract_epi32(t4, 1);
  out = (i64)(hi * 100000000ULL + lo);
  return true;
}

static FORCE_INLINE bool parse_f1(const u8* s, u32 n, i64& out) {
  if (n != 20 || s[19] != 'Z') return false;
  return civil19(s, out);
}

static bool parse_ts_slow(const u8* s, u32 n, i64& out) {
  if (n == 0) return false;
  u32 i = 0;
  while (i < n && s[i] == '0') i++;
  if (n - i > 17) return false;  // >= 1e17 ms: outside any window (or not digits at all)
  i64 v = 0;
  for (; i < n; i++) {
    u32 c = s[i] - '0';
    if (c > 9) return false;
    v = v * 10 + c;
  }
  out = v;
  return true;
}

static FORCE_INLINE bool parse_ts(const u8* s, u32 n, i64& out) {
  if (n == 20 && s[4] == '-') {
    if (s[19] != 'Z') return false;
    return civil19(s, out);
  }
  if (n == 13) return digits13(s, out);
  if (n == 25 && s[4] == '-') {
    if ((s[19] != '+' && s[19] != '-') || s[22] != ':') return false;
    u32 a = s[20] - '0', b = s[21] - '0', c = s[23] - '0', e = s[24] - '0';
    if (a > 9 || b > 9 || c > 9 || e > 9) return false;
    u32 oh = a * 10 + b, om = c * 10 + e;
    if (oh > 23 || om > 59) return false;
    i64 base;
    if (!civil19(s, base)) return false;
    i64 off = (i64)(oh * 60 + om) * 60000LL;
    out = (s[19] == '+') ? base - off : base + off;
    return true;
  }
  return parse_ts_slow(s, n, out);
}

// Scalar reference of parse_ts, for self-tests.
static bool parse_ts_ref(const u8* s, u32 n, i64& out) {
  if (n == 20 && s[4] == '-') {
    if (s[19] != 'Z') return false;
    return civil19_scalar(s, out);
  }
  if (n == 25 && s[4] == '-') {
    if ((s[19] != '+' && s[19] != '-') || s[22] != ':') return false;
    if (!is_dig(s[20]) || !is_dig(s[21]) || !is_dig(s[23]) || !is_dig(s[24])) return false;
    u32 oh = (s[20] - '0') * 10 + (s[21] - '0'), om = (s[23] - '0') * 10 + (s[24] - '0');
    if (oh > 23 || om > 59) return false;
    i64 base;
    if (!civil19_scalar(s, base)) return false;
    i64 off = (i64)(oh * 60 + om) * 60000LL;
    out = (s[19] == '+') ? base - off : base + off;
    return true;
  }
  return parse_ts_slow(s, n, out);
}

// ------------------------------------------------------------------------------------------
// Fast path for F1 timestamps ("YYYY-MM-DDTHH:MM:SSZ") in the years the window can touch.
//
// The year is compared as text with the (at most two) candidate years; the other 16 bytes are validated in
// one vector compare, and the date is turned into milliseconds with one table lookup. Anything these
// kernels do not accept is decided by parse_ts / parse_f1, so they only have to be right when they say yes.

static u32 g_ytxt[2];          // candidate years as their 4 ASCII digits (0: none)
static i64 g_dayms[2][416];    // [candidate][month * 32 + day] -> ms at 00:00 UTC, DAYMS_NONE if no such date
static constexpr i64 DAYMS_NONE = INT64_MIN;
static i64 g_daycode[2][416];  // the same in whole seconds since a base chosen by the dense path, plus 1

static void civil_from_days(i64 days, i64& y, i64& m, i64& d) {
  i64 z = days + 719468;
  i64 era = (z >= 0 ? z : z - 146096) / 146097;
  i64 doe = z - era * 146097;
  i64 yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  y = yoe + era * 400;
  i64 doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  i64 mp = (5 * doy + 2) / 153;
  d = doy - (153 * mp + 2) / 5 + 1;
  m = mp < 10 ? mp + 3 : mp - 9;
  y += (m <= 2);
}

// Candidate years: those of lo_ms and hi_ms. Any choice is safe; it only decides what takes the fast path.
static void init_fast_years(i64 lo_ms, i64 hi_ms) {
  auto fdiv = [](i64 a, i64 b) { return a >= 0 ? a / b : -((-a + b - 1) / b); };
  i64 ys[2];
  i64 m, d;
  civil_from_days(fdiv(lo_ms, DAY), ys[0], m, d);
  civil_from_days(fdiv(hi_ms, DAY), ys[1], m, d);
  for (int k = 0; k < 2; k++) {
    g_ytxt[k] = 0;
    for (int i = 0; i < 416; i++) g_dayms[k][i] = DAYMS_NONE;
    if (ys[k] < 1000 || ys[k] > 9999) continue;
    char t[8];
    snprintf(t, sizeof t, "%04d", (int)ys[k]);
    memcpy(&g_ytxt[k], t, 4);
    for (int mo = 1; mo <= 12; mo++) {
      const int dim = DIM[mo] + ((mo == 2 && is_leap((u32)ys[k])) ? 1 : 0);
      for (int dd = 1; dd <= dim; dd++) g_dayms[k][mo * 32 + dd] = days_from_civil(ys[k], mo, dd) * DAY;
    }
  }
}

// Two F1 timestamps at once. True iff both are valid F1 in a candidate year; then ta / tb are their values.
// Needs 20 readable bytes at a and at b. OKM: the positions (bytes 4..19 of a, then of b) that are checked;
// leaving out bit 15 accepts any byte in place of a's 'Z'.
// (cb: b once more, in seconds: its day's entry in g_daycode plus the seconds of the day.)
template <u32 OKM>
static FORCE_INLINE bool f1_pair_mc(const u8* a, const u8* b, i64& ta, i64& tb, i64& cb) {
  // bytes 4..19 of both: "-MM-DDTHH:MM:SSZ"
  const __m256i v = _mm256_inserti128_si256(_mm256_castsi128_si256(_mm_loadu_si128((const __m128i*)(a + 4))),
                                            _mm_loadu_si128((const __m128i*)(b + 4)), 1);
  const __m256i d = _mm256_sub_epi8(v, _mm256_set1_epi8('0'));
  const __m256i isdig = _mm256_cmpeq_epi8(_mm256_min_epu8(d, _mm256_set1_epi8(9)), d);
  const __m256i sepval = _mm256_setr_epi8('-', 0, 0, '-', 0, 0, 'T', 0, 0, ':', 0, 0, ':', 0, 0, 'Z', '-', 0, 0, '-',
                                          0, 0, 'T', 0, 0, ':', 0, 0, ':', 0, 0, 'Z');
  const __m256i sepm = _mm256_setr_epi8(-1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, -1, 0, 0, -1, 0, 0, -1, 0,
                                        0, -1, 0, 0, -1, 0, 0, -1);
  const u32 okm = (u32)_mm256_movemask_epi8(_mm256_blendv_epi8(isdig, _mm256_cmpeq_epi8(v, sepval), sepm));
  const __m256i g = _mm256_shuffle_epi8(d, _mm256_setr_epi8(1, 2, 4, 5, 7, 8, 10, 11, 13, 14, -1, -1, -1, -1, -1, -1, 1,
                                                            2, 4, 5, 7, 8, 10, 11, 13, 14, -1, -1, -1, -1, -1, -1));
  const __m256i p = _mm256_maddubs_epi16(
      g, _mm256_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0, 0, 0, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0,
                          0, 0, 0, 0));  // month, day, hour, minute, second
  // upper bounds (they also keep month * 32 + day inside the table); month 0 / day 0 are not in the table
  const __m256i bad = _mm256_cmpgt_epi16(p, _mm256_setr_epi16(12, 31, 23, 59, 59, 0, 0, 0, 12, 31, 23, 59, 59, 0, 0, 0));
  if (((okm & OKM) != OKM) | !_mm256_testz_si256(bad, bad)) return false;
  const __m256i q = _mm256_madd_epi16(p, _mm256_setr_epi16(32, 1, 60, 1, 1, 0, 0, 0, 32, 1, 60, 1, 1, 0, 0, 0));
  alignas(32) u32 Lbuf[8];  // month * 32 + day, hour * 60 + minute, second, 0
  _mm256_store_si256((__m256i*)Lbuf, q);
  const u32* L = Lbuf;
  __asm__("" : "+r"(L));  // plain loads instead of vector extracts
  u32 ya, yb;
  memcpy(&ya, a, 4);
  memcpy(&yb, b, 4);
  const u32 ka = ya == g_ytxt[1], kb = yb == g_ytxt[1];
  if (!(((ya == g_ytxt[0]) | ka) & ((yb == g_ytxt[0]) | kb))) return false;
  const i64 da = g_dayms[ka][L[0]], db = g_dayms[kb][L[4]];
  if ((da == DAYMS_NONE) | (db == DAYMS_NONE)) return false;
  ta = da + (i64)(L[1] * 60 + L[2]) * 1000;
  const u32 sb = L[5] * 60 + L[6];
  tb = db + (i64)sb * 1000;
  cb = g_daycode[kb][L[4]] + (i64)sb;
  return true;
}
template <u32 OKM>
static FORCE_INLINE bool f1_pair_m(const u8* a, const u8* b, i64& ta, i64& tb) {
  i64 cb;
  return f1_pair_mc<OKM>(a, b, ta, tb, cb);
}

static FORCE_INLINE bool f1_pair(const u8* a, const u8* b, i64& ta, i64& tb) {
  return f1_pair_m<0xFFFFFFFFu>(a, b, ta, tb);
}

// One F1 timestamp. Needs 20 readable bytes at a. OKM as above (16 bits).
template <u32 OKM>
static FORCE_INLINE bool f1_one_mc(const u8* a, i64& ta, i64& ca) {
  const __m128i v = _mm_loadu_si128((const __m128i*)(a + 4));
  const __m128i d = _mm_sub_epi8(v, _mm_set1_epi8('0'));
  const __m128i isdig = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
  const __m128i sepval = _mm_setr_epi8('-', 0, 0, '-', 0, 0, 'T', 0, 0, ':', 0, 0, ':', 0, 0, 'Z');
  const __m128i sepm = _mm_setr_epi8(-1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1);
  const u32 okm = (u32)_mm_movemask_epi8(_mm_blendv_epi8(isdig, _mm_cmpeq_epi8(v, sepval), sepm));
  const __m128i g = _mm_shuffle_epi8(d, _mm_setr_epi8(1, 2, 4, 5, 7, 8, 10, 11, 13, 14, -1, -1, -1, -1, -1, -1));
  const __m128i p = _mm_maddubs_epi16(g, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0, 0, 0));
  const __m128i bad = _mm_cmpgt_epi16(p, _mm_setr_epi16(12, 31, 23, 59, 59, 0, 0, 0));
  if (((okm & OKM) != OKM) | !_mm_testz_si128(bad, bad)) return false;
  const __m128i q = _mm_madd_epi16(p, _mm_setr_epi16(32, 1, 60, 1, 1, 0, 0, 0));
  alignas(16) u32 Lbuf[4];
  _mm_store_si128((__m128i*)Lbuf, q);
  const u32* L = Lbuf;
  __asm__("" : "+r"(L));
  u32 ya;
  memcpy(&ya, a, 4);
  const u32 ka = ya == g_ytxt[1];
  if (!((ya == g_ytxt[0]) | ka)) return false;
  const i64 da = g_dayms[ka][L[0]];
  if (da == DAYMS_NONE) return false;
  const u32 sa = L[1] * 60 + L[2];
  ta = da + (i64)sa * 1000;
  ca = g_daycode[ka][L[0]] + (i64)sa;
  return true;
}
template <u32 OKM>
static FORCE_INLINE bool f1_one_m(const u8* a, i64& ta) {
  i64 ca;
  return f1_one_mc<OKM>(a, ta, ca);
}
static FORCE_INLINE bool f1_one(const u8* a, i64& ta) { return f1_one_m<0xFFFFu>(a, ta); }

// The UTC offset of a 25-byte timestamp "YYYY-MM-DDTHH:MM:SS+HH:MM" at a (bytes 19..24: a sign, then HH:MM
// with HH <= 23 and MM <= 59), as the milliseconds to add to its civil time: negative for '+'. False if
// those six bytes are anything else.
static FORCE_INLINE bool f3_offset(const u8* a, i64& add_ms) {
  u64 x;
  memcpy(&x, a + 17, 8);  // S S sign H H : M M
  const u32 sg = (u32)(x >> 16) & 0xFF;
  const u64 t = (x >> 24) ^ 0x30303A3030ULL;  // the digits as values, the colon as 0
  // a digit byte is fine iff it is <= 9, the colon byte iff it is 0: adding 0x76 / 0x7F sets the top bit of
  // any other value below 0x80, and a value from 0x80 has its top bit set to begin with
  const u64 bad = ((t + 0x76767F7676ULL) | t) & 0x8080808080ULL;
  const u64 u = t * 0x0A01;  // byte 1: HH, byte 4: MM
  const u32 oh = (u32)(u >> 8) & 0xFF, om = (u32)(u >> 32) & 0xFF;
  if ((bad != 0) | (((sg - 0x2B) & ~2u) != 0) | (oh > 23) | (om > 59)) return false;  // sign: '+' 0x2B or '-' 0x2D
  add_ms = (i64)((i32)sg - 0x2C) * (i64)((oh * 60 + om) * 60000u);
  return true;
}

// A 25-byte timestamp with a UTC offset (a) and an F1 timestamp (b) at once: true iff both are valid and
// their civil years are candidate years. Needs 25 readable bytes at a and 20 at b.
static FORCE_INLINE bool f3f1_pair(const u8* a, const u8* b, i64& ta, i64& tb) {
  i64 add;
  if (!f3_offset(a, add) || !f1_pair_m<0xFFFF7FFFu>(a, b, ta, tb)) return false;
  ta += add;
  return true;
}

// One 25-byte timestamp with a UTC offset. Needs 25 readable bytes at a.
static FORCE_INLINE bool f3_one(const u8* a, i64& ta) {
  i64 add;
  if (!f3_offset(a, add) || !f1_one_m<0x7FFFu>(a, ta)) return false;
  ta += add;
  return true;
}

// ------------------------------------------------------------------------------------------
// files

struct MFile {
  const u8* data = nullptr;
  size_t size = 0;
};

static MFile map_file(const std::string& path) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) die(("open " + path).c_str());
  struct stat st;
  if (fstat(fd, &st) != 0) die("fstat");
  size_t size = (size_t)st.st_size;
  size_t span = ((size + 4095) & ~(size_t)4095) + 8192;
  void* base = mmap(nullptr, span, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (base == MAP_FAILED) die("mmap reserve");
  if (size) {
    void* p = mmap(base, size, PROT_READ, MAP_PRIVATE | MAP_FIXED, fd, 0);
    if (p == MAP_FAILED) die("mmap file");
  }
  close(fd);
  return {(const u8*)base, size};
}

static std::vector<std::string> list_csv(const std::string& dir) {
  std::vector<std::string> out;
  DIR* d = opendir(dir.c_str());
  if (!d) return out;
  while (dirent* e = readdir(d)) {
    std::string n = e->d_name;
    if (n.size() < 5 || n[0] == '.' || n.compare(n.size() - 4, 4, ".csv") != 0) continue;
    std::string p = dir + "/" + n;
    struct stat st;
    if (stat(p.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) continue;
    out.push_back(p);
  }
  closedir(d);
  std::sort(out.begin(), out.end());
  return out;
}

static std::string read_small(const std::string& path) {
  FILE* f = fopen(path.c_str(), "rb");
  if (!f) die(("open " + path).c_str());
  std::string s;
  char buf[65536];
  size_t n;
  while ((n = fread(buf, 1, sizeof buf, f)) > 0) s.append(buf, n);
  fclose(f);
  return s;
}

// ------------------------------------------------------------------------------------------
// CSV scanning: see scan.h
using scan::scan_chunk;

struct Chunk {
  int file;
  size_t nominal;  // nominal start
  size_t nominal_end;
};

static bool g_drop_parsed = true;
// Drop the mapping's page-table entries for a parsed chunk, on the worker that parsed it: otherwise the
// kernel unmaps every input page serially at process exit.
static inline void drop_parsed(const MFile& f, size_t a, size_t b) {
  if (!g_drop_parsed) return;
  a = (a + 4095) & ~(size_t)4095;
  b &= ~(size_t)4095;
  if (b > a) madvise((void*)(f.data + a), b - a, MADV_DONTNEED);
}

static inline size_t line_start_at_or_after(const MFile& f, size_t x) {
  if (x == 0) return 0;
  if (x >= f.size) return f.size;
  const u8* nl = (const u8*)memchr(f.data + x - 1, '\n', f.size - (x - 1));
  return nl ? (size_t)(nl - f.data) + 1 : f.size;
}

static void make_chunks(const std::vector<MFile>& files, size_t chunk, std::vector<Chunk>& out) {
  for (int i = 0; i < (int)files.size(); i++) {
    size_t sz = files[i].size;
    if (sz == 0) continue;
    size_t n = (sz + chunk - 1) / chunk;
    for (size_t k = 0; k < n; k++) out.push_back({i, k * chunk, std::min(sz, (k + 1) * chunk)});
  }
}

// ------------------------------------------------------------------------------------------
// global data

static i64 g_ws, g_we;  // window [ws, we) in ms
static i64 g_day0;      // floor(ws / DAY)
static int g_ndays;

// categories
static std::vector<std::string> g_cats;  // sorted byte-wise, includes "unknown"
static u32 g_cat_unknown;
static std::vector<u8> g_cat_by_num;  // canonical "p<num>" -> category id (or unknown)
static std::unordered_map<std::string, u32> g_cat_slow;

// campaigns
static std::vector<std::string> g_camp_ids, g_camp_channel;
static std::vector<i32> g_camp_by_num;  // canonical "c_<num>" -> index or -1
static std::unordered_map<std::string, u32> g_camp_slow;
static u32 g_ncamp;

static inline u32 cat_of(const u8* s, u32 n) {
  if (n >= 2 && n <= 9 && s[0] == 'p' && (s[1] != '0' || n == 2)) {
    u32 v = 0;
    bool ok = true;
    for (u32 i = 1; i < n; i++) {
      u32 c = s[i] - '0';
      if (c > 9) {
        ok = false;
        break;
      }
      v = v * 10 + c;
    }
    if (ok && v < g_cat_by_num.size()) return g_cat_by_num[v];
  }
  if (g_cat_slow.empty()) return g_cat_unknown;
  auto it = g_cat_slow.find(std::string((const char*)s, n));
  return it == g_cat_slow.end() ? g_cat_unknown : it->second;
}

static inline i32 camp_of(const u8* s, u32 n) {
  if (n >= 3 && n <= 11 && s[0] == 'c' && s[1] == '_' && (s[2] != '0' || n == 3)) {
    u32 v = 0;
    bool ok = true;
    for (u32 i = 2; i < n; i++) {
      u32 c = s[i] - '0';
      if (c > 9) {
        ok = false;
        break;
      }
      v = v * 10 + c;
    }
    if (ok && v < g_camp_by_num.size()) return g_camp_by_num[v];
  }
  if (g_camp_slow.empty()) return -1;
  auto it = g_camp_slow.find(std::string((const char*)s, n));
  return it == g_camp_slow.end() ? -1 : (i32)it->second;
}

// nd (1..8) ASCII digits at s -> value, without a loop: the digits are right-aligned in a 64-bit word,
// checked and converted with three multiplications. False if any of the bytes is not a digit.
// Needs 8 readable bytes at s.
static FORCE_INLINE bool digits_le8(const u8* s, u32 nd, u64& v) {
  u64 w;
  memcpy(&w, s, 8);
  const u32 sh = 8 * (8 - nd);  // the first digit goes to byte 8 - nd; the bytes before it become '0'
  w = (w << sh) | _bzhi_u64(0x3030303030303030ULL, sh);
  if (((w & 0xF0F0F0F0F0F0F0F0ULL) | (((w + 0x0606060606060606ULL) & 0xF0F0F0F0F0F0F0F0ULL) >> 4)) !=
      0x3333333333333333ULL)
    return false;
  w = ((w & 0x0F0F0F0F0F0F0F0FULL) * 2561) >> 8;
  w = ((w & 0x00FF00FF00FF00FFULL) * 6553601) >> 16;
  v = ((w & 0x0000FFFF0000FFFFULL) * 42949672960001ULL) >> 32;
  return true;
}

// camp_of for the usual form "c_<1..8 digits>" without a loop; everything else goes to camp_of.
// Needs 10 readable bytes at s.
static FORCE_INLINE i32 camp_fast(const u8* s, u32 n) {
  const u32 nd = n - 2;
  u16 pre;
  memcpy(&pre, s, 2);
  u64 v;
  if (__builtin_expect((nd - 1 > 7) | (pre != 0x5F63) | ((s[2] == '0') & (n != 3)), 0)) return camp_of(s, n);
  if (__builtin_expect(!digits_le8(s + 2, nd, v) || v >= g_camp_by_num.size(), 0)) return camp_of(s, n);
  return g_camp_by_num[v];
}

// H: link ID -> person, open addressing with linear probing. Keys are hash_id of the ID's key (0: empty slot;
// no ID has key 0). Once the links are built, the value is hash_id of the person's name key. While they are
// being built it is the ID's union-find parent: 0 for a root, else the parent's slot index + 1. Zero-filled
// memory is thus an empty table in which every ID that arrives starts as its own root.
struct HSlot {
  u64 key;
  u64 val;
};
static HSlot* g_H = nullptr;
static u32 g_Hbits = 0;
// Blocked Bloom filter of the IDs whose person differs from the ID itself (the only ones that need H).
// Three bits per ID in one 64-bit word; the word is picked by the top bits of the hash, the bits by the
// 18 bits below them.
static u64* g_F = nullptr;
static u32 g_Fshift = 63;  // word index = hash >> g_Fshift
static FORCE_INLINE u64 f_bits(u64 h, u32 fs) {
  return (1ULL << ((h >> (fs - 6)) & 63)) | (1ULL << ((h >> (fs - 12)) & 63)) | (1ULL << ((h >> (fs - 18)) & 63));
}

// The person (as a hash) of the ID with hash h: its value in H, or h itself if the ID is in no link.
static inline u64 h_lookup(u64 h) {
  const u64 mask = (1ULL << g_Hbits) - 1;
  u64 i = h >> (64 - g_Hbits);
  for (;;) {
    const HSlot& s = g_H[i];
    if (s.key == h) return s.val;
    if (s.key == 0) return h;
    i = (i + 1) & mask;
  }
}

// ---- while the links are being built (all threads at once)

// The slot of the ID with hash h, which is inserted if it is not there yet.
static FORCE_INLINE u64 h_slot_for(u64 h, u64 mask, u32 hshift) {
  u64 i = h >> hshift;
  for (;;) {
    u64 k = __atomic_load_n(&g_H[i].key, __ATOMIC_RELAXED);
    if (k == h) return i;
    if (k == 0) {
      if (__atomic_compare_exchange_n(&g_H[i].key, &k, h, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) return i;
      if (k == h) return i;  // another thread has just put the same ID here
    }
    i = (i + 1) & mask;
  }
}

static inline u64 uf_find(u64 x) {
  for (;;) {
    const u64 p = __atomic_load_n(&g_H[x].val, __ATOMIC_RELAXED);
    if (p == 0) return x;
    const u64 gp = __atomic_load_n(&g_H[p - 1].val, __ATOMIC_RELAXED);
    if (gp == 0) return p - 1;
    __atomic_store_n(&g_H[x].val, gp, __ATOMIC_RELAXED);  // any ancestor is a valid parent
    x = gp - 1;
  }
}

// The root of a slot, without writing anything.
static inline u64 uf_root(u64 x) {
  for (;;) {
    const u64 p = __atomic_load_n(&g_H[x].val, __ATOMIC_RELAXED);
    if (p == 0) return x;
    x = p - 1;
  }
}

// Joins the sets of two slots. The root whose ID is byte-wise smaller stays the root, so in the end every
// root is the smallest ID of its set: the person's name.
static inline void uf_unite(u64 a, u64 b) {
  for (;;) {
    a = uf_find(a);
    b = uf_find(b);
    if (a == b) return;
    if (key_cmp(unhash_id(g_H[a].key), unhash_id(g_H[b].key)) > 0) std::swap(a, b);
    u64 exp = 0;  // b must still be a root
    if (__atomic_compare_exchange_n(&g_H[b].val, &exp, a + 1, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED)) return;
  }
}

// records
struct u64x2 {
  u64 a, b;
};

struct R1 {  // event line, partitioned by event_id; eid = hash_id(event_id key)
  u64 eid;
  i64 ingest;
  u64 uid;      // hash_id(user_id key) (valid lines only)
  u32 ts;       // ms since window start, TS_INVALID if the line is not a valid event
  u32 payload;  // purchase: cat << 24 | price; else 0
};
struct R2 {  // surviving event, partitioned by person; h = hash_id(person name key)
  u64 h;
  u32 ts;
  u32 payload;
};
struct R2C {  // valid click, partitioned by person; h = hash_id(person name key)
  u64 h;
  u64 click;
  u32 ts;  // ms since window start - 7 days
  u32 camp;
  u64 pad;
};

static u32 g_P1bits, g_P2bits;

struct CStat {
  u32 sess, purch;
  i64 rev;
};

// The columns of user_daily, appended row by row in the form they have in the file (Parquet PLAIN), in
// blocks that are handed to the writer as they are.
struct OutCols {
  Stream<u8> ids;   // the user_id of each person that has rows, once: 4-byte length, then the bytes
  Stream<u8> runs;  // ... and how many rows that person has
  Stream<u8> day;   // day of the window
  Stream<u8> sess;  // (a day has room for at most 48 sessions: their starts are more than 30 minutes apart)
  Stream<u32> evs;
  Stream<i64> rev;
  Stream<u8> top;  // category, 0xFF: none
  u64 nrows = 0, npersons = 0;
  u32 max_sess = 0, max_evs = 0;
  void finish() {
    ids.finish();
    runs.finish();
    day.finish();
    sess.finish();
    evs.finish();
    rev.finish();
    top.finish();
  }
};

// Records of the dense path (dense.h)
struct D16 {  // event line, partitioned by event index
  u64 w0;  // bits 0..30: ts in ms since the window start, plus 1 (0: not a valid event); bit 31: bits 32..63
           // are not the user number but thread << 24 | index of a side entry (XEv); bits 32..63: user number
  u64 w1;  // bits 0..23: ingest_ts code; bits 24..45: purchase, category << 17 | price; bits 46..59: slot
};
struct C16 {  // valid click, partitioned by person index
  u64 w0;  // bits 0..31: ts in ms since the window start - 7 days; bits 32..63: user number
  u64 w1;  // bits 0..39: number of the click_id; bits 40..63: campaign
};
struct X1Rec {  // event line whose event_id is not regular (hashes as in R1)
  u64 eid;
  i64 ingest;
  u64 uid;
  u32 ts, payload;
  u32 num;  // user number, or 0xFFFFFFFF if the user_id is not regular
};
struct XEv {  // side entry of a valid event whose user or payload does not fit a D16
  u64 uid;  // hash_id(user_id key)
  u32 payload;
  u32 num;  // user number, or 0xFFFFFFFF
};
struct XClk {  // valid click whose click_id or user_id is not regular
  u64 uid;    // hash_id(user_id key)
  u64 click;  // click_id key
  u32 ts, camp;
  u32 num;  // user number, or 0xFFFFFFFF
};

struct TL {
  Arena arena;
  WCScatter<R1> r1;
  WCScatter<R2> r2;
  WCScatter<R2C> r2c;
  WCScatter<D16> ev;
  WCScatter<C16> ck;
  std::vector<X1Rec> x1;
  std::vector<XEv> xev;
  std::vector<XClk> xclk;
  OutCols out;
  std::vector<CStat> c_stat;   // ndays x (ncamp + 1): credited sessions, purchasing sessions, revenue
  std::vector<u32> c_assist;   // ndays x (ncamp + 1)
  u64 n_event_lines = 0, n_click_lines = 0;
};
static std::vector<TL> g_tl;

// Person lookup and scatter for a stream of items, a batch at a time. Each item comes with the hash of its
// user ID. Two simple loops instead of one with data-dependent branches: (1) split the batch, without
// branching, into items whose ID is its own person and items the filter flags, prefetching the H slot of the
// latter; (2) emit both lists. emit(cursor, person hash, item) is called once per item, in no particular
// order. The filter word of an item is prefetched when the item is added.
template <class Item, class Rec, class Emit>
struct Router {
  static constexpr u32 CAP = 128;
  using Cursor = typename WCScatter<Rec>::Cursor;
  u64 key[CAP];
  Item item[CAP];
  u8 ld[CAP + 1], lh[CAP + 1];
  u32 n = 0;
  Cursor& cur;
  Emit& emit;
  Router(Cursor& c, Emit& e) : cur(c), emit(e) {}
  // What a loop that adds items needs, as a local of that loop (so it stays in registers).
  struct Feed {
    u64* key;
    Item* item;
    const u64* F;
    Router* r;
    u32 fs;
    u32 n;
    // The item is stored either way; it stays only if keep is 1.
    FORCE_INLINE void add_if(u64 uh, const Item& it, u32 keep) {
      key[n] = uh;
      item[n] = it;
      _mm_prefetch((const char*)&F[uh >> fs], _MM_HINT_T0);  // (a null F is harmless here)
      n += keep;
      if (__builtin_expect(n == CAP, 0)) {
        r->n = n;
        r->run();
        n = 0;
      }
    }
  };
  Feed feed() { return Feed{key, item, g_F, this, g_Fshift, n}; }
  void close(const Feed& f) { n = f.n; }
  NOINLINE void run() {
    const u32 m = n;
    n = 0;
    Cursor c = cur;  // in registers for the loops below
    if (!g_H) {
      for (u32 i = 0; i < m; i++) emit(c, key[i], item[i]);
      cur = c;
      return;
    }
    const u64* const F = g_F;
    const HSlot* const H = g_H;
    const u32 fs = g_Fshift, hshift = 64 - g_Hbits;
    const uintptr_t dummy = (uintptr_t)key;
    u32 nh = 0;
    for (u32 i = 0; i < m; i++) {
      const u64 x = key[i];
      const u64 b = f_bits(x, fs);
      const u32 maybe = (F[x >> fs] & b) == b;
      // The item joins one of the two lists. Both entries are written: the one in the list it does not join
      // lies just past that list's end (the direct items so far number i - nh).
      ld[i - nh] = (u8)i;
      lh[nh] = (u8)i;
      nh += maybe;
      // prefetch the H slot of flagged items only (the others get a harmless address); a conditional move,
      // spelled out so that it does not become a branch
      uintptr_t a = (uintptr_t)&H[x >> hshift];
      __asm__("testl %[m], %[m]\n\tcmove %[d], %[a]" : [a] "+r"(a) : [m] "r"(maybe), [d] "r"(dummy) : "cc");
      _mm_prefetch((const char*)a, _MM_HINT_T0);
    }
    const u32 nd = m - nh;
    for (u32 j = 0; j < nd; j++) {
      const u32 i = ld[j];
      emit(c, key[i], item[i]);
    }
    for (u32 j = 0; j < nh; j++) {
      const u32 i = lh[j];
      emit(c, h_lookup(key[i]), item[i]);
    }
    cur = c;
  }
};

// ------------------------------------------------------------------------------------------
// lookups

static void load_lookups(const std::string& data) {
  std::string man = read_small(data + "/manifest.json");
  auto get = [&](const char* name) -> i64 {
    size_t p = man.find(std::string("\"") + name + "\"");
    if (p == std::string::npos) die("manifest key");
    p = man.find(':', p);
    p = man.find('"', p);
    size_t q = man.find('"', p + 1);
    std::string v = man.substr(p + 1, q - p - 1);
    v.append(32, '\0');
    i64 out;
    if (!parse_ts_ref((const u8*)v.data(), (u32)(q - p - 1), out)) die("manifest timestamp");
    return out;
  };
  g_ws = get("window_start");
  g_we = get("window_end");
  if (g_we <= g_ws) die("empty window");
  if ((u64)(g_we - g_ws) + (u64)WEEK >= 0xFFFFFFF0ULL) die("window too long for 32-bit offsets");
  auto fdiv = [](i64 a, i64 b) { return a >= 0 ? a / b : -((-a + b - 1) / b); };
  g_day0 = fdiv(g_ws, DAY);
  g_ndays = (int)(fdiv(g_we - 1, DAY) - g_day0 + 1);
  init_fast_years(g_ws - WEEK, g_we + 6 * DAY);

  auto read_pairs = [&](const std::string& path, std::vector<std::pair<std::string, std::string>>& rows) {
    std::string s = read_small(path);
    size_t n = s.size();
    s.append(64, '\0');
    scan_chunk<2>((const u8*)s.data(), (const u8*)s.data() + n, true, [&](const u8* sb, const u32* fo, const u32* fl) {
      rows.emplace_back(std::string((const char*)(sb + fo[0]), fl[0]), std::string((const char*)(sb + fo[1]), fl[1]));
    });
  };
  {
    // products: canonical "p<num>" ids go to a direct array; categories are few
    std::string sbuf = read_small(data + "/products.csv");
    size_t sn = sbuf.size();
    sbuf.append(64, '\0');
    std::vector<std::string> cat_names;  // first-seen order
    std::vector<std::pair<u32, u32>> canon;  // (num, first-seen category index)
    std::vector<std::pair<std::string, u32>> other;
    std::unordered_map<std::string, u32> seen_other;
    u32 maxnum = 0;
    // category index: a small direct-mapped cache on the length and the first 8 bytes of the name, in front
    // of a linear scan (there are only a few categories, and nearly every line hits the cache)
    struct CatSlot {
      u64 sig;
      u32 len, ci;
    };
    CatSlot ccache[64];
    for (auto& c : ccache) c = CatSlot{0, 0, UINT32_MAX};
    scan_chunk<2>((const u8*)sbuf.data(), (const u8*)sbuf.data() + sn, true, [&](const u8* sb, const u32* fo, const u32* fl) {
      const u8* const cn = sb + fo[1];
      const u32 cl = fl[1];
      u64 sig;
      memcpy(&sig, cn, 8);  // (64 readable bytes follow the text)
      if (cl < 8) sig = _bzhi_u64(sig, 8 * cl);
      CatSlot& cs = ccache[((sig * 0x9E3779B97F4A7C15ULL) >> 58) ^ (cl & 63)];
      u32 ci = UINT32_MAX;
      if (cs.ci != UINT32_MAX && cs.sig == sig && cs.len == cl && (cl <= 8 || memcmp(cat_names[cs.ci].data(), cn, cl) == 0)) {
        ci = cs.ci;
      } else {
        for (u32 i = 0; i < cat_names.size(); i++)
          if (cat_names[i].size() == cl && memcmp(cat_names[i].data(), cn, cl) == 0) {
            ci = i;
            break;
          }
        if (ci == UINT32_MAX) {
          ci = (u32)cat_names.size();
          cat_names.emplace_back((const char*)cn, cl);
          if (cat_names.size() > 4096) die("too many categories");
        }
        cs = CatSlot{sig, cl, ci};
      }
      const u8* id = (sb + fo[0]);
      u32 n = fl[0];
      bool canonical = n >= 2 && n <= 9 && id[0] == 'p' && (id[1] != '0' || n == 2);
      u32 v = 0;
      for (u32 i = 1; canonical && i < n; i++) {
        if (!is_dig(id[i]))
          canonical = false;
        else
          v = v * 10 + (id[i] - '0');
      }
      if (canonical && v < (1u << 24)) {
        canon.push_back({v, ci});
        maxnum = std::max(maxnum, v);
      } else {
        std::string key((const char*)id, n);
        if (seen_other.emplace(key, ci).second) other.push_back({key, ci});
      }
    });
    std::vector<std::string> cats = cat_names;
    bool has_unknown = false;
    for (auto& c : cats) has_unknown |= (c == "unknown");
    if (!has_unknown) cats.push_back("unknown");
    std::sort(cats.begin(), cats.end());
    if (cats.size() > 255) die("more than 255 categories");
    std::vector<u32> rank(cat_names.size());
    for (u32 i = 0; i < cat_names.size(); i++)
      rank[i] = (u32)(std::lower_bound(cats.begin(), cats.end(), cat_names[i]) - cats.begin());
    g_cats = cats;
    g_cat_unknown = (u32)(std::lower_bound(cats.begin(), cats.end(), std::string("unknown")) - cats.begin());
    g_cat_by_num.assign(canon.empty() ? 0 : (size_t)maxnum + 1, 0xFF);
    for (auto& c : canon)
      if (g_cat_by_num[c.first] == 0xFF) g_cat_by_num[c.first] = (u8)rank[c.second];  // first wins
    for (auto& x : g_cat_by_num)
      if (x == 0xFF) x = (u8)g_cat_unknown;
    for (auto& o : other) g_cat_slow.emplace(o.first, rank[o.second]);
  }
  {
    std::vector<std::pair<std::string, std::string>> rows;
    read_pairs(data + "/campaigns.csv", rows);
    u32 maxnum = 0;
    std::vector<std::pair<u32, u32>> canon;
    std::unordered_map<std::string, u32> seen_ids;
    for (auto& r : rows) {
      if (seen_ids.count(r.first)) continue;
      u32 idx = (u32)g_camp_ids.size();
      seen_ids.emplace(r.first, idx);
      g_camp_ids.push_back(r.first);
      g_camp_channel.push_back(r.second);
      const std::string& id = r.first;
      bool canonical =
          id.size() >= 3 && id.size() <= 11 && id[0] == 'c' && id[1] == '_' && (id[2] != '0' || id.size() == 3);
      u32 v = 0;
      for (size_t i = 2; canonical && i < id.size(); i++) {
        if (!is_dig((u8)id[i]))
          canonical = false;
        else
          v = v * 10 + (id[i] - '0');
      }
      if (canonical && v < (1u << 24)) {
        canon.push_back({v, idx});
        maxnum = std::max(maxnum, v);
      } else {
        g_camp_slow.emplace(id, idx);
      }
    }
    g_camp_by_num.assign(canon.empty() ? 0 : (size_t)maxnum + 1, -1);
    for (auto& c : canon) g_camp_by_num[c.first] = (i32)c.second;
    g_ncamp = (u32)g_camp_ids.size();
  }
}

// ------------------------------------------------------------------------------------------
// links

static size_t count_newlines(const u8* p, size_t n) {  // may read up to 31 bytes past the end (zeros or text)
  const __m256i nl = _mm256_set1_epi8('\n');
  size_t c = 0, i = 0;
  for (; i + 32 <= n; i += 32)
    c += (u32)__builtin_popcount((u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_loadu_si256((const __m256i*)(p + i)), nl)));
  for (; i < n; i++) c += p[i] == '\n';
  return c;
}

struct LkBatch {
  static constexpr u32 CAP = 32;
  alignas(32) u32 off[CAP][8];
  alignas(32) u32 len[CAP][8];
  u64 ha[CAP], hb[CAP];
  u64 sa[CAP], sb[CAP];
};

// A batch of link lines: hash both IDs and prefetch their slots; find or insert them; join them.
static NOINLINE void process_links(LkBatch& B, const u32 n, const u8* const base) {
  const u32 hshift = 64 - g_Hbits;
  const u64 mask = (1ULL << g_Hbits) - 1;
  for (u32 i = 0; i < n; i++) {
    const u32* off = B.off[i];
    const u32* len = B.len[i];
    const u64 ha = hash_id(make_key(base + off[0], len[0])), hb = hash_id(make_key(base + off[1], len[1]));
    B.ha[i] = ha;
    B.hb[i] = hb;
    __builtin_prefetch(&g_H[ha >> hshift], 1, 3);
    __builtin_prefetch(&g_H[hb >> hshift], 1, 3);
  }
  for (u32 i = 0; i < n; i++) {
    const u64 sa = h_slot_for(B.ha[i], mask, hshift), sb = h_slot_for(B.hb[i], mask, hshift);
    B.sa[i] = sa;
    B.sb[i] = sb;
    // their parents, if they have any (a root gets a harmless address)
    _mm_prefetch((const char*)&g_H[(__atomic_load_n(&g_H[sa].val, __ATOMIC_RELAXED) - 1) & mask], _MM_HINT_T0);
    _mm_prefetch((const char*)&g_H[(__atomic_load_n(&g_H[sb].val, __ATOMIC_RELAXED) - 1) & mask], _MM_HINT_T0);
  }
  for (u32 i = 0; i < n; i++) uf_unite(B.sa[i], B.sb[i]);
}

static NOINLINE u64 parse_link_chunk(const u8* begin, const u8* end, bool skip_header) {
  LkBatch B;
  u32 n = 0;
  u64 nl = 0;
  const u8* base = begin;
  scan::scan_lines<2>(begin, end, skip_header, [&](const u8* sb, __m256i st, __m256i ln) __attribute__((always_inline)) {
    _mm256_store_si256((__m256i*)B.off[n], st);
    _mm256_store_si256((__m256i*)B.len[n], ln);
    base = sb;
    // a line with an empty ID links nothing: its slot is reused
    n += (_mm256_movemask_epi8(_mm256_cmpeq_epi32(ln, _mm256_setzero_si256())) & 0xFF) == 0;
    if (__builtin_expect(n == LkBatch::CAP, 0)) {
      process_links(B, n, sb);
      nl += n;
      n = 0;
    }
  });
  if (n) process_links(B, n, base);
  return nl + n;
}

// What is left to do once every link is joined in H (g_Hbits slots): two passes over the table, both in runs
// of SUB slots: the used slots of a run are listed without branching while the slots they point at are
// prefetched, then the listed slots are processed. nlinks is only reported.
static void finish_links(size_t nlinks) {
  const u64 nslots = 1ULL << g_Hbits;
  const int ntask = (int)std::min<u64>(nslots >> 10, 1024);
  const u64 mask = nslots - 1;
  constexpr u32 SUB = 256;
  // Pass 1: point every ID at its root, directly. Each slot is written by one thread only here (uf_find would
  // also rewrite the slots it passes, and could then leave an ID pointing at an ancestor that is not the root).
  std::vector<size_t> nused(ntask, 0), nroots(ntask, 0);
  parallel_for(ntask, [&](int t, int) {
    const u64 a = nslots * t / ntask, b = nslots * (t + 1) / ntask;
    size_t used = 0, roots = 0;
    u32 idx[SUB];
    for (u64 s0 = a; s0 < b; s0 += SUB) {
      const u32 m = (u32)std::min<u64>(SUB, b - s0);
      u32 n = 0;
      for (u32 j = 0; j < m; j++) {
        const HSlot& sl = g_H[s0 + j];
        idx[n] = j;
        n += sl.key != 0;
        // (an empty slot or a root gets a harmless address)
        _mm_prefetch((const char*)&g_H[(__atomic_load_n(&sl.val, __ATOMIC_RELAXED) - 1) & mask], _MM_HINT_T0);
      }
      for (u32 k = 0; k < n; k++) {
        const u64 i = s0 + idx[k];
        const u64 r = uf_root(i);
        __atomic_store_n(&g_H[i].val, r == i ? 0 : r + 1, __ATOMIC_RELAXED);
        roots += r == i;
      }
      used += n;
    }
    nused[t] = used;
    nroots[t] = roots;
  });
  size_t ids = 0, remap = 0;
  for (int t = 0; t < ntask; t++) {
    ids += nused[t];
    remap += nused[t] - nroots[t];
  }
  u64 words = 1 << 10;
  while (words * 8 < remap) words <<= 1;  // >= 8 bits per ID in the filter
  g_F = (u64*)vm_alloc(words * 8);
  g_Fshift = 64 - ceil_log2(words);
  // Pass 2: parent -> person. The root's key is the hash of the person's name. Nothing reads another slot's
  // value any more, so the values can be replaced in place.
  parallel_for(ntask, [&](int t, int) {
    const u64 a = nslots * t / ntask, b = nslots * (t + 1) / ntask;
    const u32 fs = g_Fshift;
    u32 idx[SUB];
    for (u64 s0 = a; s0 < b; s0 += SUB) {
      const u32 m = (u32)std::min<u64>(SUB, b - s0);
      u32 n = 0;
      for (u32 j = 0; j < m; j++) {
        const HSlot& sl = g_H[s0 + j];
        idx[n] = j;
        n += sl.key != 0;
        _mm_prefetch((const char*)&g_H[(sl.val - 1) & mask], _MM_HINT_T0);
      }
      for (u32 k = 0; k < n; k++) {
        HSlot& sl = g_H[s0 + idx[k]];
        const u64 v = sl.val;
        if (v) {
          sl.val = g_H[v - 1].key;
          __atomic_fetch_or(&g_F[sl.key >> fs], f_bits(sl.key, fs), __ATOMIC_RELAXED);
        } else {
          sl.val = sl.key;
        }
      }
    }
  });
  LOG("links: %zu lines, %zu ids in %llu slots, %zu with another name; filter %llu KB", nlinks, ids,
      (unsigned long long)nslots, remap, (unsigned long long)(words * 8 / 1024));
}

// The general link phase: every ID of every link line goes into H.
static void build_links(std::vector<MFile>& files) {
  std::vector<Chunk> chunks;
  make_chunks(files, 1 << 18, chunks);
  if (chunks.empty()) return;
  const int nchunks = (int)chunks.size();
  // An upper bound on the number of link lines sizes the table: at most two new IDs per line.
  std::vector<size_t> part(nchunks, 0);
  parallel_for(nchunks, [&](int t, int) {
    const Chunk& c = chunks[t];
    part[t] = count_newlines(files[c.file].data + c.nominal, c.nominal_end - c.nominal);
  });
  size_t max_lines = files.size();  // a last line without a newline
  for (size_t c : part) max_lines += c;
  g_Hbits = std::max<u32>(10, ceil_log2(max_lines * 2 + 2));  // more slots than IDs even if every ID is new
  g_H = (HSlot*)vm_alloc(sizeof(HSlot) << g_Hbits);
  parallel_for(nchunks, [&](int t, int) {
    const Chunk& c = chunks[t];
    const MFile& f = files[c.file];
    size_t b = line_start_at_or_after(f, c.nominal), e = line_start_at_or_after(f, c.nominal_end);
    part[t] = b < e ? parse_link_chunk(f.data + b, f.data + e, b == 0) : 0;
  });
  size_t nlinks = 0;
  for (size_t c : part) nlinks += c;
  finish_links(nlinks);
}

namespace dn {
static std::atomic<u64>* g_acc = nullptr;  // per person index: what the links say about it (dense.h)
}  // namespace dn

// ------------------------------------------------------------------------------------------
// events + clicks parsing

static inline int event_type(const u8* s, u32 n) {
  if (n == 4) {
    u32 w;
    memcpy(&w, s, 4);
    if (w == 0x77656976u) return 0;  // "view"
    if (w == 0x74726163u) return 1;  // "cart"
    return -1;
  }
  if (n == 8) {
    u64 w;
    memcpy(&w, s, 8);
    if (w == 0x6573616863727570ULL) return 2;  // "purchase"
  }
  return -1;
}

static inline bool parse_price(const u8* s, u32 n, u32& out) {
  if (n == 0) return false;
  u32 i = 0;
  while (i < n && s[i] == '0') i++;
  if (n - i > 8) return false;
  u32 v = 0;
  for (; i < n; i++) {
    u32 c = s[i] - '0';
    if (c > 9) return false;
    v = v * 10 + c;
  }
  if (v < 1 || v > 10000000) return false;
  out = v;
  return true;
}

// Event lines are parsed a batch at a time, in three simple loops over the batch instead of one long loop
// body per line: keys and hashes; timestamps, grouped by the form of `ts` so each group runs one kernel
// without the form changing under it; validation, record assembly and the scatter.
// What stands in for a timestamp that does not parse: no window contains TS_NONE, and ING_NONE is later than
// 24 hours after any ts a window can contain (windows lie in the years 0000-9999, about +-2.6e14 ms), and
// later than every ingest_ts that does parse.
static constexpr i64 TS_NONE = INT64_MIN, ING_NONE = INT64_MAX / 2;

struct EvBatch {
  static constexpr u32 CAP = 64;
  alignas(32) u32 off[CAP][8];  // field offsets from the chunk base
  alignas(32) u32 len[CAP][8];  // field lengths
  alignas(32) R1 rec[CAP];      // the records, filled in step by step (ingest: ING_NONE if not a timestamp)
  i64 ts[CAP];                  // TS_NONE if not a timestamp
  u8 idx[4][CAP];               // lines by form of ts: 20 bytes, 13 bytes, 25 bytes, other
};

static NOINLINE void ts_exact(EvBatch& B, u32 i, const u8* base) {
  const u32* off = B.off[i];
  const u32* len = B.len[i];
  if (!parse_ts(base + off[2], len[2], B.ts[i])) B.ts[i] = TS_NONE;
  if (!parse_f1(base + off[3], len[3], B.rec[i].ingest)) B.rec[i].ingest = ING_NONE;
}

static NOINLINE void process_events(EvBatch& B, const u32 n, const u8* const base, WCScatter<R1>::Cursor& cur_out,
                                    const u32 p1shift, const i64 ws, const i64 we) {
  // ---- keys
  for (u32 i = 0; i < n; i++) {
    const u32* off = B.off[i];
    const u32* len = B.len[i];
    B.rec[i].eid = hash_id(make_key(base + off[0], len[0]));
    u64 uid = 0;
    if (__builtin_expect(len[1] != 0, 1)) uid = hash_id(make_key(base + off[1], len[1]));
    B.rec[i].uid = uid;
  }
  // ---- timestamps
  static const u8 CLS[32] = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 1, 3, 3, 3, 3, 3, 3, 0, 3, 3, 3, 3, 2, 3, 3, 3, 3, 3, 3};
  u32 cnt[4] = {0, 0, 0, 0};
  for (u32 i = 0; i < n; i++) {
    const u32 n2 = B.len[i][2];
    const u32 c = CLS[n2 < 31 ? n2 : 31];
    B.idx[c][cnt[c]++] = (u8)i;
  }
  for (u32 j = 0; j < cnt[0]; j++) {  // ts and ingest_ts both F1
    const u32 i = B.idx[0][j];
    const u32* off = B.off[i];
    i64 ta, tb;
    if (__builtin_expect(B.len[i][3] == 20 && f1_pair(base + off[2], base + off[3], ta, tb), 1)) {
      B.ts[i] = ta;
      B.rec[i].ingest = tb;
    } else {
      ts_exact(B, i, base);
    }
  }
  for (u32 j = 0; j < cnt[1]; j++) {  // ts as 13 digits
    const u32 i = B.idx[1][j];
    const u32* off = B.off[i];
    i64 ta, tb;
    if (__builtin_expect(B.len[i][3] == 20 && digits13(base + off[2], ta) && f1_one(base + off[3], tb), 1)) {
      B.ts[i] = ta;
      B.rec[i].ingest = tb;
    } else {
      ts_exact(B, i, base);
    }
  }
  for (u32 j = 0; j < cnt[2]; j++) {  // ts with a UTC offset
    const u32 i = B.idx[2][j];
    const u32* off = B.off[i];
    i64 ta, tb;
    if (__builtin_expect(B.len[i][3] == 20 && f3f1_pair(base + off[2], base + off[3], ta, tb), 1)) {
      B.ts[i] = ta;
      B.rec[i].ingest = tb;
    } else {
      ts_exact(B, i, base);
    }
  }
  for (u32 j = 0; j < cnt[3]; j++) ts_exact(B, B.idx[3][j], base);
  // ---- validation; the last field of each record
  const u64 wlen = (u64)(we - ws);
  for (u32 i = 0; i < n; i++) {
    const u32* off = B.off[i];
    const u32* len = B.len[i];
    const i64 ts = B.ts[i], ing = B.rec[i].ingest;
    const u64 uid = B.rec[i].uid;
    const u8* s4 = base + off[4];
    const u32 n4 = len[4];
    u32 w4;
    memcpy(&w4, s4, 4);
    // ts in the window, not more than 24 h late, user and product present (TS_NONE is in no window, ING_NONE
    // is always late, and the hash of a user ID is 0 only if there is none)
    const u64 rel = (u64)ts - (u64)ws;
    bool valid = (rel < wlen) & ((i64)((u64)ing - (u64)ts) <= DAY) & (uid != 0) & (len[5] != 0);
    u32 payload = 0;
    if (__builtin_expect(!((n4 == 4) & ((w4 == 0x77656976u) | (w4 == 0x74726163u))), 0)) {  // not view / cart
      u64 w8;
      memcpy(&w8, s4, 8);
      u32 price;
      if (valid && n4 == 8 && w8 == 0x6573616863727570ULL && parse_price(base + off[6], len[6], price))
        payload = (cat_of(base + off[5], len[5]) << 24) | price;
      else
        valid = false;
    }
    const u64 tspl = valid ? ((u64)(u32)rel | ((u64)payload << 32)) : (u64)TS_INVALID;
    memcpy(&B.rec[i].ts, &tspl, 8);
  }
  // ---- scatter (a loop of its own: few values to keep in registers, and the records are loaded whole long
  // after their fields were stored)
  auto cur = cur_out;
  for (u32 i = 0; i < n; i++) {
    const __m256i r = _mm256_load_si256((const __m256i*)&B.rec[i]);
    WCScatter<R1>::push32(cur, (u32)(B.rec[i].eid >> p1shift), r);
  }
  cur_out = cur;
}

// One chunk of an events file -> R1 records. Returns the number of lines with a non-empty event_id.
static NOINLINE u64 parse_event_chunk(const u8* begin, const u8* end, bool skip_header, TL& tl, const u32 p1shift,
                                      const i64 ws, const i64 we) {
  EvBatch B;
  u32 n = 0;
  u64 nl = 0;
  const u8* base = begin;
  auto cur = tl.r1.cursor(tl.arena);
  scan::scan_lines<7>(begin, end, skip_header, [&](const u8* sb, __m256i st, __m256i ln) __attribute__((always_inline)) {
    _mm256_store_si256((__m256i*)B.off[n], st);
    _mm256_store_si256((__m256i*)B.len[n], ln);
    base = sb;
    n += (u32)_mm256_cvtsi256_si32(ln) != 0;  // a line with an empty event_id is dropped: its slot is reused
    if (__builtin_expect(n == EvBatch::CAP, 0)) {
      process_events(B, n, sb, cur, p1shift, ws, we);
      nl += n;
      n = 0;
    }
  });
  if (n) process_events(B, n, base, cur, p1shift, ws, we);
  nl += n;
  tl.r1.close(cur);
  tl.r1.flush(tl.arena);
  return nl;
}

// Click lines, in batches like the event lines: timestamps by form, then keys, campaign and the record.
struct CkBatch {
  static constexpr u32 CAP = 64;
  alignas(32) u32 off[CAP][8];
  alignas(32) u32 len[CAP][8];
  i64 ts[CAP];     // INT64_MIN if not a timestamp
  u8 idx[4][CAP];  // lines by form of ts: 20 bytes, 13 bytes, 25 bytes, other
};

static NOINLINE void ck_ts_exact(CkBatch& B, u32 i, const u8* base) {
  if (!parse_ts(base + B.off[i][3], B.len[i][3], B.ts[i])) B.ts[i] = INT64_MIN;
}

// The valid clicks of the batch go to the router (person lookup, then the person's partition).
template <class RT>
static NOINLINE void process_clicks(CkBatch& B, const u32 n, const u8* const base, RT& router, const i64 cws,
                                    const i64 we) {
  static const u8 CLS[32] = {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 1, 3, 3, 3, 3, 3, 3, 0, 3, 3, 3, 3, 2, 3, 3, 3, 3, 3, 3};
  u32 cnt[4] = {0, 0, 0, 0};
  for (u32 i = 0; i < n; i++) {
    const u32 n3 = B.len[i][3];
    const u32 c = CLS[n3 < 31 ? n3 : 31];
    B.idx[c][cnt[c]++] = (u8)i;
  }
  {  // F1, two lines at a time
    u32 j = 0;
    for (; j + 2 <= cnt[0]; j += 2) {
      const u32 i0 = B.idx[0][j], i1 = B.idx[0][j + 1];
      i64 ta, tb;
      if (__builtin_expect(f1_pair(base + B.off[i0][3], base + B.off[i1][3], ta, tb), 1)) {
        B.ts[i0] = ta;
        B.ts[i1] = tb;
      } else {
        ck_ts_exact(B, i0, base);
        ck_ts_exact(B, i1, base);
      }
    }
    if (j < cnt[0]) ck_ts_exact(B, B.idx[0][j], base);
  }
  for (u32 j = 0; j < cnt[1]; j++) {  // 13 digits
    const u32 i = B.idx[1][j];
    i64 t;
    if (__builtin_expect(digits13(base + B.off[i][3], t), 1))
      B.ts[i] = t;
    else
      ck_ts_exact(B, i, base);
  }
  for (u32 j = 0; j < cnt[2]; j++) {  // with a UTC offset
    const u32 i = B.idx[2][j];
    i64 t;
    if (__builtin_expect(f3_one(base + B.off[i][3], t), 1))
      B.ts[i] = t;
    else
      ck_ts_exact(B, i, base);
  }
  for (u32 j = 0; j < cnt[3]; j++) ck_ts_exact(B, B.idx[3][j], base);
  // ---- keys and campaign; every line is handed to the router, which keeps the valid ones
  const u64 wlen = (u64)(we - cws);
  auto feed = router.feed();
  for (u32 i = 0; i < n; i++) {
    const u32* off = B.off[i];
    const u32* len = B.len[i];
    const u64 click = make_key(base + off[0], len[0]);
    const u64 uid = hash_id(make_key(base + off[1], len[1]));
    const i32 camp = camp_fast(base + off[2], len[2]);
    const u64 rel = (u64)B.ts[i] - (u64)cws;  // in [0, wlen) iff the click is inside [window start - 7 days, window end)
    const u64 tc = (u64)(u32)rel | ((u64)(u32)camp << 32);
    feed.add_if(uid, u64x2{click, tc}, (u32)((rel < wlen) & (camp >= 0)));
  }
  router.close(feed);
}

// One chunk of a clicks file -> R2C records. Returns the number of lines with a click_id and a user_id.
static NOINLINE u64 parse_click_chunk(const u8* begin, const u8* end, bool skip_header, TL& tl, const i64 cws,
                                      const i64 we) {
  CkBatch B;
  u32 n = 0;
  u64 nl = 0;
  const u8* base = begin;
  const u32 p2shift = 64 - g_P2bits;
  auto cur = tl.r2c.cursor(tl.arena);
  auto emit = [p2shift](WCScatter<R2C>::Cursor& c, u64 ph, const u64x2& v) __attribute__((always_inline)) {
    WCScatter<R2C>::push32(c, (u32)(ph >> p2shift), _mm256_set_epi64x(0, (long long)v.b, (long long)v.a, (long long)ph));
  };
  Router<u64x2, R2C, decltype(emit)> router(cur, emit);
  scan::scan_lines<4>(begin, end, skip_header, [&](const u8* sb, __m256i st, __m256i ln) __attribute__((always_inline)) {
    _mm256_store_si256((__m256i*)B.off[n], st);
    _mm256_store_si256((__m256i*)B.len[n], ln);
    base = sb;
    // a line without click_id or user_id is dropped: its slot is reused
    n += (_mm256_movemask_epi8(_mm256_cmpeq_epi32(ln, _mm256_setzero_si256())) & 0xFF) == 0;
    if (__builtin_expect(n == CkBatch::CAP, 0)) {
      process_clicks(B, n, sb, router, cws, we);
      nl += n;
      n = 0;
    }
  });
  if (n) process_clicks(B, n, base, router, cws, we);
  router.run();
  tl.r2c.close(cur);
  tl.r2c.flush(tl.arena);
  return nl + n;
}

static void parse_events_clicks(std::vector<MFile>& ev, std::vector<MFile>& ck) {
  size_t ev_bytes = 0, ck_bytes = 0;
  for (auto& f : ev) ev_bytes += f.size;
  for (auto& f : ck) ck_bytes += f.size;
  size_t chunk = std::max<size_t>(1 << 18, std::min<size_t>(8 << 20, (ev_bytes + ck_bytes) / (NT * 24 + 1)));
  std::vector<Chunk> ce, cc;
  make_chunks(ev, chunk, ce);
  make_chunks(ck, chunk, cc);
  const u32 p1shift = 64 - g_P1bits;
  const i64 ws = g_ws, we = g_we, cws = g_ws - WEEK;
  parallel_for((int)(ce.size() + cc.size()), [&](int t, int tid) {
    TL& tl = g_tl[tid];
    if (t < (int)ce.size()) {
      const Chunk& c = ce[t];
      const MFile& f = ev[c.file];
      size_t b = line_start_at_or_after(f, c.nominal), e = line_start_at_or_after(f, c.nominal_end);
      if (b >= e) return;
      tl.n_event_lines += parse_event_chunk(f.data + b, f.data + e, b == 0, tl, p1shift, ws, we);
      drop_parsed(f, c.nominal, c.nominal_end);
    } else {
      const Chunk& c = cc[t - ce.size()];
      const MFile& f = ck[c.file];
      size_t b = line_start_at_or_after(f, c.nominal), e = line_start_at_or_after(f, c.nominal_end);
      if (b >= e) return;
      tl.n_click_lines += parse_click_chunk(f.data + b, f.data + e, b == 0, tl, cws, we);
      drop_parsed(f, c.nominal, c.nominal_end);
    }
  });
  parallel_for(NT, [&](int t, int) { g_tl[t].r1.finish(g_tl[t].arena); });
}

// ------------------------------------------------------------------------------------------
// dedup (per event_id partition) and person lookups

// Dedup of one event_id partition, then the survivors are routed to their person partition.
//
// A bit map with 8+ bits per record finds the records whose event_id may have been seen before; their IDs
// go into a small exact table. A second pass sends every record whose ID is not in that table straight to
// the router (it is the only copy) and keeps, per ID in the table, the copy with the smallest ingest_ts.
// The bit map and the table stay in L1/L2 whatever the partition size, and nothing is written back to the
// records.
static void dedup_and_route() {
  u32 P1 = 1u << g_P1bits;
  const u32 p2shift = 64 - g_P2bits;
  std::vector<size_t> sizes(P1, 0);
  for (u32 p = 0; p < P1; p++)
    for (int t = 0; t < NT; t++) sizes[p] += g_tl[t].r1.size(p);
  std::vector<int> order(P1);
  for (u32 p = 0; p < P1; p++) order[p] = (int)p;
  std::sort(order.begin(), order.end(), [&](int a, int b) { return sizes[a] > sizes[b]; });
  struct DSlot {
    u64 eid;
    const R1* best;
  };
  struct Scratch {
    std::vector<u64> seen, cand;  // bit maps: IDs seen so far; IDs that may have copies
    std::vector<u64> ids;         // the IDs of the records that found their bit set
    std::vector<DSlot> tab;
    std::vector<std::pair<const R1*, u32>> blk;  // the partition's blocks
  };
  std::vector<Scratch> scr(NT);
  parallel_for((int)P1, [&](int oi, int tid) {
    TL& tl = g_tl[tid];
    int p = order[oi];
    const size_t n = sizes[p];
    if (n == 0) return;
    Scratch& S = scr[tid];
    // ---- pass 1: which IDs may occur more than once?
    const u32 mbits = std::max<u32>(12, ceil_log2(n * 8));
    const size_t words = (size_t)1 << (mbits - 6);
    if (S.seen.size() < words) {
      S.seen.resize(words);
      S.cand.resize(words);
    }
    if (S.ids.size() < n + 1) S.ids.resize(n + 1);
    u64* seen = S.seen.data();
    memset(seen, 0, words * 8);
    u64* ids = S.ids.data();
    size_t nids = 0;
    const u32 P1bits = g_P1bits, ishift = 64 - mbits;
    S.blk.clear();
    for (int t = 0; t < NT; t++)
      for (auto& b : g_tl[t].r1.blocks[p])
        if (b.second) S.blk.push_back({b.first, b.second});
    const size_t nblk = S.blk.size();
    S.blk.push_back(S.blk[0]);  // (something harmless to prefetch after the last block)
    const std::pair<const R1*, u32>* const blk = S.blk.data();
    for (size_t k = 0; k < nblk; k++) {
      // The blocks are scattered pages: the first lines of the next one are fetched while this one is read,
      // so the processor's own stream detection starts with a lead instead of a few misses per page.
      const char* const nx = (const char*)blk[k + 1].first;
      _mm_prefetch(nx, _MM_HINT_T0);
      _mm_prefetch(nx + 64, _MM_HINT_T0);
      _mm_prefetch(nx + 128, _MM_HINT_T0);
      _mm_prefetch(nx + 192, _MM_HINT_T0);
      const R1* rec = blk[k].first;
      const u32 cnt = blk[k].second;
      for (u32 j = 0; j < cnt; j++) {
        const u64 e = rec[j].eid;
        const u64 i = (e << P1bits) >> ishift;
        u64 w = seen[i >> 6];
        ids[nids] = e;
        // test and set bit i of the word; the old bit is added to nids
        __asm__("btsq %[i], %[w]\n\tadcq $0, %[n]" : [w] "+r"(w), [n] "+r"(nids) : [i] "r"(i) : "cc");
        seen[i >> 6] = w;
      }
    }
    auto cur = tl.r2.cursor(tl.arena);
    auto emit = [p2shift](WCScatter<R2>::Cursor& c, u64 ph, const u64& tp) __attribute__((always_inline)) {
      WCScatter<R2>::push16(c, (u32)(ph >> p2shift), _mm_set_epi64x((long long)tp, (long long)ph));
    };
    Router<u64, R2, decltype(emit)> router(cur, emit);
    auto feed = router.feed();
    // Hands a record to the router, which keeps it only if `keep` is 1 (no branch on the data).
    auto route_if = [&](const R1& r, u32 keep) __attribute__((always_inline)) {
      u64 tp;
      memcpy(&tp, &r.ts, 8);
      feed.add_if(r.uid, tp, keep);
    };
    if (nids == 0) {
      // ---- every ID is unique
      for (size_t k = 0; k < nblk; k++) {
        const R1* rec = blk[k].first;
        const u32 cnt = blk[k].second;
        for (u32 j = 0; j < cnt; j++) route_if(rec[j], rec[j].ts != TS_INVALID);
      }
    } else {
      // ---- exact table of the candidate IDs (and a bit map of them, to skip the table for most records).
      // Every slot starts with NONE as its best copy: later than any real ingest_ts and not a valid event,
      // so neither the comparisons nor the final sweep need to ask whether a slot is in use.
      static const R1 NONE = {0, INT64_MAX, 0, TS_INVALID, 0};
      u64* cand = S.cand.data();
      memset(cand, 0, words * 8);
      const u32 tbits = ceil_log2(nids * 2 + 8);
      const size_t tsz = (size_t)1 << tbits;
      if (S.tab.size() < tsz) S.tab.resize(tsz);
      DSlot* tab = S.tab.data();
      for (size_t q = 0; q < tsz; q++) tab[q] = DSlot{0, &NONE};
      const u64 tmask = tsz - 1;
      const u32 tshift = 64 - tbits;
      for (size_t k = 0; k < nids; k++) {
        const u64 e = ids[k];
        const u64 i = (e << P1bits) >> ishift;
        cand[i >> 6] |= 1ULL << (i & 63);
        u64 q = (e << P1bits) >> tshift;
        while (tab[q].eid != e && tab[q].eid != 0) q = (q + 1) & tmask;
        tab[q].eid = e;  // eid is a bijective hash of a non-empty key; 0 would need key 0, which no ID has
      }
      const R1* cl[WCScatter<R1>::BLK];  // the records of a block whose ID may be in the table
      for (size_t bi = 0; bi < nblk; bi++) {
        const R1* rec = blk[bi].first;
        const u32 cnt = blk[bi].second;
        u32 nc = 0;
        for (u32 j = 0; j < cnt; j++) {
          const R1& r = rec[j];
          const u64 i = (r.eid << P1bits) >> ishift;
          const u32 c = (u32)(cand[i >> 6] >> (i & 63)) & 1;
          cl[nc] = &r;
          nc += c;
          route_if(r, (u32)(r.ts != TS_INVALID) & (c ^ 1));  // the only copy of its event
        }
        for (u32 k = 0; k < nc; k++) {
          const R1& r = *cl[k];
          const u64 e = r.eid;
          u64 q = (e << P1bits) >> tshift;
          while (tab[q].eid != e && tab[q].eid != 0) q = (q + 1) & tmask;
          if (__builtin_expect(tab[q].eid == e, 1)) {
            const R1* best = tab[q].best;  // the first copy with the smallest ingest_ts stays
            tab[q].best = r.ingest < best->ingest ? &r : best;
          } else {
            route_if(r, r.ts != TS_INVALID);  // the bit map was wrong about it
          }
        }
      }
      for (size_t q = 0; q < tsz; q++) route_if(*tab[q].best, tab[q].best->ts != TS_INVALID);
    }
    router.close(feed);
    router.run();
    tl.r2.close(cur);
    tl.r2.flush(tl.arena);
    for (int t = 0; t < NT; t++) g_tl[t].r1.release(p, tl.arena);
  });
  parallel_for(NT, [&](int t, int) {
    TL& tl = g_tl[t];
    tl.r2.finish(tl.arena);
    tl.r2c.finish(tl.arena);
  });
}

// ------------------------------------------------------------------------------------------
// per person
//
// Events of a partition are counting-sorted by (local person, 2^20 ms time bucket). A bucket spans less
// than 30 minutes, so all events of a bucket fall in one session, and the gap between two consecutive
// non-empty buckets of a person is (min of the later) - (max of the earlier). No full sort is needed.

static constexpr u32 BSHIFT = 20;

struct XRec {
  u64 tp;  // ts | payload << 32, as in R2
  u32 lid;
  u32 pad;
};

static inline bool click_less(u64 a, u64 b) {  // byte-wise order of click ids
  if (!is_slow(a) && !is_slow(b)) return a < b;
  return key_cmp(a, b) < 0;
}

// Number of entries <= x in nv vectors of timestamps (padding entries are 0xFFFFFFFF; x is below that).
static FORCE_INLINE u32 count_le(const u32* ts, u32 nv, u32 x) {
  const __m256i vx = _mm256_set1_epi32((int)x);
  u32 c = 0;
  for (u32 v = 0; v < nv; v++) {
    const __m256i t = _mm256_loadu_si256((const __m256i*)(ts + 8 * v));
    c += (u32)__builtin_popcount(
        (u32)_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(_mm256_max_epu32(t, vx), vx))));
  }
  return c;
}

// One person's clicks (n > 0: ts / camp / click, in any order) into time order: sts / scamp / sclick, with
// 0xFFFFFFFF in sts[n .. n + 8). Equal timestamps keep their order. tin is scratch for n + 16 entries; at
// least 7 entries after ts[n - 1] must be readable.
//
// Up to RANK_MAX clicks, the rank of each one is counted with vector compares against all of them: no
// data-dependent branch, and most persons have only a few clicks. Beyond that, a radix sort.
static constexpr u32 RANK_MAX = 64;
// The rank sort for up to 4 * NG clicks, as a fixed run of steps: nothing in it depends on the number of
// clicks or on their times. The lanes after the last click get the largest key and all land on entry n.
template <int NG>
static FORCE_INLINE void rank_sort_fixed(const u32* ts, const u32* camp, const u64* click, const u32 n, u32* sts,
                                         u32* scamp, u64* sclick) {
  alignas(32) u64 key[4 * NG];
  __m256i kv[NG], cnt[NG];
  const __m256i flip = _mm256_set1_epi64x((long long)0x8000000000000000ULL);
  const __m256i big = _mm256_set1_epi64x(INT64_MAX), last = _mm256_set1_epi64x((long long)n - 1);
  for (int g = 0; g < NG; g++) {
    const __m256i pos = _mm256_setr_epi64x(4 * g, 4 * g + 1, 4 * g + 2, 4 * g + 3);
    const __m256i t = _mm256_cvtepu32_epi64(_mm_loadu_si128((const __m128i*)(ts + 4 * g)));
    const __m256i k = _mm256_xor_si256(_mm256_or_si256(_mm256_slli_epi64(t, 32), pos), flip);
    kv[g] = _mm256_blendv_epi8(k, big, _mm256_cmpgt_epi64(pos, last));
    _mm256_store_si256((__m256i*)(key + 4 * g), kv[g]);
    cnt[g] = _mm256_setzero_si256();
  }
  for (int k = 0; k < 4 * NG; k++) {
    const __m256i vk = _mm256_set1_epi64x((long long)key[k]);
    for (int g = 0; g < NG; g++) cnt[g] = _mm256_sub_epi64(cnt[g], _mm256_cmpgt_epi64(kv[g], vk));
  }
  for (int g = 0; g < NG; g++) _mm256_store_si256((__m256i*)(key + 4 * g), cnt[g]);
  for (u32 i = 0; i < 4 * NG; i++) {
    const u32 r = (u32)key[i], j = i < n ? i : n - 1;  // (a lane after the last click: entry n, never used)
    sts[r] = ts[j];
    scamp[r] = camp[j];
    sclick[r] = click[j];
  }
}

static NOINLINE void sort_person_clicks(const u32* ts, const u32* camp, const u64* click, const u32 n, u32* tin,
                                        u32* sts, u32* scamp, u64* sclick, std::vector<u64>& tmp) {
  (void)tin;
  const __m256i ones = _mm256_set1_epi32(-1);
  if (n <= 8) {
    rank_sort_fixed<2>(ts, camp, click, n, sts, scamp, sclick);
  } else if (n <= 16) {
    rank_sort_fixed<4>(ts, camp, click, n, sts, scamp, sclick);
  } else if (n <= RANK_MAX) {
    // Keys that all differ: the time, then the position. The rank of a key is the number of keys below it,
    // counted for four keys at once against each key in turn. (Signed compares: the top bit is flipped.)
    alignas(32) u64 key[RANK_MAX + 8];
    __m256i cnt[RANK_MAX / 4];
    const u32 ng = (n + 3) / 4;
    const __m256i flip = _mm256_set1_epi64x((long long)0x8000000000000000ULL);
    __m256i pos = _mm256_setr_epi64x(0, 1, 2, 3);
    for (u32 g = 0; g < ng; g++) {
      const __m256i t = _mm256_cvtepu32_epi64(_mm_loadu_si128((const __m128i*)(ts + 4 * g)));
      _mm256_store_si256((__m256i*)(key + 4 * g), _mm256_xor_si256(_mm256_or_si256(_mm256_slli_epi64(t, 32), pos), flip));
      pos = _mm256_add_epi64(pos, _mm256_set1_epi64x(4));
      cnt[g] = _mm256_setzero_si256();
    }
    for (u32 k = 0; k < n; k++) {
      const __m256i vk = _mm256_set1_epi64x((long long)key[k]);
      for (u32 g = 0; g < ng; g++)
        cnt[g] = _mm256_sub_epi64(cnt[g], _mm256_cmpgt_epi64(_mm256_load_si256((const __m256i*)(key + 4 * g)), vk));
    }
    for (u32 g = 0; g < ng; g++) _mm256_store_si256((__m256i*)(key + 4 * g), cnt[g]);  // (the keys are done with)
    for (u32 i = 0; i < n; i++) {
      const u32 r = (u32)key[i];
      sts[r] = ts[i];
      scamp[r] = camp[i];
      sclick[r] = click[i];
    }
  } else {
    // LSD radix sort of (ts, index), 8 bits a pass; the counts of all four passes are taken in one go
    if (tmp.size() < 2 * (size_t)n) tmp.resize(2 * (size_t)n);
    u64* a = tmp.data();
    u64* b = a + n;
    u32 cnt[4][256];
    memset(cnt, 0, sizeof cnt);
    for (u32 i = 0; i < n; i++) {
      const u32 t = ts[i];
      a[i] = ((u64)t << 32) | i;
      cnt[0][t & 255]++;
      cnt[1][(t >> 8) & 255]++;
      cnt[2][(t >> 16) & 255]++;
      cnt[3][t >> 24]++;
    }
    for (int pass = 0; pass < 4; pass++) {
      u32* const c = cnt[pass];
      u32 sum = 0;
      for (int k = 0; k < 256; k++) {
        const u32 x = c[k];
        c[k] = sum;
        sum += x;
      }
      const int sh = 32 + 8 * pass;
      for (u32 i = 0; i < n; i++) b[c[(a[i] >> sh) & 255]++] = a[i];
      std::swap(a, b);
    }
    for (u32 i = 0; i < n; i++) {
      const u32 idx = (u32)a[i];
      sts[i] = (u32)(a[i] >> 32);
      scamp[i] = camp[idx];
      sclick[i] = click[idx];
    }
  }
  _mm256_storeu_si256((__m256i*)(sts + n), ones);
}

static void person_phase(bool keep_stats = false) {
  u32 P2 = 1u << g_P2bits;
  std::vector<size_t> ne(P2, 0), nc(P2, 0);
  for (u32 p = 0; p < P2; p++)
    for (int t = 0; t < NT; t++) {
      ne[p] += g_tl[t].r2.size(p);
      nc[p] += g_tl[t].r2c.size(p);
    }
  std::vector<int> order(P2);
  for (u32 p = 0; p < P2; p++) order[p] = (int)p;
  std::sort(order.begin(), order.end(), [&](int a, int b) { return ne[a] > ne[b]; });
  const u32 NC = g_ncamp, ND = (u32)g_ndays, ORG = NC;
  const u32 NB = (u32)(((u64)(g_we - g_ws - 1) >> BSHIFT) + 1);
  const u32 NW = (NB + 63) / 64;
  const u32 NCAT = (u32)g_cats.size();
  if (!keep_stats)
    parallel_for(NT, [&](int t, int) {
      TL& tl = g_tl[t];
      tl.c_stat.assign((size_t)(NC + 1) * ND, CStat{0, 0, 0});
      tl.c_assist.assign((size_t)(NC + 1) * ND, 0);
    });
  struct PSlot {
    u64 h;  // 0: free (the hash of a person is never 0)
    u32 lid;
    u32 pad;
  };
  struct BAgg {
    u32 mn, mx, cnt, pad;
    i64 rev;
  };
  struct Purch {
    u32 bucket, cat, price;
  };
  struct Scratch {
    std::vector<PSlot> tab;
    std::vector<u64> hs;
    std::vector<u32> cntL, posL, cntC, offC;
    std::vector<XRec> X;
    std::vector<u64> E;
    std::vector<std::pair<const R2*, u32>> blk;  // the partition's blocks of events
    std::vector<u32> Cts, Ccamp;  // clicks grouped by person, in any order
    std::vector<u64> Cclick;
    std::vector<u32> tin, sts, scamp;  // one person's clicks in time order
    std::vector<u64> sclick, rtmp;
    std::vector<u32> lidC;
    std::vector<u32> cov;     // per campaign: the person's sessions below this index are already counted for it
    std::vector<u32> sstart;  // start of each session of the person (padded like the click times)
    std::vector<u32> dstart;  // first session of each of the person's days
    std::vector<BAgg> agg;
    std::vector<u64> bitmap;
    std::vector<u16> bday;
    std::vector<Purch> purch;
    std::vector<u32> dsess, dev;
    std::vector<i64> drev, catsum;
    std::vector<u32> days;
  };
  std::vector<Scratch> scr(NT);
  const i64 ws = g_ws, day0 = g_day0;
  const u32 P2bits = g_P2bits;
  parallel_for((int)P2, [&](int oi, int tid) {
    int p = order[oi];
    size_t n_e = ne[p], n_c = nc[p];
    if (n_e == 0) return;
    Scratch& S = scr[tid];
    TL& tl = g_tl[tid];
    if (S.cov.size() < NC + 1) {
      S.cov.assign(NC + 1, 0);
      S.sstart.assign((size_t)NB + 16, 0);
      S.dstart.assign((size_t)ND + 2, 0);
      S.agg.assign(NB, BAgg{0xFFFFFFFFu, 0, 0, 0, 0});
      S.bitmap.assign(NW, 0);
      S.bday.assign(NB, 0);
      S.dsess.assign(ND, 0);
      S.dev.assign(ND, 0);
      S.drev.assign(ND, 0);
      S.catsum.assign((size_t)ND * NCAT, 0);
    }
    // ---- local person ids
    u32 bits = std::max<u32>(8, ceil_log2(n_e / 8 + 64));
    size_t tsz = (size_t)1 << bits;
    S.tab.assign(tsz, PSlot{0, 0, 0});
    S.hs.clear();
    S.cntL.clear();
    auto rehash = [&]() {
      bits++;
      tsz = (size_t)1 << bits;
      S.tab.assign(tsz, PSlot{0, 0, 0});
      const u64 m = tsz - 1;
      for (u32 lid = 0; lid < S.hs.size(); lid++) {
        u64 i = (S.hs[lid] << P2bits) >> (64 - bits);
        while (S.tab[i].h) i = (i + 1) & m;
        S.tab[i] = PSlot{S.hs[lid], lid, 0};
      }
    };
    if (S.X.size() < n_e) {
      S.X.resize(n_e);
      S.E.resize(n_e);
    }
    S.blk.clear();
    for (int t = 0; t < NT; t++)
      for (auto& b : g_tl[t].r2.blocks[p])
        if (b.second) S.blk.push_back({b.first, b.second});
    const size_t nblk = S.blk.size();
    S.blk.push_back(S.blk[0]);  // (something harmless to prefetch after the last block)
    XRec* X = S.X.data();
    {
      // the table, its geometry and the counters as plain locals, so the loop keeps them in registers; a new
      // person (the rare case) refreshes them
      PSlot* tab = S.tab.data();
      u64 mask = tsz - 1;
      u32 sh = 64 - bits;
      u32* cntL = S.cntL.data();
      u32 np = 0;
      size_t k = 0;
      for (size_t bi = 0; bi < nblk; bi++) {
        const char* const nx = (const char*)S.blk[bi + 1].first;  // (see the same step in dedup_and_route)
        _mm_prefetch(nx, _MM_HINT_T0);
        _mm_prefetch(nx + 64, _MM_HINT_T0);
        _mm_prefetch(nx + 128, _MM_HINT_T0);
        _mm_prefetch(nx + 192, _MM_HINT_T0);
        const R2* r = S.blk[bi].first;
        const u32 cnt = S.blk[bi].second;
        for (u32 j = 0; j < cnt; j++) {
          const u64 h = r[j].h;
          u64 i = (h << P2bits) >> sh;
          u32 lid;
          for (;;) {
            PSlot& sl = tab[i];
            if (__builtin_expect(sl.h == h, 1)) {
              lid = sl.lid;
              break;
            }
            if (sl.h == 0) {
              lid = np++;
              sl.h = h;
              sl.lid = lid;
              S.hs.push_back(h);
              S.cntL.push_back(0);
              if ((size_t)np * 2 > tsz) rehash();
              tab = S.tab.data();
              mask = tsz - 1;
              sh = 64 - bits;
              cntL = S.cntL.data();
              break;
            }
            i = (i + 1) & mask;
          }
          memcpy(&X[k].tp, &r[j].ts, 8);
          X[k].lid = lid;
          k++;
          cntL[lid]++;
        }
      }
    }
    const u32 np = (u32)S.hs.size();
    const u64 mask = tsz - 1;
    S.posL.resize(np + 1);
    {
      u32* posL = S.posL.data();
      u32 sum = 0;
      for (u32 l = 0; l < np; l++) {
        posL[l] = sum;
        sum += S.cntL[l];
      }
      posL[np] = sum;
      u64* E = S.E.data();
      for (size_t i = 0; i < n_e; i++) E[posL[X[i].lid]++] = X[i].tp;
      // posL[l] is now the end of person l
    }
    // ---- clicks of these persons, grouped by person
    S.cntC.assign(np + 1, 0);
    S.lidC.resize(n_c);
    {
      size_t kk = 0;
      for (int t = 0; t < NT; t++)
        for (auto& b : g_tl[t].r2c.blocks[p]) {
          const R2C* r = b.first;
          for (u32 j = 0; j < b.second; j++) {
            u64 h = r[j].h;
            u64 i = (h << P2bits) >> (64 - bits);
            u32 lid = np;  // a click of a person without events
            for (;;) {
              const PSlot& sl = S.tab[i];
              if (sl.h == h) {
                lid = sl.lid;
                break;
              }
              if (sl.h == 0) break;
              i = (i + 1) & mask;
            }
            S.lidC[kk++] = lid;
            S.cntC[lid]++;
          }
        }
    }
    S.offC.resize(np + 2);
    {
      u32 sum = 0, mcmax = 0;
      for (u32 l = 0; l <= np; l++) {
        S.offC[l] = sum;
        sum += S.cntC[l];
        if (l < np) mcmax = std::max(mcmax, S.cntC[l]);
      }
      S.offC[np + 1] = sum;
      if (S.Cts.size() < (size_t)sum + 16) {
        S.Cts.resize((size_t)sum + 16);
        S.Ccamp.resize((size_t)sum + 16);
        S.Cclick.resize((size_t)sum + 16);
      }
      if (S.sts.size() < (size_t)mcmax + 16) {
        S.tin.resize((size_t)mcmax + 16);
        S.sts.resize((size_t)mcmax + 16);
        S.scamp.resize((size_t)mcmax + 16);
        S.sclick.resize((size_t)mcmax + 16);
      }
      u32* pos = S.cntC.data();
      for (u32 l = 0; l <= np; l++) pos[l] = S.offC[l];
      u32* Cts = S.Cts.data();
      u32* Ccamp = S.Ccamp.data();
      u64* Cclick = S.Cclick.data();
      size_t kk = 0;
      for (int t = 0; t < NT; t++)
        for (auto& b : g_tl[t].r2c.blocks[p]) {
          const R2C* r = b.first;
          for (u32 j = 0; j < b.second; j++) {
            const u32 q = pos[S.lidC[kk++]]++;
            Cts[q] = r[j].ts;
            Ccamp[q] = r[j].camp;
            Cclick[q] = r[j].click;
          }
        }
    }
    // ---- per person
    CStat* c_stat = tl.c_stat.data();  // both indexed [day][campaign]
    u32* c_assist = tl.c_assist.data();
    const size_t NC1 = (size_t)NC + 1;
    BAgg* agg = S.agg.data();
    u64* bitmap = S.bitmap.data();
    u16* bday = S.bday.data();
    u32* dsess = S.dsess.data();
    u32* dev = S.dev.data();
    i64* drev = S.drev.data();
    i64* catsum = S.catsum.data();
    u32* cov = S.cov.data();
    u32* sstart = S.sstart.data();
    u32* dstart = S.dstart.data();
    const u32* const sts = S.sts.data();
    const u32* const scamp = S.scamp.data();
    const u64* const sclick = S.sclick.data();
    OutCols& out = tl.out;
    const u32* posL = S.posL.data();
    const u64* Eall = S.E.data();
    for (u32 lid = 0; lid < np; lid++) {
      const u32 eend = posL[lid], ebeg = eend - S.cntL[lid];
      // this person's clicks, in time order
      const u32 c0 = S.offC[lid], mc = S.offC[lid + 1] - c0;
      if (mc)
        sort_person_clicks(S.Cts.data() + c0, S.Ccamp.data() + c0, S.Cclick.data() + c0, mc, S.tin.data(), S.sts.data(),
                           S.scamp.data(), S.sclick.data(), S.rtmp);
      const u32 nv = (mc + 7) / 8;
      // bucket aggregates; `summary` has a bit for every word of the bit map that is in use
      S.purch.clear();
      u64 summary = 0;
      u32 ncell = 0;
      for (u32 i = ebeg; i < eend; i++) {
        const u64 e = Eall[i];
        const u32 ts = (u32)e, pl = (u32)(e >> 32);
        const u32 b = ts >> BSHIFT;
        // every aggregate starts as {mn = max, mx = 0, cnt = 0, rev = 0} (reset when its bucket is consumed),
        // so the first event of a bucket needs no special case
        BAgg& a = agg[b];
        bitmap[b >> 6] |= 1ULL << (b & 63);
        summary |= 1ULL << (b >> 6);
        ncell += a.cnt == 0;
        a.mn = ts < a.mn ? ts : a.mn;
        a.mx = ts > a.mx ? ts : a.mx;
        a.cnt++;
        if (pl) {
          a.rev += pl & 0xFFFFFF;
          S.purch.push_back(Purch{b, pl >> 24, pl & 0xFFFFFF});
        }
      }
      // sessions over the buckets in time order
      u32 ns = 0;  // sessions of this person so far
      u32 hi = 0;  // (for a long click list: the clicks up to the current session start)
      bool open = false;
      u32 s_start = 0, s_max = 0, s_nev = 0, s_day = 0;
      i64 s_rev = 0;
      S.days.clear();
      auto close_session = [&]() {
        // credit: the latest click in the 7 days up to the session start, the smallest click_id among equal
        // times (click times count from 7 days before the window, session times from the window start)
        u32 credit = ORG;
        if (mc) {
          const u32 upper = s_start + (u32)WEEK;
          if (mc <= 32)
            hi = count_le(sts, nv, upper);
          else
            while (hi < mc && sts[hi] <= upper) hi++;
          if (hi > 0 && sts[hi - 1] >= s_start) {
            u32 bi = hi - 1;
            const u32 tsmax = sts[bi];
            for (u32 q = bi; q > 0 && sts[q - 1] == tsmax;) {
              q--;
              if (click_less(sclick[q], sclick[bi])) bi = q;
            }
            credit = scamp[bi];
          }
        }
        CStat& cs = c_stat[(size_t)s_day * NC1 + credit];
        cs.sess++;
        cs.purch += (s_rev > 0);
        cs.rev += s_rev;
        if (dsess[s_day]++ == 0) {  // the person's next day: its sessions start here
          dstart[S.days.size()] = ns;
          S.days.push_back(s_day);
        }
        sstart[ns++] = s_start;
        dev[s_day] += s_nev;
        drev[s_day] += s_rev;
      };
      {
        u64 w = 0;  // what is left of the current word of the bit map
        u32 wi = 0;
        for (u32 k = 0; k < ncell; k++) {
          // the next bucket in use; moving on to the next word of the bit map without a branch
          const u64 adv = 0 - (u64)(w == 0);
          const u32 wnext = (u32)_tzcnt_u64(summary) & 63;
          wi = (u32)((wi & ~adv) | (wnext & adv));
          summary = (summary & ~adv) | (_blsr_u64(summary) & adv);
          const u64 wl = bitmap[wi];
          bitmap[wi] = 0;
          w = (w & ~adv) | (wl & adv);
          const u32 b = wi * 64 + (u32)_tzcnt_u64(w);
          w = _blsr_u64(w);
          const BAgg a = agg[b];
          if (!open || a.mn - s_max > (u32)MIN30) {
            if (open) close_session();
            open = true;
            s_start = a.mn;
            s_nev = 0;
            s_rev = 0;
            s_day = (u32)((ws + (i64)a.mn) / DAY - day0);
          }
          s_max = a.mx;
          s_nev += a.cnt;
          s_rev += a.rev;
          bday[b] = (u16)s_day;
          agg[b] = BAgg{0xFFFFFFFFu, 0, 0, 0, 0};
        }
      }
      if (open) close_session();
      // Assists. A click assists the sessions that start in the 7 days after it: a run [a, b) of the person's
      // sessions. Going through the clicks in time order, the runs of one campaign only move forward, so the
      // part of a run that an earlier click of the campaign has not covered is [max(a, cov), b). Its sessions
      // are counted per day from where each day's sessions start, with the same few steps for every click.
      if (mc) {
        const u32 ndays = (u32)S.days.size();
        dstart[ndays] = ns;
        _mm256_storeu_si256((__m256i*)(sstart + ns), _mm256_set1_epi32(-1));
        const u32 nsv = (ns + 7) / 8;
        u32* rows[64];  // assist counters of the person's days (a window has at most 50 days)
        for (u32 j = 0; j < ndays; j++) rows[j] = c_assist + (size_t)S.days[j] * NC1;
        u32 a = 0, b = 0;  // (two pointers for a long session list)
        for (u32 k = 0; k < mc; k++) {
          const u32 t = sts[k], c = scamp[k];
          if (ns <= 32) {
            b = count_le(sstart, nsv, t);
            a = t > (u32)WEEK ? count_le(sstart, nsv, t - (u32)WEEK - 1) : 0;
          } else {
            while (b < ns && sstart[b] <= t) b++;
            while (a < b && (u64)sstart[a] + (u64)WEEK < t) a++;
          }
          const u32 x = std::max(a, cov[c]);
          cov[c] = std::max(cov[c], b);
          for (u32 j = 0; j < ndays; j++) {
            const i32 n = (i32)std::min(b, dstart[j + 1]) - (i32)std::max(x, dstart[j]);
            rows[j][c] += (u32)(n > 0 ? n : 0);
          }
        }
        for (u32 k = 0; k < mc; k++) cov[scamp[k]] = 0;
      }
      // purchases -> category sums per day of their session
      for (const Purch& q : S.purch) catsum[(size_t)bday[q.bucket] * NCAT + q.cat] += q.price;
      // one output row per day with a session; the user_id is written once, with the number of rows
      const u64 pkey = unhash_id(S.hs[lid]);
      const u32 nd = (u32)S.days.size();
      if (nd) {
        if (__builtin_expect(!is_slow(pkey), 1)) {
          u8 kb[16];  // the user_id as it stands in the file: length, bytes
          const u32 klen = fast_key_bytes(pkey, kb + 4);
          memcpy(kb, &klen, 4);
          u8* ip = out.ids.reserve(16, tl.arena);
          memcpy(ip, kb, 16);
          out.ids.cur = ip + 4 + klen;
        } else {
          std::string name;
          key_str(pkey, name);
          const u32 klen = (u32)name.size();
          const bool fits = 4 + (size_t)klen <= Stream<u8>::BLK;
          u8* ip = fits ? out.ids.reserve(4 + klen, tl.arena) : out.ids.big(4 + (size_t)klen, tl.arena);
          memcpy(ip, &klen, 4);
          memcpy(ip + 4, name.data(), klen);
          if (fits) out.ids.cur = ip + 4 + klen;
        }
        *out.runs.slot(tl.arena) = (u8)nd;
        out.npersons++;
      }
      u8* const dayp = out.day.reserve(nd, tl.arena);
      u8* const sessp = out.sess.reserve(nd, tl.arena);
      u32* const evsp = out.evs.reserve(nd, tl.arena);
      i64* const revp = out.rev.reserve(nd, tl.arena);
      u8* const topp = out.top.reserve(nd, tl.arena);
      for (u32 di = 0; di < nd; di++) {
        const u32 d = S.days[di];
        u16 top = 0xFFFF;
        if (drev[d] > 0) {
          i64 best = -1;
          i64* cs = catsum + (size_t)d * NCAT;
          for (const Purch& q : S.purch) {
            if (bday[q.bucket] != d) continue;
            const i64 v = cs[q.cat];
            if (v > best || (v == best && q.cat < top)) {
              best = v;
              top = (u16)q.cat;
            }
          }
          for (const Purch& q : S.purch)
            if (bday[q.bucket] == d) cs[q.cat] = 0;
        }
        dayp[di] = (u8)d;
        sessp[di] = (u8)dsess[d];
        evsp[di] = dev[d];
        out.max_sess = std::max(out.max_sess, dsess[d]);
        out.max_evs = std::max(out.max_evs, dev[d]);
        revp[di] = drev[d];
        topp[di] = (u8)top;  // categories are below 255, so "none" becomes 0xFF
        dsess[d] = 0;
        dev[d] = 0;
        drev[d] = 0;
      }
      out.day.cur += nd;
      out.sess.cur += nd;
      out.evs.cur += nd;
      out.rev.cur += nd;
      out.top.cur += nd;
      out.nrows += nd;
    }
    for (int t = 0; t < NT; t++) {
      g_tl[t].r2.release(p, tl.arena);
      g_tl[t].r2c.release(p, tl.arena);
    }
  });
}

#include "dense.h"

// ------------------------------------------------------------------------------------------
// output

static void write_outputs(const std::string& out) {
  std::string ud = out + "/user_daily", cd = out + "/campaign_daily";
  mkdir(ud.c_str(), 0777);
  mkdir(cd.c_str(), 0777);
  std::atomic<bool> fail{false};
  u64 total_rows = 0;
  for (auto& t : g_tl) total_rows += t.out.nrows;
  parallel_for(NT + 1, [&](int t, int) {
    if (t == NT) {
      // campaign_daily
      const u32 NC = g_ncamp, ND = (u32)g_ndays;
      const size_t sz = (size_t)(NC + 1) * ND;
      std::vector<u64> sess(sz, 0), purch(sz, 0), assist(sz, 0);
      std::vector<i64> rev(sz, 0);
      for (auto& tl : g_tl) {
        if (tl.c_stat.empty()) continue;
        for (size_t i = 0; i < sz; i++) {
          sess[i] += tl.c_stat[i].sess;
          purch[i] += tl.c_stat[i].purch;
          assist[i] += tl.c_assist[i];
          rev[i] += tl.c_stat[i].rev;
        }
      }
      std::vector<pq::Column> cols(7);
      const char* names[7] = {"campaign_id",   "channel",          "day", "sessions", "purchasing_sessions",
                              "revenue_cents", "assisted_sessions"};
      for (int i = 0; i < 7; i++) {
        cols[i].name = names[i];
        cols[i].type = pq::T_INT64;
      }
      cols[0].type = pq::T_BYTE_ARRAY;
      cols[0].is_string = true;
      cols[1].type = pq::T_BYTE_ARRAY;
      cols[1].is_string = true;
      cols[2].type = pq::T_INT32;
      cols[2].is_date = true;
      pq::Buf ids, chans;
      std::vector<i32> days(sz);
      std::vector<i64> v[4];
      for (auto& x : v) x.resize(sz);
      size_t n = 0;
      const std::string org = "organic";
      for (u32 c = 0; c <= NC; c++)
        for (u32 d = 0; d < ND; d++) {
          const size_t i = (size_t)d * (NC + 1) + c;
          if (sess[i] == 0 && assist[i] == 0) continue;
          const std::string& id = (c == NC) ? org : g_camp_ids[c];
          const std::string& ch = (c == NC) ? org : g_camp_channel[c];
          pq::put_str(ids, id.data(), (u32)id.size());
          pq::put_str(chans, ch.data(), (u32)ch.size());
          days[n] = (i32)(g_day0 + d);
          v[0][n] = (i64)sess[i];
          v[1][n] = (i64)purch[i];
          v[2][n] = rev[i];
          v[3][n] = (i64)assist[i];
          n++;
        }
      cols[0].body.push_back({ids.data(), ids.size()});
      cols[1].body.push_back({chans.data(), chans.size()});
      cols[2].body.push_back({days.data(), n * 4});
      for (int k = 0; k < 4; k++) cols[3 + k].body.push_back({v[k].data(), n * 8});
      if (!pq::write_file(cd + "/part-0.parquet", cols, (int64_t)n)) fail = true;
      return;
    }
    OutCols& oc = g_tl[t].out;
    oc.finish();
    const size_t n = oc.nrows;
    if (n == 0 && !(t == 0 && total_rows == 0)) return;
    std::vector<pq::Column> cols(6);
    cols[0].name = "user_id";
    cols[0].type = pq::T_BYTE_ARRAY;
    cols[0].is_string = true;
    cols[1].name = "day";
    cols[1].type = pq::T_INT32;
    cols[1].is_date = true;
    cols[2].name = "sessions";
    cols[2].type = pq::T_INT32;
    cols[3].name = "events";
    cols[3].type = pq::T_INT32;
    cols[4].name = "revenue_cents";
    cols[4].type = pq::T_INT64;
    cols[5].name = "top_category";
    cols[5].type = pq::T_BYTE_ARRAY;
    cols[5].is_string = true;
    cols[5].optional = true;
    cols[5].use_dict = true;
    for (auto& c : g_cats) pq::put_str(cols[5].dict_values, c.data(), (u32)c.size());
    cols[5].dict_count = (int)g_cats.size();
    // user_id: the persons are the dictionary, and each person's rows are one run of its index
    cols[0].use_dict = true;
    cols[0].dict_count = (int)oc.npersons;
    for (auto& b : oc.ids.blocks) cols[0].dict_body.push_back({b.first, b.second});
    {
      const int bw = std::max(1, pq::bit_width(oc.npersons ? (u32)(oc.npersons - 1) : 0));
      cols[0].pre.u8((u8)bw);
      u32 first = 0;
      for (auto& b : oc.runs.blocks) {
        pq::encode_runs(cols[0].pre, b.first, b.second, first, bw);
        first += b.second;
      }
    }
    // day, sessions, events: small numbers. The dictionary is 0 .. max (for day: the days of the window), so a
    // value is its own index, and the indices are bit-packed eight at a time.
    std::unique_ptr<u8[]> tmp(new u8[2 * n + 64]);  // (not initialised: every use fills what it reads)
    auto small = [&](pq::Column& c, u32 maxv, i32 base, auto&& gather) {
      c.use_dict = true;
      c.dict_count = (int)maxv + 1;
      for (u32 v = 0; v <= maxv; v++) c.dict_values.le32((u32)(base + (i32)v));
      gather(tmp.get());
      memset(tmp.get() + n, 0, 16);
      const int bw = std::max(1, pq::bit_width(maxv));
      c.pre.u8((u8)bw);
      pq::encode_packed8(c.pre, tmp.get(), n, bw);
    };
    auto bytes_of = [](Stream<u8>& st) {
      return [&st](u8* o) {
        for (auto& b : st.blocks) {
          memcpy(o, b.first, b.second);
          o += b.second;
        }
      };
    };
    small(cols[1], (u32)std::max(g_ndays, 1) - 1, (i32)g_day0, bytes_of(oc.day));
    small(cols[2], oc.max_sess, 0, bytes_of(oc.sess));
    if (oc.max_evs <= 255) {
      small(cols[3], oc.max_evs, 0, [&](u8* o) {
        for (auto& b : oc.evs.blocks)
          for (u32 j = 0; j < b.second; j++) *o++ = (u8)b.first[j];
      });
    } else if (oc.max_evs <= 65535) {
      pq::Column& c = cols[3];
      c.use_dict = true;
      c.dict_count = (int)oc.max_evs + 1;
      for (u32 v = 0; v <= oc.max_evs; v++) c.dict_values.le32(v);
      u16* const t16 = (u16*)tmp.get();
      size_t k = 0;
      for (auto& b : oc.evs.blocks)
        for (u32 j = 0; j < b.second; j++) t16[k++] = (u16)b.first[j];
      memset(t16 + n, 0, 32);
      const int bw = pq::bit_width(oc.max_evs);
      c.pre.u8((u8)bw);
      pq::encode_packed16(c.pre, t16, n, bw);
    } else {
      for (auto& b : oc.evs.blocks) cols[3].body.push_back({b.first, (size_t)b.second * 4});
    }
    for (auto& b : oc.rev.blocks) cols[4].body.push_back({b.first, (size_t)b.second * 8});
    // top_category: definition levels, then the dictionary indices of the rows that have one
    {
      std::vector<u8> def(n + 8), idx(n + 8);
      size_t k = 0, m = 0;
      for (auto& b : oc.top.blocks)
        for (u32 j = 0; j < b.second; j++) {
          const u8 c = b.first[j];
          def[k++] = c != 0xFF;
          idx[m] = c;
          m += c != 0xFF;
        }
      pq::Buf lv;
      pq::encode_levels(lv, def.data(), n);
      cols[5].pre.le32((u32)lv.size());
      cols[5].pre.bytes(lv.data(), lv.size());
      int bw = pq::bit_width(cols[5].dict_count > 0 ? (u32)(cols[5].dict_count - 1) : 0);
      if (bw < 1) bw = 1;
      cols[5].pre.u8((u8)bw);
      pq::encode_indices(cols[5].pre, idx.data(), m, bw);
    }
    std::string path = ud + "/part-" + std::to_string(t) + ".parquet";
    if (!pq::write_file(path, cols, (int64_t)n)) fail = true;
  });
  if (fail) die("writing output");
}

// Everything is written: give the memory back from all threads at once. The kernel would otherwise free it
// page by page, on one CPU, while the process exits.
static void release_memory() {
  parallel_for(NT, [&](int t, int) {
    for (auto& s : g_tl[t].arena.slabs) madvise(s.first, s.second, MADV_DONTNEED);
    if (t == 0 && g_H) madvise(g_H, sizeof(HSlot) << g_Hbits, MADV_DONTNEED);
  });
}

// ------------------------------------------------------------------------------------------
// self test of the SIMD parsers against the scalar references

static int selftest() {
  init_years();
  u64 rng = 88172645463325252ULL;
  auto next = [&]() {
    rng ^= rng << 13;
    rng ^= rng >> 7;
    rng ^= rng << 17;
    return rng;
  };
  const char* alphabet = "0123456789-:TZz+ .";
  long bad = 0, n = 0;
  std::vector<u8> buf(64);
  for (long it = 0; it < 20000000; it++) {
    // start from a valid-ish timestamp and mutate
    i64 ms = (i64)(next() % 300000000000000ULL) - 1000000000000LL;
    int form = (int)(next() % 4);
    char s[64];
    int len;
    if (form == 3) {
      len = snprintf(s, sizeof s, "%lld", (long long)(ms < 0 ? -ms : ms));
    } else {
      i64 days = ms / DAY;
      i64 rem = ms % DAY;
      if (rem < 0) {
        rem += DAY;
        days--;
      }
      // civil from days
      i64 z = days + 719468;
      i64 era = (z >= 0 ? z : z - 146096) / 146097;
      i64 doe = z - era * 146097;
      i64 yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
      i64 y = yoe + era * 400;
      i64 doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
      i64 mp = (5 * doy + 2) / 153;
      i64 d = doy - (153 * mp + 2) / 5 + 1;
      i64 m = mp < 10 ? mp + 3 : mp - 9;
      y += (m <= 2);
      if (y < 0) y = -y;
      y %= 10000;
      len = snprintf(s, sizeof s, "%04lld-%02lld-%02lldT%02lld:%02lld:%02lld", (long long)y, (long long)m,
                     (long long)d, (long long)(rem / 3600000), (long long)(rem / 60000 % 60),
                     (long long)(rem / 1000 % 60));
      if (form == 1)
        len += snprintf(s + len, sizeof s - len, "Z");
      else
        len += snprintf(s + len, sizeof s - len, "%c%02d:%02d", (next() & 1) ? '+' : '-', (int)(next() % 26),
                        (int)(next() % 62));
    }
    int muts = (int)(next() % 3);
    for (int k = 0; k < muts; k++) {
      int pos = (int)(next() % (len + 1));
      if (pos < len) s[pos] = alphabet[next() % strlen(alphabet)];
    }
    if (next() % 50 == 0 && len > 0) len--;
    memset(buf.data(), 0x55, buf.size());
    memcpy(buf.data(), s, len);
    i64 a = 0, b = 0;
    bool ra = parse_ts(buf.data(), len, a), rb = parse_ts_ref(buf.data(), len, b);
    n++;
    if (ra != rb || (ra && a != b)) {
      if (bad < 10) fprintf(stderr, "MISMATCH '%.*s': simd %d %lld ref %d %lld\n", len, s, ra, (long long)a, rb, (long long)b);
      bad++;
    }
    // dual-lane parser against the scalar civil parser (both 20-byte F1 candidates)
    if (len == 20 && s[19] == 'Z') {
      static std::vector<u8> prev(64, 0);
      static bool have = false;
      if (have) {
        i64 x1 = 0, x2 = 0, y1 = 0, y2 = 0;
        u32 r = civil19x2(buf.data(), prev.data(), x1, x2);
        bool r1 = civil19_scalar(buf.data(), y1), r2 = civil19_scalar(prev.data(), y2);
        if (((r & 1) != 0) != r1 || ((r & 2) != 0) != r2 || (r1 && x1 != y1) || (r2 && x2 != y2)) {
          if (bad < 10) fprintf(stderr, "X2 MISMATCH '%.*s' '%.20s'\n", len, s, (const char*)prev.data());
          bad++;
        }
      }
      memcpy(prev.data(), buf.data(), 64);
      have = true;
    }
  }
  fprintf(stderr, "selftest: %ld cases, %ld mismatches\n", n, bad);
  // ---- F1 fast kernels: whatever they accept must equal the reference, and they must accept every valid
  // F1 timestamp in a candidate year
  long n2 = 0, bad2 = 0, in_year = 0, accepted = 0;
  const int year_sets[][2] = {{2027, 2028}, {2028, 2028}, {2024, 2025}, {2099, 2100}, {1999, 2000}, {1969, 1970}};
  std::vector<u8> pa(64), pb(64);
  for (const auto& ys : year_sets) {
    init_fast_years(days_from_civil(ys[0], 12, 20) * DAY, days_from_civil(ys[1], 1, 15) * DAY);
    for (long it = 0; it < 3000000; it++) {
      auto gen = [&](u8* dst) {
        char t[64];
        int yr = (next() % 16 == 0) ? ys[0] + (int)(next() % 5) - 2 : ys[next() & 1];
        int mo = 1 + (int)(next() % 12), dd = 1 + (int)(next() % 31), hh = (int)(next() % 24), mi = (int)(next() % 60),
            se = (int)(next() % 60);
        if (next() % 40 == 0) mo = (int)(next() % 20);
        if (next() % 40 == 0) dd = (int)(next() % 40);
        if (next() % 40 == 0) hh = (int)(next() % 30);
        if (next() % 40 == 0) mi = (int)(next() % 70);
        if (next() % 40 == 0) se = (int)(next() % 70);
        int len = snprintf(t, sizeof t, "%04d-%02d-%02dT%02d:%02d:%02dZ", yr, mo, dd, hh, mi, se);
        if (next() % 5 == 0) {
          int pos = (int)(next() % len);
          t[pos] = alphabet[next() % strlen(alphabet)];
        }
        for (int k = 0; k < 64; k++) dst[k] = (u8)("0123456789,\"Z:+-T"[next() % 17]);
        memcpy(dst, t, len);
      };
      gen(pa.data());
      gen(pb.data());
      i64 ra = 0, rb = 0, fa = 0, fb = 0, f1 = 0;
      const bool va = pa[19] == 'Z' && civil19_scalar(pa.data(), ra);
      const bool vb = pb[19] == 'Z' && civil19_scalar(pb.data(), rb);
      const bool rp = f1_pair(pa.data(), pb.data(), fa, fb);
      const bool r1 = f1_one(pa.data(), f1);
      n2++;
      bool wrong = false;
      if (rp && (!va || !vb || fa != ra || fb != rb)) wrong = true;
      if (r1 && (!va || f1 != ra)) wrong = true;
      u32 ya, yb;
      memcpy(&ya, pa.data(), 4);
      memcpy(&yb, pb.data(), 4);
      const bool ca = va && (ya == g_ytxt[0] || ya == g_ytxt[1]), cb = vb && (yb == g_ytxt[0] || yb == g_ytxt[1]);
      if (ca) {
        in_year++;
        accepted += r1;
        if (!r1) wrong = true;
      }
      if (ca && cb && !rp) wrong = true;
      if (wrong) {
        if (bad2 < 10)
          fprintf(stderr, "F1 KERNEL MISMATCH a='%.20s' b='%.20s' pair=%d one=%d ref=%d,%d\n", (const char*)pa.data(),
                  (const char*)pb.data(), rp, r1, va, vb);
        bad2++;
      }
    }
  }
  fprintf(stderr, "selftest F1 kernels: %ld cases, %ld mismatches; accepted %ld of %ld valid in-year values\n", n2, bad2,
          accepted, in_year);
  // ---- kernels for timestamps with a UTC offset: the same two requirements, against parse_ts_ref
  long n5 = 0, bad5 = 0, in_year5 = 0, accepted5 = 0;
  for (const auto& ys : year_sets) {
    init_fast_years(days_from_civil(ys[0], 12, 20) * DAY, days_from_civil(ys[1], 1, 15) * DAY);
    for (long it = 0; it < 3000000; it++) {
      auto gen = [&](u8* dst, bool offset) {
        char t[64];
        int yr = (next() % 16 == 0) ? ys[0] + (int)(next() % 5) - 2 : ys[next() & 1];
        int mo = 1 + (int)(next() % 12), dd = 1 + (int)(next() % 31), hh = (int)(next() % 24), mi = (int)(next() % 60),
            se = (int)(next() % 60), oh = (int)(next() % 24), om = (int)(next() % 60);
        if (next() % 40 == 0) mo = (int)(next() % 20);
        if (next() % 40 == 0) dd = (int)(next() % 40);
        if (next() % 40 == 0) hh = (int)(next() % 30);
        if (next() % 40 == 0) mi = (int)(next() % 70);
        if (next() % 40 == 0) se = (int)(next() % 70);
        if (next() % 20 == 0) oh = (int)(next() % 100);
        if (next() % 20 == 0) om = (int)(next() % 100);
        int len = snprintf(t, sizeof t, "%04d-%02d-%02dT%02d:%02d:%02d", yr, mo, dd, hh, mi, se);
        if (offset)
          len += snprintf(t + len, sizeof t - len, "%c%02d:%02d", "+-+-+-+-+-+-+-Z,"[next() % 16], oh, om);
        else
          len += snprintf(t + len, sizeof t - len, "Z");
        if (next() % 5 == 0) {
          int pos = (int)(next() % len);
          t[pos] = alphabet[next() % strlen(alphabet)];
        }
        if (next() % 16 == 0) {
          int pos = 17 + (int)(next() % (len - 17));
          t[pos] = (char)(next() % 256);
        }
        for (int k = 0; k < 64; k++) dst[k] = (u8)("0123456789,\"Z:+-T"[next() % 17]);
        memcpy(dst, t, len);
      };
      gen(pa.data(), true);
      gen(pb.data(), false);
      i64 ra = 0, rb = 0, fa = 0, fb = 0, f3 = 0;
      const bool va = parse_ts_ref(pa.data(), 25, ra) && pa[4] == '-';  // the 25-byte form (not 25 digits)
      const bool vb = pb[19] == 'Z' && civil19_scalar(pb.data(), rb);
      const bool rp = f3f1_pair(pa.data(), pb.data(), fa, fb);
      const bool r1 = f3_one(pa.data(), f3);
      n5++;
      bool wrong = false;
      if (rp && (!va || !vb || fa != ra || fb != rb)) wrong = true;
      if (r1 && (!va || f3 != ra)) wrong = true;
      u32 ya, yb;
      memcpy(&ya, pa.data(), 4);
      memcpy(&yb, pb.data(), 4);
      const bool ca = va && (ya == g_ytxt[0] || ya == g_ytxt[1]), cb = vb && (yb == g_ytxt[0] || yb == g_ytxt[1]);
      if (ca) {
        in_year5++;
        accepted5 += r1;
        if (!r1) wrong = true;
      }
      if (ca && cb && !rp) wrong = true;
      if (wrong) {
        if (bad5 < 10)
          fprintf(stderr, "OFFSET KERNEL MISMATCH a='%.25s' b='%.20s' pair=%d one=%d ref=%d,%d\n", (const char*)pa.data(),
                  (const char*)pb.data(), rp, r1, va, vb);
        bad5++;
      }
    }
  }
  fprintf(stderr, "selftest offset kernels: %ld cases, %ld mismatches; accepted %ld of %ld valid in-year values\n", n5,
          bad5, accepted5, in_year5);
  // ---- digits_le8 and camp_fast against the loops
  long n3 = 0, bad3 = 0;
  {
    std::vector<i32> saved_num = g_camp_by_num;
    g_camp_by_num.assign(5000, -1);
    for (u32 i = 0; i < 5000; i += 3) g_camp_by_num[i] = (i32)(i * 7 + 1);
    u8 t[40];
    alignas(32) u64 q4raw[4];
    alignas(16) u32 q4ln[4];
    bool q4pre[4];
    u8 q4s[4][40];
    for (long it = 0; it < 4000000; it++) {
      for (int k = 0; k < 40; k++) t[k] = (u8)("0123456789,\"c_ /:9a"[next() % 19]);
      const u32 nd = 1 + (u32)(next() % 8);
      for (u32 k = 0; k < nd; k++)
        if (next() % 12) t[k] = (u8)('0' + next() % 10);
      u64 v = 0, ref = 0;
      bool okr = true;
      for (u32 k = 0; k < nd; k++) {
        if (!is_dig(t[k])) okr = false;
        ref = ref * 10 + (u64)(t[k] - '0');
      }
      const bool ok = digits_le8(t, nd, v);
      n3++;
      if (ok != okr || (ok && v != ref)) {
        if (bad3 < 10) fprintf(stderr, "DIGITS MISMATCH '%.*s'\n", (int)nd, (const char*)t);
        bad3++;
      }
      // a campaign id, mostly well-formed
      char c[40];
      int len = snprintf(c, sizeof c, "c_%u", (unsigned)(next() % ((next() & 1) ? 6000 : 3000000000u)));
      if (next() % 6 == 0) c[next() % len] = "0c_9 x"[next() % 6];
      if (next() % 9 == 0) {
        memmove(c + 3, c + 2, len - 1);  // a leading zero
        c[2] = '0';
        len++;
      }
      memset(t, ',', sizeof t);
      memcpy(t, c, len);
      n3++;
      if (camp_fast(t, (u32)len) != camp_of(t, (u32)len)) {
        if (bad3 < 10) fprintf(stderr, "CAMPAIGN MISMATCH '%.*s'\n", len, c);
        bad3++;
      }
      // the same through the four-lane decoder of the dense click parse and its table rule
      {
        const u32 j = (u32)(it & 3);
        memcpy(&q4raw[j], t + 2, 8);
        q4ln[j] = (u32)len;
        q4pre[j] = t[0] == 'c' && t[1] == '_';
        memcpy(q4s[j], t, sizeof t);
        if (j == 3) {
          __m256i val;
          const u32 bad = dn::dec10x4(q4raw, q4ln, val);
          alignas(32) u64 vv[4];
          _mm256_store_si256((__m256i*)vv, val);
          for (u32 k = 0; k < 4; k++) {
            i32 got;
            if (((bad >> k) & 1) || !q4pre[k])
              got = camp_of(q4s[k], q4ln[k]);
            else
              got = vv[k] < g_camp_by_num.size() ? g_camp_by_num[vv[k]] : -1;
            n3++;
            if (got != camp_of(q4s[k], q4ln[k])) {
              if (bad3 < 10) fprintf(stderr, "CAMPAIGN x4 MISMATCH '%.*s'\n", (int)q4ln[k], (const char*)q4s[k]);
              bad3++;
            }
          }
        }
      }
    }
    g_camp_by_num = saved_num;
  }
  fprintf(stderr, "selftest digits / campaign ids: %ld cases, %ld mismatches\n", n3, bad3);
  // ---- fast keys: the key gives the string back, and keys order like the strings
  long n4 = 0, bad4 = 0, nfast = 0;
  {
    u8 a[32], b[32], back[16];
    std::string prev;
    u64 kprev = 0;
    for (long it = 0; it < 4000000; it++) {
      const u32 na = 1 + (u32)(next() % ((next() % 8) ? 9 : 11));
      for (u32 k = 0; k < 32; k++) a[k] = (u8)("az09AZ_-\x7f\x01~ c"[next() % 14]);
      for (u32 k = 0; k < na; k++) {
        const u32 r = (u32)(next() % 64);
        if (r == 0) a[k] = 0;
        if (r == 1) a[k] = (u8)(0x80 + next() % 128);
      }
      memcpy(b, a, 32);
      const u64 ka = key_fast(a, na);
      const bool want = na <= 9 && a[0] != 0 && a[na - 1] != 0 && [&] {
        for (u32 k = 1; k < na; k++)
          if (a[k] >= 0x80) return false;
        return true;
      }();
      n4++;
      bool wrong = (ka != 0) != want;
      if (ka) {
        nfast++;
        const u32 nb = fast_key_bytes(ka, back);
        if (nb != na || memcmp(back, a, na) != 0 || is_slow(ka)) wrong = true;
        const std::string sa((const char*)a, na);
        if (kprev && ((ka < kprev) != (sa < prev) || (ka == kprev) != (sa == prev))) wrong = true;
        prev = sa;
        kprev = ka;
      }
      if (wrong) {
        if (bad4 < 10) fprintf(stderr, "KEY MISMATCH (length %u)\n", na);
        bad4++;
      }
    }
  }
  fprintf(stderr, "selftest keys: %ld cases (%ld fast), %ld mismatches\n", n4, nfast, bad4);
  const long bad6 = dn::selftest(next);
  return (bad || bad2 || bad3 || bad4 || bad5 || bad6) ? 1 : 0;
}

// ------------------------------------------------------------------------------------------

static int bench_parse(const char* data, const char* file, int reps);

// A profile-collecting build (clang -fprofile-instr-generate, used only to make pgo/clang18.profdata) writes its
// profile at exit, which _exit skips; in every other build this symbol is absent and the call does nothing.
extern "C" int __llvm_profile_write_file(void) __attribute__((weak));
static inline void exit_now() {
  if (__llvm_profile_write_file) __llvm_profile_write_file();
  _exit(0);
}

int main(int argc, char** argv) {
  g_t0 = now_s();
  if (argc >= 2 && strcmp(argv[1], "--selftest") == 0) return selftest();
  if (argc >= 4 && strcmp(argv[1], "--bench-parse") == 0) return bench_parse(argv[2], argv[3], argc >= 5 ? atoi(argv[4]) : 5);
  const char* lg = getenv("SS_LOG");
  g_log = lg && *lg && *lg != '0';
  if (g_log) fprintf(stderr, "ABS start %.6f\n", g_t0);
  if (const char* e = getenv("SS_NODROP")) g_drop_parsed = !(*e && *e != '0');
  std::string data = getenv("DATA_DIR") ? getenv("DATA_DIR") : "/data";
  std::string out = getenv("OUTPUT_DIR") ? getenv("OUTPUT_DIR") : "/output";
  if (argc >= 3) {
    data = argv[argc - 2];
    out = argv[argc - 1];
  }
  int hw = (int)std::thread::hardware_concurrency();
  NT = hw > 0 ? hw : 8;
  if (const char* nc = getenv("NCPU")) {
    int v = atoi(nc);
    if (v > 0) NT = v;
  }
  if (const char* nt = getenv("SS_THREADS")) {
    int v = atoi(nt);
    if (v > 0) NT = v;
  }
  NT = std::max(1, std::min(NT, 256));
  init_years();

  std::vector<std::string> evp = list_csv(data + "/events"), ckp = list_csv(data + "/clicks"),
                           lkp = list_csv(data + "/links");
  std::vector<MFile> ev, ck, lk;
  for (auto& p : evp) ev.push_back(map_file(p));
  for (auto& p : ckp) ck.push_back(map_file(p));
  for (auto& p : lkp) lk.push_back(map_file(p));
  size_t ev_bytes = 0;
  for (auto& f : ev) ev_bytes += f.size;
  u64 est_lines = ev_bytes / 70 + 1;
  const u32 P1bits_gen = std::min<u32>(12, std::max<u32>(4, ceil_log2(est_lines / 16384 + 1)));
  const u32 P2bits_gen = std::min<u32>(12, std::max<u32>(4, ceil_log2(est_lines / 32768 + 1)));
  g_P1bits = P1bits_gen;
  g_P2bits = P2bits_gen;
  // The dense path (dense.h), if a sample of the events gives the key of the user IDs.
  std::vector<dn::Samp> samp;
  {
    const char* e = getenv("SS_DENSE");
    dn::g_ne_cap = ((u64)(ev_bytes / 56) | (dn::ESLOTS - 1)) + 1;
    const u64 nu = ((dn::g_ne_cap / 30 + 1024) | ((1u << dn::KC) - 1)) + 1;
    if (!(e && *e == '0') && nu <= (1u << 23)) {
      dn::g_nu_cap = (u32)nu;
      dn::g_PE = (u32)(dn::g_ne_cap >> dn::KE);
      dn::g_PC = dn::g_nu_cap >> dn::KC;
      dn::sample<7>(ev, 'e', 'u', 4096, samp);
      if (dn::recover_user_key(samp)) {
        dn::g_on = true;
        dn::g_acc = (std::atomic<u64>*)vm_alloc(sizeof(std::atomic<u64>) * dn::g_nu_cap);
        g_P2bits = std::min<u32>(8, std::max<u32>(4, ceil_log2(est_lines / (32768 * 16) + 1)));
      }
    }
  }
  LOG("mapped %zu event files (%zu bytes), %zu click files, %zu link files; P1=%u P2=%u threads=%d dense=%d ukey=%llx",
      ev.size(), ev_bytes, ck.size(), lk.size(), 1u << g_P1bits, 1u << g_P2bits, NT, (int)dn::g_on,
      (unsigned long long)dn::g_ukey);
  // The lookups and the per-thread state are not needed for the links: set them up on the side meanwhile.
  std::thread setup([&] {
    load_lookups(data);
    if (dn::g_on && !dn::recover_event_key(samp)) dn::fail("no key for the event ids");
    if (dn::g_on && (u64)(g_we - g_ws) > 0x7FFFFFF0ULL) dn::fail("window too long");
    if (dn::g_on && g_cats.size() > 255) dn::fail("too many categories");
    if (dn::g_on && g_ndays > 16) dn::fail("more than 16 days in the window");
    if (dn::g_on) {
      dn::g_ing_base = g_ws - 30 * DAY;
      dn::init_daycode();
    }
    g_tl.resize(NT);
    for (auto& t : g_tl) {
      if (dn::g_on && !dn::g_fail.load()) {
        t.ev.init(dn::g_PE);
        t.ck.init(dn::g_PC);
      } else {
        t.r1.init(1u << g_P1bits);
      }
      t.r2.init(1u << g_P2bits);
      t.r2c.init(1u << g_P2bits);
    }
  });
  g_phase = 1;
  if (dn::g_on)
    dn::build_links_dense(lk);
  else
    build_links(lk);
  setup.join();
  LOG("lookups: window %lld..%lld, %zu categories, %u campaigns, %d days", (long long)g_ws, (long long)g_we,
      g_cats.size(), g_ncamp, g_ndays);

  if (dn::g_on && !dn::g_fail.load()) {
    g_phase = 2;
    dn::parse(ev, ck);
    {
      u64 a = 0, b = 0;
      for (auto& t : g_tl) {
        a += t.n_event_lines;
        b += t.n_click_lines;
      }
      LOG("parsed (dense): %llu event lines, %llu click lines", (unsigned long long)a, (unsigned long long)b);
    }
    if (const char* e = getenv("SS_DENSE_FAIL"))  // (tests of the way back to the general pipeline)
      if (!strcmp(e, "parse")) dn::fail("test: after the parse");
    if (!dn::g_fail.load()) {
      g_phase = 3;
      dn::resolve_special();
      dn::scan_all();
      if (const char* e = getenv("SS_DENSE_FAIL"))
        if (!strcmp(e, "scan")) dn::fail("test: after the scan");
    }
    if (!dn::g_fail.load()) {
      LOG("dense scan done");
      g_phase = 4;
      person_phase(true);
      LOG("general person phase done");
      g_phase = 5;
      write_outputs(out);
      LOG("output written");
      if (g_log) {
        double tot = 0, tots = 0;
        for (int i = 0; i < 6; i++) {
          tot += g_phase_cycles[i].load() / 1e6;
          tots += g_phase_sys[i].load() / 1e6;
        }
        fprintf(stderr, "user s:");
        for (int i = 1; i < 6; i++) fprintf(stderr, " %s %.3f", g_phase_names[i], g_phase_cycles[i].load() / 1e6);
        fprintf(stderr, " | total %.3f   sys s: total %.3f (parse %.3f)\n", tot, tots, g_phase_sys[2].load() / 1e6);
      }
      release_memory();
      if (g_log) fprintf(stderr, "ABS end %.6f\n", now_s());
      fflush(nullptr);
      exit_now();
    }
  }
  if (dn::g_on) {
    // The data does not have the shape the dense path needs: start over with the general pipeline.
    LOG("dense path given up (%s): general pipeline", dn::g_fail_why);
    for (auto& t : g_tl)  // (the records of the attempt)
      for (auto& sl : t.arena.slabs) madvise(sl.first, sl.second, MADV_DONTNEED);
    dn::g_acc = nullptr;
    // The link table of the attempt holds only the links that the words of the dense path could not: the
    // general pipeline needs all of them.
    g_H = nullptr;
    g_F = nullptr;
    g_Fshift = 63;
    g_Hbits = 0;
    g_phase = 1;
    build_links(lk);
    g_P1bits = P1bits_gen;
    g_P2bits = P2bits_gen;
    g_tl.clear();
    g_tl.resize(NT);
    for (auto& t : g_tl) {
      t.r1.init(1u << g_P1bits);
      t.r2.init(1u << g_P2bits);
      t.r2c.init(1u << g_P2bits);
    }
  }

  g_phase = 2;
  parse_events_clicks(ev, ck);
  {
    u64 a = 0, b = 0;
    for (auto& t : g_tl) {
      a += t.n_event_lines;
      b += t.n_click_lines;
    }
    LOG("parsed: %llu event lines, %llu click lines", (unsigned long long)a, (unsigned long long)b);
  }
  if (getenv("SS_STOP") && !strcmp(getenv("SS_STOP"), "parse")) return 0;
  g_phase = 3;
  dedup_and_route();
  LOG("dedup + routing done");
  if (getenv("SS_STOP") && !strcmp(getenv("SS_STOP"), "dedup")) return 0;
  g_phase = 4;
  person_phase();
  LOG("person phase done");
  g_phase = 5;
  write_outputs(out);
  LOG("output written");
  if (g_log) {
    double tot = 0, tots = 0;
    for (int i = 0; i < 6; i++) {
      tot += g_phase_cycles[i].load() / 1e6;
      tots += g_phase_sys[i].load() / 1e6;
    }
    fprintf(stderr, "user s:");
    for (int i = 1; i < 6; i++) fprintf(stderr, " %s %.3f", g_phase_names[i], g_phase_cycles[i].load() / 1e6);
    fprintf(stderr, " | total %.3f   sys s: total %.3f (parse %.3f)\n", tot, tots, g_phase_sys[2].load() / 1e6);
  }
  release_memory();
  if (g_log) fprintf(stderr, "ABS end %.6f\n", now_s());
  fflush(nullptr);
  exit_now();  // nothing left to clean up that the kernel does not do anyway
}

// ------------------------------------------------------------------------------------------
// micro-benchmark: event parsing of one file, single thread (R1 records are produced as usual)
static int bench_parse(const char* data, const char* file, int reps) {
  init_years();
  NT = 1;
  load_lookups(data);
  std::vector<MFile> ev{map_file(file)}, ck;
  g_P1bits = 11;
  g_P2bits = 10;
  g_tl.resize(1);
  // touch the file once
  volatile u64 sink = 0;
  for (size_t i = 0; i < ev[0].size; i += 4096) sink += ev[0].data[i];
  const size_t slab = (size_t)1 << 30;
  u8* mem = (u8*)vm_alloc(slab);
  memset(mem, 0, slab);  // pre-touch
  for (int r = 0; r < reps; r++) {
    TL& t = g_tl[0];
    t = TL();
    t.arena.cur = mem;
    t.arena.end = mem + slab;
    t.r1.init(1u << g_P1bits);
    t.r2.init(1u << g_P2bits);
    t.r2c.init(1u << g_P2bits);
    u64 c0 = __rdtsc();
    parse_events_clicks(ev, ck);
    u64 c1 = __rdtsc();
    fprintf(stderr, "rep %d: %.1f cycles/line (%llu lines)\n", r, (double)(c1 - c0) / t.n_event_lines,
            (unsigned long long)t.n_event_lines);
  }
  return 0;
}
