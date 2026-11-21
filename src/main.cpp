// Shopping Sessions: native pipeline (C++17, x86-64 with BMI2/AVX2).
//
// Phases:
//   1. lookups: manifest, products, campaigns
//   2. links: parse -> distinct IDs (hash partitioned) -> lock-free union-find -> table H: id -> person name
//   3. events + clicks: parse in parallel chunks.
//        events -> R1 records, partitioned by hash(event_id)       (for dedup)
//        clicks -> R2C records, partitioned by hash(person)        (for attribution)
//   4. dedup per event_id partition -> R2 records, partitioned by hash(person)
//   5. per person partition: group, sort, sessionize, attribute, aggregate
//   6. write Parquet
#include <dirent.h>
#include <fcntl.h>
#include <immintrin.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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

template <class F>
static void parallel_for(int ntasks, F&& f) {
  if (ntasks <= 0) return;
  std::atomic<int> next{0};
  auto worker = [&](int tid) {
    for (;;) {
      int t = next.fetch_add(1, std::memory_order_relaxed);
      if (t >= ntasks) break;
      f(t, tid);
    }
  };
  int n = std::min(NT, ntasks);
  std::vector<std::thread> th;
  th.reserve(n);
  for (int i = 1; i < n; i++) th.emplace_back(worker, i);
  worker(0);
  for (auto& t : th) t.join();
}

template <class F>
static void parallel_threads(F&& f) {
  std::vector<std::thread> th;
  for (int i = 1; i < NT; i++) th.emplace_back(f, i);
  f(0);
  for (auto& t : th) t.join();
}

static void* vm_alloc(size_t bytes) {
  if (bytes == 0) bytes = 64;
  void* p = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (p == MAP_FAILED) die("mmap anonymous");
  madvise(p, bytes, MADV_HUGEPAGE);
  return p;
}

static constexpr size_t BLOCK_BYTES = 16384;

struct Arena {
  u8* cur = nullptr;
  u8* end = nullptr;
  std::vector<void*> free_blocks;  // recycled BLOCK_BYTES blocks
  void* alloc(size_t n) {
    n = (n + 63) & ~(size_t)63;
    if ((size_t)(end - cur) < n) {
      size_t sz = std::max(n, (size_t)256 << 20);
      cur = (u8*)vm_alloc(sz);
      end = cur + sz;
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

static inline u64 mix64(u64 x) {
  x ^= x >> 31;
  x *= 0x7fb5d329728ea185ULL;
  x ^= x >> 27;
  x *= 0x81dadef4bc2dd44dULL;
  x ^= x >> 33;
  return x;
}

static inline u64 unmix64(u64 x) {
  x ^= x >> 33;
  x *= 0x4d6dff26c61d8485ULL;
  x ^= (x >> 27) ^ (x >> 54);
  x *= 0x4c5ff4596f4a2f4dULL;
  x ^= (x >> 31) ^ (x >> 62);
  return x;
}

static inline u32 ceil_log2(u64 x) {
  u32 b = 0;
  while ((1ULL << b) < x) b++;
  return b;
}

// ------------------------------------------------------------------------------------------
// IDs as 64-bit keys.
//
// Fast keys: strings of 1..9 bytes, first byte non-zero, the rest in 0x01..0x7F.
//   key = first byte << 56 | the other bytes' low 7 bits packed big-endian (zero padded).
//   Integer order of fast keys equals byte-wise order of the strings.
// Slow keys: any other string, interned; key = index + 1 (< 2^56, so the top byte is 0).

static inline u64 key_fast(const u8* s, u32 n) {
  u32 m = n - 1;
  if (m > 8) return 0;
  u64 w;
  memcpy(&w, s + 1, 8);
  u64 keep = (m == 8) ? ~0ULL : ((1ULL << (8 * m)) - 1);
  w &= keep;
  u64 v = w | ~keep;
  if ((w & 0x8080808080808080ULL) | ((v - 0x0101010101010101ULL) & ~v & 0x8080808080808080ULL)) return 0;
  if (s[0] == 0) return 0;
  return ((u64)s[0] << 56) | _pext_u64(__builtin_bswap64(w), 0x7F7F7F7F7F7F7F7FULL);
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

static inline u64 make_key(const u8* s, u32 n) {
  u64 k = key_fast(s, n);
  if (__builtin_expect(k != 0, 1)) return k;
  return g_intern.get(s, n);
}
static inline bool is_slow(u64 k) { return (k >> 56) == 0; }

static void key_str(u64 k, std::string& out) {
  if (is_slow(k)) {
    out = g_intern.strs[k - 1];
    return;
  }
  out.clear();
  out.push_back((char)(k >> 56));
  for (int g = 0; g < 8; g++) {
    char c = (char)((k >> (49 - 7 * g)) & 0x7F);
    if (!c) break;
    out.push_back(c);
  }
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

#define FORCE_INLINE inline __attribute__((always_inline))

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
  alignas(32) u32 L[8];
  _mm256_store_si256((__m256i*)L, q);
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

// H: link ID -> person name key. Open addressing; the top g_Hpbits bits of the slot index pick a
// region that is probed (linearly, wrapping) on its own.
struct HSlot {
  u64 key;
  u64 val;
};
static HSlot* g_H = nullptr;
static u32 g_Hbits = 0, g_Hpbits = 0;

static inline u64 h_slot_index(u64 h) { return h >> (64 - g_Hbits); }

static inline u64 person_of_h(u64 k, u64 h) {
  u32 sbits = g_Hbits - g_Hpbits;
  u64 smask = (1ULL << sbits) - 1;
  u64 idx = h_slot_index(h);
  u64 base = idx & ~smask;
  u64 i = idx & smask;
  for (;;) {
    const HSlot& s = g_H[base + i];
    if (s.key == k) return s.val;
    if (s.key == 0) return k;
    i = (i + 1) & smask;
  }
}

// Calls emit(i, person) for i in [0, n) where person = person of key_at(i), with the H probes
// software-prefetched D items ahead.
template <class KeyAt, class Emit>
static inline void lookup_pipeline(size_t n, KeyAt&& key_at, Emit&& emit) {
  if (!g_H) {
    for (size_t i = 0; i < n; i++) emit(i, key_at(i));
    return;
  }
  constexpr size_t D = 32;
  u64 ks[D], hs[D];
  size_t pre = n < D ? n : D;
  for (size_t i = 0; i < pre; i++) {
    ks[i] = key_at(i);
    hs[i] = mix64(ks[i]);
    _mm_prefetch((const char*)&g_H[h_slot_index(hs[i])], _MM_HINT_T0);
  }
  for (size_t i = 0; i < n; i++) {
    size_t s = i & (D - 1);
    u64 person = person_of_h(ks[s], hs[s]);
    if (i + D < n) {
      ks[s] = key_at(i + D);
      hs[s] = mix64(ks[s]);
      _mm_prefetch((const char*)&g_H[h_slot_index(hs[s])], _MM_HINT_T0);
    }
    emit(i, person);
  }
}

struct UNode {
  std::atomic<u32> parent;
  u32 pad;
  u64 key;
};
static UNode* g_nodes = nullptr;

static inline u32 uf_find(u32 x) {
  for (;;) {
    u32 p = g_nodes[x].parent.load(std::memory_order_relaxed);
    if (p == x) return x;
    u32 gp = g_nodes[p].parent.load(std::memory_order_relaxed);
    if (gp == p) return p;
    g_nodes[x].parent.store(gp, std::memory_order_relaxed);
    x = gp;
  }
}

static inline void uf_unite(u32 a, u32 b) {
  for (;;) {
    a = uf_find(a);
    b = uf_find(b);
    if (a == b) return;
    if (key_cmp(g_nodes[a].key, g_nodes[b].key) > 0) std::swap(a, b);
    u32 exp = b;
    if (g_nodes[b].parent.compare_exchange_strong(exp, a, std::memory_order_acq_rel)) return;
  }
}

// records
struct R1 {  // event line, partitioned by event_id
  u64 eid;
  i64 ingest;
  u64 uid;      // user_id key (valid lines only)
  u32 ts;       // ms since window start, TS_INVALID if the line is not a valid event
  u32 payload;  // purchase: cat << 24 | price; else 0
};
struct C0 {  // valid click line, unpartitioned
  u64 uid;
  u64 click;
  u32 ts;  // ms since window start - 7 days
  u32 camp;
};
struct R2 {  // surviving event, partitioned by person; h = mix64(person key)
  u64 h;
  u32 ts;
  u32 payload;
};
struct R2C {  // valid click, partitioned by person; h = mix64(person key)
  u64 h;
  u64 click;
  u32 ts;
  u32 camp;
};

static u32 g_P1bits, g_P2bits;

struct OutRows {
  std::vector<u64> key;
  std::vector<i32> day;
  std::vector<u32> sess, evs;
  std::vector<i64> rev;
  std::vector<u16> top;
};

struct TL {
  Arena arena;
  std::vector<Stream<R1>> r1;
  Stream<C0> c0;
  std::vector<Stream<R2>> r2;
  std::vector<Stream<R2C>> r2c;
  std::vector<std::pair<u64, u64>> links;
  OutRows out;
  std::vector<u32> c_sess, c_purch, c_assist;  // (ncamp + 1) x ndays
  std::vector<i64> c_rev;
  u64 n_event_lines = 0, n_click_lines = 0;
};
static std::vector<TL> g_tl;

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

  auto read_pairs = [&](const std::string& path, std::vector<std::pair<std::string, std::string>>& rows) {
    std::string s = read_small(path);
    size_t n = s.size();
    s.append(64, '\0');
    scan_chunk<2>((const u8*)s.data(), (const u8*)s.data() + n, true, [&](const u8* const* fs, const u32* fl) {
      rows.emplace_back(std::string((const char*)fs[0], fl[0]), std::string((const char*)fs[1], fl[1]));
    });
  };
  {
    std::vector<std::pair<std::string, std::string>> rows;
    read_pairs(data + "/products.csv", rows);
    std::unordered_map<std::string, u32> catset;
    for (auto& r : rows) catset.emplace(r.second, 0);
    catset.emplace("unknown", 0);
    std::vector<std::string> cats;
    for (auto& kv : catset) cats.push_back(kv.first);
    std::sort(cats.begin(), cats.end());
    if (cats.size() > 255) die("more than 255 categories");
    for (u32 i = 0; i < cats.size(); i++) catset[cats[i]] = i;
    g_cats = cats;
    g_cat_unknown = catset["unknown"];
    u32 maxnum = 0;
    std::vector<std::pair<u32, u32>> canon;
    std::unordered_map<std::string, bool> seen;
    for (auto& r : rows) {
      if (!seen.emplace(r.first, true).second) continue;  // first wins
      u32 cat = catset[r.second];
      const std::string& id = r.first;
      bool canonical = id.size() >= 2 && id.size() <= 9 && id[0] == 'p' && (id[1] != '0' || id.size() == 2);
      u32 v = 0;
      for (size_t i = 1; canonical && i < id.size(); i++) {
        if (!is_dig((u8)id[i]))
          canonical = false;
        else
          v = v * 10 + (id[i] - '0');
      }
      if (canonical && v < (1u << 24)) {
        canon.push_back({v, cat});
        maxnum = std::max(maxnum, v);
      } else {
        g_cat_slow.emplace(id, cat);
      }
    }
    g_cat_by_num.assign(canon.empty() ? 0 : (size_t)maxnum + 1, (u8)g_cat_unknown);
    for (auto& c : canon) g_cat_by_num[c.first] = (u8)c.second;
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

static void build_links(std::vector<MFile>& files) {
  std::vector<Chunk> chunks;
  make_chunks(files, 1 << 20, chunks);
  parallel_for((int)chunks.size(), [&](int t, int tid) {
    const Chunk& c = chunks[t];
    const MFile& f = files[c.file];
    size_t b = line_start_at_or_after(f, c.nominal), e = line_start_at_or_after(f, c.nominal_end);
    if (b >= e) return;
    auto& out = g_tl[tid].links;
    scan_chunk<2>(f.data + b, f.data + e, b == 0, [&](const u8* const* fs, const u32* fl) {
      if (fl[0] == 0 || fl[1] == 0) return;
      out.push_back({make_key(fs[0], fl[0]), make_key(fs[1], fl[1])});
    });
  });
  size_t nlinks = 0;
  for (auto& t : g_tl) nlinks += t.links.size();
  LOG("links: %zu lines", nlinks);
  if (nlinks == 0) return;

  u32 pbits = std::min<u32>(10, std::max<u32>(1, ceil_log2(nlinks / 4096 + 1)));
  u32 P = 1u << pbits;
  // scatter keys into partitions (per producing thread)
  std::vector<std::vector<std::vector<u64>>> kp(NT, std::vector<std::vector<u64>>(P));
  parallel_for(NT, [&](int t, int) {
    auto& mine = kp[t];
    size_t expect = g_tl[t].links.size() * 2 / P + 16;
    for (auto& v : mine) v.reserve(expect + expect / 4);
    for (auto& pr : g_tl[t].links) {
      mine[mix64(pr.first) >> (64 - pbits)].push_back(pr.first);
      mine[mix64(pr.second) >> (64 - pbits)].push_back(pr.second);
    }
  });
  std::vector<std::vector<u64>> distinct(P);
  parallel_for((int)P, [&](int p, int) {
    size_t raw = 0;
    for (int t = 0; t < NT; t++) raw += kp[t][p].size();
    u32 bits = ceil_log2(raw * 2 + 16);
    std::vector<u64> tab((size_t)1 << bits, 0);
    u64 mask = ((u64)1 << bits) - 1;
    auto& d = distinct[p];
    d.reserve(raw);
    for (int t = 0; t < NT; t++)
      for (u64 k : kp[t][p]) {
        u64 i = (mix64(k) >> 8) & mask;
        for (;;) {
          if (tab[i] == k) break;
          if (tab[i] == 0) {
            tab[i] = k;
            d.push_back(k);
            break;
          }
          i = (i + 1) & mask;
        }
      }
    for (int t = 0; t < NT; t++) std::vector<u64>().swap(kp[t][p]);
  });
  size_t maxd = 0;
  std::vector<size_t> off(P + 1, 0);
  for (u32 p = 0; p < P; p++) {
    maxd = std::max(maxd, distinct[p].size());
    off[p + 1] = off[p] + distinct[p].size();
  }
  size_t total = off[P];
  u32 sbits = std::max<u32>(4, ceil_log2(maxd * 2 + 1));
  g_Hpbits = pbits;
  g_Hbits = pbits + sbits;
  g_H = (HSlot*)vm_alloc(sizeof(HSlot) << g_Hbits);
  g_nodes = (UNode*)vm_alloc(sizeof(UNode) * (total + 1));
  LOG("links: %zu distinct ids, H bits %u (%u partitions)", total, g_Hbits, P);
  parallel_for((int)P, [&](int p, int) {
    u64 smask = (1ULL << sbits) - 1;
    for (size_t j = 0; j < distinct[p].size(); j++) {
      u64 k = distinct[p][j];
      u32 id = (u32)(off[p] + j);
      g_nodes[id].parent.store(id, std::memory_order_relaxed);
      g_nodes[id].key = k;
      u64 idx = h_slot_index(mix64(k));
      u64 base = idx & ~smask, i = idx & smask;
      for (;;) {
        HSlot& s = g_H[base + i];
        if (s.key == 0) {
          s.key = k;
          s.val = id;
          break;
        }
        i = (i + 1) & smask;
      }
    }
    std::vector<u64>().swap(distinct[p]);
  });
  // map both ends to node ids (prefetched), then unite
  parallel_for(NT, [&](int t, int) {
    auto& L = g_tl[t].links;
    size_t n = L.size();
    std::vector<u32> ids(2 * n);
    lookup_pipeline(
        2 * n, [&](size_t i) { return (i & 1) ? L[i >> 1].second : L[i >> 1].first; },
        [&](size_t i, u64 v) { ids[i] = (u32)v; });
    std::vector<std::pair<u64, u64>>().swap(L);
    constexpr size_t D = 8;
    for (size_t i = 0; i < n; i++) {
      if (i + D < n) {
        _mm_prefetch((const char*)&g_nodes[ids[2 * (i + D)]], _MM_HINT_T0);
        _mm_prefetch((const char*)&g_nodes[ids[2 * (i + D) + 1]], _MM_HINT_T0);
      }
      uf_unite(ids[2 * i], ids[2 * i + 1]);
    }
  });
  // finalize: H value = name key of the root
  u64 nslots = 1ULL << g_Hbits;
  int ntask = 256;
  parallel_for(ntask, [&](int t, int) {
    u64 a = nslots * t / ntask, b = nslots * (t + 1) / ntask;
    for (u64 i = a; i < b; i++) {
      if (g_H[i].key) g_H[i].val = g_nodes[uf_find((u32)g_H[i].val)].key;
    }
  });
}

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
  int ntasks = (int)(ce.size() + cc.size());
  parallel_for(ntasks, [&](int t, int tid) {
    TL& tl = g_tl[tid];
    if (t < (int)ce.size()) {
      const Chunk& c = ce[t];
      const MFile& f = ev[c.file];
      size_t b = line_start_at_or_after(f, c.nominal), e = line_start_at_or_after(f, c.nominal_end);
      if (b >= e) return;
      u64 nl = 0;
      scan_chunk<7>(f.data + b, f.data + e, b == 0, [&](const u8* const* fs, const u32* fl) {
        if (fl[0] == 0) return;
        nl++;
        u64 eid = make_key(fs[0], fl[0]);
        i64 ingest = INT64_MAX, ts = 0;
        bool ts_ok;
        if (fl[2] == 20 && fs[2][19] == 'Z' && fl[3] == 20 && fs[3][19] == 'Z') {
          u32 r = civil19x2(fs[2], fs[3], ts, ingest);
          if (!(r & 2)) ingest = INT64_MAX;
          ts_ok = r & 1;
        } else {
          if (!parse_f1(fs[3], fl[3], ingest)) ingest = INT64_MAX;
          ts_ok = parse_ts(fs[2], fl[2], ts);
        }
        u64 uid = 0, tspl = TS_INVALID;
        if (fl[1] != 0 && fl[5] != 0 && ts_ok && ts >= ws && ts < we && ingest - ts <= DAY) {
          int ty = event_type(fs[4], fl[4]);
          if (ty >= 0) {
            bool ok = true;
            u32 payload = 0;
            if (ty == 2) {
              u32 price;
              if (parse_price(fs[6], fl[6], price))
                payload = (cat_of(fs[5], fl[5]) << 24) | price;
              else
                ok = false;
            }
            if (ok) {
              tspl = (u64)(u32)(ts - ws) | ((u64)payload << 32);
              uid = make_key(fs[1], fl[1]);
            }
          }
        }
        R1* d = tl.r1[mix64(eid) >> p1shift].slot(tl.arena);
        _mm256_storeu_si256((__m256i*)d, _mm256_set_epi64x((long long)tspl, (long long)uid, (long long)ingest, (long long)eid));
      });
      tl.n_event_lines += nl;
    } else {
      const Chunk& c = cc[t - ce.size()];
      const MFile& f = ck[c.file];
      size_t b = line_start_at_or_after(f, c.nominal), e = line_start_at_or_after(f, c.nominal_end);
      if (b >= e) return;
      u64 nl = 0;
      scan_chunk<4>(f.data + b, f.data + e, b == 0, [&](const u8* const* fs, const u32* fl) {
        if (fl[0] == 0 || fl[1] == 0) return;
        nl++;
        i64 ts;
        if (!parse_ts(fs[3], fl[3], ts) || ts < cws || ts >= we) return;
        i32 camp = camp_of(fs[2], fl[2]);
        if (camp < 0) return;
        u64 click = make_key(fs[0], fl[0]);
        u64 uid = make_key(fs[1], fl[1]);
        u64 tc = (u64)(u32)(ts - cws) | ((u64)(u32)camp << 32);
        C0* d = tl.c0.slot(tl.arena);
        d->uid = uid;
        d->click = click;
        memcpy(&d->ts, &tc, 8);
      });
      tl.n_click_lines += nl;
    }
  });
  parallel_for(NT, [&](int t, int) {
    for (auto& s : g_tl[t].r1) s.finish();
    g_tl[t].c0.finish();
  });
}

// ------------------------------------------------------------------------------------------
// dedup (per event_id partition) and person lookups; click person lookups

static void dedup_and_route() {
  u32 P1 = 1u << g_P1bits;
  const u32 p2shift = 64 - g_P2bits;
  std::vector<size_t> sizes(P1, 0);
  for (u32 p = 0; p < P1; p++)
    for (int t = 0; t < NT; t++) sizes[p] += g_tl[t].r1[p].size();
  std::vector<int> order(P1);
  for (u32 p = 0; p < P1; p++) order[p] = (int)p;
  std::sort(order.begin(), order.end(), [&](int a, int b) { return sizes[a] > sizes[b]; });
  // click tasks: groups of blocks of each thread's c0 stream
  struct CTask {
    int t;
    size_t b0, b1;
  };
  std::vector<CTask> ctasks;
  for (int t = 0; t < NT; t++) {
    size_t nb = g_tl[t].c0.blocks.size();
    for (size_t b = 0; b < nb; b += 16) ctasks.push_back({t, b, std::min(nb, b + 16)});
  }
  struct Slot {
    u64 eid;
    R1* ptr;
  };
  struct Scratch {
    std::vector<Slot> tab;
    std::vector<R1*> surv;
  };
  std::vector<Scratch> scr(NT);
  int ntasks = (int)(P1 + ctasks.size());
  parallel_for(ntasks, [&](int oi, int tid) {
    TL& tl = g_tl[tid];
    if (oi >= (int)P1) {
      const CTask& ct = ctasks[oi - P1];
      auto& blocks = g_tl[ct.t].c0.blocks;
      for (size_t b = ct.b0; b < ct.b1; b++) {
        const C0* r = blocks[b].first;
        lookup_pipeline(
            blocks[b].second, [&](size_t i) { return r[i].uid; },
            [&](size_t i, u64 person) {
              u64 h = mix64(person);
              R2C* d = tl.r2c[h >> p2shift].slot(tl.arena);
              d->h = h;
              d->click = r[i].click;
              u64 tc;
              memcpy(&tc, &r[i].ts, 8);
              memcpy(&d->ts, &tc, 8);
            });
        tl.arena.free_blocks.push_back(blocks[b].first);
      }
      return;
    }
    int p = order[oi];
    size_t n = sizes[p];
    if (n == 0) return;
    Scratch& S = scr[tid];
    u32 bits = ceil_log2(n * 2 + 16);
    size_t tsz = (size_t)1 << bits;
    if (S.tab.size() < tsz) S.tab.resize(tsz);
    memset(S.tab.data(), 0, tsz * sizeof(Slot));
    u64 mask = tsz - 1;
    Slot* tab = S.tab.data();
    for (int t = 0; t < NT; t++)
      for (auto& b : g_tl[t].r1[p].blocks) {
        R1* rec = b.first;
        for (u32 j = 0; j < b.second; j++) {
          R1* r = &rec[j];
          u64 h = (mix64(r->eid) << g_P1bits) >> (64 - bits);
          for (;;) {
            Slot& s = tab[h];
            if (s.ptr == nullptr) {
              s.eid = r->eid;
              s.ptr = r;
              break;
            }
            if (s.eid == r->eid) {
              if (r->ingest < s.ptr->ingest) {
                s.ptr->ts = TS_INVALID;
                s.ptr = r;
              } else {
                r->ts = TS_INVALID;
              }
              break;
            }
            h = (h + 1) & mask;
          }
        }
      }
    S.surv.clear();
    for (int t = 0; t < NT; t++)
      for (auto& b : g_tl[t].r1[p].blocks) {
        R1* rec = b.first;
        for (u32 j = 0; j < b.second; j++)
          if (rec[j].ts != TS_INVALID) S.surv.push_back(&rec[j]);
      }
    R1** sv = S.surv.data();
    lookup_pipeline(
        S.surv.size(), [&](size_t i) { return sv[i]->uid; },
        [&](size_t i, u64 person) {
          u64 tp;
          memcpy(&tp, &sv[i]->ts, 8);
          u64 h = mix64(person);
          R2* d = tl.r2[h >> p2shift].slot(tl.arena);
          _mm_storeu_si128((__m128i*)d, _mm_set_epi64x((long long)tp, (long long)h));
        });
    for (int t = 0; t < NT; t++) g_tl[t].r1[p].release(tl.arena);
  });
  parallel_for(NT, [&](int t, int) {
    for (auto& s : g_tl[t].r2) s.finish();
    for (auto& s : g_tl[t].r2c) s.finish();
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
  u32 lid;
  u32 ts;
  u32 payload;
  u32 pad;
};

struct CRec {
  u32 ts;
  u32 camp;
  u64 click;
};

static inline bool click_less(u64 a, u64 b) {  // byte-wise order of click ids
  if (!is_slow(a) && !is_slow(b)) return a < b;
  return key_cmp(a, b) < 0;
}

static void sort_clicks(CRec* a, size_t n, std::vector<CRec>& tmp) {
  if (n < 2) return;
  if (n <= 32) {
    for (size_t i = 1; i < n; i++) {
      CRec x = a[i];
      size_t j = i;
      while (j > 0 && a[j - 1].ts > x.ts) {
        a[j] = a[j - 1];
        j--;
      }
      a[j] = x;
    }
    return;
  }
  // LSD radix sort on ts (11 + 11 + 10 bits)
  if (tmp.size() < n) tmp.resize(n);
  CRec* src = a;
  CRec* dst = tmp.data();
  const int shifts[3] = {0, 11, 22};
  for (int pass = 0; pass < 3; pass++) {
    u32 cnt[2048] = {0};
    int sh = shifts[pass];
    for (size_t i = 0; i < n; i++) cnt[(src[i].ts >> sh) & 2047]++;
    u32 sum = 0;
    for (int b = 0; b < 2048; b++) {
      u32 c = cnt[b];
      cnt[b] = sum;
      sum += c;
    }
    for (size_t i = 0; i < n; i++) dst[cnt[(src[i].ts >> sh) & 2047]++] = src[i];
    std::swap(src, dst);
  }
  if (src != a) memcpy(a, src, n * sizeof(CRec));
}

static void person_phase() {
  u32 P2 = 1u << g_P2bits;
  std::vector<size_t> ne(P2, 0), nc(P2, 0);
  for (u32 p = 0; p < P2; p++)
    for (int t = 0; t < NT; t++) {
      ne[p] += g_tl[t].r2[p].size();
      nc[p] += g_tl[t].r2c[p].size();
    }
  std::vector<int> order(P2);
  for (u32 p = 0; p < P2; p++) order[p] = (int)p;
  std::sort(order.begin(), order.end(), [&](int a, int b) { return ne[a] > ne[b]; });
  const u32 NC = g_ncamp, ND = (u32)g_ndays, ORG = NC;
  const u32 NB = (u32)(((u64)(g_we - g_ws - 1) >> BSHIFT) + 1);
  parallel_for(NT, [&](int t, int) {
    TL& tl = g_tl[t];
    tl.c_sess.assign((size_t)(NC + 1) * ND, 0);
    tl.c_purch.assign((size_t)(NC + 1) * ND, 0);
    tl.c_assist.assign((size_t)(NC + 1) * ND, 0);
    tl.c_rev.assign((size_t)(NC + 1) * ND, 0);
  });
  struct PSlot {
    u64 h;
    u32 lid;
    u32 used;
  };
  struct Scratch {
    std::vector<PSlot> tab;
    std::vector<u64> hs;
    std::vector<u32> cntL, posL, cntB, cntC, offC;
    std::vector<XRec> X, Y;
    std::vector<CRec> C, Ctmp;
    std::vector<u32> lidC;
    std::vector<u32> ccnt, cpos, active;
    std::vector<i64> catsum;
    std::vector<u32> touched;
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
    if (S.ccnt.size() < NC + 1) {
      S.ccnt.assign(NC + 1, 0);
      S.cpos.assign(NC + 1, 0);
      S.catsum.assign(256, 0);
      S.cntB.assign(NB + 1, 0);
    }
    // ---- local person ids
    u32 bits = std::max<u32>(8, ceil_log2(n_e / 8 + 64));
    size_t tsz = (size_t)1 << bits;
    S.tab.assign(tsz, PSlot{0, 0, 0});
    u64 mask = tsz - 1;
    S.hs.clear();
    S.cntL.clear();
    auto rehash = [&]() {
      bits++;
      tsz = (size_t)1 << bits;
      mask = tsz - 1;
      S.tab.assign(tsz, PSlot{0, 0, 0});
      for (u32 lid = 0; lid < S.hs.size(); lid++) {
        u64 i = (S.hs[lid] << P2bits) >> (64 - bits);
        while (S.tab[i].used) i = (i + 1) & mask;
        S.tab[i] = PSlot{S.hs[lid], lid, 1};
      }
    };
    if (S.X.size() < n_e) {
      S.X.resize(n_e);
      S.Y.resize(n_e);
    }
    XRec* X = S.X.data();
    XRec* Y = S.Y.data();
    u32* cntB = S.cntB.data();
    memset(cntB, 0, sizeof(u32) * (NB + 1));
    size_t k = 0;
    for (int t = 0; t < NT; t++)
      for (auto& b : g_tl[t].r2[p].blocks) {
        const R2* r = b.first;
        for (u32 j = 0; j < b.second; j++) {
          u64 h = r[j].h;
          u64 i = (h << P2bits) >> (64 - bits);
          u32 lid;
          for (;;) {
            PSlot& s = S.tab[i];
            if (!s.used) {
              lid = (u32)S.hs.size();
              s = PSlot{h, lid, 1};
              S.hs.push_back(h);
              S.cntL.push_back(0);
              if (S.hs.size() * 2 > tsz) rehash();
              break;
            }
            if (s.h == h) {
              lid = s.lid;
              break;
            }
            i = (i + 1) & mask;
          }
          u32 ts = r[j].ts;
          X[k].lid = lid;
          X[k].ts = ts;
          X[k].payload = r[j].payload;
          k++;
          S.cntL[lid]++;
          cntB[ts >> BSHIFT]++;
        }
      }
    const u32 np = (u32)S.hs.size();
    // ---- pass 1: by time bucket (stable); pass 2: by person (stable)
    {
      u32 sum = 0;
      for (u32 b = 0; b < NB; b++) {
        u32 c = cntB[b];
        cntB[b] = sum;
        sum += c;
      }
      for (size_t i = 0; i < n_e; i++) Y[cntB[X[i].ts >> BSHIFT]++] = X[i];
      S.posL.resize(np + 1);
      u32* posL = S.posL.data();
      sum = 0;
      for (u32 l = 0; l < np; l++) {
        posL[l] = sum;
        sum += S.cntL[l];
      }
      posL[np] = sum;
      for (size_t i = 0; i < n_e; i++) X[posL[Y[i].lid]++] = Y[i];
      // posL[l] is now the end of person l; start = end - cnt
    }
    // ---- clicks of these persons, grouped by person
    S.cntC.assign(np + 1, 0);
    S.lidC.resize(n_c);
    {
      size_t kk = 0;
      for (int t = 0; t < NT; t++)
        for (auto& b : g_tl[t].r2c[p].blocks) {
          const R2C* r = b.first;
          for (u32 j = 0; j < b.second; j++) {
            u64 h = r[j].h;
            u64 i = (h << P2bits) >> (64 - bits);
            u32 lid = np;
            for (;;) {
              const PSlot& s = S.tab[i];
              if (!s.used) break;
              if (s.h == h) {
                lid = s.lid;
                break;
              }
              i = (i + 1) & mask;
            }
            S.lidC[kk++] = lid;
            S.cntC[lid]++;
          }
        }
    }
    S.offC.resize(np + 2);
    {
      u32 sum = 0;
      for (u32 l = 0; l <= np; l++) {
        S.offC[l] = sum;
        sum += S.cntC[l];
      }
      S.offC[np + 1] = sum;
      S.C.resize(sum);
      u32* pos = S.cntC.data();
      for (u32 l = 0; l <= np; l++) pos[l] = S.offC[l];
      size_t kk = 0;
      for (int t = 0; t < NT; t++)
        for (auto& b : g_tl[t].r2c[p].blocks) {
          const R2C* r = b.first;
          for (u32 j = 0; j < b.second; j++) {
            u32 lid = S.lidC[kk++];
            S.C[pos[lid]++] = CRec{r[j].ts, r[j].camp, r[j].click};
          }
        }
    }
    // ---- per person
    u32* ccnt = S.ccnt.data();
    u32* cpos = S.cpos.data();
    i64* catsum = S.catsum.data();
    u32* c_sess = tl.c_sess.data();
    u32* c_purch = tl.c_purch.data();
    u32* c_assist = tl.c_assist.data();
    i64* c_rev = tl.c_rev.data();
    OutRows& out = tl.out;
    const u32* posL = S.posL.data();
    for (u32 lid = 0; lid < np; lid++) {
      const u32 eend = posL[lid], ebeg = eend - S.cntL[lid];
      CRec* C = S.C.data() + S.offC[lid];
      size_t mc = S.offC[lid + 1] - S.offC[lid];
      sort_clicks(C, mc, S.Ctmp);
      size_t lo = 0, hi = 0;
      i64 cur_day = -1;
      u32 d_sess = 0, d_ev = 0;
      i64 d_rev = 0;
      const u64 pkey = unmix64(S.hs[lid]);
      auto flush = [&]() {
        u16 top = 0xFFFF;
        i64 best = -1;
        for (u32 c : S.touched) {
          i64 v = catsum[c];
          if (v > best || (v == best && c < top)) {
            best = v;
            top = (u16)c;
          }
          catsum[c] = 0;
        }
        S.touched.clear();
        out.key.push_back(pkey);
        out.day.push_back((i32)(day0 + cur_day));
        out.sess.push_back(d_sess);
        out.evs.push_back(d_ev);
        out.rev.push_back(d_rev);
        out.top.push_back(top);
      };
      // session state
      bool open = false;
      u32 s_start = 0, s_max = 0, s_nev = 0;
      i64 s_rev = 0;
      i64 s_day = 0;
      auto close_session = [&]() {
        // attribution: clicks with s_start <= click.ts <= s_start + WEEK (click time is relative to ws - 7d)
        u64 upper = (u64)s_start + (u64)WEEK;
        while (hi < mc && (u64)C[hi].ts <= upper) {
          u32 c = C[hi].camp;
          if (ccnt[c]++ == 0) {
            cpos[c] = (u32)S.active.size();
            S.active.push_back(c);
          }
          hi++;
        }
        while (lo < hi && C[lo].ts < s_start) {
          u32 c = C[lo].camp;
          if (--ccnt[c] == 0) {
            u32 ps = cpos[c];
            u32 last = S.active.back();
            S.active[ps] = last;
            cpos[last] = ps;
            S.active.pop_back();
          }
          lo++;
        }
        u32 credit = ORG;
        if (hi > lo) {
          size_t bi = hi - 1;
          u32 tsmax = C[bi].ts;
          for (size_t q = bi; q > lo && C[q - 1].ts == tsmax;) {
            q--;
            if (click_less(C[q].click, C[bi].click)) bi = q;
          }
          credit = C[bi].camp;
        }
        size_t ci = (size_t)credit * ND + (size_t)s_day;
        c_sess[ci]++;
        c_purch[ci] += (s_rev > 0);
        c_rev[ci] += s_rev;
        for (u32 c : S.active) c_assist[(size_t)c * ND + (size_t)s_day]++;
        d_sess++;
        d_ev += s_nev;
        d_rev += s_rev;
      };
      u32 i = ebeg;
      while (i < eend) {
        // one time bucket [i, j)
        u32 b = X[i].ts >> BSHIFT;
        u32 gmin = X[i].ts, gmax = gmin, gpay = X[i].payload;
        u32 j = i + 1;
        while (j < eend && (X[j].ts >> BSHIFT) == b) {
          u32 t = X[j].ts;
          gmin = t < gmin ? t : gmin;
          gmax = t > gmax ? t : gmax;
          gpay |= X[j].payload;
          j++;
        }
        if (!open || gmin - s_max > (u32)MIN30) {
          if (open) close_session();
          open = true;
          s_start = gmin;
          s_nev = 0;
          s_rev = 0;
          s_day = (ws + (i64)gmin) / DAY - day0;
          if (s_day != cur_day) {
            if (cur_day >= 0) flush();
            cur_day = s_day;
            d_sess = 0;
            d_ev = 0;
            d_rev = 0;
          }
        }
        s_max = gmax;
        s_nev += j - i;
        if (gpay) {
          for (u32 q = i; q < j; q++) {
            u32 pl = X[q].payload;
            if (pl) {
              u32 price = pl & 0xFFFFFF, cat = pl >> 24;
              s_rev += price;
              if (catsum[cat] == 0) S.touched.push_back(cat);
              catsum[cat] += price;
            }
          }
        }
        i = j;
      }
      if (open) close_session();
      if (cur_day >= 0) flush();
      for (u32 c : S.active) ccnt[c] = 0;
      S.active.clear();
    }
    for (int t = 0; t < NT; t++) {
      g_tl[t].r2[p].release(tl.arena);
      g_tl[t].r2c[p].release(tl.arena);
    }
  });
}

// ------------------------------------------------------------------------------------------
// output

static inline void put_key(std::vector<u8>& buf, size_t& at, u64 k) {
  if (is_slow(k)) {
    const std::string& s = g_intern.strs[k - 1];
    if (buf.size() < at + 4 + s.size()) buf.resize((at + 4 + s.size()) * 2);
    u32 n = (u32)s.size();
    memcpy(&buf[at], &n, 4);
    memcpy(&buf[at + 4], s.data(), n);
    at += 4 + n;
    return;
  }
  if (buf.size() < at + 16) buf.resize((at + 16) * 2);
  u8* o = &buf[at + 4];
  u32 n = 0;
  o[n++] = (u8)(k >> 56);
  for (int g = 0; g < 8; g++) {
    u8 c = (u8)((k >> (49 - 7 * g)) & 0x7F);
    if (!c) break;
    o[n++] = c;
  }
  memcpy(&buf[at], &n, 4);
  at += 4 + n;
}

static void write_outputs(const std::string& out) {
  std::string ud = out + "/user_daily", cd = out + "/campaign_daily";
  mkdir(ud.c_str(), 0777);
  mkdir(cd.c_str(), 0777);
  std::atomic<bool> fail{false};
  size_t total_rows = 0;
  for (auto& t : g_tl) total_rows += t.out.key.size();
  parallel_for(NT + 1, [&](int t, int) {
    if (t == NT) {
      // campaign_daily
      const u32 NC = g_ncamp, ND = (u32)g_ndays;
      size_t sz = (size_t)(NC + 1) * ND;
      std::vector<u64> sess(sz, 0), purch(sz, 0), assist(sz, 0);
      std::vector<i64> rev(sz, 0);
      for (auto& tl : g_tl) {
        if (tl.c_sess.empty()) continue;
        for (size_t i = 0; i < sz; i++) {
          sess[i] += tl.c_sess[i];
          purch[i] += tl.c_purch[i];
          assist[i] += tl.c_assist[i];
          rev[i] += tl.c_rev[i];
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
      int64_t n = 0;
      const std::string org = "organic";
      for (u32 c = 0; c <= NC; c++)
        for (u32 d = 0; d < ND; d++) {
          size_t i = (size_t)c * ND + d;
          if (sess[i] == 0 && assist[i] == 0) continue;
          const std::string& id = (c == NC) ? org : g_camp_ids[c];
          const std::string& ch = (c == NC) ? org : g_camp_channel[c];
          pq::put_str(cols[0].values, id.data(), (u32)id.size());
          pq::put_str(cols[1].values, ch.data(), (u32)ch.size());
          pq::put_i32(cols[2].values, (i32)(g_day0 + d));
          pq::put_i64(cols[3].values, (i64)sess[i]);
          pq::put_i64(cols[4].values, (i64)purch[i]);
          pq::put_i64(cols[5].values, rev[i]);
          pq::put_i64(cols[6].values, (i64)assist[i]);
          n++;
        }
      if (!pq::write_file(cd + "/part-0.parquet", cols, n)) fail = true;
      return;
    }
    OutRows& o = g_tl[t].out;
    size_t n = o.key.size();
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
    {
      std::vector<u8>& b = cols[0].values.d;
      b.resize(n * 14 + 64);
      size_t at = 0;
      for (size_t i = 0; i < n; i++) put_key(b, at, o.key[i]);
      b.resize(at);
    }
    auto raw = [](pq::Buf& b, const void* p, size_t bytes) {
      b.d.resize(bytes);
      if (bytes) memcpy(b.d.data(), p, bytes);
    };
    raw(cols[1].values, o.day.data(), n * 4);
    raw(cols[2].values, o.sess.data(), n * 4);
    raw(cols[3].values, o.evs.data(), n * 4);
    raw(cols[4].values, o.rev.data(), n * 8);
    cols[5].defs.resize(n);
    cols[5].indices.reserve(n / 3 + 16);
    for (size_t i = 0; i < n; i++) {
      if (o.top[i] != 0xFFFF) {
        cols[5].defs[i] = 1;
        cols[5].indices.push_back(o.top[i]);
      } else {
        cols[5].defs[i] = 0;
        cols[5].num_nulls++;
      }
    }
    std::string path = ud + "/part-" + std::to_string(t) + ".parquet";
    if (!pq::write_file(path, cols, (int64_t)n)) fail = true;
  });
  if (fail) die("writing output");
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
  return bad ? 1 : 0;
}

// ------------------------------------------------------------------------------------------

int main(int argc, char** argv) {
  g_t0 = now_s();
  if (argc >= 2 && strcmp(argv[1], "--selftest") == 0) return selftest();
  const char* lg = getenv("SS_LOG");
  g_log = lg && *lg && *lg != '0';
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

  load_lookups(data);
  LOG("lookups: window %lld..%lld, %zu categories, %u campaigns, %d days", (long long)g_ws, (long long)g_we,
      g_cats.size(), g_ncamp, g_ndays);

  std::vector<std::string> evp = list_csv(data + "/events"), ckp = list_csv(data + "/clicks"),
                           lkp = list_csv(data + "/links");
  std::vector<MFile> ev, ck, lk;
  for (auto& p : evp) ev.push_back(map_file(p));
  for (auto& p : ckp) ck.push_back(map_file(p));
  for (auto& p : lkp) lk.push_back(map_file(p));
  size_t ev_bytes = 0;
  for (auto& f : ev) ev_bytes += f.size;
  u64 est_lines = ev_bytes / 70 + 1;
  g_P1bits = std::min<u32>(12, std::max<u32>(4, ceil_log2(est_lines / 16384 + 1)));
  g_P2bits = std::min<u32>(12, std::max<u32>(4, ceil_log2(est_lines / 32768 + 1)));
  g_tl.resize(NT);
  for (auto& t : g_tl) {
    t.r1.resize(1u << g_P1bits);
    t.r2.resize(1u << g_P2bits);
    t.r2c.resize(1u << g_P2bits);
  }
  LOG("mapped %zu event files (%zu bytes), %zu click files, %zu link files; P1=%u P2=%u threads=%d", ev.size(),
      ev_bytes, ck.size(), lk.size(), 1u << g_P1bits, 1u << g_P2bits, NT);

  build_links(lk);
  LOG("links done");
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
  dedup_and_route();
  LOG("dedup + routing done");
  if (getenv("SS_STOP") && !strcmp(getenv("SS_STOP"), "dedup")) return 0;
  person_phase();
  LOG("person phase done");
  write_outputs(out);
  LOG("output written");
  return 0;
}
