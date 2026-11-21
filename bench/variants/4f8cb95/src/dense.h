// The dense path: a faster route through the same rules for datasets whose IDs have the shape the
// organisers' generator gives them. Included by main.cpp; everything here is checked against the data, and
// whenever the data does not fit, run() reports failure and the general pipeline redoes the whole job.
//
// What it relies on (all of it verified at run time, none of it assumed):
//   * IDs are a prefix letter and a base-36 number below 2^40. The generator makes that number from a running
//     index with a fixed, invertible mixing function and one secret key per kind of ID. The keys are found
//     from a sample of the data (a few million trial keys at most); undoing the mixing then turns every
//     regular ID back into its index. Any key gives a bijection, so nothing depends on having "the" key: a
//     wrong one only makes the indexes sparse, and the general pipeline takes over.
//   * Event indexes are dense and run in the order (user, time). So events are partitioned by index range,
//     and a partition is put in order by writing each record to the slot of its index: that is the whole
//     dedup (the copy with the smallest ingest_ts keeps the slot) and the whole sort. A person's events then
//     follow one another in time order, and sessions fall out of one sequential pass.
//   * A user number is (person index << 9) + alias. Links normally join exactly the aliases 0..n-1 of one
//     index; the link phase works out, per person index, whether that is so. Persons for whom it is
//     ("dense" persons) are named and grouped by index alone. Everything else (IDs of any other form, link
//     sets that mix indexes or contain other IDs, aliases outside the set) goes through the general person
//     phase, record by record.
//
// Checks that send the whole job back to the general pipeline: a dense person's events do not arrive in one
// run of increasing time (the runs of all dense persons must be in strictly increasing index order, which
// also proves no person has two runs), an ingest_ts that is not a whole second near the window, or too many
// irregular lines.
namespace dn {

static constexpr u32 NONE = 0xFFFFFFFFu;
static constexpr u64 M40 = (1ULL << 40) - 1;
static constexpr u64 SC1 = 0x9E3779B97F4BULL, SC2 = 0xD6E8FEB86659ULL;
static constexpr u64 SC1I = inv_odd(SC1) & M40, SC2I = inv_odd(SC2) & M40;
static_assert(((SC1 * SC1I) & M40) == 1 && ((SC2 * SC2I) & M40) == 1, "inverse multipliers");

// The generator's mixing of an index into an ID number (both below 2^40), and its inverse.
static inline u64 scramble(u64 v, u64 key) {
  u64 m = (((v & M40) * SC1) ^ key) & M40;
  m ^= m >> 17;
  return (m * SC2) & M40;
}
static FORCE_INLINE u64 unmix(u64 v) {  // = (index * SC1 ^ key) & M40
  u64 m = (v * SC2I) & M40;
  m ^= m >> 17;
  m ^= m >> 34;
  return m;
}
static FORCE_INLINE u64 unscramble(u64 v, u64 key) { return ((unmix(v) ^ key) * SC1I) & M40; }

static constexpr u64 NOT36 = ~0ULL;
// The number written as n lowercase base-36 digits at s, as the generator writes it (no leading zero, below
// 2^40), or NOT36.
static inline u64 b36_scalar(const u8* s, u32 n) {
  if (n < 1 || n > 8 || (n > 1 && s[0] == '0')) return NOT36;
  u64 v = 0;
  for (u32 i = 0; i < n; i++) {
    const u32 c = s[i];
    u32 d;
    if (c - '0' < 10)
      d = c - '0';
    else if (c - 'a' < 26)
      d = c - 'a' + 10;
    else
      return NOT36;
    v = v * 36 + d;
  }
  return v <= M40 ? v : NOT36;
}
static inline u32 b36_write(u64 v, u8* out) {  // the digits of v; returns how many (at most 13)
  u8 buf[13];
  u32 at = 13;
  do {
    buf[--at] = (u8) "0123456789abcdefghijklmnopqrstuvwxyz"[v % 36];
    v /= 36;
  } while (v);
  memcpy(out, buf + at, 13 - at);
  return 13 - at;
}
// The key (as make_key gives it) of the ID with this prefix and number (below 36^8: at most 8 digits).
static inline u64 id_key_ref(u8 prefix, u64 v) {
  u8 b[24] = {0};
  b[0] = prefix;
  const u32 n = 1 + b36_write(v, b + 1);
  return key_fast(b, n);
}
static FORCE_INLINE u64 id_key(u8 prefix, u64 v) {
  const u32 hi = (u32)(v / 1679616), lo = (u32)(v % 1679616);  // four digits each
  const u32 a1 = hi / 1296, a0 = hi % 1296, b1 = lo / 1296, b0 = lo % 1296;
  // the eight digits as bytes, the most significant in the lowest byte
  const u64 d = (u64)(a1 / 36) | ((u64)(a1 % 36) << 8) | ((u64)(a0 / 36) << 16) | ((u64)(a0 % 36) << 24) |
                ((u64)(b1 / 36) << 32) | ((u64)(b1 % 36) << 40) | ((u64)(b0 / 36) << 48) | ((u64)(b0 % 36) << 56);
  // digit -> character: '0' + d, and 39 more from 10 on
  const u64 c = d + 0x3030303030303030ULL + ((((d + 0x7676767676767676ULL) & 0x8080808080808080ULL) >> 7) * 0x27);
  const u32 z = d ? ((u32)_tzcnt_u64(d) >> 3) : 7;  // leading zero digits to drop (all but the last)
  const u64 w = c >> (8 * z);                       // the digits as the ID has them: bytes 1.. of the string
  return ((u64)prefix << 56) | _pext_u64(__builtin_bswap64(w), KEY_M7);
}

// The digits of two IDs at once: a has na of them, b has nb (1..8 each, 8 readable bytes at both). va and vb
// get the numbers, with no check of their range or of leading zeros; the result is 0xFFFF if every byte is a
// base-36 digit (the low 8 bits answer for a, the high 8 for b).
static FORCE_INLINE u32 b36_pair(const u8* a, u32 na, const u8* b, u32 nb, u64& va, u64& vb) {
  constexpr u64 Z = 0x3030303030303030ULL;
  u64 xa, xb;
  memcpy(&xa, a, 8);
  memcpy(&xb, b, 8);
  // right-aligned in 8 bytes, most significant digit first, '0' in front
  xa = (xa << (64 - 8 * na)) | _bzhi_u64(Z, 64 - 8 * na);
  xb = (xb << (64 - 8 * nb)) | _bzhi_u64(Z, 64 - 8 * nb);
  const __m128i v = _mm_set_epi64x((long long)xb, (long long)xa);
  const __m128i letters = _mm_cmpgt_epi8(v, _mm_set1_epi8(0x60));
  const __m128i d = _mm_sub_epi8(v, _mm_add_epi8(_mm_set1_epi8(0x30), _mm_and_si128(letters, _mm_set1_epi8(0x27))));
  const __m128i lim = _mm_add_epi8(_mm_set1_epi8(9), _mm_and_si128(letters, _mm_set1_epi8(26)));
  const u32 ok = (u32)_mm_movemask_epi8(_mm_cmpeq_epi8(_mm_max_epu8(d, lim), lim));
  const __m128i m1 = _mm_maddubs_epi16(d, _mm_set1_epi16(0x0124));       // digit pairs: 36 * hi + lo
  const __m128i m2 = _mm_madd_epi16(m1, _mm_set1_epi32(0x00010510));    // four digits: 1296 * hi + lo
  const __m128i q = _mm_add_epi64(_mm_mul_epu32(m2, _mm_set1_epi64x(1679616)), _mm_srli_epi64(m2, 32));
  va = (u64)_mm_cvtsi128_si64(q);
  vb = (u64)_mm_extract_epi64(q, 1);
  return ok;
}

// ------------------------------------------------------------------------------------------
// state

static bool g_on = false;
static std::atomic<bool> g_fail{false};
static const char* g_fail_why = "";
static NOINLINE void fail(const char* why) {
  if (!g_fail.exchange(true)) g_fail_why = why;
}

static u64 g_ukey = 0, g_ekey = 0;
static u64 g_ne_cap = 0;  // regular event indexes are below this
static u32 g_nu_cap = 0;  // regular person indexes are below this (at most 2^23, so a user number fits 32 bits)
static constexpr u32 KE = 14, KC = 9;  // event indexes per event partition, person indexes per click partition
static constexpr u32 ESLOTS = 1u << KE, ELOM = ESLOTS - 1;
static u32 g_PE = 0, g_PC = 0;
static i64 g_ing_base = 0;  // ingest_ts is kept as whole seconds since this, plus 1, in 24 bits
static constexpr u64 ING_SPAN = 0xFFFFFEULL * 1000;
static constexpr u32 ING_NOCODE = 0xFFFFFF;  // an ingest_ts that does not parse: later than all others

// Per person index, filled in by the link phase. A linked ID x "counts" for its index if x is a regular user
// ID and the name of its link set (the set's smallest ID) is a regular user ID of the same index:
//   bits 0..9    how many IDs count
//   bits 10..27  the sum of their aliases
//   bits 28..37  how many of them are the name of their set (so: how many sets they are in)
//   bit 46       alias 0 counts
//   bit 63       some ID of this index is in a set with an ID that is not a regular ID of this index
//   bit 62       the person has events or clicks kept outside the partitions (g_extra)
static constexpr u64 ACC_POISON = 1ULL << 63, ACC_EXTRA = 1ULL << 62, ACC_HAS0 = 1ULL << 46;

// How many aliases the dense person of an index has: its IDs are exactly the aliases below this, and nothing
// else is linked to them. 1 if alias 0 is in no link. 0: the index has no dense person (every record of its
// IDs goes the general way).
//   Not poisoned means every set that holds an ID of this index holds regular IDs of this index only, so all
//   their members count. If they are in one set (one of them is a name), alias 0 is among them, and their n
//   aliases sum to 0 + 1 + .. + n - 1, which only the aliases 0..n-1 do, then that set is exactly those aliases.
static FORCE_INLINE u32 dense_cnt(u64 a) {
  if (__builtin_expect((a & ~ACC_EXTRA) == 0, 1)) return 1;
  if ((i64)a < 0) return 0;
  if (!(a & ACC_HAS0)) return 1;
  const u32 n = (u32)a & 0x3FF, sa = (u32)(a >> 10) & 0x3FFFF, sets = (u32)(a >> 28) & 0x3FF;
  return (sets == 1 && sa * 2 == n * (n - 1)) ? n : 0;
}

// (person index << 9 | alias) of the link ID whose key has hash h, or NONE if it is not a regular user ID.
static FORCE_INLINE u32 reg_user_of_hash(u64 h) {
  const u64 k = unhash_id(h);
  const u64 w = __builtin_bswap64(_pdep_u64(k, KEY_M7));  // the bytes after the first, zero padded
  if ((k >> 56) != 'u' || w == 0) return NONE;
  const u32 n = 8 - ((u32)__builtin_clzll(w) >> 3);  // digits
  u64 va, vb;
  const u32 ok = b36_pair((const u8*)&w, n, (const u8*)&w, n, va, vb);
  if (ok != 0xFFFF || (((u8)w == '0') & (n > 1)) || va > M40) return NONE;
  const u64 num = unscramble(va, g_ukey);
  return (num >> 9) < g_nu_cap ? (u32)num : NONE;
}

// Called by the link phase for every linked ID: x and r are the user numbers (or NONE) of the ID and of the
// name of its set. (An ID is the name of its set exactly when the two are equal.)
static FORCE_INLINE void link_member_num(u32 x, u32 r) {
  if (__builtin_expect(x != NONE && r != NONE && (x >> 9) == (r >> 9), 1)) {
    const u64 a = x & 511;
    g_acc[x >> 9].fetch_add(1 | (a << 10) | ((u64)(x == r) << 28) | (a == 0 ? ACC_HAS0 : 0), std::memory_order_relaxed);
  } else {
    if (x != NONE) g_acc[x >> 9].fetch_or(ACC_POISON, std::memory_order_relaxed);
    if (r != NONE) g_acc[r >> 9].fetch_or(ACC_POISON, std::memory_order_relaxed);
  }
}
static inline void link_member(u64 hx, u64 hr) {
  const u32 x = reg_user_of_hash(hx);
  link_member_num(x, hx == hr ? x : reg_user_of_hash(hr));
}

static inline u64 person_of(u64 h) { return g_H ? h_lookup(h) : h; }

// Events and clicks of dense persons that are not in the partitions: events whose event_id is not regular,
// clicks whose click_id is not. Built after the parse, read-only afterwards.
struct Extra {
  std::vector<u64> ev;                 // ts | payload << 32 (payload as in R2)
  std::vector<std::array<u64, 2>> ck;  // ts | camp << 32, index of the click key in g_extra_keys
};
static std::unordered_map<u32, Extra> g_extra;
static constexpr u64 CLICK_IS_KEY = 1ULL << 63;  // a click number that is an index into g_extra_keys instead
static std::vector<u64> g_extra_keys;
static inline u64 click_key_of(u64 c) { return (c & CLICK_IS_KEY) ? g_extra_keys[c & ~CLICK_IS_KEY] : id_key('k', c); }
static inline bool click_less(u64 a, u64 b) { return key_cmp(click_key_of(a), click_key_of(b)) < 0; }

// ------------------------------------------------------------------------------------------
// keys from a sample

struct Samp {
  u64 a, b;  // numbers of the first two fields' IDs
};
// The first max_lines lines of the files (headers aside): those that are well formed and whose first two
// fields are regular IDs with the prefixes pa and pb.
template <int NF>
static void sample(const std::vector<MFile>& files, u8 pa, u8 pb, size_t max_lines, std::vector<Samp>& out) {
  size_t lines = 0;
  for (const MFile& f : files) {
    const u8* p = f.data;
    const u8* const e = f.data + f.size;
    const u8* q = (const u8*)memchr(p, '\n', (size_t)(e - p));
    if (!q) continue;
    p = q + 1;
    while (p < e && lines < max_lines) {
      q = (const u8*)memchr(p, '\n', (size_t)(e - p));
      if (!q) q = e;
      const u8* fs[8];
      u32 fl[8];
      if (q > p && scan::slow_line<NF>(p, q, fs, fl) && fl[0] >= 2 && fl[1] >= 2 && fs[0][0] == pa && fs[1][0] == pb) {
        const u64 a = b36_scalar(fs[0] + 1, fl[0] - 1), b = b36_scalar(fs[1] + 1, fl[1] - 1);
        if (a != NOT36 && b != NOT36) out.push_back({a, b});
      }
      lines++;
      p = q + 1;
    }
    if (lines >= max_lines) break;
  }
}

// The generator's five busiest users have the person indexes 0..4 and a handful of aliases each, and between
// them a good share of all lines. Assume a sampled user ID is one of theirs: that gives a key; the right one
// turns most of the sample into small numbers.
static bool recover_user_key(const std::vector<Samp>& s) {
  const size_t nt = std::min<size_t>(s.size(), 64);
  if (nt < 8) return false;
  for (size_t i = 0; i < std::min<size_t>(s.size(), 512); i++) {
    const u64 m = unmix(s[i].b);
    for (u32 w = 0; w < 5; w++)
      for (u32 a = 0; a < 8; a++) {
        const u64 key = m ^ (((u64)(w * 512 + a) * SC1) & M40);
        size_t ok = 0;
        for (size_t j = 0; j < nt; j++) {
          const u64 n = unscramble(s[j].b, key);
          ok += ((n >> 9) < g_nu_cap) & ((n & 511) < 8);
        }
        if (ok * 10 >= nt * 6) {
          g_ukey = key;
          return true;
        }
      }
  }
  return false;
}

// Person index 0 has the first event indexes, at most 2% of them all. Take one of its sampled events, try
// every index it could have, and keep the key that puts its other sampled events in that range too.
static bool recover_event_key(const std::vector<Samp>& s) {
  std::vector<u64> w0;
  for (const Samp& x : s)
    if ((unscramble(x.b, g_ukey) >> 9) == 0) w0.push_back(x.a);
  std::sort(w0.begin(), w0.end());
  w0.erase(std::unique(w0.begin(), w0.end()), w0.end());
  if (w0.size() < 5) return false;
  const u64 bound = g_ne_cap / 40;
  const u64 m = unmix(w0[0]), m1 = unmix(w0[1]);
  const size_t nchk = std::min<size_t>(w0.size(), 12);
  u64 x = 0;  // j * SC1 mod 2^40
  for (u64 j = 0; j < bound; j++, x = (x + SC1) & M40) {
    const u64 key = m ^ x;
    if ((((m1 ^ key) * SC1I) & M40) >= bound) continue;
    bool all = true;
    for (size_t t = 2; t < nchk; t++)
      if (unscramble(w0[t], key) >= bound) {
        all = false;
        break;
      }
    if (all) {
      g_ekey = key;
      return true;
    }
  }
  return false;
}

// ------------------------------------------------------------------------------------------
// parse

// Four 64-bit lanes at a time, with AVX-512 mask compares and 64-bit multiplies where the build has them
// (the scorer's processors do) and plain AVX2 otherwise.
#if defined(__AVX512VL__) && defined(__AVX512DQ__) && defined(__AVX512F__)
static FORCE_INLINE u32 nz64x4(__m256i a) { return _mm256_test_epi64_mask(a, a); }                 // lanes that are not 0
static FORCE_INLINE u32 gtu64x4(__m256i a, __m256i b) { return _mm256_cmpgt_epu64_mask(a, b); }  // a > b, unsigned
static FORCE_INLINE __m256i mullo64x4(__m256i a, __m256i b) { return _mm256_mullo_epi64(a, b); }
static FORCE_INLINE __m128i low32x4(__m256i a) { return _mm256_cvtepi64_epi32(a); }
#else
static FORCE_INLINE u32 nz64x4(__m256i a) {
  return 0xF ^ (u32)_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(a, _mm256_setzero_si256())));
}
static FORCE_INLINE u32 gtu64x4(__m256i a, __m256i b) {
  const __m256i sg = _mm256_set1_epi64x(INT64_MIN);
  return (u32)_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(_mm256_xor_si256(a, sg), _mm256_xor_si256(b, sg))));
}
static FORCE_INLINE __m256i mullo64x4(__m256i a, __m256i b) {
  const __m256i cross = _mm256_add_epi64(_mm256_mul_epu32(_mm256_srli_epi64(a, 32), b), _mm256_mul_epu32(a, _mm256_srli_epi64(b, 32)));
  return _mm256_add_epi64(_mm256_mul_epu32(a, b), _mm256_slli_epi64(cross, 32));
}
static FORCE_INLINE __m128i low32x4(__m256i a) {
  return _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(a, _mm256_setr_epi32(0, 2, 4, 6, 0, 0, 0, 0)));
}
#endif

// Four IDs at once. raw: the 8 bytes after each prefix; sh: 72 - 8 * (field length), which is how far the
// digits have to move to sit right-aligned in their 8 bytes (0..56 for 1..8 digits). val gets the numbers.
// The result has a bit for every ID that is not 1..8 base-36 digits without a leading zero.
static FORCE_INLINE u32 b36x4(const __m256i raw, const __m256i sh, __m256i& val) {
  const __m256i x = _mm256_or_si256(_mm256_sllv_epi64(raw, sh),
                                    _mm256_andnot_si256(_mm256_sllv_epi64(_mm256_set1_epi64x(-1), sh), _mm256_set1_epi8('0')));
  const __m256i letters = _mm256_cmpgt_epi8(x, _mm256_set1_epi8(0x60));
  const __m256i d = _mm256_sub_epi8(x, _mm256_add_epi8(_mm256_set1_epi8(0x30), _mm256_and_si256(letters, _mm256_set1_epi8(0x27))));
  const __m256i lim = _mm256_add_epi8(_mm256_set1_epi8(9), _mm256_and_si256(letters, _mm256_set1_epi8(26)));
  const __m256i over = _mm256_subs_epu8(d, lim);                                               // a byte that is no digit
  const __m256i lead = _mm256_and_si256(_mm256_srlv_epi64(d, sh), _mm256_set1_epi64x(0xFF));  // the first digit
  const __m256i one = _mm256_set1_epi64x(56);  // the shift of a single digit
  const u32 bad = nz64x4(over) | ((0xF ^ nz64x4(lead)) & nz64x4(_mm256_xor_si256(sh, one))) | gtu64x4(sh, one);
  const __m256i m1 = _mm256_maddubs_epi16(d, _mm256_set1_epi16(0x0124));     // digit pairs: 36 * hi + lo
  const __m256i m2 = _mm256_madd_epi16(m1, _mm256_set1_epi32(0x00010510));  // four digits: 1296 * hi + lo
  val = _mm256_add_epi64(_mm256_mul_epu32(m2, _mm256_set1_epi64x(1679616)), _mm256_srli_epi64(m2, 32));
  return bad;
}
static FORCE_INLINE __m256i unscramble4(const __m256i v, const __m256i key) {
  const __m256i M = _mm256_set1_epi64x((long long)M40);
  __m256i m = _mm256_and_si256(mullo64x4(v, _mm256_set1_epi64x((long long)SC2I)), M);
  m = _mm256_xor_si256(m, _mm256_srli_epi64(m, 17));
  m = _mm256_xor_si256(m, _mm256_srli_epi64(m, 34));
  return _mm256_and_si256(mullo64x4(_mm256_xor_si256(m, key), _mm256_set1_epi64x((long long)SC1I)), M);
}

// An ingest_ts in ms as a code: whole seconds since the base, plus 1 (any i64; the caller checks the range).
static constexpr i64 ICODE_NONE = INT64_MAX;  // not a timestamp
static NOINLINE i64 icode_of_ms(i64 ing) {
  if (ing == ING_NONE) return ICODE_NONE;
  const i64 d = ing - g_ing_base;
  if (d % 1000 != 0) {
    fail("ingest_ts is not a whole second");
    return ICODE_NONE;
  }
  return d / 1000 + 1;
}
static void init_daycode() {
  for (int k = 0; k < 2; k++)
    for (int i = 0; i < 416; i++) g_daycode[k][i] = g_dayms[k][i] == DAYMS_NONE ? 0 : (g_dayms[k][i] - g_ing_base) / 1000 + 1;
}

// Eight 32-bit lanes at a time (mask compares and compress-store with AVX-512, plain AVX2 otherwise).
#if defined(__AVX512VL__) && defined(__AVX512DQ__) && defined(__AVX512F__)
static FORCE_INLINE __m256i movm32x8(u32 m) { return _mm256_movm_epi32((__mmask8)m); }  // -1 in the lanes of set bits
static FORCE_INLINE u32 eq32x8(__m256i a, __m256i b) { return _mm256_cmpeq_epi32_mask(a, b); }
// the indexes first + j of the set bits j of m, appended at dst (which has room for 8); returns how many
static FORCE_INLINE u32 list8(u32* dst, u32 m, u32 first) {
  _mm256_mask_compressstoreu_epi32(dst, (__mmask8)m, _mm256_add_epi32(_mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7), _mm256_set1_epi32((int)first)));
  return (u32)_mm_popcnt_u32(m);
}
#else
static FORCE_INLINE __m256i movm32x8(u32 m) {
  const __m256i bits = _mm256_setr_epi32(1, 2, 4, 8, 16, 32, 64, 128);
  return _mm256_cmpeq_epi32(_mm256_and_si256(_mm256_set1_epi32((int)m), bits), bits);
}
static FORCE_INLINE u32 eq32x8(__m256i a, __m256i b) { return (u32)_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(a, b))); }
static FORCE_INLINE u32 list8(u32* dst, u32 m, u32 first) {
  u32 c = 0;
  for (; m; m = _blsr_u32(m)) dst[c++] = first + (u32)_tzcnt_u32(m);
  return c;
}
#endif

// A batch of lines as the scanner hands them over: per field, a column of starts (offsets from the chunk
// base) and a column of lengths. Groups of eight lines arrive as columns of separators (scan_cols); single
// lines as two vectors of eight values.
template <int NF, u32 CAPN>
struct Cols {
  static constexpr u32 CAP = CAPN;  // lines; a batch is processed once it has more than CAP - 8
  alignas(32) u32 st[NF][CAP + 8];
  alignas(32) u32 ln[NF][CAP + 8];
  u32 n = 0;
  FORCE_INLINE void add_group(const __m256i* C, const u32* q) {
    const __m256i one = _mm256_set1_epi32(1);
    for (int k = 0; k < NF; k++) {
      const __m256i qm = movm32x8(q[k]);  // -1: quoted
      _mm256_storeu_si256((__m256i*)(st[k] + n), _mm256_sub_epi32(_mm256_add_epi32(C[k], one), qm));
      _mm256_storeu_si256((__m256i*)(ln[k] + n),
                          _mm256_add_epi32(_mm256_sub_epi32(_mm256_sub_epi32(C[k + 1], C[k]), one), _mm256_add_epi32(qm, qm)));
    }
    n += 8;
  }
  FORCE_INLINE void add_row(__m256i s, __m256i l, u32 keep) {  // (the row stays only if keep is 1)
    alignas(32) u32 a[8], b[8];
    _mm256_store_si256((__m256i*)a, s);
    _mm256_store_si256((__m256i*)b, l);
    for (int k = 0; k < NF; k++) {
      st[k][n] = a[k];
      ln[k][n] = b[k];
    }
    n += keep;
  }
  // the lanes after the last line read as empty fields at offset 0 (they are loaded, and gathered from)
  void pad() {
    for (int k = 0; k < NF; k++) {
      _mm256_storeu_si256((__m256i*)(st[k] + n), _mm256_setzero_si256());
      _mm256_storeu_si256((__m256i*)(ln[k] + n), _mm256_setzero_si256());
    }
  }
};

// Event lines -> D16 records by event index.
struct EvB : Cols<7, 64> {
  alignas(32) u64 eq[CAP + 8], uq[CAP + 8];  // the 8 bytes after the prefix of event_id / user_id
  alignas(32) u32 w4[CAP + 8];               // the first 4 bytes of event_type
  alignas(32) u8 pfx[CAP + 8];               // nonzero: a prefix byte is not the expected one
  alignas(32) u64 idx[CAP + 8];    // event index
  alignas(32) u32 num[CAP + 8];    // user number
  alignas(32) i64 ts[CAP + 8];     // ms, TS_NONE if not a timestamp
  alignas(32) i64 icode[CAP + 8];  // ingest_ts as a code (see icode_of_ms)
  alignas(32) D16 rec[CAP + 8];    // the record of a plain line
  alignas(32) u32 part[CAP + 8];   // ... and its partition
  u32 lst[4][CAP + 8];             // lines by form of ts: 20 bytes, 13 bytes, 25 bytes, other
};

static NOINLINE void ev_ts_exact(EvB& B, u32 i, const u8* base) {
  if (!parse_ts(base + B.st[2][i], B.ln[2][i], B.ts[i])) B.ts[i] = TS_NONE;
  i64 ing;
  if (!parse_f1(base + B.st[3][i], B.ln[3][i], ing)) ing = ING_NONE;
  B.icode[i] = icode_of_ms(ing);
}

// A line that is not a valid view or cart event of regular IDs: everything about it once more, one step at
// a time. Lines with an event_id that is not regular are kept aside whole (with the hashes the general
// pipeline uses); a valid event whose user or payload does not fit a record gets a side entry.
static NOINLINE void ev_slow(TL& tl, int tid, EvB& B, u32 i, bool regular, const u8* base, const i64 ws, const u64 wlen,
                             WCScatter<D16>::Cursor& cur) {
  const u8* const s0 = base + B.st[0][i];
  const u8* const s1 = base + B.st[1][i];
  const u32 l0 = B.ln[0][i], l1 = B.ln[1][i];
  u64 idx = B.idx[i], num = B.num[i];
  bool e_reg = regular, u_reg = regular;  // (regular: both IDs are, and the batch has their numbers)
  if (!regular) {
    idx = num = 0;
    if (l0 >= 2 && s0[0] == 'e') {
      const u64 v = b36_scalar(s0 + 1, l0 - 1);
      if (v != NOT36) {
        idx = unscramble(v, g_ekey);
        e_reg = idx < g_ne_cap;
      }
    }
    if (l1 >= 2 && s1[0] == 'u') {
      const u64 v = b36_scalar(s1 + 1, l1 - 1);
      if (v != NOT36) {
        num = unscramble(v, g_ukey);
        u_reg = (num >> 9) < g_nu_cap;
      }
    }
  }
  const i64 ts = B.ts[i], ic = B.icode[i];
  const u64 rel = (u64)ts - (u64)ws;
  u32 code = ING_NOCODE;
  bool late = true;  // an ingest_ts that is no timestamp counts as late, as in the general pipeline
  if (ic != ICODE_NONE) {
    if ((u64)(ic - 1) >= 0xFFFFFEULL) {
      fail("ingest_ts far from the window");
    } else {
      code = (u32)ic;
      late = g_ing_base + (ic - 1) * 1000 - ts > DAY;
    }
  }
  bool valid = (rel < wlen) & !late & (l1 != 0) & (B.ln[5][i] != 0);
  u32 payload = 0;
  const int et = event_type(base + B.st[4][i], B.ln[4][i]);
  if (et < 0) valid = false;
  if (et == 2) {
    u32 price;
    if (valid && parse_price(base + B.st[6][i], B.ln[6][i], price))
      payload = (cat_of(base + B.st[5][i], B.ln[5][i]) << 24) | price;
    else
      valid = false;
  }
  const u64 uid = l1 ? hash_id(make_key(s1, l1)) : 0;
  if (!e_reg) {
    // (among these lines the ingest code orders the copies as well as the timestamp would)
    tl.x1.push_back(X1Rec{hash_id(make_key(s0, l0)), ic, uid, valid ? (u32)rel : TS_INVALID, payload, u_reg ? (u32)num : NONE});
    if (tl.x1.size() > (1u << 22)) fail("too many irregular event ids");
    return;
  }
  const bool fits = ((payload >> 24) < 32) & ((payload & 0xFFFFFF) < (1u << 17));
  u64 w0, w1 = (u64)code | ((idx & ELOM) << 46);
  if (!valid) {
    w0 = 0;
  } else if (u_reg && fits) {
    w0 = (u64)((u32)rel + 1) | (num << 32);
    w1 |= (u64)(((payload >> 24) << 17) | (payload & 0x1FFFF)) << 24;
  } else {
    const u32 sidx = (u32)tl.xev.size();
    if (sidx >= (1u << 22)) {
      fail("too many irregular events");
      return;
    }
    tl.xev.push_back(XEv{uid, payload, u_reg ? (u32)num : NONE});
    w0 = (u64)((u32)rel + 1) | 0x80000000ULL | ((u64)(((u32)tid << 24) | sidx) << 32);
  }
  WCScatter<D16>::push16(cur, (u32)(idx >> KE), _mm_set_epi64x((long long)w1, (long long)w0));
}

// The IDs of four lines, from the 8 bytes after their prefix and their field lengths. val gets the numbers;
// the result has a bit per line whose ID is not 1..8 base-36 digits without a leading zero (the prefix byte
// is checked separately).
// (The bytes are fetched with plain loads, one line at a time, not with gather instructions: on the scorer's
// processors those are slow.)
static FORCE_INLINE u32 ids4(const u64* raw, const u32* ln, __m256i& val) {
  const __m256i sh = _mm256_sub_epi64(_mm256_set1_epi64x(72), _mm256_slli_epi64(_mm256_cvtepu32_epi64(_mm_load_si128((const __m128i*)ln)), 3));
  return b36x4(_mm256_load_si256((const __m256i*)raw), sh, val);
}
// The lines (a bit each) whose byte in f is not 0; f has 64 bytes.
static FORCE_INLINE u64 nonzero64(const u8* f) {
  const __m256i z = _mm256_setzero_si256();
  const u32 a = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_load_si256((const __m256i*)f), z));
  const u32 b = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(_mm256_load_si256((const __m256i*)(f + 32)), z));
  return ~((u64)a | ((u64)b << 32));
}

// One batch. Returns the number of its lines that have an event_id.
static NOINLINE u32 process_events(EvB& B, const u8* const base, WCScatter<D16>::Cursor& cur_out, TL& tl, int tid, const i64 ws,
                                   const i64 we) {
  const u32 n = B.n;
  B.pad();
  const u64 nmask = n >= 64 ? ~0ULL : ((1ULL << n) - 1);
  // ---- the bytes the vector code needs, fetched line by line
  for (u32 i = 0; i < n; i++) {
    const u8* const s0 = base + B.st[0][i];
    const u8* const s1 = base + B.st[1][i];
    memcpy(&B.eq[i], s0 + 1, 8);
    memcpy(&B.uq[i], s1 + 1, 8);
    B.pfx[i] = (u8)((s0[0] ^ 'e') | (s1[0] ^ 'u'));
    memcpy(&B.w4[i], base + B.st[4][i], 4);
  }
  // ---- IDs -> event index, user number; four lines at a time
  u64 irr = nonzero64(B.pfx);  // lines with an ID that is not regular
  {
    const __m256i ekey = _mm256_set1_epi64x((long long)g_ekey), ukey = _mm256_set1_epi64x((long long)g_ukey);
    const __m256i M = _mm256_set1_epi64x((long long)M40);
    // (the largest regular event index and user number: the caps are not 0)
    const __m256i emax = _mm256_set1_epi64x((long long)(g_ne_cap - 1)), umax = _mm256_set1_epi64x((long long)(((u64)g_nu_cap << 9) - 1));
    for (u32 i = 0; i < n; i += 4) {
      __m256i ve, vu;
      u32 be = ids4(B.eq + i, B.ln[0] + i, ve);
      u32 bu = ids4(B.uq + i, B.ln[1] + i, vu);
      const __m256i idx = unscramble4(ve, ekey), num = unscramble4(vu, ukey);
      be |= gtu64x4(ve, M) | gtu64x4(idx, emax);
      bu |= gtu64x4(vu, M) | gtu64x4(num, umax);
      _mm256_store_si256((__m256i*)(B.idx + i), idx);
      _mm_store_si128((__m128i*)(B.num + i), low32x4(num));
      irr |= (u64)(be | bu) << i;
    }
  }
  // ---- timestamps: the lines by the form of ts (ingest_ts has to be 20 bytes for the fast forms), then one
  // kernel per form, as in the general pipeline; the ingest_ts also as its code
  u32 cnt[4] = {0, 0, 0, 0};
  {
    const __m256i c20 = _mm256_set1_epi32(20), c13 = _mm256_set1_epi32(13), c25 = _mm256_set1_epi32(25);
    for (u32 i = 0; i < n; i += 8) {
      const __m256i l2 = _mm256_load_si256((const __m256i*)(B.ln[2] + i)), l3 = _mm256_load_si256((const __m256i*)(B.ln[3] + i));
      const u32 live = (u32)(nmask >> i) & 0xFF, ing = eq32x8(l3, c20);
      const u32 m0 = eq32x8(l2, c20) & ing & live, m1 = eq32x8(l2, c13) & ing & live, m2 = eq32x8(l2, c25) & ing & live;
      cnt[0] += list8(B.lst[0] + cnt[0], m0, i);
      cnt[1] += list8(B.lst[1] + cnt[1], m1, i);
      cnt[2] += list8(B.lst[2] + cnt[2], m2, i);
      cnt[3] += list8(B.lst[3] + cnt[3], live & ~(m0 | m1 | m2), i);
    }
  }
  for (u32 j = 0; j < cnt[0]; j++) {
    const u32 i = B.lst[0][j];
    i64 ta, tb, cb;
    if (__builtin_expect(f1_pair_mc<0xFFFFFFFFu>(base + B.st[2][i], base + B.st[3][i], ta, tb, cb), 1)) {
      B.ts[i] = ta;
      B.icode[i] = cb;
    } else {
      ev_ts_exact(B, i, base);
    }
  }
  for (u32 j = 0; j < cnt[1]; j++) {
    const u32 i = B.lst[1][j];
    i64 ta, tb, cb;
    if (__builtin_expect(digits13(base + B.st[2][i], ta) && f1_one_mc<0xFFFFu>(base + B.st[3][i], tb, cb), 1)) {
      B.ts[i] = ta;
      B.icode[i] = cb;
    } else {
      ev_ts_exact(B, i, base);
    }
  }
  for (u32 j = 0; j < cnt[2]; j++) {
    const u32 i = B.lst[2][j];
    i64 ta, tb, cb, add;
    if (__builtin_expect(f3_offset(base + B.st[2][i], add) && f1_pair_mc<0xFFFF7FFFu>(base + B.st[2][i], base + B.st[3][i], ta, tb, cb), 1)) {
      B.ts[i] = ta + add;
      B.icode[i] = cb;
    } else {
      ev_ts_exact(B, i, base);
    }
  }
  for (u32 j = 0; j < cnt[3]; j++) ev_ts_exact(B, B.lst[3][j], base);
  // ---- the records of plain lines (valid view or cart events of regular IDs), four lines at a time: the
  // checks on the two timestamps come down to a handful of sign bits
  const u64 wlen = (u64)(we - ws);
  u64 bad = 0;  // lines with ts outside the window, ingest_ts too late or without a code
  {
    const __m256i vws = _mm256_set1_epi64x(ws), vone = _mm256_set1_epi64x(1);
    const __m256i wlast = _mm256_set1_epi64x((long long)wlen - 1), cmax = _mm256_set1_epi64x(0xFFFFFD);
    // (code - 1) * 1000 may exceed the ts since the window start by this much: 24 hours, base to window start
    const __m256i late0 = _mm256_set1_epi64x(DAY + ws - g_ing_base), k1000 = _mm256_set1_epi64x(1000);
    const __m256i lom = _mm256_set1_epi64x(ELOM);
    for (u32 i = 0; i < n; i += 4) {
      const __m256i rel = _mm256_sub_epi64(_mm256_load_si256((const __m256i*)(B.ts + i)), vws);
      const __m256i ic1 = _mm256_sub_epi64(_mm256_load_si256((const __m256i*)(B.icode + i)), vone);
      const __m256i idx = _mm256_load_si256((const __m256i*)(B.idx + i));
      const __m256i num = _mm256_cvtepu32_epi64(_mm_load_si128((const __m128i*)(B.num + i)));
      // negative if: ts before the window, ts after it, the code out of range (either side), ingest too late
      const __m256i late = _mm256_sub_epi64(_mm256_add_epi64(late0, rel), mullo64x4(ic1, k1000));
      const __m256i neg = _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(rel, _mm256_sub_epi64(wlast, rel)),
                                                          _mm256_or_si256(ic1, _mm256_sub_epi64(cmax, ic1))),
                                          late);
      bad |= (u64)(u32)_mm256_movemask_pd(_mm256_castsi256_pd(neg)) << i;
      const __m256i w0 = _mm256_or_si256(_mm256_add_epi64(rel, vone), _mm256_slli_epi64(num, 32));
      const __m256i w1 = _mm256_or_si256(_mm256_add_epi64(ic1, vone), _mm256_slli_epi64(_mm256_and_si256(idx, lom), 46));
      const __m256i lo = _mm256_unpacklo_epi64(w0, w1), hi = _mm256_unpackhi_epi64(w0, w1);  // records 0, 2 / 1, 3
      _mm256_store_si256((__m256i*)(B.rec + i), _mm256_permute2x128_si256(lo, hi, 0x20));
      _mm256_store_si256((__m256i*)(B.rec + i + 2), _mm256_permute2x128_si256(lo, hi, 0x31));
      _mm_store_si128((__m128i*)(B.part + i), low32x4(_mm256_srli_epi64(idx, KE)));
    }
  }
  // ---- which lines are plain: a user and a product, the type view or cart; eight lines at a time
  u64 plain = 0, kept = 0;  // kept: the line has an event_id (a line without one is dropped)
  {
    const __m256i zero = _mm256_setzero_si256(), c4 = _mm256_set1_epi32(4);
    const __m256i view = _mm256_set1_epi32(0x77656976), cart = _mm256_set1_epi32(0x74726163);
    for (u32 i = 0; i < n; i += 8) {
      const __m256i w4 = _mm256_load_si256((const __m256i*)(B.w4 + i));
      const u32 simple = eq32x8(_mm256_load_si256((const __m256i*)(B.ln[4] + i)), c4) & (eq32x8(w4, view) | eq32x8(w4, cart));
      const u32 empty = eq32x8(_mm256_load_si256((const __m256i*)(B.ln[1] + i)), zero) | eq32x8(_mm256_load_si256((const __m256i*)(B.ln[5] + i)), zero);
      const u32 noid = eq32x8(_mm256_load_si256((const __m256i*)(B.ln[0] + i)), zero);
      plain |= (u64)(simple & ~empty & 0xFF) << i;
      kept |= (u64)(~noid & 0xFF) << i;
    }
  }
  kept &= nmask;
  plain &= kept & ~(irr | bad);
  // ---- the scatter
  auto cur = cur_out;
  for (u64 m = plain; m; m = _blsr_u64(m)) {
    const u32 i = (u32)_tzcnt_u64(m);
    WCScatter<D16>::push16(cur, B.part[i], _mm_load_si128((const __m128i*)&B.rec[i]));
  }
  for (u64 m = kept & ~plain; m; m = _blsr_u64(m)) {
    const u32 i = (u32)_tzcnt_u64(m);
    ev_slow(tl, tid, B, i, !((irr >> i) & 1), base, ws, wlen, cur);
  }
  cur_out = cur;
  B.n = 0;
  return (u32)_mm_popcnt_u64(kept);
}

static NOINLINE u64 parse_event_chunk(const u8* begin, const u8* end, bool skip_header, TL& tl, int tid, const i64 ws,
                                      const i64 we) {
  EvB B;
  u64 nl = 0;
  const u8* base = begin;
  auto cur = tl.ev.cursor(tl.arena);
  scan::scan_cols<7>(
      begin, end, skip_header,
      [&](const u8* sb, const __m256i* C, const u32* q) __attribute__((always_inline)) {
        base = sb;
        B.add_group(C, q);
        if (__builtin_expect(B.n > EvB::CAP - 8, 0)) nl += dn::process_events(B, sb, cur, tl, tid, ws, we);
      },
      [&](const u8* sb, __m256i st, __m256i ln) __attribute__((always_inline)) {
        base = sb;
        B.add_row(st, ln, 1);
        if (__builtin_expect(B.n > EvB::CAP - 8, 0)) nl += dn::process_events(B, sb, cur, tl, tid, ws, we);
      });
  if (B.n) nl += dn::process_events(B, base, cur, tl, tid, ws, we);
  tl.ev.close(cur);
  tl.ev.flush(tl.arena);
  return nl;
}

// Click lines -> C16 records by person index.
struct CkB : Cols<4, 64> {
  alignas(32) u64 kq[CAP + 8], uq[CAP + 8];  // the 8 bytes after the prefix of click_id / user_id
  alignas(32) u8 pfx[CAP + 8];               // nonzero: a prefix byte is not the expected one
  alignas(32) u64 kv[CAP + 8];   // number of the click_id
  alignas(32) u32 num[CAP + 8];  // user number
  alignas(32) i64 ts[CAP + 8];
  u32 lst[4][CAP + 8];
};

static NOINLINE void ck_ts_exact(CkB& B, u32 i, const u8* base) {
  if (!parse_ts(base + B.st[3][i], B.ln[3][i], B.ts[i])) B.ts[i] = INT64_MIN;
}

// A valid click with an ID that is not regular: kept aside.
static NOINLINE void ck_slow(TL& tl, CkB& B, u32 i, const u8* base, u32 rel, u32 camp) {
  const u8* const s1 = base + B.st[1][i];
  const u32 l1 = B.ln[1][i];
  u32 num = NONE;
  if (l1 >= 2 && s1[0] == 'u') {
    const u64 v = b36_scalar(s1 + 1, l1 - 1);
    if (v != NOT36) {
      const u64 x = unscramble(v, g_ukey);
      if ((x >> 9) < g_nu_cap) num = (u32)x;
    }
  }
  tl.xclk.push_back(XClk{hash_id(make_key(s1, l1)), make_key(base + B.st[0][i], B.ln[0][i]), rel, camp, num});
  if (tl.xclk.size() > (1u << 22)) fail("too many irregular clicks");
}

// One batch. Returns the number of its lines that have a click_id and a user_id.
static NOINLINE u32 process_clicks(CkB& B, const u8* const base, WCScatter<C16>::Cursor& cur_out, TL& tl, const i64 cws,
                                   const i64 we) {
  const u32 n = B.n;
  B.pad();
  const u64 nmask = n >= 64 ? ~0ULL : ((1ULL << n) - 1);
  for (u32 i = 0; i < n; i++) {
    const u8* const s0 = base + B.st[0][i];
    const u8* const s1 = base + B.st[1][i];
    memcpy(&B.kq[i], s0 + 1, 8);
    memcpy(&B.uq[i], s1 + 1, 8);
    B.pfx[i] = (u8)((s0[0] ^ 'k') | (s1[0] ^ 'u'));
  }
  u64 irr = nonzero64(B.pfx);
  {
    const __m256i ukey = _mm256_set1_epi64x((long long)g_ukey);
    const __m256i M = _mm256_set1_epi64x((long long)M40);
    const __m256i umax = _mm256_set1_epi64x((long long)(((u64)g_nu_cap << 9) - 1));
    for (u32 i = 0; i < n; i += 4) {
      __m256i vk, vu;
      u32 bk = ids4(B.kq + i, B.ln[0] + i, vk);
      u32 bu = ids4(B.uq + i, B.ln[1] + i, vu);
      const __m256i num = unscramble4(vu, ukey);
      bk |= gtu64x4(vk, M);
      bu |= gtu64x4(vu, M) | gtu64x4(num, umax);
      _mm256_store_si256((__m256i*)(B.kv + i), vk);
      _mm_store_si128((__m128i*)(B.num + i), low32x4(num));
      irr |= (u64)(bk | bu) << i;
    }
  }
  u32 cnt[4] = {0, 0, 0, 0};
  u64 kept = 0;  // the line has a click_id and a user_id
  {
    const __m256i c20 = _mm256_set1_epi32(20), c13 = _mm256_set1_epi32(13), c25 = _mm256_set1_epi32(25), zero = _mm256_setzero_si256();
    for (u32 i = 0; i < n; i += 8) {
      const __m256i l3 = _mm256_load_si256((const __m256i*)(B.ln[3] + i));
      const u32 some = 0xFF & ~(eq32x8(_mm256_load_si256((const __m256i*)(B.ln[0] + i)), zero) |
                                eq32x8(_mm256_load_si256((const __m256i*)(B.ln[1] + i)), zero));
      const u32 live = (u32)(nmask >> i) & some;
      kept |= (u64)live << i;
      const u32 m0 = eq32x8(l3, c20) & live, m1 = eq32x8(l3, c13) & live, m2 = eq32x8(l3, c25) & live;
      cnt[0] += list8(B.lst[0] + cnt[0], m0, i);
      cnt[1] += list8(B.lst[1] + cnt[1], m1, i);
      cnt[2] += list8(B.lst[2] + cnt[2], m2, i);
      cnt[3] += list8(B.lst[3] + cnt[3], live & ~(m0 | m1 | m2), i);
    }
  }
  {
    u32 j = 0;
    for (; j + 2 <= cnt[0]; j += 2) {  // F1, two lines at a time
      const u32 i0 = B.lst[0][j], i1 = B.lst[0][j + 1];
      i64 ta, tb;
      if (__builtin_expect(f1_pair(base + B.st[3][i0], base + B.st[3][i1], ta, tb), 1)) {
        B.ts[i0] = ta;
        B.ts[i1] = tb;
      } else {
        ck_ts_exact(B, i0, base);
        ck_ts_exact(B, i1, base);
      }
    }
    if (j < cnt[0]) ck_ts_exact(B, B.lst[0][j], base);
  }
  for (u32 j = 0; j < cnt[1]; j++) {
    const u32 i = B.lst[1][j];
    i64 t;
    if (__builtin_expect(digits13(base + B.st[3][i], t), 1))
      B.ts[i] = t;
    else
      ck_ts_exact(B, i, base);
  }
  for (u32 j = 0; j < cnt[2]; j++) {
    const u32 i = B.lst[2][j];
    i64 t;
    if (__builtin_expect(f3_one(base + B.st[3][i], t), 1))
      B.ts[i] = t;
    else
      ck_ts_exact(B, i, base);
  }
  for (u32 j = 0; j < cnt[3]; j++) ck_ts_exact(B, B.lst[3][j], base);
  const u64 wlen = (u64)(we - cws);
  auto cur = cur_out;
  for (u64 m = kept; m; m = _blsr_u64(m)) {
    const u32 i = (u32)_tzcnt_u64(m);
    const i32 camp = camp_fast(base + B.st[2][i], B.ln[2][i]);
    const u64 rel = (u64)B.ts[i] - (u64)cws;  // in [0, wlen) iff the click is inside [window start - 7 days, window end)
    if (__builtin_expect(!((rel < wlen) & (camp >= 0)), 0)) continue;
    if (__builtin_expect((irr >> i) & 1, 0)) {
      ck_slow(tl, B, i, base, (u32)rel, (u32)camp);
      continue;
    }
    const u64 w0 = (u64)(u32)rel | ((u64)B.num[i] << 32);
    const u64 w1 = B.kv[i] | ((u64)(u32)camp << 40);
    WCScatter<C16>::push16(cur, B.num[i] >> (9 + KC), _mm_set_epi64x((long long)w1, (long long)w0));
  }
  cur_out = cur;
  B.n = 0;
  return (u32)_mm_popcnt_u64(kept);
}

static NOINLINE u64 parse_click_chunk(const u8* begin, const u8* end, bool skip_header, TL& tl, const i64 cws, const i64 we) {
  CkB B;
  u64 nl = 0;
  const u8* base = begin;
  auto cur = tl.ck.cursor(tl.arena);
  scan::scan_cols<4>(
      begin, end, skip_header,
      [&](const u8* sb, const __m256i* C, const u32* q) __attribute__((always_inline)) {
        base = sb;
        B.add_group(C, q);
        if (__builtin_expect(B.n > CkB::CAP - 8, 0)) nl += dn::process_clicks(B, sb, cur, tl, cws, we);
      },
      [&](const u8* sb, __m256i st, __m256i ln) __attribute__((always_inline)) {
        base = sb;
        B.add_row(st, ln, 1);
        if (__builtin_expect(B.n > CkB::CAP - 8, 0)) nl += dn::process_clicks(B, sb, cur, tl, cws, we);
      });
  if (B.n) nl += dn::process_clicks(B, base, cur, tl, cws, we);
  tl.ck.close(cur);
  tl.ck.flush(tl.arena);
  return nl;
}

static void parse(std::vector<MFile>& ev, std::vector<MFile>& ck) {
  size_t ev_bytes = 0, ck_bytes = 0;
  for (auto& f : ev) ev_bytes += f.size;
  for (auto& f : ck) ck_bytes += f.size;
  size_t chunk = std::max<size_t>(1 << 18, std::min<size_t>(8 << 20, (ev_bytes + ck_bytes) / (NT * 24 + 1)));
  std::vector<Chunk> ce, cc;
  make_chunks(ev, chunk, ce);
  make_chunks(ck, chunk, cc);
  const i64 ws = g_ws, we = g_we, cws = g_ws - WEEK;
  parallel_for((int)(ce.size() + cc.size()), [&](int t, int tid) {
    if (g_fail.load(std::memory_order_relaxed)) return;
    TL& tl = g_tl[tid];
    if (t < (int)ce.size()) {
      const Chunk& c = ce[t];
      const MFile& f = ev[c.file];
      size_t b = line_start_at_or_after(f, c.nominal), e = line_start_at_or_after(f, c.nominal_end);
      if (b >= e) return;
      tl.n_event_lines += dn::parse_event_chunk(f.data + b, f.data + e, b == 0, tl, tid, ws, we);
      drop_parsed(f, c.nominal, c.nominal_end);
    } else {
      const Chunk& c = cc[t - ce.size()];
      const MFile& f = ck[c.file];
      size_t b = line_start_at_or_after(f, c.nominal), e = line_start_at_or_after(f, c.nominal_end);
      if (b >= e) return;
      tl.n_click_lines += dn::parse_click_chunk(f.data + b, f.data + e, b == 0, tl, cws, we);
      drop_parsed(f, c.nominal, c.nominal_end);
    }
  });
  parallel_for(NT, [&](int t, int) {
    g_tl[t].ev.finish(g_tl[t].arena);
    g_tl[t].ck.finish(g_tl[t].arena);
  });
}

// ------------------------------------------------------------------------------------------
// after the parse: the lines kept aside

static inline void push_r2(WCScatter<R2>::Cursor& c, u32 p2shift, u64 ph, u32 ts, u32 payload) {
  WCScatter<R2>::push16(c, (u32)(ph >> p2shift), _mm_set_epi64x((long long)((u64)ts | ((u64)payload << 32)), (long long)ph));
}
static inline void push_r2c(WCScatter<R2C>::Cursor& c, u32 p2shift, u64 ph, u64 click, u32 ts, u32 camp) {
  WCScatter<R2C>::push32(c, (u32)(ph >> p2shift),
                         _mm256_set_epi64x(0, (long long)((u64)ts | ((u64)camp << 32)), (long long)click, (long long)ph));
}

static void resolve_special() {
  TL& tl = g_tl[0];
  const u32 p2shift = 64 - g_P2bits;
  auto c2 = tl.r2.cursor(tl.arena);
  auto c2c = tl.r2c.cursor(tl.arena);
  // events with an event_id that is not regular: their copies can only be among themselves
  std::vector<X1Rec> all;
  for (auto& t : g_tl) all.insert(all.end(), t.x1.begin(), t.x1.end());
  std::stable_sort(all.begin(), all.end(), [](const X1Rec& a, const X1Rec& b) { return a.eid != b.eid ? a.eid < b.eid : a.ingest < b.ingest; });
  size_t n_extra = 0, n_gen = 0;
  for (size_t i = 0; i < all.size(); i++) {
    if (i && all[i].eid == all[i - 1].eid) continue;
    const X1Rec& w = all[i];
    if (w.ts == TS_INVALID) continue;
    if (w.num != NONE && (w.num & 511) < dense_cnt(g_acc[w.num >> 9].load(std::memory_order_relaxed))) {
      g_extra[w.num >> 9].ev.push_back((u64)w.ts | ((u64)w.payload << 32));
      g_acc[w.num >> 9].fetch_or(ACC_EXTRA, std::memory_order_relaxed);
      n_extra++;
    } else {
      push_r2(c2, p2shift, person_of(w.uid), w.ts, w.payload);
      n_gen++;
    }
  }
  // clicks with a click_id or a user_id that is not regular
  size_t nck = 0;
  for (auto& t : g_tl)
    for (const XClk& x : t.xclk) {
      nck++;
      if (x.num != NONE && (x.num & 511) < dense_cnt(g_acc[x.num >> 9].load(std::memory_order_relaxed))) {
        g_extra[x.num >> 9].ck.push_back({(u64)x.ts | ((u64)x.camp << 32), (u64)g_extra_keys.size()});
        g_extra_keys.push_back(x.click);
        g_acc[x.num >> 9].fetch_or(ACC_EXTRA, std::memory_order_relaxed);
      } else {
        push_r2c(c2c, p2shift, person_of(x.uid), x.click, x.ts, x.camp);
      }
    }
  for (auto& kv : g_extra) std::sort(kv.second.ev.begin(), kv.second.ev.end(), [](u64 a, u64 b) { return (u32)a < (u32)b; });
  tl.r2.close(c2);
  tl.r2.flush(tl.arena);
  tl.r2c.close(c2c);
  tl.r2c.flush(tl.arena);
  LOG("dense: %zu lines with an irregular event id (%zu extra events of dense persons, %zu general), %zu irregular clicks",
      all.size(), n_extra, n_gen, nck);
}

// ------------------------------------------------------------------------------------------
// the scan: per event partition, records -> slots (dedup, order) -> persons, sessions, rows

struct DS {
  D16* T = nullptr;
  std::vector<std::pair<const D16*, u32>> blk;
  // the open person (as in the general person phase)
  std::vector<u32> cov, sstart, dstart, dsess, dev, days;
  std::vector<i64> drev, catsum;
  struct Purch {
    u32 day, cat;
  };
  std::vector<Purch> purch;
  std::vector<u32> tin, sts, scamp;
  std::vector<u64> sclick, rtmp;
  std::vector<u32> pts, pcamp;  // the person's clicks before they are put in time order
  std::vector<u64> pclick;
  std::vector<u64> xq;  // the person's extra events in time order
  std::vector<u32> stamp;  // per campaign: the last session (by tag) it was counted as an assist for
  u32 tag = 0;
  // the click partition at hand, grouped by person index
  u32 cp = NONE;
  std::vector<u32> coff, cpos, Cts, Ccamp, Cnum;
  std::vector<u64> Cclick;
  // counters
  u64 n_persons = 0, n_events = 0, n_general = 0;
};

static std::atomic<u8>* g_ck_emitted = nullptr;

// All records of an event partition into their slots; the copy with the smallest ingest code keeps a slot.
// False if the partition has no records.
static NOINLINE bool place(u32 j, D16* T, DS& S) {
  S.blk.clear();
  for (int t = 0; t < NT; t++)
    for (auto& b : g_tl[t].ev.blocks[j])
      if (b.second) S.blk.push_back({b.first, b.second});
  const size_t nblk = S.blk.size();
  if (!nblk) return false;
  S.blk.push_back(S.blk[0]);  // (something harmless to prefetch after the last block)
  for (size_t bi = 0; bi < nblk; bi++) {
    const char* const nx = (const char*)S.blk[bi + 1].first;
    _mm_prefetch(nx, _MM_HINT_T0);
    _mm_prefetch(nx + 64, _MM_HINT_T0);
    _mm_prefetch(nx + 128, _MM_HINT_T0);
    _mm_prefetch(nx + 192, _MM_HINT_T0);
    const D16* r = S.blk[bi].first;
    const u32 n = S.blk[bi].second;
    for (u32 i = 0; i < n; i++) {
      const u64 w0 = r[i].w0, w1 = r[i].w1;
      D16& s = T[(w1 >> 46) & ELOM];
      const u64 o0 = s.w0, o1 = s.w1;
      // (an empty slot has code 0, which wraps to the largest value)
      const bool take = (u32)(((u32)w1 & 0xFFFFFF) - 1) < (u32)(((u32)o1 & 0xFFFFFF) - 1);
      s.w0 = take ? w0 : o0;
      s.w1 = take ? w1 : o1;
    }
  }
  return true;
}

// The valid event of a regular user in a slot, if there is one: its user number.
static FORCE_INLINE bool slot_user(const D16& s, u32& un) {
  const u32 tsf = (u32)s.w0;
  if ((tsf & 0x7FFFFFFF) == 0) return false;
  un = (u32)(s.w0 >> 32);
  if (__builtin_expect((i32)tsf < 0, 0)) {
    const XEv& x = g_tl[un >> 24].xev[un & 0xFFFFFF];
    if (x.num == NONE) return false;
    un = x.num;
  }
  return true;
}
// ... and whether it is an event of a dense person: the person index.
static FORCE_INLINE bool slot_dense(const D16& s, u32& p) {
  u32 un;
  if (!slot_user(s, un)) return false;
  p = un >> 9;
  return (un & 511) < dense_cnt(g_acc[p].load(std::memory_order_relaxed));
}

// The dense person of the last event before partition pb (NONE if there is none).
static u32 find_prev_person(u32 pb, DS& S) {
  for (i64 j = (i64)pb - 1; j >= 0; j--) {
    if (!place((u32)j, S.T, S)) continue;
    u32 q = NONE;
    for (u32 i = ESLOTS; i-- > 0;)
      if (slot_dense(S.T[i], q)) break; else q = NONE;
    memset(S.T, 0, sizeof(D16) * ESLOTS);
    if (q != NONE) return q;
  }
  return NONE;
}

// The clicks of click partition cp that do not belong to a dense person -> the general person phase.
static NOINLINE void emit_other_clicks(u32 cp, WCScatter<R2C>::Cursor& c2c, u32 p2shift) {
  for (int t = 0; t < NT; t++)
    for (auto& b : g_tl[t].ck.blocks[cp]) {
      const C16* r = b.first;
      for (u32 j = 0; j < b.second; j++) {
        const u32 un = (u32)(r[j].w0 >> 32);
        if (__builtin_expect((un & 511) < dense_cnt(g_acc[un >> 9].load(std::memory_order_relaxed)), 1)) continue;
        push_r2c(c2c, p2shift, person_of(hash_id(id_key('u', scramble(un, g_ukey)))), id_key('k', r[j].w1 & M40),
                 (u32)r[j].w0, (u32)(r[j].w1 >> 40));
      }
    }
}

// Click partition cp grouped by person index (a counting sort), in S.
static NOINLINE void load_clicks(DS& S, u32 cp, WCScatter<R2C>::Cursor& c2c, u32 p2shift) {
  S.cp = cp;
  const u32 G = 1u << KC;
  S.coff.assign(G + 2, 0);
  u32* const coff = S.coff.data();
  size_t n = 0;
  for (int t = 0; t < NT; t++)
    for (auto& b : g_tl[t].ck.blocks[cp]) {
      const C16* r = b.first;
      for (u32 j = 0; j < b.second; j++) coff[((u32)(r[j].w0 >> 41) & (G - 1)) + 1]++;
      n += b.second;
    }
  for (u32 g = 0; g < G; g++) coff[g + 1] += coff[g];
  if (S.Cts.size() < n + 16) {
    S.Cts.resize(n + 16);
    S.Ccamp.resize(n + 16);
    S.Cnum.resize(n + 16);
    S.Cclick.resize(n + 16);
  }
  S.cpos.assign(coff, coff + G);
  u32* const cpos = S.cpos.data();
  for (int t = 0; t < NT; t++)
    for (auto& b : g_tl[t].ck.blocks[cp]) {
      const C16* r = b.first;
      for (u32 j = 0; j < b.second; j++) {
        const u64 w0 = r[j].w0, w1 = r[j].w1;
        const u32 q = cpos[(u32)(w0 >> 41) & (G - 1)]++;
        S.Cts[q] = (u32)w0;
        S.Cnum[q] = (u32)(w0 >> 32);
        S.Ccamp[q] = (u32)(w1 >> 40);
        S.Cclick[q] = w1 & M40;
      }
    }
  if (!g_ck_emitted[cp].exchange(1, std::memory_order_relaxed)) {
    // the clicks that do not belong to a dense person -> the general person phase
    for (u32 g = 0; g < G; g++) {
      const u32 c0 = coff[g], c1 = coff[g + 1];
      if (c0 == c1) continue;
      const u32 cnt = dense_cnt(g_acc[(cp << KC) | g].load(std::memory_order_relaxed));
      for (u32 q = c0; q < c1; q++) {
        const u32 un = S.Cnum[q];
        if (__builtin_expect((un & 511) < cnt, 1)) continue;
        push_r2c(c2c, p2shift, person_of(hash_id(id_key('u', scramble(un, g_ukey)))), id_key('k', S.Cclick[q]), S.Cts[q],
                 S.Ccamp[q]);
      }
    }
  }
}

struct SegStat {
  u64 n_persons = 0, n_events = 0, n_general = 0;
};

// Partitions [pb, pe): a thread's share of the scan. The first dense person met may have begun before pb:
// its events here are left to the thread that scans the partitions before (it reads on past its own share
// until that person ends), just as this thread reads on past pe for its last person.
static void scan_segment(const u32 pb, const u32 pe, const int tid, DS& S, SegStat& st) {
  TL& tl = g_tl[tid];
  const u32 NC = g_ncamp, ND = (u32)g_ndays, ORG = NC;
  const u32 NB = (u32)(((u64)(g_we - g_ws - 1) >> BSHIFT) + 1);
  const u32 NCAT = (u32)g_cats.size();
  const size_t NC1 = (size_t)NC + 1;
  const u32 p2shift = 64 - g_P2bits;
  const u64 ws_off = (u64)(g_ws - g_day0 * DAY);  // ms from the start of day 0 to the window start
  if (!S.T) {
    S.T = (D16*)aligned_alloc(64, sizeof(D16) * ESLOTS);
    memset(S.T, 0, sizeof(D16) * ESLOTS);
    S.cov.assign(NC + 1, 0);
    S.sstart.assign((size_t)NB + 16, 0);
    S.dstart.assign((size_t)ND + 2, 0);
    S.dsess.assign(ND, 0);
    S.dev.assign(ND, 0);
    S.drev.assign(ND, 0);
    S.catsum.assign((size_t)ND * NCAT, 0);
    S.stamp.assign(NC + 1, 0);
  }
  D16* const T = S.T;
  u32* const stamp = S.stamp.data();
  u32 tag = S.tag;
  bool by_session = true;  // the open person's assists are counted session by session (few clicks)
  auto c2 = tl.r2.cursor(tl.arena);
  auto c2c = tl.r2c.cursor(tl.arena);
  CStat* const c_stat = tl.c_stat.data();
  u32* const c_assist = tl.c_assist.data();
  u32* const dsess = S.dsess.data();
  u32* const dev = S.dev.data();
  i64* const drev = S.drev.data();
  i64* const catsum = S.catsum.data();
  u32* const cov = S.cov.data();
  u32* const sstart = S.sstart.data();
  u32* const dstart = S.dstart.data();
  OutCols& out = tl.out;
  const std::atomic<u64>* const acc = g_acc;
  const u64 ukey = g_ukey;

  // ---- the open person
  u32 cur_p = NONE, cur_cnt = 0;
  u32 mc = 0, nv = 0;  // its clicks (in S.sts / S.scamp / S.sclick, in time order)
  u32 ns = 0, hi = 0;  // its sessions so far; (for a long click list: the clicks up to the current session start)
  i64 s_max = INT64_MIN / 4, s_rev = 0;  // the open session: its last event so far, ...
  u32 s_start = 0, s_nev = 0, s_day = 0;
  u32 x_next = NONE;  // time of the person's next extra event
  size_t x_i = 0;
  u64 n_persons = 0, n_events = 0, n_general = 0;

  auto close_session = [&]() {
    const u32* const sts = S.sts.data();
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
          if (click_less(S.sclick[q], S.sclick[bi])) bi = q;
        }
        credit = S.scamp[bi];
      }
      // Assists: every campaign with a click in the 7 days up to the start, once. With few clicks, here and
      // now: the clicks of that week are the ones just below `hi`.
      if (by_session) {
        u32* const arow = c_assist + (size_t)s_day * NC1;
        tag++;
        for (u32 k = hi; k > 0 && sts[k - 1] >= s_start;) {
          k--;
          const u32 c = S.scamp[k];
          arow[c] += stamp[c] != tag;
          stamp[c] = tag;
        }
      }
    }
    CStat& cs = c_stat[(size_t)s_day * NC1 + credit];
    cs.sess++;
    cs.purch += (s_rev > 0);
    cs.rev += s_rev;
    if (dsess[s_day]++ == 0) {
      dstart[S.days.size()] = ns;
      S.days.push_back(s_day);
    }
    sstart[ns++] = s_start;
    dev[s_day] += s_nev;
    drev[s_day] += s_rev;
  };
  // An event of the open person; they come in time order.
  auto feed = [&](const u32 ts, const u32 cat, const u32 price) __attribute__((always_inline)) {  // price 0: no purchase
    const i64 gap = (i64)ts - s_max;
    if (gap > MIN30) {
      if (s_nev) close_session();
      s_start = ts;
      s_nev = 0;
      s_rev = 0;
      s_day = (u32)((ws_off + ts) / (u64)DAY);
    } else if (__builtin_expect(gap < 0, 0)) {
      fail("a person's events are not in time order");
    }
    s_max = ts;
    s_nev++;
    if (price) {
      s_rev += price;
      catsum[(size_t)s_day * NCAT + cat] += price;
      S.purch.push_back(DS::Purch{s_day, cat});
    }
  };
  auto feed_extras = [&](const u32 upto) {  // the extra events up to a time
    while (x_i < S.xq.size() && (u32)S.xq[x_i] <= upto) {
      const u64 e = S.xq[x_i++];
      const u32 pl = (u32)(e >> 32);
      feed((u32)e, pl >> 24, pl & 0xFFFFFF);
      n_events++;
    }
    x_next = x_i < S.xq.size() ? (u32)S.xq[x_i] : NONE;
  };
  auto close_person = [&]() {
    if (cur_p == NONE) return;
    if (x_next != NONE) feed_extras(NONE - 1);
    if (s_nev) close_session();
    const u32* const sts = S.sts.data();
    const u32* const scamp = S.scamp.data();
    // assists of a person with many clicks: click by click, as in the general person phase
    if (mc && !by_session) {
      const u32 ndays = (u32)S.days.size();
      dstart[ndays] = ns;
      _mm256_storeu_si256((__m256i*)(sstart + ns), _mm256_set1_epi32(-1));
      const u32 nsv = (ns + 7) / 8;
      u32* rows[64];
      for (u32 j = 0; j < ndays; j++) rows[j] = c_assist + (size_t)S.days[j] * NC1;
      u32 a = 0, b = 0;
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
        if (x < b) {  // sessions [x, b) are new for this campaign: count them on their days
          u32 j = 0;
          while (dstart[j + 1] <= x) j++;
          for (; j < ndays && dstart[j] < b; j++) rows[j][c] += std::min(b, dstart[j + 1]) - std::max(x, dstart[j]);
        }
      }
      for (u32 k = 0; k < mc; k++) cov[scamp[k]] = 0;
    }
    // rows: the name is the smallest of the person's IDs
    const u32 nd = (u32)S.days.size();
    if (nd) {
      u64 pkey = id_key('u', scramble((u64)cur_p << 9, ukey));
      for (u32 a = 1; a < cur_cnt; a++) pkey = std::min(pkey, id_key('u', scramble(((u64)cur_p << 9) | a, ukey)));
      u8 kb[16];
      const u32 klen = fast_key_bytes(pkey, kb + 4);
      memcpy(kb, &klen, 4);
      u8* ip = out.ids.reserve(16, tl.arena);
      memcpy(ip, kb, 16);
      out.ids.cur = ip + 4 + klen;
      *out.runs.slot(tl.arena) = (u8)nd;
      out.npersons++;
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
          for (const DS::Purch& q : S.purch) {
            if (q.day != d) continue;
            const i64 v = cs[q.cat];
            if (v > best || (v == best && q.cat < top)) {
              best = v;
              top = (u16)q.cat;
            }
          }
          for (const DS::Purch& q : S.purch)
            if (q.day == d) cs[q.cat] = 0;
        }
        dayp[di] = (u8)d;
        sessp[di] = (u8)dsess[d];
        evsp[di] = dev[d];
        out.max_sess = std::max(out.max_sess, dsess[d]);
        out.max_evs = std::max(out.max_evs, dev[d]);
        revp[di] = drev[d];
        topp[di] = (u8)top;
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
    n_persons++;
    cur_p = NONE;
  };
  i64 prev_p = -1;  // the dense person before the open one
  auto open_person = [&](const u32 p, const u32 cnt, const u64 ac) {
    if (__builtin_expect((i64)p <= prev_p, 0)) fail("persons are not in index order");
    prev_p = p;
    cur_p = p;
    cur_cnt = cnt;
    ns = 0;
    hi = 0;
    s_max = INT64_MIN / 4;
    s_nev = 0;
    s_rev = 0;
    S.days.clear();
    S.purch.clear();
    // its clicks: those of its click partition whose alias is one of its own
    const u32 cp = p >> KC;
    if (__builtin_expect(cp != S.cp, 0)) load_clicks(S, cp, c2c, p2shift);
    const u32 c0 = S.coff[p & ((1u << KC) - 1)], c1 = S.coff[(p & ((1u << KC) - 1)) + 1];
    const Extra* ex = nullptr;
    if (__builtin_expect((ac & ACC_EXTRA) != 0, 0)) ex = &g_extra.find(p)->second;
    const size_t cap = (size_t)(c1 - c0) + (ex ? ex->ck.size() : 0) + 16;
    if (S.pts.size() < cap) {
      S.pts.resize(cap);
      S.pcamp.resize(cap);
      S.pclick.resize(cap);
      S.tin.resize(cap);
      S.sts.resize(cap);
      S.scamp.resize(cap);
      S.sclick.resize(cap);
    }
    u32 m = 0;
    for (u32 q = c0; q < c1; q++) {
      S.pts[m] = S.Cts[q];
      S.pcamp[m] = S.Ccamp[q];
      S.pclick[m] = S.Cclick[q];
      m += (S.Cnum[q] & 511) < cnt;
    }
    x_next = NONE;
    x_i = 0;
    S.xq.clear();
    if (ex) {
      for (auto& c : ex->ck) {
        S.pts[m] = (u32)c[0];
        S.pcamp[m] = (u32)(c[0] >> 32);
        S.pclick[m] = CLICK_IS_KEY | c[1];
        m++;
      }
      S.xq = ex->ev;
      if (!S.xq.empty()) x_next = (u32)S.xq[0];
    }
    mc = m;
    nv = (mc + 7) / 8;
    by_session = mc <= 64;
    if (mc)
      sort_person_clicks(S.pts.data(), S.pcamp.data(), S.pclick.data(), mc, S.tin.data(), S.sts.data(), S.scamp.data(),
                         S.sclick.data(), S.rtmp);
  };
  u32 last_un = NONE;  // (the general person of the last such user, kept for runs of the same one)
  u64 last_ph = 0;
  auto emit_general = [&](const u32 un, const u32 ts, const u32 cat, const u32 price) {
    if (un != last_un) {
      last_un = un;
      last_ph = person_of(hash_id(id_key('u', scramble(un, ukey))));
    }
    push_r2(c2, p2shift, last_ph, ts, price ? ((cat << 24) | price) : 0);
    n_general++;
  };

  // ---- this thread's partitions
  u32 q_skip = pb ? find_prev_person(pb, S) : NONE;  // the events of this person at the start are not ours
  if (q_skip != NONE) prev_p = q_skip;
  bool skipping = q_skip != NONE;
  for (u32 j = pb; j < pe && !g_fail.load(std::memory_order_relaxed); j++) {
    if (!place(j, T, S)) continue;
    for (u32 i = 0; i < ESLOTS; i++) {
      const u64 w0 = T[i].w0;
      const u32 tsf = (u32)w0;
      if ((tsf & 0x7FFFFFFF) == 0) continue;  // no record, or an event that is not valid
      const u32 ts = (tsf & 0x7FFFFFFF) - 1;
      u32 un = (u32)(w0 >> 32), cat, price;
      if (__builtin_expect((i32)tsf < 0, 0)) {  // a side entry
        const XEv& x = g_tl[un >> 24].xev[un & 0xFFFFFF];
        if (x.num == NONE) {
          push_r2(c2, p2shift, person_of(x.uid), ts, x.payload);
          n_general++;
          continue;
        }
        un = x.num;
        cat = x.payload >> 24;
        price = x.payload & 0xFFFFFF;
      } else {
        const u32 pay = (u32)(T[i].w1 >> 24) & 0x3FFFFF;
        cat = pay >> 17;
        price = pay & 0x1FFFF;
      }
      const u32 p = un >> 9, a = un & 511;
      if (__builtin_expect(p != cur_p, 0)) {
        const u64 ac = acc[p].load(std::memory_order_relaxed);
        const u32 cnt = dense_cnt(ac);
        if (a >= cnt) {
          emit_general(un, ts, cat, price);
          continue;
        }
        if (skipping) {
          if (p == q_skip) continue;
          skipping = false;
        }
        close_person();
        open_person(p, cnt, ac);
      } else if (__builtin_expect(a >= cur_cnt, 0)) {
        emit_general(un, ts, cat, price);
        continue;
      }
      if (__builtin_expect(ts >= x_next, 0)) feed_extras(ts);
      feed(ts, cat, price);
      n_events++;
    }
    memset(T, 0, sizeof(D16) * ESLOTS);
  }
  // ---- the rest of the open person, in the partitions after ours
  if (cur_p != NONE) {
    for (u32 j = pe; j < g_PE && !g_fail.load(std::memory_order_relaxed); j++) {
      if (!place(j, T, S)) continue;
      bool stop = false;
      for (u32 i = 0; i < ESLOTS; i++) {
        const u64 w0 = T[i].w0;
        const u32 tsf = (u32)w0;
        if ((tsf & 0x7FFFFFFF) == 0) continue;
        const u32 ts = (tsf & 0x7FFFFFFF) - 1;
        u32 un = (u32)(w0 >> 32), cat, price;
        if (__builtin_expect((i32)tsf < 0, 0)) {
          const XEv& x = g_tl[un >> 24].xev[un & 0xFFFFFF];
          if (x.num == NONE) continue;
          un = x.num;
          cat = x.payload >> 24;
          price = x.payload & 0xFFFFFF;
        } else {
          const u32 pay = (u32)(T[i].w1 >> 24) & 0x3FFFFF;
          cat = pay >> 17;
          price = pay & 0x1FFFF;
        }
        const u32 p = un >> 9, a = un & 511;
        if (p != cur_p) {
          if (a < dense_cnt(acc[p].load(std::memory_order_relaxed))) {  // the next dense person: ours has ended
            stop = true;
            break;
          }
          continue;
        }
        if (a >= cur_cnt) continue;
        if (__builtin_expect(ts >= x_next, 0)) feed_extras(ts);
        feed(ts, cat, price);
        n_events++;
      }
      memset(T, 0, sizeof(D16) * ESLOTS);
      if (stop) break;
    }
  }
  close_person();
  tl.r2.close(c2);
  tl.r2.flush(tl.arena);
  tl.r2c.close(c2c);
  tl.r2c.flush(tl.arena);
  S.tag = tag;
  st.n_persons = n_persons;
  st.n_events = n_events;
  st.n_general = n_general;
}

static void scan_all() {
  // segments: runs of partitions with about the same number of records
  std::vector<u64> cnt(g_PE, 0);
  u64 total = 0;
  u32 used = 0;
  for (u32 j = 0; j < g_PE; j++) {
    for (int t = 0; t < NT; t++) cnt[j] += g_tl[t].ev.size(j);
    total += cnt[j];
    if (cnt[j]) used = j + 1;
  }
  const u32 nseg = std::max<u32>(1, std::min<u32>(used, (u32)NT * 4));
  std::vector<u32> sb(nseg + 1, used);
  {
    u64 run = 0;
    u32 s = 1;
    sb[0] = 0;
    for (u32 j = 0; j < used && s < nseg; j++) {
      run += cnt[j];
      if (run * nseg >= total * s) sb[s++] = j + 1;
    }
  }
  g_ck_emitted = new std::atomic<u8>[g_PC];
  for (u32 i = 0; i < g_PC; i++) g_ck_emitted[i].store(0, std::memory_order_relaxed);
  const u32 NC = g_ncamp, ND = (u32)g_ndays;
  parallel_for(NT, [&](int t, int) {
    g_tl[t].c_stat.assign((size_t)(NC + 1) * ND, CStat{0, 0, 0});
    g_tl[t].c_assist.assign((size_t)(NC + 1) * ND, 0);
  });
  std::vector<DS> scr(NT);
  std::vector<SegStat> st(nseg);
  parallel_for((int)nseg, [&](int s, int tid) {
    if (sb[s] < sb[s + 1]) scan_segment(sb[s], sb[s + 1], tid, scr[tid], st[s]);
  });
  // clicks in partitions no dense person asked for
  const u32 p2shift = 64 - g_P2bits;
  parallel_for((int)g_PC, [&](int cp, int tid) {
    if (g_ck_emitted[cp].exchange(1, std::memory_order_relaxed)) return;
    bool any = false;
    for (int t = 0; t < NT && !any; t++) any = !g_tl[t].ck.blocks[cp].empty();
    if (!any) return;
    TL& tl = g_tl[tid];
    auto c2c = tl.r2c.cursor(tl.arena);
    emit_other_clicks((u32)cp, c2c, p2shift);
    tl.r2c.close(c2c);
    tl.r2c.flush(tl.arena);
  });
  parallel_for(NT, [&](int t, int) {
    TL& tl = g_tl[t];
    tl.r2.finish(tl.arena);
    tl.r2c.finish(tl.arena);
  });
  u64 np = 0, ne = 0, ng = 0;
  for (auto& s : st) {
    np += s.n_persons;
    ne += s.n_events;
    ng += s.n_general;
  }
  LOG("dense scan: %u partitions in %u segments, %llu persons, %llu events; %llu events to the general phase", used, nseg,
      (unsigned long long)np, (unsigned long long)ne, (unsigned long long)ng);
}

// The ID kernels against their plain versions. Returns the number of mismatches.
template <class Next>
static long selftest(Next&& next) {
  long bad = 0, n = 0;
  const char* alpha = "0123456789abcdefghijklmnopqrstuvwxyz";
  const char* odd = "0123456789abcdefghijklmnopqrstuvwxyzAZ ,\"_-\x80";
  // mixing and its inverse; ID text and keys
  for (int it = 0; it < 2000000; it++) {
    const u64 v = next() & M40, key = next() & M40;
    if (unscramble(scramble(v, key), key) != v) bad++;
    u8 b[24] = {0};
    b[0] = 'u';
    const u32 len = 1 + b36_write(v, b + 1);
    if (b36_scalar(b + 1, len - 1) != v) bad++;
    if (id_key('u', v) != id_key_ref('u', v) || id_key('u', v) != make_key(b, len)) bad++;
    const u64 small = v >> (next() % 41);  // short IDs too
    if (id_key('k', small) != id_key_ref('k', small)) bad++;
    n++;
  }
  // digits: two at a time, four at a time
  alignas(32) u64 raw[4], sh[4];
  u8 txt[4][24];
  u32 ln[4];
  for (int it = 0; it < 3000000; it++) {
    for (int k = 0; k < 4; k++) {
      memset(txt[k], ',', 24);
      const u32 mode = (u32)(next() % 8);
      u32 l;
      if (mode < 5) {  // a number as the generator writes it
        const u64 v = (next() & M40) >> (next() % 41);
        l = b36_write(v, txt[k]);
      } else {  // any digits (leading zeros, too long, too large), now and then a byte that is none
        l = (u32)(next() % 11);
        for (u32 j = 0; j < l; j++) txt[k][j] = (u8)(mode == 7 && next() % 5 == 0 ? odd[next() % 44] : alpha[next() % 36]);
      }
      ln[k] = l;
      memcpy(&raw[k], txt[k], 8);
      sh[k] = (u64)72 - 8 * (u64)(l + 1);  // (the field has a prefix byte)
    }
    __m256i val;
    const u32 bm = b36x4(_mm256_load_si256((const __m256i*)raw), _mm256_load_si256((const __m256i*)sh), val);
    alignas(32) u64 vv[4];
    _mm256_store_si256((__m256i*)vv, val);
    for (int k = 0; k < 4; k++) {
      const u64 ref = b36_scalar(txt[k], ln[k]);
      const bool isbad = ((bm >> k) & 1) || vv[k] > M40;
      if (isbad != (ref == NOT36) || (!isbad && vv[k] != ref)) {
        if (bad < 10) fprintf(stderr, "B36X4 MISMATCH '%.*s'\n", (int)ln[k], txt[k]);
        bad++;
      }
    }
    if (ln[0] >= 1 && ln[0] <= 8 && ln[1] >= 1 && ln[1] <= 8) {
      u64 va, vb;
      const u32 ok = b36_pair(txt[0], ln[0], txt[1], ln[1], va, vb);
      const u64 ra = b36_scalar(txt[0], ln[0]), rb = b36_scalar(txt[1], ln[1]);
      const bool ba = (ok & 0xFF) != 0xFF || (txt[0][0] == '0' && ln[0] > 1) || va > M40;
      const bool bb = (ok >> 8) != 0xFF || (txt[1][0] == '0' && ln[1] > 1) || vb > M40;
      if (ba != (ra == NOT36) || bb != (rb == NOT36) || (!ba && va != ra) || (!bb && vb != rb)) bad++;
    }
    {  // four at a time: mixing undone
      const u64 key = next() & M40;
      alignas(32) u64 in[4], o[4];
      for (int k = 0; k < 4; k++) in[k] = next() & M40;
      _mm256_store_si256((__m256i*)o, unscramble4(_mm256_load_si256((const __m256i*)in), _mm256_set1_epi64x((long long)key)));
      for (int k = 0; k < 4; k++)
        if (o[k] != unscramble(in[k], key)) bad++;
    }
    n++;
  }
  fprintf(stderr, "selftest ids: %ld cases, %ld mismatches\n", n, bad);
  return bad;
}

}  // namespace dn
