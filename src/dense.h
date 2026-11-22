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

// A number that orders ID numbers (below 2^40) as their IDs are ordered byte-wise, without making the IDs:
// the digits moved up to eight places (so that first digits meet first digits), then the number of digits (a
// shorter ID that the longer one starts with comes first).
struct IdOrderTables {
  u8 nd[65];    // by the number of significant bits: the digits of the smallest number with that many
  u64 pw[10];   // 36^k
  u64 up[10];   // 36^(8 - k)
  constexpr IdOrderTables() : nd(), pw(), up() {
    pw[0] = 1;
    for (int k = 1; k < 10; k++) pw[k] = pw[k - 1] * 36;
    for (int k = 0; k <= 8; k++) up[k] = pw[8 - k];
    up[9] = 0;
    nd[0] = 1;
    for (int b = 1; b <= 64; b++) {
      const u64 lo = b == 64 ? (1ULL << 63) : (1ULL << (b - 1));
      int k = 1;
      while (k < 9 && lo >= pw[k]) k++;
      nd[b] = (u8)k;
    }
  }
};
static constexpr IdOrderTables ID_ORDER;
static FORCE_INLINE u64 id_order(const u64 v) {
  const u32 bits = 64 - (u32)_lzcnt_u64(v);
  u32 k = ID_ORDER.nd[bits];
  k += v >= ID_ORDER.pw[k];
  return ((v * ID_ORDER.up[k]) << 4) | k;
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
static constexpr u32 KE = 14, KC = 8;  // event indexes per event partition, person indexes per click partition
static constexpr u32 ESLOTS = 1u << KE, ELOM = ESLOTS - 1;
static u32 g_PE = 0, g_PC = 0;
static i64 g_ing_base = 0;  // ingest_ts is kept as whole seconds since this, plus 1, in 24 bits
static constexpr u64 ING_SPAN = 0xFFFFFEULL * 1000;
static constexpr u32 ING_NOCODE = 0xFFFFFF;  // an ingest_ts that does not parse: later than all others

// Per person index, a word that the link phase fills in (g_acc). Nearly every link line joins two aliases of
// one person index, and such a line only changes this word: it never reaches the general link table H.
//   bits 0..39   four bits per alias a < KA: the smallest alias of the link set a is in (0 while a is in none)
//   bits 40..49  the aliases that occur in a link (also in one to themselves)
//   bits 52..55  set when all links are read: how many aliases the dense person of the index has (see dense_cnt)
//   bit 62       the person has events or clicks kept outside the partitions (g_extra)
//   bit 63       the index has no dense person. During the link phase: one of its IDs is in a link line that
//                does not fit the word (the other ID is not a regular ID of this index, or an alias is >= KA).
// Everything the general pipeline may be asked about is put into H when the links are read: the lines that do
// not fit a word, and, for every index without a dense person or with links beside it, the links in its word.
static constexpr u32 KA = 10;
static constexpr u64 ACC_POISON = 1ULL << 63, ACC_EXTRA = 1ULL << 62, ACC_LAB = (1ULL << (4 * KA)) - 1;
static constexpr u32 ACC_MSH = 4 * KA, ACC_NSH = 52;

// How many aliases the dense person of an index has: its IDs are exactly the aliases below this, and nothing
// else is linked to them. 1 if alias 0 is in no link. 0: the index has no dense person (every record of its
// IDs goes the general way). Valid once the link phase has classified the words.
static FORCE_INLINE u32 dense_cnt(u64 a) {
  if (__builtin_expect((a & ~ACC_EXTRA) == 0, 1)) return 1;
  if ((i64)a < 0) return 0;
  return (u32)(a >> ACC_NSH) & 15;
}

// A link between the aliases a and b (both below KA) of person index p: their sets become one in the word.
// A set is labelled by its smallest alias, so joining two sets relabels the members of the one with the
// larger label. Any number of threads may do this at once.
static FORCE_INLINE void link_aliases(u32 p, u32 a, u32 b) {
  constexpr u64 N1 = 0x1111111111ULL, N7 = 0x7777777777ULL, N8 = 0x8888888888ULL;
  std::atomic<u64>& word = g_acc[p];
  u64 cur = word.load(std::memory_order_relaxed);
  for (;;) {
    u64 nw = cur;
    if (!((nw >> (ACC_MSH + a)) & 1)) nw |= (1ULL << (ACC_MSH + a)) | ((u64)a << (4 * a));  // a set of its own
    if (!((nw >> (ACC_MSH + b)) & 1)) nw |= (1ULL << (ACC_MSH + b)) | ((u64)b << (4 * b));
    const u32 la = (u32)(nw >> (4 * a)) & 15, lb = (u32)(nw >> (4 * b)) & 15;
    if (la != lb) {
      const u64 lo = la < lb ? la : lb, hi = la < lb ? lb : la;
      const u64 t = (nw & ACC_LAB) ^ (hi * N1);    // a zero nibble where the label is hi (hi > 0: no unused nibble)
      const u64 z = ~(((t & N7) + N7) | t) & N8;   // the top bit of those nibbles
      const u64 m = z >> 3;
      nw = (nw & ~(m * 15)) | (m * lo);
    }
    if (nw == cur) return;
    if (word.compare_exchange_weak(cur, nw, std::memory_order_relaxed)) return;
  }
}

// The user number (person index << 9 | alias) of the ID of len bytes at s, or NONE if it is not a regular
// user ID: 'u', then 1 to 8 base-36 digits without a leading zero, a number below 2^40 whose index is in range.
// (The same conditions as in the event and click parsers.)
static inline u32 reg_user(const u8* s, u32 len) {
  if (len < 2 || len > 9 || s[0] != 'u') return NONE;
  const u64 v = b36_scalar(s + 1, len - 1);
  if (v == NOT36) return NONE;
  const u64 num = unscramble(v, g_ukey);
  return (num >> 9) < g_nu_cap ? (u32)num : NONE;
}

static inline u64 person_of(u64 h) { return g_H ? h_lookup(h) : h; }

// ------------------------------------------------------------------------------------------
// links
//
// The link phase of the dense path. A line whose two IDs are regular IDs of one person index, with aliases
// below KA, is recorded in that index's word (link_aliases) and nowhere else. Any other line is kept for the
// general link table H, and the indexes of its regular IDs are poisoned. When all lines are read, the words
// are classified (dense_cnt), the links inside the words that the general pipeline can be asked about are
// added to the kept lines, and H is built from those alone: thousands of IDs instead of millions.

using LinkList = std::vector<std::array<u64, 2>>;  // links for H: the hashes of the two IDs

// reg_user of the two IDs of a link line, both at once when they have the usual shape (8 readable bytes after
// the first byte of each are needed, as everywhere).
static FORCE_INLINE void reg_user_pair(const u8* a, u32 la, const u8* b, u32 lb, const u64 ukey, const u32 nu, u32& x, u32& y) {
  if (__builtin_expect((la - 2 <= 7) & (lb - 2 <= 7) & (a[0] == 'u') & (b[0] == 'u'), 1)) {
    u64 va, vb;
    const u32 ok = b36_pair(a + 1, la - 1, b + 1, lb - 1, va, vb);
    const u64 na = unscramble(va, ukey), nb = unscramble(vb, ukey);
    x = (((ok & 0xFF) == 0xFF) & !((a[1] == '0') & (la > 2)) & (va <= M40) & ((na >> 9) < nu)) ? (u32)na : NONE;
    y = (((ok >> 8) == 0xFF) & !((b[1] == '0') & (lb > 2)) & (vb <= M40) & ((nb >> 9) < nu)) ? (u32)nb : NONE;
  } else {
    x = reg_user(a, la);
    y = reg_user(b, lb);
  }
}

struct LkD {
  static constexpr u32 CAP = 64;
  alignas(32) u32 off[CAP][8];
  alignas(32) u32 len[CAP][8];
  u32 x[CAP], y[CAP];  // the user numbers of the two IDs, NONE if not regular
};

static NOINLINE void process_links_dense(LkD& B, const u32 n, const u8* const base, LinkList& keep) {
  const u64 ukey = g_ukey;
  const u32 nu = g_nu_cap;
  for (u32 i = 0; i < n; i++) {
    const u8* const a = base + B.off[i][0];
    const u8* const b = base + B.off[i][1];
    const u32 la = B.len[i][0], lb = B.len[i][1];
    u32 x, y;
    reg_user_pair(a, la, b, lb, ukey, nu, x, y);
    B.x[i] = x;
    B.y[i] = y;
    __builtin_prefetch(&g_acc[x == NONE ? 0 : (x >> 9)], 1, 3);
  }
  for (u32 i = 0; i < n; i++) {
    const u32 x = B.x[i], y = B.y[i];
    if (__builtin_expect((x != NONE) & (y != NONE) & ((x >> 9) == (y >> 9)) & ((x & 511) < KA) & ((y & 511) < KA), 1)) {
      link_aliases(x >> 9, x & 511, y & 511);
    } else {
      keep.push_back({hash_id(make_key(base + B.off[i][0], B.len[i][0])), hash_id(make_key(base + B.off[i][1], B.len[i][1]))});
      if (x != NONE) g_acc[x >> 9].fetch_or(ACC_POISON, std::memory_order_relaxed);
      if (y != NONE) g_acc[y >> 9].fetch_or(ACC_POISON, std::memory_order_relaxed);
    }
  }
}

static NOINLINE u64 parse_link_chunk_dense(const u8* begin, const u8* end, bool skip_header, LinkList& keep) {
  LkD B;
  u32 n = 0;
  u64 nl = 0;
  const u8* base = begin;
  scan::scan_lines<2>(begin, end, skip_header, [&](const u8* sb, __m256i st, __m256i ln) __attribute__((always_inline)) {
    _mm256_store_si256((__m256i*)B.off[n], st);
    _mm256_store_si256((__m256i*)B.len[n], ln);
    base = sb;
    // a line with an empty ID links nothing: its slot is reused
    n += (_mm256_movemask_epi8(_mm256_cmpeq_epi32(ln, _mm256_setzero_si256())) & 0xFF) == 0;
    if (__builtin_expect(n == LkD::CAP, 0)) {
      process_links_dense(B, n, sb, keep);
      nl += n;
      n = 0;
    }
  });
  if (n) process_links_dense(B, n, base, keep);
  return nl + n;
}

static void build_links_dense(std::vector<MFile>& files) {
  std::vector<Chunk> chunks;
  make_chunks(files, 1 << 18, chunks);
  const int nchunks = (int)chunks.size();
  std::vector<LinkList> keep(NT);  // per thread
  std::vector<size_t> part(std::max(nchunks, 1), 0);
  parallel_for(nchunks, [&](int t, int tid) {
    const Chunk& c = chunks[t];
    const MFile& f = files[c.file];
    size_t b = line_start_at_or_after(f, c.nominal), e = line_start_at_or_after(f, c.nominal_end);
    part[t] = b < e ? parse_link_chunk_dense(f.data + b, f.data + e, b == 0, keep[tid]) : 0;
  });
  size_t nlinks = 0, nkept = 0;
  for (size_t c : part) nlinks += c;
  for (auto& k : keep) nkept += k.size();
  // Classify the words. An index has a dense person of n aliases if it is not poisoned and either alias 0 is
  // in no link (n = 1), or all linked aliases are in one set (every label is 0, the smallest alias) and they
  // are exactly the aliases 0..n-1. Whatever links a word holds beside its dense person, or without one, go
  // to H: each member of a set with the set's smallest alias.
  const u32 nu = g_nu_cap;
  const int ntask = (int)std::max<u32>(1, std::min<u32>(nu >> 13, 512));
  std::vector<size_t> ndense(NT, 0);
  parallel_for(ntask, [&](int t, int tid) {
    const u32 pa = (u32)((u64)nu * t / ntask), pb = (u32)((u64)nu * (t + 1) / ntask);
    LinkList& out = keep[tid];
    const u64 ukey = g_ukey;
    size_t nd = 0;
    for (u32 p = pa; p < pb; p++) {
      const u64 w = g_acc[p].load(std::memory_order_relaxed);
      if (w == 0) continue;
      const u32 mask = (u32)(w >> ACC_MSH) & ((1u << KA) - 1);
      bool general = (i64)w < 0;
      u32 cnt = 1;
      if (!general && (mask & 1)) {
        if ((w & ACC_LAB) == 0 && (mask & (mask + 1)) == 0)
          cnt = (u32)_mm_popcnt_u32(mask);
        else
          general = true;
      }
      if (general || !(mask & 1)) {
        for (u32 m = mask; m; m = _blsr_u32(m)) {
          const u32 al = (u32)_tzcnt_u32(m), lab = (u32)(w >> (4 * al)) & 15;
          if (lab != al)
            out.push_back({hash_id(id_key('u', scramble(((u64)p << 9) | al, ukey))), hash_id(id_key('u', scramble(((u64)p << 9) | lab, ukey)))});
        }
      }
      nd += !general && cnt > 1;
      g_acc[p].store(general ? ACC_POISON : ((w & ~ACC_POISON) | ((u64)cnt << ACC_NSH)), std::memory_order_relaxed);
    }
    ndense[tid] += nd;
  });
  size_t npairs = 0, nlinked = 0;
  for (auto& k : keep) npairs += k.size();
  for (size_t c : ndense) nlinked += c;
  // H from the kept links: at most two new IDs per link
  g_Hbits = std::max<u32>(10, ceil_log2(npairs * 2 + 2));
  g_H = (HSlot*)vm_alloc(sizeof(HSlot) << g_Hbits);
  {
    const u32 hshift = 64 - g_Hbits;
    const u64 hmask = (1ULL << g_Hbits) - 1;
    parallel_for(NT, [&](int t, int) {
      for (const auto& pr : keep[t]) uf_unite(h_slot_for(pr[0], hmask, hshift), h_slot_for(pr[1], hmask, hshift));
    });
  }
  LOG("links (dense): %zu lines, %zu do not fit a word, %zu links to the general table, %zu indexes with linked aliases only",
      nlinks, nkept, npairs, nlinked);
  finish_links(nlinks);
}

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
// Byte-wise order of two click ids, given as click numbers. From 36^7 on a number has eight digits, and among
// those the order of the ids is the order of the numbers (digits and letters rise with their value).
static inline bool click_less(u64 a, u64 b) {
  constexpr u64 D8 = 78364164096ULL;  // 36^7
  if (__builtin_expect(((a | b) & CLICK_IS_KEY) == 0 && a >= D8 && b >= D8, 1)) return a < b;
  return key_cmp(click_key_of(a), click_key_of(b)) < 0;
}

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
  memcpy(g_day_m[1], g_daycode[g_ymain_row], sizeof g_day_m[1]);
}

// Eight 32-bit lanes at a time (mask compares with AVX-512, plain AVX2 otherwise).
#if defined(__AVX512VL__) && defined(__AVX512DQ__) && defined(__AVX512F__)
static FORCE_INLINE __m256i movm32x8(u32 m) { return _mm256_movm_epi32((__mmask8)m); }  // -1 in the lanes of set bits
static FORCE_INLINE u32 eq32x8(__m256i a, __m256i b) { return _mm256_cmpeq_epi32_mask(a, b); }
#else
static FORCE_INLINE __m256i movm32x8(u32 m) {
  const __m256i bits = _mm256_setr_epi32(1, 2, 4, 8, 16, 32, 64, 128);
  return _mm256_cmpeq_epi32(_mm256_and_si256(_mm256_set1_epi32((int)m), bits), bits);
}
static FORCE_INLINE u32 eq32x8(__m256i a, __m256i b) { return (u32)_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(a, b))); }
#endif

// The lanes of v at the set bits of m (8 bits), moved down to lanes 0, 1, ... in order (the other lanes are
// left undefined), by a table of the lane numbers for each m. pack8x also gives those lane numbers.
// (One table load and one permute; with AVX-512 a compress instruction would do the same in about as many uops.)
struct Lanes8 {
  u64 ix[256];
  constexpr Lanes8() : ix() {
    for (u32 m = 0; m < 256; m++) {
      u32 c = 0;
      for (u32 l = 0; l < 8; l++)
        if (m >> l & 1) ix[m] |= (u64)l << (8 * c++);
    }
  }
};
alignas(64) static constexpr Lanes8 k_lanes8{};
static FORCE_INLINE __m256i lanes_of8(u32 m) { return _mm256_cvtepu8_epi32(_mm_loadl_epi64((const __m128i*)(k_lanes8.ix + m))); }
static FORCE_INLINE __m256i pack8(__m256i v, u32 m) { return _mm256_permutevar8x32_epi32(v, lanes_of8(m)); }
static FORCE_INLINE __m256i pack8x(__m256i v, u32 m, __m256i& from) {
  from = lanes_of8(m);
  return _mm256_permutevar8x32_epi32(v, from);
}
// the indexes first + j of the set bits j of m, appended at dst (which has room for 8); returns how many.
// (By the lane table with AVX-512 too, in place of a compress-store to memory.)
static FORCE_INLINE u32 list8(u32* dst, u32 m, u32 first) {
  _mm256_storeu_si256((__m256i*)dst, _mm256_add_epi32(lanes_of8(m), _mm256_set1_epi32((int)first)));
  return (u32)_mm_popcnt_u32(m);
}

// A batch of lines as the scanner hands them over: per field, a column of starts (offsets from the chunk
// base) and a column of lengths. Groups of eight lines arrive as columns of separators (scan_cols); single
// lines as two vectors of eight values.
template <int NF, u32 CAPN>
struct Cols {
  static constexpr u32 CAP = CAPN;  // lines; a batch is processed once it has more than CAP - 8
  alignas(32) u32 st[NF][CAP + 8];
  alignas(32) u32 ln[NF][CAP + 8];
  u32 n = 0;
  // (n is read once into a local: the stores into st and ln could alias it as far as the compiler knows)
  FORCE_INLINE void add_group(const __m256i* C, const u32* q) {
    const __m256i one = _mm256_set1_epi32(1);
    const u32 n0 = n;
    for (int k = 0; k < NF; k++) {
      const __m256i qm = movm32x8(q[k]);  // -1: quoted
      _mm256_storeu_si256((__m256i*)(st[k] + n0), _mm256_sub_epi32(_mm256_add_epi32(C[k], one), qm));
      _mm256_storeu_si256((__m256i*)(ln[k] + n0),
                          _mm256_add_epi32(_mm256_sub_epi32(_mm256_sub_epi32(C[k + 1], C[k]), one), _mm256_add_epi32(qm, qm)));
    }
    n = n0 + 8;
  }
  FORCE_INLINE void add_row(__m256i s, __m256i l, u32 keep) {  // (the row stays only if keep is 1)
    alignas(32) u32 a[8], b[8];
    _mm256_store_si256((__m256i*)a, s);
    _mm256_store_si256((__m256i*)b, l);
    const u32 n0 = n;
    for (int k = 0; k < NF; k++) {
      st[k][n0] = a[k];
      ln[k][n0] = b[k];
    }
    n = n0 + keep;
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
  u64 w0, w1 = (u64)(0x1000000u - code) | ((idx & ELOM) << 46);
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
    const __m256i cnot = _mm256_set1_epi64x(0xFFFFFF);  // 2^24 - code = 0xFFFFFF - (code - 1)
    for (u32 i = 0; i < n; i += 4) {
      const __m256i rel = _mm256_sub_epi64(_mm256_load_si256((const __m256i*)(B.ts + i)), vws);
      const __m256i ic1 = _mm256_sub_epi64(_mm256_load_si256((const __m256i*)(B.icode + i)), vone);
      const __m256i idx = _mm256_load_si256((const __m256i*)(B.idx + i));
      const __m256i num = _mm256_cvtepu32_epi64(_mm_load_si128((const __m128i*)(B.num + i)));
      // negative if: ts before the window, ts after it, the code out of range (either side), ingest too late
      // (the low 32 bits of ic1 times 1000 is its product wherever it matters: a code out of [1, 0xFFFFFE] is
      // caught by the two code terms below either way)
      const __m256i late = _mm256_sub_epi64(_mm256_add_epi64(late0, rel), _mm256_mul_epu32(ic1, k1000));
      const __m256i neg = _mm256_or_si256(_mm256_or_si256(_mm256_or_si256(rel, _mm256_sub_epi64(wlast, rel)),
                                                          _mm256_or_si256(ic1, _mm256_sub_epi64(cmax, ic1))),
                                          late);
      bad |= (u64)(u32)_mm256_movemask_pd(_mm256_castsi256_pd(neg)) << i;
      const __m256i w0 = _mm256_or_si256(_mm256_add_epi64(rel, vone), _mm256_slli_epi64(num, 32));
      const __m256i w1 = _mm256_or_si256(_mm256_sub_epi64(cnot, ic1), _mm256_slli_epi64(_mm256_and_si256(idx, lom), 46));
      const __m256i lo = _mm256_unpacklo_epi64(w0, w1), hi = _mm256_unpackhi_epi64(w0, w1);  // records 0, 2 / 1, 3
      _mm256_store_si256((__m256i*)(B.rec + i), _mm256_permute2x128_si256(lo, hi, 0x20));
      _mm256_store_si256((__m256i*)(B.rec + i + 2), _mm256_permute2x128_si256(lo, hi, 0x31));
      _mm_store_si128((__m128i*)(B.part + i), low32x4(_mm256_srli_epi64(idx, KE)));
    }
  }
  // ---- which lines are plain: a user and a product, the type view or cart; eight lines at a time. (purch:
  // the same with a type that starts "purc" in 8 bytes, the candidates for a purchase.)
  u64 plain = 0, kept = 0, purch = 0;  // kept: the line has an event_id (a line without one is dropped)
  {
    const __m256i zero = _mm256_setzero_si256(), c4 = _mm256_set1_epi32(4), c8 = _mm256_set1_epi32(8);
    const __m256i view = _mm256_set1_epi32(0x77656976), cart = _mm256_set1_epi32(0x74726163), purc = _mm256_set1_epi32(0x63727570);
    for (u32 i = 0; i < n; i += 8) {
      const __m256i w4 = _mm256_load_si256((const __m256i*)(B.w4 + i)), l4 = _mm256_load_si256((const __m256i*)(B.ln[4] + i));
      const u32 simple = eq32x8(l4, c4) & (eq32x8(w4, view) | eq32x8(w4, cart));
      const u32 empty = eq32x8(_mm256_load_si256((const __m256i*)(B.ln[1] + i)), zero) | eq32x8(_mm256_load_si256((const __m256i*)(B.ln[5] + i)), zero);
      const u32 noid = eq32x8(_mm256_load_si256((const __m256i*)(B.ln[0] + i)), zero);
      plain |= (u64)(simple & ~empty & 0xFF) << i;
      purch |= (u64)(eq32x8(l4, c8) & eq32x8(w4, purc) & ~empty & 0xFF) << i;
      kept |= (u64)(~noid & 0xFF) << i;
    }
  }
  kept &= nmask;
  plain &= kept & ~(irr | bad);
  purch &= kept & ~(irr | bad);
  // ---- the scatter
  auto cur = cur_out;
  // (c: a copy of the cursor whose address never escapes, so that the compiler keeps it in registers;
  // cur itself is handed to ev_slow below)
  auto c = cur;
#if SS_PFD > 0
  if (plain) {
    // the staging line of the record SS_PFD records ahead is fetched while this one is pushed (when there is
    // none left, the current one's again: harmless, and no branch)
    u64 ma = plain;
    for (int k = 0; k < SS_PFD; k++) {
      WCScatter<D16>::prefetch(c, B.part[(u32)_tzcnt_u64(ma ? ma : plain)]);
      ma = _blsr_u64(ma);
    }
    for (u64 m = plain; m; m = _blsr_u64(m)) {
      const u32 i = (u32)_tzcnt_u64(m);
      WCScatter<D16>::prefetch(c, B.part[(u32)_tzcnt_u64(ma ? ma : m)]);
      ma = _blsr_u64(ma);
      WCScatter<D16>::push16(c, B.part[i], _mm_load_si128((const __m128i*)&B.rec[i]));
    }
  }
#else
  for (u64 m = plain; m; m = _blsr_u64(m)) {
    const u32 i = (u32)_tzcnt_u64(m);
    WCScatter<D16>::push16(c, B.part[i], _mm_load_si128((const __m128i*)&B.rec[i]));
  }
#endif
  // purchases with a valid price whose category and price fit the record: the plain record and the payload.
  // Anything else about them (a bad price, a payload that does not fit) is left to ev_slow, as before.
  for (u64 m = purch; m; m = _blsr_u64(m)) {
    const u32 i = (u32)_tzcnt_u64(m);
    u64 w8;
    memcpy(&w8, base + B.st[4][i], 8);
    u32 price;
    if (w8 != 0x6573616863727570ULL || !price_fast(base + B.st[6][i], B.ln[6][i], price)) continue;  // "purchase"
    const u32 cat = cat_fast(base + B.st[5][i], B.ln[5][i]);
    if (cat >= 32 || price >= (1u << 17)) continue;
    plain |= 1ULL << i;
    const u64 w1 = B.rec[i].w1 | ((u64)((cat << 17) | price) << 24);
    WCScatter<D16>::push16(c, B.part[i], _mm_set_epi64x((long long)w1, (long long)B.rec[i].w0));
  }
  cur = c;
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
  alignas(32) u64 cq[CAP + 8];               // the 8 bytes after the "c_" of campaign_id
  alignas(32) u8 pfx[CAP + 8];               // nonzero: a prefix byte is not the expected one
  alignas(32) u8 cpx[CAP + 8];               // nonzero: campaign_id does not start with "c_"
  alignas(32) u64 kv[CAP + 8];   // number of the click_id
  alignas(32) u64 cv[CAP + 8];   // number of the campaign_id (when it is canonical)
  alignas(32) u32 num[CAP + 8];  // user number
  alignas(32) i32 camp[CAP + 8];  // campaign index, -1 if not a campaign
  alignas(32) i64 ts[CAP + 8];
  alignas(32) C16 rec[CAP + 8];   // the record of a line
  alignas(32) u32 part[CAP + 8];  // ... and its partition
  u32 lst[4][CAP + 8];
};

// Four campaign numbers: "c_" and then 1 to 8 decimal digits, no leading zero unless the number is 0 itself.
// raw: the 8 bytes after "c_"; ln: the lengths of the whole IDs. Returns the lanes that are not of that form
// (their value is unspecified); the form itself is what camp_of accepts on its fast path.
static FORCE_INLINE u32 dec10x4(const u64* raw, const u32* ln, __m256i& val) {
  // the digits go to the top bytes: the first one to byte 8 - nd, the bytes before it become '0'
  const __m256i sh = _mm256_sub_epi64(_mm256_set1_epi64x(80), _mm256_slli_epi64(_mm256_cvtepu32_epi64(_mm_load_si128((const __m128i*)ln)), 3));
  const __m256i x = _mm256_or_si256(_mm256_sllv_epi64(_mm256_load_si256((const __m256i*)raw), sh),
                                    _mm256_andnot_si256(_mm256_sllv_epi64(_mm256_set1_epi64x(-1), sh), _mm256_set1_epi8('0')));
  const __m256i d = _mm256_sub_epi8(x, _mm256_set1_epi8('0'));
  const __m256i over = _mm256_subs_epu8(d, _mm256_set1_epi8(9));                              // a byte that is no digit
  const __m256i lead = _mm256_and_si256(_mm256_srlv_epi64(d, sh), _mm256_set1_epi64x(0xFF));  // the first digit
  const __m256i one = _mm256_set1_epi64x(56);  // the shift of a single digit
  const u32 bad = nz64x4(over) | ((0xF ^ nz64x4(lead)) & nz64x4(_mm256_xor_si256(sh, one))) | gtu64x4(sh, one);
  const __m256i m1 = _mm256_maddubs_epi16(d, _mm256_set1_epi16(0x010A));     // digit pairs: 10 * hi + lo
  const __m256i m2 = _mm256_madd_epi16(m1, _mm256_set1_epi32(0x00010064));  // four digits: 100 * hi + lo
  val = _mm256_add_epi64(_mm256_mul_epu32(m2, _mm256_set1_epi64x(10000)), _mm256_srli_epi64(m2, 32));
  return bad;
}

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
    const u8* const s2 = base + B.st[2][i];
    memcpy(&B.kq[i], s0 + 1, 8);
    memcpy(&B.uq[i], s1 + 1, 8);
    memcpy(&B.cq[i], s2 + 2, 8);
    u16 c2;
    memcpy(&c2, s2, 2);
    B.pfx[i] = (u8)((s0[0] ^ 'k') | (s1[0] ^ 'u'));
    B.cpx[i] = (u8)(c2 != 0x5F63);  // "c_"
  }
  u64 irr = nonzero64(B.pfx);
  u64 cslow = nonzero64(B.cpx);  // campaign IDs that are not "c_" + a canonical number: camp_of decides
  {
    const __m256i ukey = _mm256_set1_epi64x((long long)g_ukey);
    const __m256i M = _mm256_set1_epi64x((long long)M40);
    const __m256i umax = _mm256_set1_epi64x((long long)(((u64)g_nu_cap << 9) - 1));
    for (u32 i = 0; i < n; i += 4) {
      __m256i vk, vu, vc;
      u32 bk = ids4(B.kq + i, B.ln[0] + i, vk);
      u32 bu = ids4(B.uq + i, B.ln[1] + i, vu);
      const u32 bc = dec10x4(B.cq + i, B.ln[2] + i, vc);
      const __m256i num = unscramble4(vu, ukey);
      bk |= gtu64x4(vk, M);
      bu |= gtu64x4(vu, M) | gtu64x4(num, umax);
      _mm256_store_si256((__m256i*)(B.kv + i), vk);
      _mm256_store_si256((__m256i*)(B.cv + i), vc);
      _mm_store_si128((__m128i*)(B.num + i), low32x4(num));
      irr |= (u64)(bk | bu) << i;
      cslow |= (u64)bc << i;
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
  static const i32 no_camp = -1;  // (the table can be empty)
  const u64 ncamp_tab = g_camp_by_num.size();
  const i32* const camp_tab = ncamp_tab ? g_camp_by_num.data() : &no_camp;
  // the campaigns: a canonical number looks up the table (out of it: not a campaign); anything else -> camp_of
  for (u32 i = 0; i < n; i++) {
    const u64 cn = B.cv[i];
    const bool in_tab = cn < ncamp_tab;
    const i32 c = camp_tab[in_tab ? cn : 0];
    B.camp[i] = in_tab ? c : -1;
  }
  for (u64 m = cslow & kept; m; m = _blsr_u64(m)) {
    const u32 i = (u32)_tzcnt_u64(m);
    B.camp[i] = camp_of(base + B.st[2][i], B.ln[2][i]);
  }
  // the records, four lines at a time; a line is good when its ts is in the click window and it has a campaign
  u64 good = 0;
  {
    const __m256i vcws = _mm256_set1_epi64x(cws), vwlen = _mm256_set1_epi64x((long long)wlen), lo32 = _mm256_set1_epi64x(0xFFFFFFFFLL);
    for (u32 i = 0; i < n; i += 4) {
      const __m256i rel = _mm256_sub_epi64(_mm256_load_si256((const __m256i*)(B.ts + i)), vcws);
      const __m128i c4 = _mm_load_si128((const __m128i*)(B.camp + i));
      const __m128i n4 = _mm_load_si128((const __m128i*)(B.num + i));
      // rel in [0, wlen) (unsigned), and a campaign (sign bit clear)
      const u32 ok = gtu64x4(vwlen, rel) & ~(u32)_mm_movemask_ps(_mm_castsi128_ps(c4)) & 0xF;
      const __m256i w0 = _mm256_or_si256(_mm256_and_si256(rel, lo32), _mm256_slli_epi64(_mm256_cvtepu32_epi64(n4), 32));
      const __m256i w1 = _mm256_or_si256(_mm256_load_si256((const __m256i*)(B.kv + i)), _mm256_slli_epi64(_mm256_cvtepu32_epi64(c4), 40));
      const __m256i lo = _mm256_unpacklo_epi64(w0, w1), hi = _mm256_unpackhi_epi64(w0, w1);  // records 0, 2 / 1, 3
      _mm256_store_si256((__m256i*)(B.rec + i), _mm256_permute2x128_si256(lo, hi, 0x20));
      _mm256_store_si256((__m256i*)(B.rec + i + 2), _mm256_permute2x128_si256(lo, hi, 0x31));
      _mm_store_si128((__m128i*)(B.part + i), _mm_srli_epi32(n4, 9 + KC));
      good |= (u64)ok << i;
    }
  }
  good &= kept;
  auto cur = cur_out;
#if SS_PFD > 0
  if (const u64 gm = good & ~irr) {
    u64 ma = gm;
    for (int k = 0; k < SS_PFD; k++) {
      WCScatter<C16>::prefetch(cur, B.part[(u32)_tzcnt_u64(ma ? ma : gm)]);
      ma = _blsr_u64(ma);
    }
    for (u64 m = gm; m; m = _blsr_u64(m)) {
      const u32 i = (u32)_tzcnt_u64(m);
      WCScatter<C16>::prefetch(cur, B.part[(u32)_tzcnt_u64(ma ? ma : m)]);
      ma = _blsr_u64(ma);
      WCScatter<C16>::push16(cur, B.part[i], _mm_load_si128((const __m128i*)&B.rec[i]));
    }
  }
#else
  for (u64 m = good & ~irr; m; m = _blsr_u64(m)) {
    const u32 i = (u32)_tzcnt_u64(m);
    WCScatter<C16>::push16(cur, B.part[i], _mm_load_si128((const __m128i*)&B.rec[i]));
  }
#endif
  for (u64 m = good & irr; m; m = _blsr_u64(m)) {
    const u32 i = (u32)_tzcnt_u64(m);
    ck_slow(tl, B, i, base, (u32)((u64)B.ts[i] - (u64)cws), (u32)B.camp[i]);
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
  // The click files come last, so their chunks decide how far apart the threads finish: they are smaller.
  const size_t chunk_ck = std::max<size_t>(1 << 18, std::min<size_t>(chunk, ck_bytes / (NT * 32 + 1)));
#if SS_PREFAULT
  // what a thread's arena will hold: its share of the records (16 bytes per line of about 72) and an open block
  // per partition, of which 90%
  g_arena_prefault = (size_t)(0.9 * ((double)(ev_bytes + ck_bytes) / 72.0 * 16.0 / NT +
                                     (double)(((ev_bytes / 72) >> KE) + g_PC) * BLOCK_BYTES));
#endif
  std::vector<Chunk> ce, cc;
  make_chunks(ev, chunk, ce);
  make_chunks(ck, chunk_ck, cc);
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
#if SS_PREFAULT
  {
    int k = 0;
    for (auto& t : g_tl) k += t.arena.prefaulted;
    LOG("arenas prefaulted: %d of %d, %zu MB each", k, NT, g_arena_prefault >> 20);
  }
#endif
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
// ---- a dense person's clicks, for the scan
// A click as one word: time << 32 | its number among the person's clicks << 12 | campaign. The number makes
// a person's keys all different and leads to the click_id, which is kept beside the keys and wanted only
// when two clicks of a person have the same time. (So a person can have CK_ORD clicks, the largest person of
// a 20M set has about 60,000, and there can be CK_CAMP campaigns; with more, the general pipeline does the
// job.)
static constexpr u32 CK_ORD = 0xFFFFF, CK_CAMP = 0xFFF;
// Click times are below this (they count from 7 days before a window of less than 2^31 ms); the times from
// here on are for the keys that the sorts put after a person's clicks.
static constexpr u32 CK_TS_END = 0xFFFFFC00u;
static FORCE_INLINE u32 ck_ts(const u64 k) { return (u32)(k >> 32); }
static FORCE_INLINE u32 ck_camp(const u64 k) { return (u32)k & CK_CAMP; }
static FORCE_INLINE u32 ck_ord(const u64 k) { return (u32)(k >> 12) & CK_ORD; }
static FORCE_INLINE u64 ck_make(const u32 ts, const u32 ord, const u32 camp) { return ((u64)ts << 32) | ((u64)ord << 12) | camp; }

// Keys are compared as unsigned numbers. Without AVX-512 there is only the signed compare: the sorts then
// work on keys with the top bit flipped (key_in, which is its own inverse).
#if defined(__AVX512VL__) && defined(__AVX512F__)
static FORCE_INLINE __m256i key_in(const __m256i v) { return v; }
static FORCE_INLINE void minmax64(const __m256i a, const __m256i b, __m256i& lo, __m256i& hi) {
  lo = _mm256_min_epu64(a, b);
  hi = _mm256_max_epu64(a, b);
}
#else
static FORCE_INLINE __m256i key_in(const __m256i v) { return _mm256_xor_si256(v, _mm256_set1_epi64x((long long)0x8000000000000000ULL)); }
static FORCE_INLINE void minmax64(const __m256i a, const __m256i b, __m256i& lo, __m256i& hi) {
  const __m256i m = _mm256_cmpgt_epi64(a, b);
  lo = _mm256_blendv_epi8(a, b, m);
  hi = _mm256_blendv_epi8(b, a, m);
}
#endif
// Two runs of four keys, each of them first rising and then falling (or the other way round), put in rising
// order: lo -> a, hi -> b.
static FORCE_INLINE void finish4x2(const __m256i lo, const __m256i hi, __m256i& a, __m256i& b) {
  __m256i l = _mm256_permute2x128_si256(lo, hi, 0x20), h = _mm256_permute2x128_si256(lo, hi, 0x31), x, y;
  minmax64(l, h, x, y);  // two lanes apart
  l = _mm256_unpacklo_epi64(x, y);
  h = _mm256_unpackhi_epi64(x, y);
  minmax64(l, h, x, y);  // neighbours
  l = _mm256_unpacklo_epi64(x, y);
  h = _mm256_unpackhi_epi64(x, y);
  a = _mm256_permute2x128_si256(l, h, 0x20);
  b = _mm256_permute2x128_si256(l, h, 0x31);
}
// Eight keys (a, then b) into rising order: a bitonic network, six layers of four compares, no branch.
static FORCE_INLINE void sort8(__m256i& a, __m256i& b) {
  __m256i lo, hi, l, h;
  minmax64(a, b, lo, hi);                                    // pairs
  minmax64(lo, _mm256_permute4x64_epi64(hi, 0xB1), lo, hi);  // two pairs: the lower and the upper two of four
  l = _mm256_unpacklo_epi64(lo, hi);
  h = _mm256_unpackhi_epi64(lo, hi);
  minmax64(l, h, lo, hi);             // fours in order: lo0 hi0 lo1 hi1, lo2 hi2 lo3 hi3
  l = _mm256_unpacklo_epi64(lo, hi);  // first four, lanes 0 1; second four, lanes 0 1
  h = _mm256_unpackhi_epi64(lo, hi);  // ... lanes 2 3
  const __m256i p = _mm256_permute2x128_si256(l, h, 0x20);                                  // the first four
  const __m256i q = _mm256_permute4x64_epi64(_mm256_permute2x128_si256(l, h, 0x31), 0x1B);  // the second, backwards
  minmax64(p, q, lo, hi);  // the lower and the upper four of eight
  finish4x2(lo, hi, a, b);
}
// Sixteen keys, a b and c d each in rising order, into rising order a b c d.
static FORCE_INLINE void merge16(__m256i& a, __m256i& b, __m256i& c, __m256i& d) {
  __m256i lo0, hi0, lo1, hi1, l0, l1, h0, h1;
  minmax64(a, _mm256_permute4x64_epi64(d, 0x1B), lo0, hi0);  // key i of the first eight with key 7 - i of the second
  minmax64(b, _mm256_permute4x64_epi64(c, 0x1B), lo1, hi1);
  minmax64(lo0, lo1, l0, l1);  // the lower eight: four lanes apart
  minmax64(hi0, hi1, h0, h1);  // the upper eight
  finish4x2(l0, l1, a, b);
  finish4x2(h0, h1, c, d);
}
#if defined(__clang__)
#define DN_UNROLL _Pragma("clang loop unroll(full)")
#else
#define DN_UNROLL _Pragma("GCC unroll 64")
#endif
// Two runs of M vectors (M even), w[0 .. M) and w[M .. 2M), each in rising order, into one: the same
// bitonic merge for any length. (The lengths are known to the compiler, which writes the loops out.)
template <u32 M>
static FORCE_INLINE void merge_runs(__m256i* const w) {
  DN_UNROLL
  for (u32 i = 0; i < M / 2; i++) {  // key i of the first run with the key that far from the end of the second
    const __m256i a = w[i], b = w[M - 1 - i], c = w[M + i], d = w[2 * M - 1 - i];
    minmax64(a, _mm256_permute4x64_epi64(d, 0x1B), w[i], w[M + i]);
    minmax64(b, _mm256_permute4x64_epi64(c, 0x1B), w[M - 1 - i], w[2 * M - 1 - i]);
  }
  DN_UNROLL
  for (u32 d = M / 2; d >= 1; d /= 2) {  // halves, quarters, ... down to neighbouring vectors
    DN_UNROLL
    for (u32 b = 0; b < 2 * M; b += 2 * d) {
      DN_UNROLL
      for (u32 i = 0; i < d; i++) {
        const __m256i x = w[b + i], y = w[b + i + d];
        minmax64(x, y, w[b + i], w[b + i + d]);
      }
    }
  }
  DN_UNROLL
  for (u32 i = 0; i < 2 * M; i += 2) {
    const __m256i x = w[i], y = w[i + 1];
    finish4x2(x, y, w[i], w[i + 1]);
  }
}
// NV vectors of keys (8 or 16) into rising order, as one straight run of steps.
template <u32 NV>
static FORCE_INLINE void sort_vectors(__m256i* const w) {
  DN_UNROLL
  for (u32 i = 0; i < NV; i += 4) {
    sort8(w[i], w[i + 1]);
    sort8(w[i + 2], w[i + 3]);
    merge16(w[i], w[i + 1], w[i + 2], w[i + 3]);
  }
  DN_UNROLL
  for (u32 b = 0; b < NV; b += 8) merge_runs<4>(w + b);
  if (NV >= 16) {
    DN_UNROLL
    for (u32 b = 0; b < NV; b += 16) merge_runs<8>(w + b);
  }
}
// The same merge and sort with loops, for the few persons with more than 64 clicks (written out, a network
// of 256 keys is 25 KB of code that would be cold every time it is wanted).
static NOINLINE void merge_runs_any(__m256i* const w, const u32 m) {
  for (u32 i = 0; i < m / 2; i++) {
    const __m256i a = w[i], b = w[m - 1 - i], c = w[m + i], d = w[2 * m - 1 - i];
    minmax64(a, _mm256_permute4x64_epi64(d, 0x1B), w[i], w[m + i]);
    minmax64(b, _mm256_permute4x64_epi64(c, 0x1B), w[m - 1 - i], w[2 * m - 1 - i]);
  }
  for (u32 d = m / 2; d >= 1; d /= 2)
    for (u32 b = 0; b < 2 * m; b += 2 * d)
      for (u32 i = 0; i < d; i++) {
        const __m256i x = w[b + i], y = w[b + i + d];
        minmax64(x, y, w[b + i], w[b + i + d]);
      }
  for (u32 i = 0; i < 2 * m; i += 2) {
    const __m256i x = w[i], y = w[i + 1];
    finish4x2(x, y, w[i], w[i + 1]);
  }
}
static NOINLINE void sort_vectors_any(__m256i* const w, const u32 nv) {  // nv: a power of two, at least 4
  for (u32 i = 0; i < nv; i += 4) {
    sort8(w[i], w[i + 1]);
    sort8(w[i + 2], w[i + 3]);
    merge16(w[i], w[i + 1], w[i + 2], w[i + 3]);
  }
  for (u32 m = 4; m < nv; m *= 2)
    for (u32 b = 0; b < nv; b += 2 * m) merge_runs_any(w + b, m);
}
// For each lane of v: bit 2 * lane + 1 is set where its key has the time of the key after it (the first
// key of nx, after the last lane).
static FORCE_INLINE u32 same_time_next(const __m256i v, const __m256i nx) {
  const __m256i s = _mm256_blend_epi32(_mm256_permute4x64_epi64(v, 0x39), _mm256_permute4x64_epi64(nx, 0x00), 0xC0);
  return (u32)_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(v, s))) & 0xAA;
}
// The lanes pos .. pos + 3 (below NET_MAX) of a list of n keys. A lane from n on gets a key above every
// click's, with a time of its own: 0xFFFFFFFF - lane (so no two of those look like clicks of one time).
static FORCE_INLINE __m256i keys_padded(const u64* in, const int pos, const u32 n) {
  const __m256i idx = _mm256_setr_epi64x(pos, pos + 1, pos + 2, pos + 3);
  const __m256i past = _mm256_cmpgt_epi64(idx, _mm256_set1_epi64x((long long)n - 1));
  const __m256i pad = _mm256_xor_si256(_mm256_slli_epi64(idx, 32), _mm256_set1_epi64x(-1));
  return key_in(_mm256_blendv_epi8(_mm256_loadu_si256((const __m256i*)(in + pos)), pad, past));
}
// The five keys after a person's last click: times that no click has, all different, the first one
// 0xFFFFFFFF. (Different, so that they never look like clicks of one time.)
static FORCE_INLINE void end_keys(u64* out) {
  for (u32 i = 0; i < 5; i++) out[i] = ck_make(0xFFFFFFFFu - i, CK_ORD, CK_CAMP);
}
// Of four keys at p and the four after them, lane by lane: bit 2 * i + 1 is set where key i and key i + 1
// have the same time.
static FORCE_INLINE u32 same_time4(const u64* p) {
  const __m256i a = _mm256_loadu_si256((const __m256i*)p), b = _mm256_loadu_si256((const __m256i*)(p + 1));
  return (u32)_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(a, b))) & 0xAA;
}
static constexpr u32 NET_MAX = 256;  // up to here a sorting network; SORT_ROOM words are read and written at most
static constexpr u32 SORT_ROOM = NET_MAX + 16;
template <u32 NV>
static NOINLINE void sort_keys_net(const u64* in, const u32 n, u64* out) {
  __m256i w[NV];
  DN_UNROLL
  for (u32 i = 0; i < NV; i++) w[i] = keys_padded(in, (int)(4 * i), n);
  sort_vectors<NV>(w);
  DN_UNROLL
  for (u32 i = 0; i < NV; i++) _mm256_storeu_si256((__m256i*)(out + 4 * i), key_in(w[i]));
}
// One person's clicks (n > 0 keys in any order, all different) into rising order at out, the end keys after
// them. Returns whether any two of them have the same time (not 0: yes). SORT_ROOM words from `in` on are
// readable, and out has room for n + SORT_ROOM; tmp is scratch.
//
// Up to NET_MAX clicks, which is nearly every person, a sorting network over the next power of two of keys
// (the rest of it filled with keys above every click's): no branch that depends on the data, no table, and
// up to 16 keys nothing through memory. Beyond that a radix sort by the time (clicks of one time keep the
// order they came in).
static NOINLINE u32 sort_click_keys(const u64* in, const u32 n, u64* out, std::vector<u64>& tmp) {
  // (a key with a time that neither a click nor a lane after the last key has)
  const __m256i none = key_in(_mm256_set1_epi64x((long long)ck_make(CK_TS_END, 0, 0)));
  static_assert(0xFFFFFFFFu - NET_MAX > CK_TS_END, "the lanes after the last key have times above CK_TS_END");
  u32 tie = 0;
  if (n <= 8) {
    __m256i a = keys_padded(in, 0, n), b = keys_padded(in, 4, n);
    sort8(a, b);
    tie = same_time_next(a, b) | same_time_next(b, none);
    _mm256_storeu_si256((__m256i*)out, key_in(a));
    _mm256_storeu_si256((__m256i*)(out + 4), key_in(b));
  } else if (n <= 16) {
    __m256i a = keys_padded(in, 0, n), b = keys_padded(in, 4, n), c = keys_padded(in, 8, n), d = keys_padded(in, 12, n);
    sort8(a, b);
    sort8(c, d);
    merge16(a, b, c, d);
    tie = same_time_next(a, b) | same_time_next(b, c) | same_time_next(c, d) | same_time_next(d, none);
    _mm256_storeu_si256((__m256i*)out, key_in(a));
    _mm256_storeu_si256((__m256i*)(out + 4), key_in(b));
    _mm256_storeu_si256((__m256i*)(out + 8), key_in(c));
    _mm256_storeu_si256((__m256i*)(out + 12), key_in(d));
  } else {
    if (n <= 32) {
      sort_keys_net<8>(in, n, out);
    } else if (n <= 64) {
      sort_keys_net<16>(in, n, out);
    } else if (n <= NET_MAX) {
      __m256i w[NET_MAX / 4];
      const u32 nv = n <= 128 ? 32 : 64;
      for (u32 i = 0; i < nv; i++) w[i] = keys_padded(in, (int)(4 * i), n);
      sort_vectors_any(w, nv);
      for (u32 i = 0; i < nv; i++) _mm256_storeu_si256((__m256i*)(out + 4 * i), key_in(w[i]));
    } else {
      // LSD radix sort by the time, 8 bits a pass; the counts of all four passes are taken in one go
      if (tmp.size() < 2 * (size_t)n) tmp.resize(2 * (size_t)n);
      u64* const a = tmp.data();
      u64* const b = a + n;
      u32 cnt[4][256];
      memset(cnt, 0, sizeof cnt);
      for (u32 i = 0; i < n; i++) {
        const u32 t = ck_ts(in[i]);
        cnt[0][t & 255]++;
        cnt[1][(t >> 8) & 255]++;
        cnt[2][(t >> 16) & 255]++;
        cnt[3][t >> 24]++;
      }
      const u64* src = in;
      u64* const dst[4] = {a, b, a, out};
      for (int pass = 0; pass < 4; pass++) {
        u32* const c = cnt[pass];
        u32 sum = 0;
        for (int k = 0; k < 256; k++) {
          const u32 x = c[k];
          c[k] = sum;
          sum += x;
        }
        const int sh = 32 + 8 * pass;
        u64* const d = dst[pass];
        for (u32 i = 0; i < n; i++) d[c[(src[i] >> sh) & 255]++] = src[i];
        src = d;
      }
    }
    end_keys(out + n);
    for (u32 base = 0; base + 1 < n; base += 4) tie |= same_time4(out + base);
    return tie;
  }
  end_keys(out + n);
  return tie;
}
// How many of a person's clicks in time order (mc of them, then the end keys) have a time of at most x
// (which is below the end keys' times).
static FORCE_INLINE u32 ck_count_le(const u64* sk, const u32 mc, const u32 x) {
  const u64 bound = ((u64)x << 32) | 0xFFFFFFFFu;
  if (mc > 16) return (u32)(std::upper_bound(sk, sk + mc, bound) - sk);
  const __m256i vb = key_in(_mm256_set1_epi64x((long long)bound));
  u32 c = 0;
  for (u32 v = 0; v < mc; v += 4) {
    const __m256i k = key_in(_mm256_loadu_si256((const __m256i*)(sk + v)));
#if defined(__AVX512VL__) && defined(__AVX512F__)
    c += (u32)_mm_popcnt_u32(_mm256_cmp_epu64_mask(k, vb, _MM_CMPINT_LE));
#else
    c += 4 - (u32)_mm_popcnt_u32((u32)_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(k, vb))));
#endif
  }
  return c;
}

// the scan: per event partition, records -> slots (dedup, order) -> persons, sessions, rows

struct DS {
  D16* T = nullptr;
  std::vector<std::pair<const D16*, u32>> blk;
  // the open person: its sessions so far
  std::vector<u32> sess_ts;  // the time of each session's first event
  std::vector<u32> sess_ev;  // how many events the person had before it
  struct Pur {
    u32 sess, cat, price;
  };
  std::vector<Pur> pur;     // its purchases, in event order
  std::vector<i64> catsum;  // per category: revenue in the row being built
  std::vector<u32> tcat;    // the categories that have some
  std::vector<u32> sw;                // the session starts once more, 7 days later
  // Per campaign: the sessions below this number are counted as assisted by it, where the open person's
  // sessions have the numbers cbase .. cbase + ns - 1 (so what an earlier person left is below all of them,
  // and nothing has to be cleared from one person to the next).
  std::vector<u32> cov;
  u32 cbase = 0;
  std::vector<u64> sk;    // the open person's clicks in time order (as keys, see ck_ts), then the end keys
  std::vector<u64> xk;    // ... and before the sort, if the person has extra clicks
  std::vector<u64> rtmp;  // scratch of the sort
  // Per campaign (the last one is organic), two lines of 16 counters, one per day of the window: sessions
  // it assisted, sessions credited to it. The click loop adds to lines16, 64 bytes per campaign. A person
  // adds to a day's counters at most the sessions it has that day, so no counter is above the sessions per
  // day of the persons since the 16-bit counters were last cleared: that sum is kept in day_sum, and before it
  // can pass 16 bits the counters are added to the 32-bit ones in `lines` and cleared (flush_lines; with 8
  // threads that happens about once per 40M events). `lines` goes to the thread's tables when the scan is over.
  u16* lines16 = nullptr;
  u32* lines = nullptr;
  alignas(32) u16 day_sum[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  u32 since = 0;  // persons closed (the check of day_sum is made every LINES_EVERY of them)
  std::vector<u64> xq;     // the person's extra events in time order
  // the click partition at hand: the clicks of its dense persons as keys, grouped by person index, and the
  // number of each one's click_id in the same places
  u32 cp = NONE;
  std::vector<u32> coff, cpos;
  std::vector<u64> ckey, cclk;
  std::vector<std::pair<const C16*, u32>> cblk;
};

// day_sum is looked at every LINES_EVERY persons, and the counters are flushed if a day's sum is above
// LINES_LIMIT: a person has at most 48 sessions on a day (their starts are more than 30 minutes apart), so the
// sum cannot pass 16 bits before the next look.
static constexpr u32 LINES_EVERY = 64;
static constexpr u32 LINES_LIMIT = 0xFFFF - 48 * LINES_EVERY;
// lines16 into lines, for the NC campaigns and the organic line.
static NOINLINE void flush_lines(DS& S, const u32 NC) {
  u16* const l16 = S.lines16;
  u32* const l32 = S.lines;
  const __m256i z = _mm256_setzero_si256();
  for (u32 c = 0; c <= NC; c++) {
    u16* const a = l16 + (size_t)c * 32;
    u32* const b = l32 + (size_t)c * 32;
    for (u32 h = 0; h < 32; h += 8)
      _mm256_store_si256((__m256i*)(b + h), _mm256_add_epi32(_mm256_load_si256((const __m256i*)(b + h)),
                                                             _mm256_cvtepu16_epi32(_mm_load_si128((const __m128i*)(a + h)))));
    _mm256_store_si256((__m256i*)a, z);
    _mm256_store_si256((__m256i*)(a + 16), z);
  }
  _mm256_store_si256((__m256i*)S.day_sum, z);
}

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
    // (the blocks were written long ago: the whole next one is asked for while this one is placed)
    const char* const nx = (const char*)S.blk[bi + 1].first;
    const u32 nxb = S.blk[bi + 1].second * (u32)sizeof(D16);
    for (u32 o = 0; o < nxb; o += 64) _mm_prefetch(nx + o, _MM_HINT_T0);
    const D16* r = S.blk[bi].first;
    const D16* const re = r + S.blk[bi].second;
    u8* const Tb = (u8*)T;
    auto put = [Tb](const D16* q) __attribute__((always_inline)) {
      const u64 w0 = q->w0, w1 = q->w1;
      D16* const s = (D16*)(Tb + ((w1 >> 42) & ((u64)ELOM << 4)));  // (the slot is bits 46 to 59)
      const u64 o0 = s->w0, o1 = s->w1;
      // (the field is 2^24 - code: the earlier ingest_ts is the larger, an empty slot 0, below them all)
      const bool take = _bzhi_u32((u32)w1, 24) > _bzhi_u32((u32)o1, 24);
      s->w0 = take ? w0 : o0;
      s->w1 = take ? w1 : o1;
    };
    for (; r + 2 <= re; r += 2) {  // (in order: two records may have the same slot)
      put(r);
      put(r + 1);
    }
    if (r < re) put(r);
  }
  return true;
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

// Click partition cp for the scan, in S: the clicks of its dense persons as keys, grouped by person index (a
// counting sort), each person's in the order they come: those of index g are ckey[coff[g] .. cpos[g]). The
// first thread to load a partition also hands its other clicks to the general person phase.
static NOINLINE void load_clicks(DS& S, u32 cp, WCScatter<R2C>::Cursor& c2c, u32 p2shift) {
  S.cp = cp;
  const u32 G = 1u << KC;
  if (S.coff.size() < G + 2) {
    S.coff.assign(G + 2, 0);
    S.cpos.assign(G + 2, 0);
  }
  u32* const coff = S.coff.data();
  u32* const cpos = S.cpos.data();
  S.cblk.clear();
  for (int t = 0; t < NT; t++)
    for (auto& b : g_tl[t].ck.blocks[cp])
      if (b.second) S.cblk.push_back({b.first, b.second});
  const size_t nblk = S.cblk.size();
  if (nblk) S.cblk.push_back(S.cblk[0]);  // (something harmless to prefetch after the last block)
  u8 cntv[1u << KC];  // how many IDs the dense person of each index has (0: none)
  for (u32 g = 0; g < G; g++) cntv[g] = (u8)dense_cnt(g_acc[(cp << KC) | g].load(std::memory_order_relaxed));
  u32 cnt[1u << KC];  // ... and how many clicks there are with that index, the dense person's or not
  memset(cnt, 0, sizeof cnt);
  for (size_t bi = 0; bi < nblk; bi++) {
    // the blocks were written long ago: the whole next one is asked for while this one is counted
    const char* const nx = (const char*)S.cblk[bi + 1].first;
    const u32 nxb = S.cblk[bi + 1].second * (u32)sizeof(C16);
    for (u32 o = 0; o < nxb; o += 64) _mm_prefetch(nx + o, _MM_HINT_T0);
    const C16* const r = S.cblk[bi].first;
    const u32 m = S.cblk[bi].second;
    for (u32 j = 0; j < m; j++) cnt[((u32)(r[j].w0 >> 41) & (G - 1))]++;
  }
  u32 nd = 0, most = 0;
  for (u32 g = 0; g < G; g++) {
    coff[g] = cpos[g] = nd;
    nd += cnt[g];
    most |= cnt[g];
  }
  coff[G] = nd;
  if (__builtin_expect(most > CK_ORD, 0)) {  // (a key has 20 bits for the click's number)
    fail("a person with more than a million clicks");
    for (u32 g = 0; g <= G; g++) coff[g] = cpos[g] = 0;
    return;
  }
  if (S.ckey.size() < (size_t)nd + SORT_ROOM) {
    S.ckey.resize((size_t)nd + SORT_ROOM + (nd >> 2));
    S.cclk.resize((size_t)nd + SORT_ROOM + (nd >> 2));
  }
  u64* const ckey = S.ckey.data();
  u64* const cclk = S.cclk.data();
  const bool emit = !g_ck_emitted[cp].exchange(1, std::memory_order_relaxed);
  for (size_t bi = 0; bi < nblk; bi++) {
    const C16* const r = S.cblk[bi].first;
    const u32 m = S.cblk[bi].second;
    for (u32 j = 0; j < m; j++) {
      const u64 w0 = r[j].w0, w1 = r[j].w1;
      const u32 un = (u32)(w0 >> 32), g = (un >> 9) & (G - 1);
      const u32 dense = (un & 511) < cntv[g];
      const u32 q = cpos[g];  // (a click that is not the dense person's is overwritten by the next one, or stays past the end)
      ckey[q] = ck_make((u32)w0, q - coff[g], (u32)(w1 >> 40));
      cclk[q] = w1 & M40;
      cpos[g] = q + dense;
      if (__builtin_expect(!dense, 0) && emit)
        push_r2c(c2c, p2shift, person_of(hash_id(id_key('u', scramble(un, g_ukey)))), id_key('k', w1 & M40), (u32)w0,
                 (u32)(w1 >> 40));
    }
  }
}

// The lanes of eight times that lie in [lo, hi].
static FORCE_INLINE u32 inwin8(const u32* t, u32 lo, u32 hi) {
  const __m256i v = _mm256_loadu_si256((const __m256i*)t);
  const __m256i vlo = _mm256_set1_epi32((int)lo), vhi = _mm256_set1_epi32((int)hi);
#if defined(__AVX512VL__) && defined(__AVX512F__)
  return (u32)(_mm256_cmp_epu32_mask(v, vlo, _MM_CMPINT_NLT) & _mm256_cmp_epu32_mask(v, vhi, _MM_CMPINT_LE));
#else
  const __m256i in = _mm256_and_si256(_mm256_cmpeq_epi32(_mm256_max_epu32(v, vlo), v), _mm256_cmpeq_epi32(_mm256_min_epu32(v, vhi), v));
  return (u32)_mm256_movemask_ps(_mm256_castsi256_ps(in));
#endif
}

// How many of the eight values at p are below t / at most t (unsigned).
static FORCE_INLINE u32 cnt_lt8(const u32* p, u32 t) {
  const __m256i v = _mm256_loadu_si256((const __m256i*)p), vt = _mm256_set1_epi32((int)t);
#if defined(__AVX512VL__) && defined(__AVX512F__)
  return (u32)_mm_popcnt_u32(_mm256_cmp_epu32_mask(v, vt, _MM_CMPINT_LT));
#else
  return 8 - (u32)_mm_popcnt_u32((u32)_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(_mm256_max_epu32(v, vt), v))));
#endif
}
static FORCE_INLINE u32 cnt_le8(const u32* p, u32 t) {
  const __m256i v = _mm256_loadu_si256((const __m256i*)p), vt = _mm256_set1_epi32((int)t);
#if defined(__AVX512VL__) && defined(__AVX512F__)
  return (u32)_mm_popcnt_u32(_mm256_cmp_epu32_mask(v, vt, _MM_CMPINT_LE));
#else
  return (u32)_mm_popcnt_u32((u32)_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(_mm256_min_epu32(v, vt), v))));
#endif
}
// An index into a rising list, moved past the values below t / at most t. The list ends with eight values
// of 0xFFFFFFFF; t is below that, or (adv_lt only) equal to it.
static FORCE_INLINE u32 adv_lt(const u32* s, u32 p, u32 t) {
  u32 n;
  do {
    n = cnt_lt8(s + p, t);
    p += n;
  } while (__builtin_expect(n == 8, 0));
  return p;
}
static FORCE_INLINE u32 adv_le(const u32* s, u32 p, u32 t) {
  u32 n;
  do {
    n = cnt_le8(s + p, t);
    p += n;
  } while (__builtin_expect(n == 8, 0));
  return p;
}

// A person's sessions by day of the window (16 lanes, in two vectors): the sessions of day d are those with
// the indexes s[d] .. e[d] - 1, n[d] of them, with v[d] events.
struct Days {
  __m256i s0, s1, e0, e1, n0, n1, v0, v1;
  __m256i s16, e16, n16;  // s, e and n once more, as 16 lanes of 16 bits in one vector
};
// Two vectors of eight 32-bit lanes (values below 2^16) as one of sixteen 16-bit lanes, in the same order.
static FORCE_INLINE __m256i pack16(__m256i a, __m256i b) { return _mm256_permute4x64_epi64(_mm256_packus_epi32(a, b), 0xD8); }
// Per day, how many of the sessions lo .. hi - 1 are that day's (none if hi <= lo): 16 lanes of 16 bits.
static FORCE_INLINE __m256i day_share(const u32 lo, const u32 hi, const Days& D) {
  const __m256i vlo = _mm256_set1_epi16((short)lo), vhi = _mm256_set1_epi16((short)hi);
  return _mm256_subs_epu16(_mm256_min_epu16(vhi, D.e16), _mm256_max_epu16(vlo, D.s16));
}
// Each of sixteen 32-bit lanes (a, then b) gets the value of the lane before it, the first one 0.
static FORCE_INLINE void lanes_up(const __m256i a, const __m256i b, __m256i& oa, __m256i& ob) {
  const __m256i z = _mm256_setzero_si256();
#if defined(__AVX512VL__) && defined(__AVX512F__)
  oa = _mm256_alignr_epi32(a, z, 7);
  ob = _mm256_alignr_epi32(b, a, 7);
#else
  const __m256i idx = _mm256_setr_epi32(0, 0, 1, 2, 3, 4, 5, 6);
  oa = _mm256_blend_epi32(_mm256_permutevar8x32_epi32(a, idx), z, 1);
  ob = _mm256_blend_epi32(_mm256_permutevar8x32_epi32(b, idx), _mm256_permutevar8x32_epi32(a, _mm256_set1_epi32(7)), 1);
#endif
}
// A person's sessions by day, from the session starts (ns of them, in time order, below 2^31; the eight
// entries after them are overwritten) and the events before each session (ns + 1 entries). bnd: the first
// time after each day. No value goes through memory on its way into a vector here (a vector load right after
// narrower stores has to wait for them): the vectors come out of compares, four sessions at a time.
static FORCE_INLINE void days_of(u32* sess_ts, const u32* sess_ev, const u32 ns, const __m256i bnd0, const __m256i bnd1, Days& D) {
  const __m256i z = _mm256_setzero_si256();
  _mm256_storeu_si256((__m256i*)(sess_ts + ns), _mm256_set1_epi32(0x7FFFFFFF));  // (below no bound: these count nowhere)
  __m256i e0 = z, e1 = z, c0 = z, c1 = z;  // per day: the sessions / the events of the days up to it
  u32 j = 0;
  do {
#define DN_DAY1(J)                                                                   \
  do {                                                                               \
    const __m256i t_ = _mm256_set1_epi32((int)sess_ts[J]);                           \
    const __m256i m0_ = _mm256_cmpgt_epi32(bnd0, t_), m1_ = _mm256_cmpgt_epi32(bnd1, t_); \
    const __m256i nv_ = _mm256_set1_epi32((int)(sess_ev[(J) + 1] - sess_ev[J]));     \
    e0 = _mm256_sub_epi32(e0, m0_);                                                  \
    e1 = _mm256_sub_epi32(e1, m1_);                                                  \
    c0 = _mm256_add_epi32(c0, _mm256_and_si256(m0_, nv_));                           \
    c1 = _mm256_add_epi32(c1, _mm256_and_si256(m1_, nv_));                           \
  } while (0)
    DN_DAY1(j);
    DN_DAY1(j + 1);
    DN_DAY1(j + 2);
    DN_DAY1(j + 3);
#undef DN_DAY1
    j += 4;
  } while (__builtin_expect(j < ns, 0));
  D.e0 = e0;
  D.e1 = e1;
  lanes_up(e0, e1, D.s0, D.s1);
  D.n0 = _mm256_sub_epi32(e0, D.s0);
  D.n1 = _mm256_sub_epi32(e1, D.s1);
  __m256i p0, p1;
  lanes_up(c0, c1, p0, p1);
  D.v0 = _mm256_sub_epi32(c0, p0);
  D.v1 = _mm256_sub_epi32(c1, p1);
  D.e16 = pack16(e0, e1);
  D.s16 = pack16(D.s0, D.s1);
  D.n16 = _mm256_sub_epi16(D.e16, D.s16);
}
// The rows of a person, one per day with a session, in day order, appended to the five columns (each has
// room for 16 more entries). rev / top: revenue and top category per day. Returns the number of rows.
static FORCE_INLINE u32 emit_rows(const Days& D, const i64* rev, const u8* top, u8* dayp, u8* sessp, u32* evsp, i64* revp,
                                  u8* topp) {
#if defined(__AVX512VL__) && defined(__AVX512F__) && defined(__AVX512BW__)
  const __mmask8 k0 = _mm256_test_epi32_mask(D.n0, D.n0), k1 = _mm256_test_epi32_mask(D.n1, D.n1);
  const u32 r0 = (u32)_mm_popcnt_u32(k0), nrow = r0 + (u32)_mm_popcnt_u32(k1);
  const __m256i i0 = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7), i1 = _mm256_setr_epi32(8, 9, 10, 11, 12, 13, 14, 15);
  _mm_storel_epi64((__m128i*)dayp, _mm256_cvtepi32_epi8(_mm256_maskz_compress_epi32(k0, i0)));
  _mm_storel_epi64((__m128i*)(dayp + r0), _mm256_cvtepi32_epi8(_mm256_maskz_compress_epi32(k1, i1)));
  _mm_storel_epi64((__m128i*)sessp, _mm256_cvtepi32_epi8(_mm256_maskz_compress_epi32(k0, D.n0)));
  _mm_storel_epi64((__m128i*)(sessp + r0), _mm256_cvtepi32_epi8(_mm256_maskz_compress_epi32(k1, D.n1)));
  _mm256_storeu_si256((__m256i*)evsp, _mm256_maskz_compress_epi32(k0, D.v0));
  _mm256_storeu_si256((__m256i*)(evsp + r0), _mm256_maskz_compress_epi32(k1, D.v1));
  const __m128i t = _mm_load_si128((const __m128i*)top);
  _mm_storel_epi64((__m128i*)topp, _mm256_cvtepi32_epi8(_mm256_maskz_compress_epi32(k0, _mm256_cvtepu8_epi32(t))));
  _mm_storel_epi64((__m128i*)(topp + r0),
                   _mm256_cvtepi32_epi8(_mm256_maskz_compress_epi32(k1, _mm256_cvtepu8_epi32(_mm_srli_si128(t, 8)))));
  const u32 ka = (u32)k0 & 15, kb = (u32)k0 >> 4, kc = (u32)k1 & 15, kd = (u32)k1 >> 4;
  u32 o = 0;
  _mm256_storeu_si256((__m256i*)revp, _mm256_maskz_compress_epi64((__mmask8)ka, _mm256_load_si256((const __m256i*)rev)));
  o += (u32)_mm_popcnt_u32(ka);
  _mm256_storeu_si256((__m256i*)(revp + o), _mm256_maskz_compress_epi64((__mmask8)kb, _mm256_load_si256((const __m256i*)(rev + 4))));
  o += (u32)_mm_popcnt_u32(kb);
  _mm256_storeu_si256((__m256i*)(revp + o), _mm256_maskz_compress_epi64((__mmask8)kc, _mm256_load_si256((const __m256i*)(rev + 8))));
  o += (u32)_mm_popcnt_u32(kc);
  _mm256_storeu_si256((__m256i*)(revp + o), _mm256_maskz_compress_epi64((__mmask8)kd, _mm256_load_si256((const __m256i*)(rev + 12))));
  return nrow;
#else
  alignas(32) u32 n[16], v[16];
  _mm256_store_si256((__m256i*)n, D.n0);
  _mm256_store_si256((__m256i*)(n + 8), D.n1);
  _mm256_store_si256((__m256i*)v, D.v0);
  _mm256_store_si256((__m256i*)(v + 8), D.v1);
  u32 nrow = 0;
  for (u32 d = 0; d < 16; d++) {
    dayp[nrow] = (u8)d;
    sessp[nrow] = (u8)n[d];
    evsp[nrow] = v[d];
    revp[nrow] = rev[d];
    topp[nrow] = top[d];
    nrow += n[d] != 0;
  }
  return nrow;
#endif
}

struct SegStat {
  u64 n_persons = 0, n_events = 0, n_general = 0;
};

// Partitions [pb, pe): a thread's share of the scan. The first dense person met in them is left to the thread
// that scans the partitions before, which reads on past its own share for the first dense person out there,
// just as this thread reads on past pe (see the two places below).
//
// A person's events arrive in time order, one after the other. Each one costs a few instructions without a
// branch that depends on the data: it is counted, and it starts a session if it comes more than 30 minutes
// after the one before. Everything else is done per person once all its events are in: its sessions' days,
// revenue, credited campaign and assists, and its rows.
static void scan_segment(const u32 pb, const u32 pe, const int tid, DS& S, SegStat& st) {
  TL& tl = g_tl[tid];
  const u32 NC = g_ncamp, ORG = NC;
  const u32 NB = (u32)(((u64)(g_we - g_ws - 1) >> BSHIFT) + 1);
  const u32 NCAT = (u32)g_cats.size();
  const size_t NC1 = (size_t)NC + 1;
  const u32 p2shift = 64 - g_P2bits;
  const u64 ws_off = (u64)(g_ws - g_day0 * DAY);  // ms from the start of day 0 to the window start
  const u32 SCAP = NB + 16;  // sessions a person can have (their starts are more than 30 minutes apart), and some
  if (!S.T) {
    S.T = (D16*)aligned_alloc(64, sizeof(D16) * ESLOTS);
    memset(S.T, 0, sizeof(D16) * ESLOTS);
    S.sess_ts.assign((size_t)SCAP + 16, 0);
    S.sess_ev.assign((size_t)SCAP + 16, 0);
    S.catsum.assign(NCAT, 0);
    S.tcat.assign(NCAT + 1, 0);
    S.sw.assign((size_t)SCAP + 16, 0);
    S.cov.assign(NC + 1, 0);
    S.sk.assign(2 * SORT_ROOM, 0);
    S.lines = (u32*)aligned_alloc(64, sizeof(u32) * 32 * ((size_t)NC + 1));
    memset(S.lines, 0, sizeof(u32) * 32 * ((size_t)NC + 1));
    S.lines16 = (u16*)aligned_alloc(64, sizeof(u16) * 32 * ((size_t)NC + 1));
    memset(S.lines16, 0, sizeof(u16) * 32 * ((size_t)NC + 1));
  }
  // the first time after each day of the window (times count from the window start and are below 2^31)
  alignas(32) u32 bndv[16];
  for (u32 d = 0; d < 16; d++) bndv[d] = (u32)std::min<u64>((u64)(d + 1) * (u64)DAY - ws_off, 0x7FFFFFFFu);
  const __m256i bnd0 = _mm256_load_si256((const __m256i*)bndv), bnd1 = _mm256_load_si256((const __m256i*)(bndv + 8));
  if (__builtin_expect(S.cbase > 0x80000000u, 0)) {  // (never with fewer than 2^31 sessions and persons)
    std::fill(S.cov.begin(), S.cov.end(), 0);
    S.cbase = 0;
  }
  D16* const T = S.T;
  __m256i vmax_s = _mm256_setzero_si256(), vmax_e = vmax_s;  // the most sessions / events of a row, in some lane
  auto c2 = tl.r2.cursor(tl.arena);
  auto c2c = tl.r2c.cursor(tl.arena);
  CStat* const c_stat = tl.c_stat.data();
  i64* const catsum = S.catsum.data();
  u32* const tcat = S.tcat.data();
  u32* const sess_ts = S.sess_ts.data();
  u32* const sess_ev = S.sess_ev.data();
  OutCols& out = tl.out;
  const std::atomic<u64>* const acc = g_acc;
  const u64 ukey = g_ukey;

  // ---- the open person. This state is read and written by the rare paths below; the loops over the slots
  // work on copies in plain locals (which the compiler then keeps in registers) and exchange them with it
  // around every call of a rare path.
  struct PS {
    u32 cur_p = NONE, cur_cnt = 0, cur_base = 0;  // its IDs are the user numbers cur_base .. cur_base + cur_cnt - 1
    u32 ns = 0, nev = 0;                          // its sessions and events so far
    i64 prev = INT64_MIN / 4;                     // the time of its last event
    i64 order = 0;                                // negative once an event has come before its predecessor
    u32 x_next = NONE;                            // time of the person's next extra event
    size_t x_i = 0;
  } ps;
  u64 n_persons = 0, n_events = 0, n_general = 0;
  i64 prev_p = -1;  // the dense person before the open one

  // An event of the open person, in terms of the state it touches. price 0: not a purchase.
#define DN_FEED(NS, NEV, PREV, ORDER, ts, cat, price)                                   \
  do {                                                                                  \
    const i64 gap_ = (i64)(ts) - (PREV);                                                \
    (ORDER) |= gap_;                                                                    \
    sess_ts[NS] = (ts);                                                                 \
    sess_ev[NS] = (NEV);                                                                \
    (NS) += gap_ > MIN30;                                                               \
    (NEV)++;                                                                            \
    (PREV) = (ts);                                                                      \
    if (__builtin_expect((price) != 0, 0)) S.pur.push_back(DS::Pur{(NS)-1, (cat), (price)}); \
    if (__builtin_expect((NS) >= SCAP, 0)) { /* (cannot happen with events in time order) */ \
      (ORDER) = -1;                                                                     \
      (NS) = 1;                                                                         \
    }                                                                                   \
  } while (0)
  auto feed_extras = [&](const u32 upto) {  // the extra events up to a time
    while (ps.x_i < S.xq.size() && (u32)S.xq[ps.x_i] <= upto) {
      const u64 e = S.xq[ps.x_i++];
      const u32 pl = (u32)(e >> 32);
      DN_FEED(ps.ns, ps.nev, ps.prev, ps.order, (u32)e, pl >> 24, pl & 0xFFFFFF);
    }
    ps.x_next = ps.x_i < S.xq.size() ? (u32)S.xq[ps.x_i] : NONE;
  };
  auto open_person = [&](const u32 p, const u32 cnt, const u64 ac) {
    if (__builtin_expect((i64)p <= prev_p, 0)) fail("persons are not in index order");
    prev_p = p;
    ps.cur_p = p;
    ps.cur_cnt = cnt;
    ps.cur_base = p << 9;
    ps.ns = 0;
    ps.nev = 0;
    ps.prev = INT64_MIN / 4;
    ps.order = 0;
    S.pur.clear();
    ps.x_next = NONE;
    ps.x_i = 0;
    if (__builtin_expect((ac & ACC_EXTRA) != 0, 0)) {
      S.xq = g_extra.find(p)->second.ev;
      if (!S.xq.empty()) ps.x_next = (u32)S.xq[0];
    }
  };
  // All events of the open person are in: its sessions, its rows.
  auto close_person = [&]() {
    if (ps.cur_p == NONE) return;
    if (ps.x_next != NONE) feed_extras(NONE - 1);
    const u32 p = ps.cur_p, cnt = ps.cur_cnt, ns = ps.ns;
    ps.cur_p = NONE;
    ps.cur_cnt = 0;
    ps.cur_base = 0;
    if (__builtin_expect(ps.order < 0, 0)) {  // (the whole attempt is dropped; nothing more is done with this person)
      fail("a person's events are not in time order");
      return;
    }
    sess_ev[ns] = ps.nev;
    n_events += ps.nev;
    n_persons++;
    // ---- its sessions by day. Sessions are in time order, so a day's sessions are a run of them.
    Days D;
    days_of(sess_ts, sess_ev, ns, bnd0, bnd1, D);
    {  // (before this person adds to the 16-bit lines)
      __m256i ds = _mm256_load_si256((const __m256i*)S.day_sum);
      if (__builtin_expect((S.since++ & (LINES_EVERY - 1)) == 0, 0) &&
          !_mm256_testz_si256(_mm256_subs_epu16(ds, _mm256_set1_epi16((short)LINES_LIMIT)), _mm256_set1_epi16(-1))) {
        flush_lines(S, NC);
        ds = _mm256_setzero_si256();
      }
      _mm256_store_si256((__m256i*)S.day_sum, _mm256_add_epi16(ds, D.n16));
    }
    vmax_s = _mm256_max_epu32(vmax_s, _mm256_max_epu32(D.n0, D.n1));
    vmax_e = _mm256_max_epu32(vmax_e, _mm256_max_epu32(D.v0, D.v1));
    // ---- its clicks: the keys of its index in the click partition
    const u32 cp = p >> KC;
    if (__builtin_expect(cp != S.cp, 0)) load_clicks(S, cp, c2c, p2shift);
    const u32 c0 = S.coff[p & ((1u << KC) - 1)];
    const u32 mc0 = S.cpos[p & ((1u << KC) - 1)] - c0;  // its clicks in the partition
    u32 mc = mc0;                                             // ... and with its extra clicks
    const u64* keys = S.ckey.data() + c0;
    const Extra* ex = nullptr;
    if (__builtin_expect((acc[p].load(std::memory_order_relaxed) & ACC_EXTRA) != 0, 0)) {
      ex = &g_extra.find(p)->second;
      if (!ex->ck.empty()) {  // extra clicks get the numbers after those of the partition
        const size_t nx = ex->ck.size();
        if ((size_t)mc0 + nx > CK_ORD) {
          fail("a person with more than a million clicks");
        } else {
          S.xk.resize((size_t)mc0 + nx + SORT_ROOM);
          memcpy(S.xk.data(), keys, (size_t)mc0 * sizeof(u64));
          for (size_t i = 0; i < nx; i++) S.xk[mc0 + i] = ck_make((u32)ex->ck[i][0], (u32)(mc0 + i), (u32)(ex->ck[i][0] >> 32));
          keys = S.xk.data();
          mc += (u32)nx;
        }
      }
    }
    if (__builtin_expect(S.sk.size() < (size_t)mc + SORT_ROOM, 0)) S.sk.resize((size_t)mc + 2 * SORT_ROOM);
    u64* const sk = S.sk.data();
    // ---- credit and assists, click by click in time order. (Click times count from 7 days before the
    // window, session starts from the window start: a click at time t is in the 7 days up to a session that
    // starts at s when s <= t <= s + 7 days.) The sessions a click assists are thus a run a .. b - 1 of the
    // person's sessions, and both ends only move forward from one click to the next. Of that run,
    //  - the sessions before the next click's run are credited to this click (the latest one for them);
    //  - the sessions no earlier click of the same campaign has assisted are counted for the campaign.
    // Either way a run of sessions is counted per day, and added to the campaign's line of days at once.
    u16* const lines = S.lines16;
    __m256i cr = _mm256_setzero_si256();  // credited sessions per day
    if (mc) {
      const u32 any_tie = sort_click_keys(keys, mc, sk, S.rtmp);
      // Among clicks of one time the one with the smallest click_id counts: it goes last of them. (About one
      // click in twenty has the time of another click of the person, and most of those are one click that
      // is in the data twice: with one campaign there is nothing to choose.)
      for (u32 base = 0, done = 0; __builtin_expect(any_tie != 0, 0) && base + 1 < mc; base += 4) {
        u32 m = same_time4(sk + base);
        while (__builtin_expect(m != 0, 0)) {
          const u32 r0 = base + ((u32)_tzcnt_u32(m) >> 1);
          m = _blsr_u32(m);
          if (r0 < done) continue;  // (inside a run that is dealt with)
          const u32 t = ck_ts(sk[r0]);
          u32 r1 = r0 + 2;
          while (ck_ts(sk[r1]) == t) r1++;  // (the end keys stop it)
          done = r1;
          u32 same = 1;
          for (u32 q = r0 + 1; q < r1; q++) same &= (u32)(ck_camp(sk[q]) == ck_camp(sk[r0]));
          if (same) continue;
          auto click_of = [&](const u64 key) -> u64 {  // the click number, as click_less takes it
            const u32 o = ck_ord(key);
            return o >= mc0 ? (CLICK_IS_KEY | ex->ck[o - mc0][1]) : S.cclk[c0 + o];
          };
          u32 best = r0;
          u64 bc = click_of(sk[r0]);
          for (u32 q = r0 + 1; q < r1; q++) {
            const u64 c = click_of(sk[q]);
            if (click_less(c, bc)) {
              best = q;
              bc = c;
            }
          }
          std::swap(sk[best], sk[r1 - 1]);
        }
      }
      u32* const sw = S.sw.data();
      {
        const __m256i wk = _mm256_set1_epi32((int)(u32)WEEK), ones = _mm256_set1_epi32(-1);
        for (u32 g = 0; g < ns; g += 8)
          _mm256_storeu_si256((__m256i*)(sw + g), _mm256_add_epi32(_mm256_loadu_si256((const __m256i*)(sess_ts + g)), wk));
        _mm256_storeu_si256((__m256i*)(sw + ns), ones);
        _mm256_storeu_si256((__m256i*)(sess_ts + ns), ones);
      }
      u32* const cov = S.cov.data();
      const u32 base = S.cbase;  // session j of this person has the number base + j in cov
      S.cbase = base + ns + 1;
      u32 a = adv_lt(sw, 0, ck_ts(sk[0])), b = 0;
      for (u32 k = 0; k < mc; k++) {
        const u64 key = sk[k];
        const u32 t = ck_ts(key), c = ck_camp(key);
        b = adv_le(sess_ts, b, t);
        const u32 an = adv_lt(sw, a, ck_ts(sk[k + 1]));  // (after the last click: all sessions)
        u16* const L = lines + (size_t)c * 32;
        __m256i o = day_share(a, std::min(an, b), D);
        _mm256_store_si256((__m256i*)(L + 16), _mm256_add_epi16(_mm256_load_si256((const __m256i*)(L + 16)), o));
        cr = _mm256_add_epi16(cr, o);
        const u32 cv = cov[c];  // (below base if the campaign is new to this person)
        cov[c] = std::max(cv, base + b);
        o = day_share(std::max(cv, base + a) - base, b, D);
        _mm256_store_si256((__m256i*)L, _mm256_add_epi16(_mm256_load_si256((const __m256i*)L), o));
        a = an;
      }
    }
    {  // the sessions without a click are organic
      u16* const L = lines + (size_t)ORG * 32;
      _mm256_store_si256((__m256i*)(L + 16),
                         _mm256_add_epi16(_mm256_load_si256((const __m256i*)(L + 16)), _mm256_sub_epi16(D.n16, cr)));
    }
    // ---- its purchases, session by session: revenue and top category of the day, and the revenue of the
    // campaign the session is credited to
    alignas(32) i64 revd[16];
    alignas(16) u8 topd[16];
    {
      const __m256i z = _mm256_setzero_si256();
      _mm256_store_si256((__m256i*)revd, z);
      _mm256_store_si256((__m256i*)(revd + 4), z);
      _mm256_store_si256((__m256i*)(revd + 8), z);
      _mm256_store_si256((__m256i*)(revd + 12), z);
      _mm_store_si128((__m128i*)topd, _mm_set1_epi8((char)0xFF));  // none
    }
    if (!S.pur.empty()) {
      const DS::Pur* pu = S.pur.data();
      const DS::Pur* const pue = pu + S.pur.size();
      u32 ntc = 0;
      auto top_of_day = [&]() -> u8 {  // (and the sums are cleared for the next day)
        u32 top = 0xFF;
        i64 best = -1;
        for (u32 k = 0; k < ntc; k++) {
          const u32 c = tcat[k];
          const i64 v = catsum[c];
          if (v > best || (v == best && c < top)) {
            best = v;
            top = c;
          }
          catsum[c] = 0;
        }
        ntc = 0;
        return (u8)top;
      };
      auto day_of = [&](const u32 j) -> u32 { return (u32)((ws_off + sess_ts[j]) / (u64)DAY); };
      u32 cd = day_of(pu->sess);
      while (pu != pue) {
        const u32 j = pu->sess, d = day_of(j);
        if (d != cd) {
          topd[cd] = top_of_day();
          cd = d;
        }
        i64 s_rev = 0;
        do {
          s_rev += pu->price;
          if (catsum[pu->cat] == 0) tcat[ntc++] = pu->cat;
          catsum[pu->cat] += pu->price;
          pu++;
        } while (pu != pue && pu->sess == j);
        revd[d] += s_rev;
        u32 credit = ORG;
        if (mc) {  // the latest click in the 7 days up to the session start
          const u32 s_start = sess_ts[j], upper = s_start + (u32)WEEK;
          const u32 h = ck_count_le(sk, mc, upper);
          if (h > 0 && ck_ts(sk[h - 1]) >= s_start) credit = ck_camp(sk[h - 1]);
        }
        CStat& cs = c_stat[(size_t)d * NC1 + credit];
        cs.purch++;
        cs.rev += s_rev;
      }
      topd[cd] = top_of_day();
    }
    // ---- its rows, one per day with a session
    u8* const dayp = out.day.reserve(16, tl.arena);
    u8* const sessp = out.sess.reserve(16, tl.arena);
    u32* const evsp = out.evs.reserve(16, tl.arena);
    i64* const revp = out.rev.reserve(16, tl.arena);
    u8* const topp = out.top.reserve(16, tl.arena);
    const u32 nrow = emit_rows(D, revd, topd, dayp, sessp, evsp, revp, topp);
    // ---- its rows: the name is the smallest of the person's IDs
    // (the two first IDs without a branch: three persons in four have no more)
    const u64 v0 = scramble((u64)p << 9, ukey), v1 = scramble(((u64)p << 9) | 1, ukey);
    const u64 o0 = id_order(v0), o1 = id_order(v1) | (0ULL - (u64)(cnt < 2));  // (all ones: no second ID)
    u64 bv = o1 < o0 ? v1 : v0, bo = o1 < o0 ? o1 : o0;
    for (u32 a = 2; a < cnt; a++) {
      const u64 v = scramble(((u64)p << 9) | a, ukey), o = id_order(v);
      bv = o < bo ? v : bv;
      bo = o < bo ? o : bo;
    }
    const u64 pkey = id_key('u', bv);
    u8 kb[16];
    const u32 klen = fast_key_bytes(pkey, kb + 4);
    memcpy(kb, &klen, 4);
    u8* ip = out.ids.reserve(16, tl.arena);
    memcpy(ip, kb, 16);
    out.ids.cur = ip + 4 + klen;
    *out.runs.slot(tl.arena) = (u8)nrow;
    out.npersons++;
    out.day.cur += nrow;
    out.sess.cur += nrow;
    out.evs.cur += nrow;
    out.rev.cur += nrow;
    out.top.cur += nrow;
    out.nrows += nrow;
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
  // The first dense person met in a thread's partitions is not its own (but in the first segment): the thread
  // before reads on past its partitions for that person, wherever it began. So nothing has to be looked up
  // before the segment, and a segment costs little.
  u32 q_skip = NONE;        // that person, once it is met
  bool skipping = pb != 0;  // until another dense person comes
  // An event that is not of the open person's IDs: of another dense person (which becomes the open one: true),
  // or of none (false; it goes to the general phase unless it is the previous thread's to deal with).
  auto other = [&](const u32 un, const u32 ts, const u32 cat, const u32 price) -> bool {
    const u32 p = un >> 9, a = un & 511;
    if (p == ps.cur_p) {
      emit_general(un, ts, cat, price);
      return false;
    }
    const u64 ac = acc[p].load(std::memory_order_relaxed);
    const u32 cnt = dense_cnt(ac);
    if (a >= cnt) {
      emit_general(un, ts, cat, price);
      return false;
    }
    if (skipping) {
      if (q_skip == NONE) {
        q_skip = p;
        prev_p = p;
      }
      if (p == q_skip) return false;
      skipping = false;
    }
    close_person();
    open_person(p, cnt, ac);
    return true;
  };
  // the open person's state in locals (see PS)
  u32 h_base = ps.cur_base, h_cnt = ps.cur_cnt, h_ns = ps.ns, h_nev = ps.nev, h_xnext = ps.x_next;
  i64 h_prev = ps.prev, h_order = ps.order;
#define DN_OUT() (ps.ns = h_ns, ps.nev = h_nev, ps.prev = h_prev, ps.order = h_order)
#define DN_IN() (h_base = ps.cur_base, h_cnt = ps.cur_cnt, h_ns = ps.ns, h_nev = ps.nev, h_xnext = ps.x_next, h_prev = ps.prev, h_order = ps.order)
  // One slot, the whole way (a macro, so that the state stays in the locals above; continue leaves it).
#define DN_SLOT(I)                                                                       \
  do {                                                                                   \
    const u64 w0 = T[I].w0;                                                              \
    const u32 tsf = (u32)w0;                                                             \
    if ((tsf & 0x7FFFFFFF) == 0) continue; /* no record, or an event that is not valid */ \
    const u32 ts = (tsf & 0x7FFFFFFF) - 1;                                               \
    u32 un = (u32)(w0 >> 32), cat = 0, price = 0;                                        \
    if (__builtin_expect((i32)tsf < 0, 0)) { /* a side entry */                          \
      const XEv& x = g_tl[un >> 24].xev[un & 0xFFFFFF];                                  \
      if (x.num == NONE) {                                                               \
        push_r2(c2, p2shift, person_of(x.uid), ts, x.payload);                           \
        n_general++;                                                                     \
        continue;                                                                        \
      }                                                                                  \
      un = x.num;                                                                        \
      cat = x.payload >> 24;                                                             \
      price = x.payload & 0xFFFFFF;                                                      \
    } else {                                                                             \
      const u32 pay = (u32)(T[I].w1 >> 24) & 0x3FFFFF;                                   \
      if (__builtin_expect(pay != 0, 0)) {                                               \
        cat = pay >> 17;                                                                 \
        price = pay & 0x1FFFF;                                                           \
      }                                                                                  \
    }                                                                                    \
    if (__builtin_expect(un - h_base >= h_cnt, 0)) { /* not an ID of the open person */  \
      DN_OUT();                                                                          \
      const bool mine = other(un, ts, cat, price);                                       \
      DN_IN();                                                                           \
      if (!mine) continue;                                                               \
    }                                                                                    \
    if (__builtin_expect(ts >= h_xnext, 0)) {                                            \
      DN_OUT();                                                                          \
      feed_extras(ts);                                                                   \
      DN_IN();                                                                           \
    }                                                                                    \
    DN_FEED(h_ns, h_nev, h_prev, h_order, ts, cat, price);                               \
  } while (0)
  static_assert(ESLOTS % 8 == 0, "the slot table is read in groups of eight");
  // Eight slots at a time. The vector path takes the valid events of a group up to the first one it cannot:
  // a side entry or an ID that is not of the open person. That one goes the whole way, and the vector path
  // goes on with the slots after it. The vector path runs only while a person is open, has no extra event
  // still to come, and its last time fits 31 bits (so the gaps fit 32-bit lanes).
  bool vec_ok = false;
  __m256i v_base = _mm256_setzero_si256(), v_cnt = _mm256_setzero_si256();
  // (the asm statements keep the compiler from building these again from scalars in every group)
#define DN_VEC()                                                                      \
  do {                                                                                \
    vec_ok = h_cnt != 0 && h_xnext == NONE && (u64)h_prev < 0x80000000ULL;            \
    v_base = _mm256_set1_epi32((int)h_base);                                          \
    v_cnt = _mm256_set1_epi32((int)h_cnt);                                            \
    __asm__("" : "+x"(v_base), "+x"(v_cnt));                                          \
  } while (0)
  __m256i k_ord = _mm256_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7), k_sh1 = _mm256_setr_epi32(0, 0, 1, 2, 3, 4, 5, 6);
  __m256i k_m31 = _mm256_set1_epi32(0x7FFFFFFF), k_min30 = _mm256_set1_epi32((int)MIN30);
  __m256i k_price = _mm256_set1_epi64x(0x1FFFFLL << 24);
  __asm__("" : "+x"(k_ord), "+x"(k_sh1), "+x"(k_m31), "+x"(k_min30), "+x"(k_price));
  alignas(32) u32 pbuf[16];
  for (u32 j = pb; j < pe && !g_fail.load(std::memory_order_relaxed); j++) {
    if (!place(j, T, S)) continue;
    DN_VEC();
    for (u32 i = 0; i < ESLOTS; i += 8) {
      u32 done = 0;  // the slots of the group that are dealt with
      for (;;) {
        u32 k;  // the first slot the vector path did not take
        if (__builtin_expect(vec_ok, 1)) {
          const __m256i* const tv = (const __m256i*)(T + i);
          const __m256i L0 = _mm256_load_si256(tv), L1 = _mm256_load_si256(tv + 1);
          const __m256i L2 = _mm256_load_si256(tv + 2), L3 = _mm256_load_si256(tv + 3);
          const __m256 X = _mm256_castsi256_ps(_mm256_unpacklo_epi64(L0, L1));  // w0 of slots 0 2 | 1 3
          const __m256 Y = _mm256_castsi256_ps(_mm256_unpacklo_epi64(L2, L3));  // ... 4 6 | 5 7
          const __m256i TF = _mm256_permutevar8x32_epi32(_mm256_castps_si256(_mm256_shuffle_ps(X, Y, 0x88)), k_ord);  // low words
          const __m256i UN = _mm256_permutevar8x32_epi32(_mm256_castps_si256(_mm256_shuffle_ps(X, Y, 0xDD)), k_ord);  // user numbers
          const __m256i TV = _mm256_and_si256(TF, k_m31);
          const u32 avail = ~((u32)_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(TV, _mm256_setzero_si256()))) | done) & 0xFF;
          // not of the open person: un - base >= cnt, unsigned; a side entry has the top bit of its low word
          const __m256i D = _mm256_sub_epi32(UN, v_base);
          const __m256i OUT = _mm256_or_si256(_mm256_cmpeq_epi32(_mm256_max_epu32(D, v_cnt), D), TF);
          const u32 irr = (u32)_mm256_movemask_ps(_mm256_castsi256_ps(OUT)) & avail;
          k = _tzcnt_u32(irr | 0x100);
          const u32 fm = _bzhi_u32(avail, k), n = (u32)_mm_popcnt_u32(fm);
          // the times of the events it takes, in order, and their gaps
          const __m256i TC = _mm256_sub_epi32(pack8(TV, fm), _mm256_set1_epi32(1));
          const u32 prev32 = (u32)h_prev;
          const __m256i SH = _mm256_blend_epi32(_mm256_permutevar8x32_epi32(TC, k_sh1), _mm256_castsi128_si256(_mm_cvtsi32_si128((int)prev32)), 1);
          const __m256i G = _mm256_sub_epi32(TC, SH);
          const u32 bad = _bzhi_u32((u32)_mm256_movemask_ps(_mm256_castsi256_ps(G)), n);
          const u32 st = _bzhi_u32((u32)_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(G, k_min30))), n);
          // the sessions they start (the lanes after the last start are written too, and are never read)
          __m256i from;
          const __m256i SS = pack8x(TC, st, from);
          const u32 ns0 = h_ns;
          _mm256_storeu_si256((__m256i*)(sess_ts + ns0), SS);
          _mm256_storeu_si256((__m256i*)(sess_ev + ns0), _mm256_add_epi32(from, _mm256_set1_epi32((int)h_nev)));
          h_ns += (u32)_mm_popcnt_u32(st);
          h_nev += n;
          h_order |= -(i64)bad;
          pbuf[0] = prev32;
          _mm256_storeu_si256((__m256i*)(pbuf + 1), TC);
          h_prev = pbuf[n];
          const __m256i P = _mm256_unpackhi_epi64(L0, L1), Q = _mm256_unpackhi_epi64(L2, L3);  // w1 of slots 0 2 1 3, 4 6 5 7
          if (__builtin_expect(!_mm256_testz_si256(_mm256_or_si256(P, Q), k_price), 0)) {  // a purchase among the 8 slots
            const __m256i z = _mm256_setzero_si256();
            const u32 pz = (u32)_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(_mm256_and_si256(_mm256_permute4x64_epi64(P, 0xD8), k_price), z))) |
                           (u32)_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(_mm256_and_si256(_mm256_permute4x64_epi64(Q, 0xD8), k_price), z))) << 4;
            for (u32 m = fm & ~pz; m; m = _blsr_u32(m)) {
              const u32 l = (u32)_tzcnt_u32(m), pay = (u32)(T[i + l].w1 >> 24) & 0x3FFFFF;
              const u32 c = (u32)_mm_popcnt_u32(fm & ((1u << l) - 1));  // its place among the events taken
              S.pur.push_back(DS::Pur{ns0 + (u32)_mm_popcnt_u32(st & ((2u << c) - 1)) - 1, pay >> 17, pay & 0x1FFFF});
            }
          }
          if (__builtin_expect(h_ns >= SCAP, 0)) {  // (as in DN_FEED)
            h_order = -1;
            h_ns = 1;
          }
          if (k >= 8) break;
        } else {
          k = (u32)_tzcnt_u32(~done);
        }
        DN_SLOT(i + k);
        DN_VEC();
        done = (2u << k) - 1;
        if (done == 0xFF) break;
      }
    }
    memset(T, 0, sizeof(D16) * ESLOTS);
  }
#undef DN_SLOT
#undef DN_VEC
  // ---- past our partitions: the first dense person met there is ours too, whether it is the open person
  // going on or a new one (the thread that has those partitions leaves it alone, see above). Two cases have
  // nothing to do: no dense person at all in our partitions (then the first one out there was already the
  // first one here, and the thread before us has it), and only the one we left to the thread before, if
  // that one is still going on out there.
  if (pb == 0 || !skipping || q_skip != NONE) {
    bool taken = false;  // the first dense person out there has come
    for (u32 j = pe; j < g_PE && !g_fail.load(std::memory_order_relaxed); j++) {
      if (!place(j, T, S)) continue;
      bool stop = false;
      for (u32 i = 0; i < ESLOTS; i++) {
        const u64 w0 = T[i].w0;
        const u32 tsf = (u32)w0;
        if ((tsf & 0x7FFFFFFF) == 0) continue;
        const u32 ts = (tsf & 0x7FFFFFFF) - 1;
        u32 un = (u32)(w0 >> 32), cat = 0, price = 0;
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
        if (un - h_base >= h_cnt) {  // not an ID of the open person
          const u32 p = un >> 9;
          if (p == ps.cur_p) continue;  // (an ID of its index that is not one of the dense person)
          const u64 ac = acc[p].load(std::memory_order_relaxed);
          const u32 cnt = dense_cnt(ac);
          if ((un & 511) >= cnt) continue;  // no dense person's event: the other thread's to hand on
          if (taken || (skipping && p == q_skip)) {  // the person after ours / the one we left is still going on
            stop = true;
            break;
          }
          DN_OUT();
          close_person();
          open_person(p, cnt, ac);
          DN_IN();
        }
        taken = true;
        if (__builtin_expect(ts >= h_xnext, 0)) {
          DN_OUT();
          feed_extras(ts);
          DN_IN();
        }
        DN_FEED(h_ns, h_nev, h_prev, h_order, ts, cat, price);
      }
      memset(T, 0, sizeof(D16) * ESLOTS);
      if (stop) break;
    }
  }
  DN_OUT();
#undef DN_OUT
#undef DN_IN
#undef DN_FEED
  close_person();
  tl.r2.close(c2);
  tl.r2.flush(tl.arena);
  tl.r2c.close(c2c);
  tl.r2c.flush(tl.arena);
  {
    alignas(32) u32 m[16];
    _mm256_store_si256((__m256i*)m, vmax_s);
    _mm256_store_si256((__m256i*)(m + 8), vmax_e);
    for (int i = 0; i < 8; i++) {
      out.max_sess = std::max(out.max_sess, m[i]);
      out.max_evs = std::max(out.max_evs, m[8 + i]);
    }
  }
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
  // (A segment costs little, one partition placed a second time, so they are small, about ten partitions:
  // the threads then end the scan close together. SS_SEGPART sets another size, for tests.)
  const u32 segpart = getenv("SS_SEGPART") ? (u32)std::max(1, atoi(getenv("SS_SEGPART"))) : 10;
  const u32 nseg = std::max<u32>(1, std::min<u32>(used, std::max<u32>((u32)NT, used / segpart)));
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
  // the campaigns' lines of days -> the tables by day and campaign that the general phase and the output use
  parallel_for(NT, [&](int t, int) {
    if (!scr[t].lines) return;
    flush_lines(scr[t], NC);
    const u32* const lines = scr[t].lines;
    TL& tl = g_tl[t];
    const size_t NC1 = (size_t)NC + 1;
    for (u32 c = 0; c <= NC; c++) {
      const u32* const L = lines + (size_t)c * 32;
      for (u32 d = 0; d < ND; d++) {
        tl.c_assist[d * NC1 + c] += L[d];
        tl.c_stat[d * NC1 + c].sess += L[16 + d];
      }
    }
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
    {  // id_order against the keys of the IDs: numbers of every length
      const u64 x = v >> (next() % 40), y = (it & 3) == 0 ? x * 36 + (next() % 3 == 0 ? 0 : next() % 36) : (key >> (next() % 40));
      const u64 xm = x & M40, ym = y & M40;
      const u64 kx = id_key('u', xm), ky = id_key('u', ym);
      if ((id_order(xm) < id_order(ym)) != (kx < ky) || (id_order(xm) == id_order(ym)) != (kx == ky)) bad++;
    }
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
  // links: the two IDs of a line at once against one at a time; the sets in a word against a plain union-find
  {
    const u64 key0 = g_ukey;
    const u32 cap0 = g_nu_cap;
    std::atomic<u64>* const acc0 = g_acc;
    g_ukey = next() & M40;
    g_nu_cap = 1u << 20;
    for (int it = 0; it < 1000000; it++) {
      u8 t[2][24];
      u32 l[2];
      for (int k = 0; k < 2; k++) {
        memset(t[k], ',', 24);
        const u32 mode = (u32)(next() % 8);
        if (mode < 5) {  // a user ID as the generator writes it: an index in range, or any number
          const u64 num = mode < 3 ? next() % ((u64)g_nu_cap << 10) : (next() & M40);
          t[k][0] = 'u';
          l[k] = 1 + b36_write(scramble(num, g_ukey), t[k] + 1);
        } else {  // any text
          l[k] = (u32)(next() % 12);
          for (u32 j = 0; j < l[k]; j++)
            t[k][j] = (j == 0 && next() % 4) ? (u8)'u' : (u8)(mode == 7 && next() % 5 == 0 ? odd[next() % 44] : alpha[next() % 36]);
        }
      }
      u32 x, y;
      reg_user_pair(t[0], l[0], t[1], l[1], g_ukey, g_nu_cap, x, y);
      if (x != reg_user(t[0], l[0]) || y != reg_user(t[1], l[1])) {
        if (bad < 10) fprintf(stderr, "LINK IDS MISMATCH '%.*s' '%.*s'\n", (int)l[0], t[0], (int)l[1], t[1]);
        bad++;
      }
      n++;
    }
    std::atomic<u64> word[1];
    g_acc = word;
    for (int it = 0; it < 300000; it++) {
      word[0].store(0);
      u32 par[KA];
      bool pres[KA];
      for (u32 k = 0; k < KA; k++) {
        par[k] = k;
        pres[k] = false;
      }
      auto find = [&](u32 v) {
        while (par[v] != v) v = par[v];
        return v;
      };
      const u32 nl = (u32)(next() % 12), range = 1 + (u32)(next() % KA);
      for (u32 j = 0; j < nl; j++) {
        const u32 a = (u32)(next() % range), b = next() % 4 == 0 ? a : (u32)(next() % range);
        link_aliases(0, a, b);
        pres[a] = pres[b] = true;
        par[find(a)] = find(b);
      }
      const u64 w = word[0].load();
      for (u32 k = 0; k < KA; k++) {
        u32 least = k;  // the smallest alias in k's set
        for (u32 j = 0; j < k; j++)
          if (pres[j] && find(j) == find(k)) {
            least = j;
            break;
          }
        const u32 lab = (u32)(w >> (4 * k)) & 15;
        if ((((w >> (ACC_MSH + k)) & 1) != 0) != pres[k] || lab != (pres[k] ? least : 0)) bad++;
      }
      if (w >> (ACC_MSH + KA)) bad++;
      n++;
    }
    g_ukey = key0;
    g_nu_cap = cap0;
    g_acc = acc0;
  }
  fprintf(stderr, "selftest ids and links: %ld cases, %ld mismatches\n", n, bad);
  // the sorts of a person's clicks, against std::sort (by time only where the order of equal times is free)
  long bad2 = 0, n2 = 0;
  {
    std::vector<u64> in, out, ref, tmp;
    for (int it = 0; it < 300000; it++) {
      const u32 r = (u32)next();
      // mostly small lists; times from a small range often, so that equal times are common
      const u32 nk = 1 + (it % 7 == 0 ? r % 400 : it % 3 == 0 ? r % 70 : r % 17);
      const u32 trange = (r >> 16) % 4 == 0 ? 8 : (r >> 16) % 4 == 1 ? 1000 : CK_TS_END;
      in.assign(nk + SORT_ROOM, 0);
      for (u32 i = 0; i < nk; i++) in[i] = ck_make((u32)(next() % trange) + ((r >> 20) & 1 ? CK_TS_END - trange : 0), i, (u32)next() & CK_CAMP);
      for (u32 i = nk; i < nk + SORT_ROOM; i++) in[i] = next();  // (what follows a list is anything)
      out.assign(nk + SORT_ROOM, 0x1234);
      const u32 tie = sort_click_keys(in.data(), nk, out.data(), tmp);
      ref.assign(in.begin(), in.begin() + nk);
      if (nk <= NET_MAX) {
        std::sort(ref.begin(), ref.end());
        for (u32 i = 0; i < nk; i++) bad2 += out[i] != ref[i];
      } else {
        std::stable_sort(ref.begin(), ref.end(), [](u64 x, u64 y) { return ck_ts(x) < ck_ts(y); });
        for (u32 i = 0; i < nk; i++) bad2 += out[i] != ref[i];
      }
      for (u32 i = 0; i < 5; i++) bad2 += out[nk + i] != ck_make(0xFFFFFFFFu - i, CK_ORD, CK_CAMP);
      const u32 x = (u32)(next() % (u64)CK_TS_END);
      u32 want = 0;
      for (u32 i = 0; i < nk; i++) want += ck_ts(out[i]) <= x;
      bad2 += ck_count_le(out.data(), nk, x) != want;
      u32 any = 0;
      for (u32 base = 0; base + 1 < nk; base += 4) {
        const u32 m = same_time4(out.data() + base);
        for (u32 j = 0; j < 4; j++) bad2 += (((m >> (2 * j + 1)) & 1) != 0) != (ck_ts(out[base + j]) == ck_ts(out[base + j + 1]));
      }
      for (u32 i = 0; i + 1 < nk; i++) any |= ck_ts(out[i]) == ck_ts(out[i + 1]);
      bad2 += (tie != 0) != (any != 0);
      n2++;
    }
  }
  fprintf(stderr, "selftest click sorts: %ld cases, %ld mismatches\n", n2, bad2);
  return bad + bad2;
}

}  // namespace dn
