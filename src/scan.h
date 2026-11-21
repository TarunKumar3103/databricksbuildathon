// CSV line scanner for the Shopping Sessions dialect.
//
// scan_chunk<NF>(begin, end, skip_header, on_line) calls on_line(base, st, ln) for every well-formed line
// with exactly NF fields: field i is the ln[i] bytes at base + st[i], surrounding quotes removed. A line is
// well-formed when every field is either free of '"' or is '"' + bytes without '"' + '"' (length >= 2).
// Lines end at '\n' only; the last line may lack it. [begin, end) must start at a line start, and at least
// 64 bytes after `end` must be readable.
//
// Stage 1 (per 64-byte block): bit masks of ',', '\n', '"'. A quote is "regular" when it sits right after
// a separator (or the chunk start) or right before one (or the chunk end), and the quote parity since the
// line start is even at every separator. Blocks containing any irregular quote are flagged; lines that touch
// a flagged block are re-validated exactly (slow path). The offsets of all separators (commas and newlines,
// in order) are flattened into one array, with two bit streams alongside: "this separator is a newline" and
// "the byte after this separator is a quote".
// Stage 2 (per line): a line is the NF + 1 consecutive separators [previous newline, NF - 1 commas, newline];
// the newline bits check that shape in one compare, and the fields are split with a few vector operations.
#pragma once
#include <immintrin.h>

#include <cstdint>
#include <cstring>

namespace scan {

using u8 = uint8_t;
using u32 = uint32_t;
using u64 = uint64_t;

// exact validation and split of one line [a, b) (no newline inside)
template <int NF>
static inline bool slow_line(const u8* a, const u8* b, const u8** fs, u32* fl) {
  int nf = 0;
  const u8* f = a;
  for (const u8* x = a;; x++) {
    if (x == b || *x == ',') {
      if (nf >= NF) return false;
      u32 len = (u32)(x - f);
      u32 nq = 0;
      for (const u8* y = f; y < x; y++) nq += (*y == '"');
      if (nq) {
        if (!(nq == 2 && len >= 2 && f[0] == '"' && f[len - 1] == '"')) return false;
        fs[nf] = f + 1;
        fl[nf] = len - 2;
      } else {
        fs[nf] = f;
        fl[nf] = len;
      }
      nf++;
      if (x == b) break;
      f = x + 1;
    }
  }
  return nf == NF;
}

static inline u64 prefix_xor(u64 x) {
  return (u64)_mm_cvtsi128_si64(_mm_clmulepi64_si128(_mm_set_epi64x(0, (long long)x), _mm_set1_epi8((char)0xFF), 0));
}

// index of the first set bit at or after `from` and below `limit` in the bit stream w; `limit` if none.
// Bits at and above `limit` in the last word are zero.
static inline u32 next_bit(const u64* w, u32 from, u32 limit) {
  if (from >= limit) return limit;
  u32 wi = from >> 6;
  const u32 last = (limit - 1) >> 6;
  u64 x = w[wi] & (~0ULL << (from & 63));
  for (;;) {
    if (x) {
      const u32 r = wi * 64 + (u32)_tzcnt_u64(x);
      return r < limit ? r : limit;
    }
    if (wi == last) return limit;
    x = w[++wi];
  }
}

// Always inlined into its caller, together with on_line: the callback's captured state then lives in
// registers instead of being reloaded through the closure after every store.
template <int NF, class F>
static inline __attribute__((always_inline)) void scan_chunk(const u8* begin, const u8* end, bool skip_header,
                                                             F&& on_line) {
  static_assert(NF >= 1 && NF <= 8, "fields per line");
  const u8* p = begin;
  if (skip_header) {
    const u8* nl = (const u8*)memchr(p, '\n', (size_t)(end - p));
    if (!nl) return;
    p = nl + 1;
  }
  if (p >= end) return;
  const u32 total = (u32)(end - p);
  constexpr u32 WB = 16;  // blocks per window (one bit each in `badw`)
  constexpr u32 CAP = WB * 64 + 32;
  // Separator entries. Entry 0 is always the newline before the open line (a virtual one at offset -1 for
  // the first line), followed by that line's commas.
  u32 Spos[CAP + 16];
  u64 Nw[CAP / 64 + 4], Qw[CAP / 64 + 4];
  Spos[0] = 0xFFFFFFFFu;
  u32 ns = 1;
  Nw[0] = 1;
  Nw[1] = 0;
  Qw[0] = (p[0] == '"');
  Qw[1] = 0;
  bool skip_open = false;  // the open line already has too many fields: no entries are kept for it
  bool line_bad = false;   // the open line touched a flagged block in an earlier window
  u32 carry = 0;           // quote parity since the start of the open line
  u64 prev_top = 1;        // was the byte before this block a separator (or the chunk start)?
  alignas(32) u32 st[8], ln[8];
  const u64 PATMASK = (2ULL << NF) - 2;  // entries 1..NF of a line
  const u64 PAT = 1ULL << NF;            // only the last of them is a newline
  const __m256i vc = _mm256_set1_epi8(','), vn = _mm256_set1_epi8('\n'), vq = _mm256_set1_epi8('"');
  const __m256i MONE = _mm256_set1_epi32(-1);
  const __m256i BITS = _mm256_setr_epi32(1, 2, 4, 8, 16, 32, 64, 128);
  for (u32 w = 0; w < total; w += WB * 64) {
    const u32 wend = (total - w < WB * 64) ? total : w + WB * 64;
    u64 badw = 0;
    // ---------------- stage 1
    for (u32 off = w, bi = 0; off < wend; off += 64, bi++) {
      const u8* blk = p + off;
      _mm_prefetch((const char*)(blk + 2048), _MM_HINT_T0);
      const __m256i a = _mm256_loadu_si256((const __m256i*)blk);
      const __m256i b = _mm256_loadu_si256((const __m256i*)(blk + 32));
      u64 mc = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vc)) |
               ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vc)) << 32);
      u64 mn = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vn)) |
               ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vn)) << 32);
      u64 mq = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vq)) |
               ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vq)) << 32);
      const u32 rem = total - off;
      if (rem < 64) {
        const u64 m = (1ULL << rem) - 1;
        mc &= m;
        mn &= m;
        mq &= m;
      }
      const u64 sep = mc | mn;
      const u64 starts = (sep << 1) | prev_top;
      u64 ends = sep >> 1;
      u64 qn = mq >> 1;  // the byte after position i is a quote
      if (rem < 64) {
        ends |= 1ULL << (rem - 1);  // the chunk end is a virtual separator
      } else if (rem == 64) {
        ends |= 1ULL << 63;
      } else {
        const u8 nx = blk[64];
        ends |= (u64)((nx == ',') | (nx == '\n')) << 63;
        qn |= (u64)(nx == '"') << 63;
      }
      prev_top = sep >> 63;
      const u64 px = prefix_xor(mq) ^ (0 - (u64)carry);
      const u64 irregular = (mq & ~(starts | ends)) | (px & sep);
      if (__builtin_expect(irregular == 0, 1)) {
        // every separator of the block sits at even parity, so the parity at the block end is also the
        // parity since the last newline
        carry = (u32)(px >> 63);
      } else {
        badw |= 1ULL << bi;
        if (mn) {
          const u32 last = 63 - (u32)__builtin_clzll(mn);
          carry = (u32)__builtin_popcountll(last == 63 ? 0 : (mq >> (last + 1))) & 1;
        } else {
          carry ^= (u32)__builtin_popcountll(mq) & 1;
        }
      }
      // append the separators of this block: newline bits, quote-after bits, offsets
      const u32 cnt = (u32)__builtin_popcountll(sep);
      const u64 nb = _pext_u64(mn, sep), qb = _pext_u64(qn, sep);
      {
        const u32 wi = ns >> 6, sh = ns & 63;
        Nw[wi] |= nb << sh;
        Nw[wi + 1] = (nb >> 1) >> (63 - sh);
        Qw[wi] |= qb << sh;
        Qw[wi + 1] = (qb >> 1) >> (63 - sh);
      }
      {
        u64 m = sep;
        u32* o = Spos + ns;
        for (u32 done = 0;; done += 8) {
#pragma GCC unroll 8
          for (int j = 0; j < 8; j++) {
            o[done + j] = off + (u32)_tzcnt_u64(m);
            m = _blsr_u64(m);
          }
          if (done + 8 >= cnt) break;
        }
      }
      ns += cnt;
    }
    // ---------------- stage 2
    u32 si = 0;  // entry of the newline before the current line
    if (skip_open) {
      const u32 k = next_bit(Nw, 0, ns);
      if (k == ns) {  // the malformed line is still open
        ns = 0;
        Nw[0] = Nw[1] = Qw[0] = Qw[1] = 0;
        continue;
      }
      si = k;
      skip_open = false;
    }
    const bool careful = (badw != 0) | line_bad;
    while (si + NF < ns) {
      u64 nbits;
      memcpy(&nbits, (const u8*)Nw + (si >> 3), 8);
      nbits >>= (si & 7);
      if (__builtin_expect((nbits & PATMASK) == PAT, 1)) {
        if (__builtin_expect(careful, 0)) {
          const u32 lstart = Spos[si] + 1, nl = Spos[si + NF];
          const u32 b0 = (lstart > w) ? (lstart - w) >> 6 : 0;
          const u32 b1 = (nl - w) >> 6;
          const u64 span = (b1 >= 63 ? ~0ULL : ((2ULL << b1) - 1)) & ~((1ULL << b0) - 1);
          const bool touched = line_bad || (badw & span);
          line_bad = false;
          if (touched) {
            const u8* fs[NF];
            u32 fl[NF];
            if (slow_line<NF>(p + lstart, p + nl, fs, fl)) {
              for (int f = 0; f < NF; f++) {
                st[f] = (u32)(fs[f] - p);
                ln[f] = fl[f];
              }
              on_line(p, (const u32*)st, (const u32*)ln);
            }
            si += NF;
            continue;
          }
        }
        const __m256i S0 = _mm256_loadu_si256((const __m256i*)(Spos + si));
        const __m256i S1 = _mm256_loadu_si256((const __m256i*)(Spos + si + 1));
        u64 qbits;
        memcpy(&qbits, (const u8*)Qw + (si >> 3), 8);
        qbits >>= (si & 7);
        // qm: -1 where the field starts with a quote
        const __m256i qm = _mm256_cmpeq_epi32(_mm256_and_si256(_mm256_set1_epi32((int)(u32)qbits), BITS), BITS);
        const __m256i t = _mm256_add_epi32(qm, MONE);
        _mm256_store_si256((__m256i*)st, _mm256_sub_epi32(S0, t));  // separator + 1 (+ 1 if quoted)
        _mm256_store_si256((__m256i*)ln, _mm256_add_epi32(_mm256_sub_epi32(S1, S0), _mm256_add_epi32(t, qm)));
        // Hide where the pointers point, so the compiler reads the fields back from memory (one load each);
        // otherwise it extracts every element from the vector registers, which costs two to three times as
        // many instructions.
        const u32* stp = st;
        const u32* lnp = ln;
        __asm__("" : "+r"(stp), "+r"(lnp));
        on_line(p, stp, lnp);
        si += NF;
      } else {
        const u64 nlb = nbits & PATMASK;
        if (nlb) {
          si += (u32)_tzcnt_u64(nlb);  // too few fields: skip to the line's newline
        } else {
          const u32 k = next_bit(Nw, si + NF + 1, ns);  // too many fields
          if (k == ns) {
            skip_open = true;
            break;
          }
          si = k;
        }
        line_bad = false;
      }
    }
    if (skip_open) {
      ns = 0;
      Nw[0] = Nw[1] = Qw[0] = Qw[1] = 0;
      line_bad = false;
      continue;
    }
    // fewer than NF + 1 entries are left: lines that end among them have too few fields
    for (;;) {
      const u32 k = next_bit(Nw, si + 1, ns);
      if (k == ns) break;
      si = k;
      line_bad = false;
    }
    // the open line: remember whether it touched a flagged block; carry its entries
    {
      const u32 lstart = Spos[si] + 1;
      if (lstart < wend) {
        const u32 b0 = (lstart > w) ? (lstart - w) >> 6 : 0;
        if (badw >> b0) line_bad = true;
      }
      const u32 rest = ns - si;  // 1..NF
      u64 qbits;
      memcpy(&qbits, (const u8*)Qw + (si >> 3), 8);
      qbits >>= (si & 7);
      _mm256_storeu_si256((__m256i*)Spos, _mm256_loadu_si256((const __m256i*)(Spos + si)));  // rest <= 8 entries
      ns = rest;
      Nw[0] = 1;
      Nw[1] = 0;
      Qw[0] = qbits & ((1ULL << rest) - 1);
      Qw[1] = 0;
    }
  }
  // last line without a newline
  if (!skip_open) {
    const u32 lstart = Spos[0] + 1;
    const u8* fs[NF];
    u32 fl[NF];
    if (lstart < total && slow_line<NF>(p + lstart, p + total, fs, fl)) {
      for (int f = 0; f < NF; f++) {
        st[f] = (u32)(fs[f] - p);
        ln[f] = fl[f];
      }
      on_line(p, (const u32*)st, (const u32*)ln);
    }
  }
}

}  // namespace scan
