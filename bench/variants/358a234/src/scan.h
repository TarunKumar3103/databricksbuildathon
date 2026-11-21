// CSV line scanner for the Shopping Sessions dialect.
//
// scan_lines<NF>(begin, end, skip_header, on_vec) calls on_vec(base, st, ln) for every well-formed line
// with exactly NF fields: st and ln are vectors of eight 32-bit values, and field i is the ln[i] bytes at
// base + st[i], surrounding quotes removed (elements NF.. are unspecified). scan_chunk is the same with
// the two vectors stored to arrays: on_line(base, const u32* st, const u32* ln). A line is well-formed when
// every field is either free of '"' or is '"' + bytes without '"' + '"' (length >= 2).
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

// Index of the lowest set bit. Written as an instruction so the compiler does not put a register-clearing
// instruction in front of it (it does that for the BMI count instructions to break a false dependency on the
// destination that these processors no longer have for tzcnt); the flattening loop below is long enough for
// one instruction in five to matter.
static inline __attribute__((always_inline)) u32 tz64(u64 x) {
  u64 r;
  __asm__("tzcnt %1, %0" : "=r"(r) : "r"(x) : "cc");
  return (u32)r;
}

// Appends cnt bits (nb to the stream N, qb to Q) at bit ns. Both streams keep every bit from ns on at 0,
// for at least 64 bits past the byte that holds bit ns. Up to 56 bits go in as one unaligned 64-bit word at
// byte ns / 8 (the bits start at ns % 8 <= 7 in it), with the 8 bytes after it cleared; more than that (64
// bytes nearly all separators) the word-wise way, with one more word cleared.
static inline __attribute__((always_inline)) void append_bits(u64* N, u64* Q, const u32 ns, const u32 cnt, const u64 nb,
                                                              const u64 qb) {
  if (__builtin_expect(cnt <= 56, 1)) {
    u8* const n8 = (u8*)N + (ns >> 3);
    u8* const q8 = (u8*)Q + (ns >> 3);
    const u32 sh = ns & 7;
    u64 x, y;
    memcpy(&x, n8, 8);
    memcpy(&y, q8, 8);
    x |= nb << sh;
    y |= qb << sh;
    memcpy(n8, &x, 8);
    memcpy(q8, &y, 8);
    const u64 z = 0;
    memcpy(n8 + 8, &z, 8);
    memcpy(q8 + 8, &z, 8);
  } else {
    const size_t wi = ns >> 6;
    const u32 lsh = ns & 63, rsh = ~ns & 63;  // rsh = 63 - lsh
    N[wi] |= nb << lsh;
    N[wi + 1] = (nb >> 1) >> rsh;
    N[wi + 2] = 0;
    Q[wi] |= qb << lsh;
    Q[wi + 1] = (qb >> 1) >> rsh;
    Q[wi + 2] = 0;
  }
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

// Always inlined into its caller, together with on_vec: the callback's captured state then lives in
// registers instead of being reloaded through the closure after every store.
template <int NF, class F>
static inline __attribute__((always_inline)) void scan_lines(const u8* begin, const u8* end, bool skip_header,
                                                             F&& on_vec) {
  static_assert(NF >= 1 && NF <= 8, "fields per line");
  // Separators flattened per round of the unrolled loop below. A 64-byte block of generated data holds 3 to 7
  // separators (event lines are 77 bytes with 7, click lines 45 bytes with 4, link lines 20 bytes with 2), so
  // one round of 7 (6 for 4-field lines) covers every regular block, one slot fewer than a round of 8.
  constexpr u32 FLAT = NF == 4 ? 6 : 7;
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
  __m128i vcarry = _mm_setzero_si128();  // bit 0: quote parity since the start of the open line
  alignas(32) u32 st[8] = {0, 0, 0, 0, 0, 0, 0, 0}, ln[8] = {0, 0, 0, 0, 0, 0, 0, 0};  // slow paths only
  const u64 PATMASK = (2ULL << NF) - 2;  // entries 1..NF of a line
  const u64 PAT = 1ULL << NF;            // only the last of them is a newline
  const __m256i vc = _mm256_set1_epi8(','), vn = _mm256_set1_epi8('\n'), vq = _mm256_set1_epi8('"');
  const __m256i MONE = _mm256_set1_epi32(-1);
  const __m256i BITS = _mm256_setr_epi32(1, 2, 4, 8, 16, 32, 64, 128);
  const __m128i ONES = _mm_set1_epi8((char)0xFF);
  // Bit masks of the 64 bytes at `off`: separators, newlines, quotes.
  auto block_masks = [&](size_t off, u64& bsep, u64& bmn, u64& bmq) __attribute__((always_inline)) {
    const u8* blk = p + off;
#if defined(__AVX512BW__)
    // one 64-byte load and three compares straight into mask registers. (512-bit, but only loads and byte
    // compares: the parse runs 256-bit integer multiplies all along, which already hold the cores at the
    // frequency these allow.)
    const __m512i x = _mm512_loadu_si512((const void*)blk);
    const u64 mc = _cvtmask64_u64(_mm512_cmpeq_epi8_mask(x, _mm512_set1_epi8(',')));
    bmn = _cvtmask64_u64(_mm512_cmpeq_epi8_mask(x, _mm512_set1_epi8('\n')));
    bmq = _cvtmask64_u64(_mm512_cmpeq_epi8_mask(x, _mm512_set1_epi8('"')));
    (void)vc;
    (void)vn;
    (void)vq;
#else
    const __m256i a = _mm256_loadu_si256((const __m256i*)blk);
    const __m256i b = _mm256_loadu_si256((const __m256i*)(blk + 32));
    const u64 mc = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vc)) |
                   ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vc)) << 32);
    bmn = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vn)) |
          ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vn)) << 32);
    bmq = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vq)) |
          ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vq)) << 32);
#endif
    bsep = mc | bmn;
  };
  // The masks of a block are computed one iteration early: what follows the last byte of a block is then
  // found in the low bits of the next block's masks.
  u64 sep, mn, mq;
  block_masks(0, sep, mn, mq);
  if (total < 64) {
    const u64 m = (1ULL << total) - 1;
    sep &= m;
    mn &= m;
    mq &= m;
  }
  u32 w = 0;  // start of the current window
  u64 badw = 0;
  // The rest of the work for the block at `off`, whose masks are sep / mn / mq. qn: the byte after position i
  // is a quote. ends2: the byte two after position i is a separator or the chunk end. A quote is regular when
  // the byte before it or the byte after it is a separator; seen from the byte before the quote, that is
  // "this byte is a separator or the one two ahead is" (the very first byte of the chunk needs no check: the
  // chunk start counts as a separator).
  auto finish_block = [&](const u32 off, const u64 qn, const u64 ends2) __attribute__((always_inline)) {
    const __m128i vpx = _mm_clmulepi64_si128(_mm_xor_si128(_mm_cvtsi64_si128((long long)mq), vcarry), ONES, 0);
    const u64 px = (u64)_mm_cvtsi128_si64(vpx);  // quote parity up to and including each position
    const u64 irregular = (qn & ~(sep | ends2)) | (px & sep);
    if (__builtin_expect(irregular == 0, 1)) {
      // every separator of the block sits at even parity, so the parity at the block end is also the
      // parity since the last newline
      vcarry = _mm_srli_epi64(vpx, 63);
    } else {
      badw |= 1ULL << ((off - w) >> 6);
      u64 carry = (u64)_mm_cvtsi128_si64(vcarry) & 1;
      if (mn) {
        const u32 last = 63 - (u32)__builtin_clzll(mn);
        carry = (u64)__builtin_popcountll(last == 63 ? 0 : (mq >> (last + 1))) & 1;
      } else {
        carry ^= (u64)__builtin_popcountll(mq) & 1;
      }
      vcarry = _mm_cvtsi64_si128((long long)carry);
    }
    // append the separators of this block: newline bits, quote-after bits, offsets
    const u32 cnt = (u32)__builtin_popcountll(sep);
    append_bits(Nw, Qw, ns, cnt, _pext_u64(mn, sep), _pext_u64(qn, sep));
    {
      u64 m = sep;
      u32* o = Spos + (size_t)ns;
      for (u32 done = 0;; done += FLAT) {
#pragma GCC unroll 8
        for (u32 j = 0; j < FLAT; j++) {
          o[done + j] = off + tz64(m);
          m = _blsr_u64(m);
        }
        if (done + FLAT >= cnt) break;
      }
    }
    ns += cnt;
  };
  const u32 fast_end = total >= 128 ? total - 127 : 0;  // blocks below it are followed by 64 more bytes
  for (; w < total; w += WB * 64) {
    const u32 wend = (total - w < WB * 64) ? total : w + WB * 64;
    badw = 0;
    // ---------------- stage 1
    u32 off = w;
    for (const u32 fend = wend < fast_end ? wend : fast_end; off < fend; off += 64) {
      // (written out, so that the address is formed from the registers the loads use, instead of from a
      // hoisted p + 2048 that the compiler then keeps on the stack and loads again every block)
      __asm__ volatile("prefetcht0 2048(%0,%1)" : : "r"(p), "r"((u64)off));
      u64 nsep, nmn, nmq;
      block_masks((size_t)off + 64, nsep, nmn, nmq);
      finish_block(off, (mq >> 1) | (nmq << 63), (sep >> 2) | (nsep << 62));
      sep = nsep;
      mn = nmn;
      mq = nmq;
    }
    for (; off < wend; off += 64) {  // the last one or two blocks of the chunk
      const u32 rem = total - off;
      u64 nsep = 0, nmn = 0, nmq = 0;
      u64 virt = sep, nvirt = 0;  // separators, with the chunk end as one more
      if (rem > 64) {
        block_masks((size_t)off + 64, nsep, nmn, nmq);
        const u32 rem2 = rem - 64;  // 1..63
        const u64 m = (1ULL << rem2) - 1;
        nsep &= m;
        nmn &= m;
        nmq &= m;
        nvirt = nsep | (1ULL << rem2);
      } else if (rem == 64) {
        nvirt = 1;
      } else {
        virt |= 1ULL << rem;
      }
      finish_block(off, (mq >> 1) | (nmq << 63), (virt >> 2) | (nvirt << 62));
      sep = nsep;
      mn = nmn;
      mq = nmq;
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
              on_vec(p, _mm256_load_si256((const __m256i*)st), _mm256_load_si256((const __m256i*)ln));
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
        on_vec(p, _mm256_sub_epi32(S0, t),  // separator + 1 (+ 1 if quoted)
               _mm256_add_epi32(_mm256_sub_epi32(S1, S0), _mm256_add_epi32(t, qm)));
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
      on_vec(p, _mm256_load_si256((const __m256i*)st), _mm256_load_si256((const __m256i*)ln));
    }
  }
}

// scan_cols: the same lines, handed over eight at a time where possible. on_group(base, C, q) gets eight
// consecutive well-formed lines as columns: C[k] holds the offsets of separator k of each of the eight lines
// (separator 0 is the newline before the line, or -1 at the chunk start; separator NF is the line's own
// newline), and bit j of q[k] says that field k of line j is quoted. Field k of a line is thus the bytes
// from C[k] + 1 + quoted up to C[k + 1] - quoted. Every other line (near irregular quotes, after a malformed
// line, the last few of a chunk) comes through on_vec exactly as in scan_lines; the order of the lines is
// kept. Needs 64 readable bytes after `end`, like scan_lines.
template <int NF, class G, class F>
static inline __attribute__((always_inline)) void scan_cols(const u8* begin, const u8* end, bool skip_header,
                                                            G&& on_group, F&& on_vec) {
  static_assert(NF >= 1 && NF <= 7, "fields per line");
  // Separators flattened per round of the unrolled loop below. A 64-byte block of generated data holds 3 to 7
  // separators (event lines are 77 bytes with 7, click lines 45 bytes with 4, link lines 20 bytes with 2), so
  // one round of 7 (6 for 4-field lines) covers every regular block, one slot fewer than a round of 8.
  constexpr u32 FLAT = NF == 4 ? 6 : 7;
  const u8* p = begin;
  if (skip_header) {
    const u8* nl = (const u8*)memchr(p, '\n', (size_t)(end - p));
    if (!nl) return;
    p = nl + 1;
  }
  if (p >= end) return;
  const u32 total = (u32)(end - p);
  constexpr u32 WB = 64;  // blocks per window (one bit each in `badw`)
  constexpr u32 CAP = WB * 64 + 64 + 32;  // (up to 8 * NF entries are carried from one window to the next)
  // Separator entries. Entry 0 is always the newline before the open line (a virtual one at offset -1 for
  // the first line), followed by that line's commas.
  u32 Spos[CAP + 80];
  u64 Nw[CAP / 64 + 4], Qw[CAP / 64 + 4];
  Spos[0] = 0xFFFFFFFFu;
  u32 ns = 1;
  Nw[0] = 1;
  Nw[1] = 0;
  Qw[0] = (p[0] == '"');
  Qw[1] = 0;
  bool skip_open = false;  // the open line already has too many fields: no entries are kept for it
  bool line_bad = false;   // the open line touched a flagged block in an earlier window
  __m128i vcarry = _mm_setzero_si128();  // bit 0: quote parity since the start of the open line
  alignas(32) u32 st[8] = {0, 0, 0, 0, 0, 0, 0, 0}, ln[8] = {0, 0, 0, 0, 0, 0, 0, 0};  // slow paths only
  const u64 PATMASK = (2ULL << NF) - 2;  // entries 1..NF of a line
  const u64 PAT = 1ULL << NF;            // only the last of them is a newline
  // the same for eight lines in a row, seen from the entry after the first line's opening newline
  u64 pat8 = 0, stride = 0;
  for (int j = 0; j < 8; j++) {
    pat8 |= 1ULL << (NF * (j + 1) - 1);
    stride |= 1ULL << (NF * j);
  }
  const u64 PAT8 = pat8, PAT8MASK = (1ULL << (8 * NF)) - 1, STRIDE = stride;
  const __m256i vc = _mm256_set1_epi8(','), vn = _mm256_set1_epi8('\n'), vq = _mm256_set1_epi8('"');
  const __m256i MONE = _mm256_set1_epi32(-1);
  const __m256i BITS = _mm256_setr_epi32(1, 2, 4, 8, 16, 32, 64, 128);
  const __m128i ONES = _mm_set1_epi8((char)0xFF);
  // Bit masks of the 64 bytes at `off`: separators, newlines, quotes.
  auto block_masks = [&](size_t off, u64& bsep, u64& bmn, u64& bmq) __attribute__((always_inline)) {
    const u8* blk = p + off;
#if defined(__AVX512BW__)
    // one 64-byte load and three compares straight into mask registers. (512-bit, but only loads and byte
    // compares: the parse runs 256-bit integer multiplies all along, which already hold the cores at the
    // frequency these allow.)
    const __m512i x = _mm512_loadu_si512((const void*)blk);
    const u64 mc = _cvtmask64_u64(_mm512_cmpeq_epi8_mask(x, _mm512_set1_epi8(',')));
    bmn = _cvtmask64_u64(_mm512_cmpeq_epi8_mask(x, _mm512_set1_epi8('\n')));
    bmq = _cvtmask64_u64(_mm512_cmpeq_epi8_mask(x, _mm512_set1_epi8('"')));
    (void)vc;
    (void)vn;
    (void)vq;
#else
    const __m256i a = _mm256_loadu_si256((const __m256i*)blk);
    const __m256i b = _mm256_loadu_si256((const __m256i*)(blk + 32));
    const u64 mc = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vc)) |
                   ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vc)) << 32);
    bmn = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vn)) |
          ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vn)) << 32);
    bmq = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vq)) |
          ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vq)) << 32);
#endif
    bsep = mc | bmn;
  };
  // The masks of a block are computed one iteration early: what follows the last byte of a block is then
  // found in the low bits of the next block's masks.
  u64 sep, mn, mq;
  block_masks(0, sep, mn, mq);
  if (total < 64) {
    const u64 m = (1ULL << total) - 1;
    sep &= m;
    mn &= m;
    mq &= m;
  }
  u32 w = 0;  // start of the current window
  u64 badw = 0;
  // The rest of the work for the block at `off`, whose masks are sep / mn / mq. qn: the byte after position i
  // is a quote. ends2: the byte two after position i is a separator or the chunk end. A quote is regular when
  // the byte before it or the byte after it is a separator; seen from the byte before the quote, that is
  // "this byte is a separator or the one two ahead is" (the very first byte of the chunk needs no check: the
  // chunk start counts as a separator).
  auto finish_block = [&](const u32 off, const u64 qn, const u64 ends2) __attribute__((always_inline)) {
    const __m128i vpx = _mm_clmulepi64_si128(_mm_xor_si128(_mm_cvtsi64_si128((long long)mq), vcarry), ONES, 0);
    const u64 px = (u64)_mm_cvtsi128_si64(vpx);  // quote parity up to and including each position
    const u64 irregular = (qn & ~(sep | ends2)) | (px & sep);
    if (__builtin_expect(irregular == 0, 1)) {
      // every separator of the block sits at even parity, so the parity at the block end is also the
      // parity since the last newline
      vcarry = _mm_srli_epi64(vpx, 63);
    } else {
      badw |= 1ULL << ((off - w) >> 6);
      u64 carry = (u64)_mm_cvtsi128_si64(vcarry) & 1;
      if (mn) {
        const u32 last = 63 - (u32)__builtin_clzll(mn);
        carry = (u64)__builtin_popcountll(last == 63 ? 0 : (mq >> (last + 1))) & 1;
      } else {
        carry ^= (u64)__builtin_popcountll(mq) & 1;
      }
      vcarry = _mm_cvtsi64_si128((long long)carry);
    }
    // append the separators of this block: newline bits, quote-after bits, offsets
    const u32 cnt = (u32)__builtin_popcountll(sep);
    append_bits(Nw, Qw, ns, cnt, _pext_u64(mn, sep), _pext_u64(qn, sep));
    {
      u64 m = sep;
      u32* o = Spos + (size_t)ns;
      for (u32 done = 0;; done += FLAT) {
#pragma GCC unroll 8
        for (u32 j = 0; j < FLAT; j++) {
          o[done + j] = off + tz64(m);
          m = _blsr_u64(m);
        }
        if (done + FLAT >= cnt) break;
      }
    }
    ns += cnt;
  };
  const u32 fast_end = total >= 128 ? total - 127 : 0;  // blocks below it are followed by 64 more bytes
  for (; w < total; w += WB * 64) {
    const u32 wend = (total - w < WB * 64) ? total : w + WB * 64;
    badw = 0;
    // ---------------- stage 1
    u32 off = w;
    for (const u32 fend = wend < fast_end ? wend : fast_end; off < fend; off += 64) {
      // (written out, so that the address is formed from the registers the loads use, instead of from a
      // hoisted p + 2048 that the compiler then keeps on the stack and loads again every block)
      __asm__ volatile("prefetcht0 2048(%0,%1)" : : "r"(p), "r"((u64)off));
      u64 nsep, nmn, nmq;
      block_masks((size_t)off + 64, nsep, nmn, nmq);
      finish_block(off, (mq >> 1) | (nmq << 63), (sep >> 2) | (nsep << 62));
      sep = nsep;
      mn = nmn;
      mq = nmq;
    }
    for (; off < wend; off += 64) {  // the last one or two blocks of the chunk
      const u32 rem = total - off;
      u64 nsep = 0, nmn = 0, nmq = 0;
      u64 virt = sep, nvirt = 0;  // separators, with the chunk end as one more
      if (rem > 64) {
        block_masks((size_t)off + 64, nsep, nmn, nmq);
        const u32 rem2 = rem - 64;  // 1..63
        const u64 m = (1ULL << rem2) - 1;
        nsep &= m;
        nmn &= m;
        nmq &= m;
        nvirt = nsep | (1ULL << rem2);
      } else if (rem == 64) {
        nvirt = 1;
      } else {
        virt |= 1ULL << rem;
      }
      finish_block(off, (mq >> 1) | (nmq << 63), (virt >> 2) | (nvirt << 62));
      sep = nsep;
      mn = nmn;
      mq = nmq;
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
    const bool last_window = wend == total;
    // One line, the one that starts after entry si (si + NF < ns). False if the line has too many fields and
    // is still open at the end of the window.
    auto step = [&]() __attribute__((always_inline)) -> bool {
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
              on_vec(p, _mm256_load_si256((const __m256i*)st), _mm256_load_si256((const __m256i*)ln));
            }
            si += NF;
            return true;
          }
        }
        const __m256i S0 = _mm256_loadu_si256((const __m256i*)(Spos + si));
        const __m256i S1 = _mm256_loadu_si256((const __m256i*)(Spos + si + 1));
        u64 qbits;
        memcpy(&qbits, (const u8*)Qw + (si >> 3), 8);
        qbits >>= (si & 7);
        const __m256i qm = _mm256_cmpeq_epi32(_mm256_and_si256(_mm256_set1_epi32((int)(u32)qbits), BITS), BITS);
        const __m256i t = _mm256_add_epi32(qm, MONE);
        on_vec(p, _mm256_sub_epi32(S0, t), _mm256_add_epi32(_mm256_sub_epi32(S1, S0), _mm256_add_epi32(t, qm)));
        si += NF;
      } else {
        const u64 nlb = nbits & PATMASK;
        if (nlb) {
          si += (u32)_tzcnt_u64(nlb);  // too few fields: skip to the line's newline
        } else {
          const u32 k = next_bit(Nw, si + NF + 1, ns);  // too many fields
          if (k == ns) {
            skip_open = true;
            return false;
          }
          si = k;
        }
        line_bad = false;
      }
      return true;
    };
    if (!careful) {
      // eight well-formed lines at a time, as columns
      while (si + 8 * NF < ns) {
        u64 nb;
        memcpy(&nb, (const u8*)Nw + ((si + 1) >> 3), 8);
        nb >>= ((si + 1) & 7);
        if (__builtin_expect((nb & PAT8MASK) == PAT8, 1)) {
          const u32* r = Spos + si;
          const __m256i r0 = _mm256_loadu_si256((const __m256i*)r), r1 = _mm256_loadu_si256((const __m256i*)(r + NF));
          const __m256i r2 = _mm256_loadu_si256((const __m256i*)(r + 2 * NF)), r3 = _mm256_loadu_si256((const __m256i*)(r + 3 * NF));
          const __m256i r4 = _mm256_loadu_si256((const __m256i*)(r + 4 * NF)), r5 = _mm256_loadu_si256((const __m256i*)(r + 5 * NF));
          const __m256i r6 = _mm256_loadu_si256((const __m256i*)(r + 6 * NF)), r7 = _mm256_loadu_si256((const __m256i*)(r + 7 * NF));
          // rows of eight separators -> columns (an 8 x 8 transpose)
          const __m256i t0 = _mm256_unpacklo_epi32(r0, r1), t1 = _mm256_unpackhi_epi32(r0, r1);
          const __m256i t2 = _mm256_unpacklo_epi32(r2, r3), t3 = _mm256_unpackhi_epi32(r2, r3);
          const __m256i t4 = _mm256_unpacklo_epi32(r4, r5), t5 = _mm256_unpackhi_epi32(r4, r5);
          const __m256i t6 = _mm256_unpacklo_epi32(r6, r7), t7 = _mm256_unpackhi_epi32(r6, r7);
          const __m256i u0 = _mm256_unpacklo_epi64(t0, t2), u1 = _mm256_unpackhi_epi64(t0, t2);
          const __m256i u2 = _mm256_unpacklo_epi64(t1, t3), u3 = _mm256_unpackhi_epi64(t1, t3);
          const __m256i u4 = _mm256_unpacklo_epi64(t4, t6), u5 = _mm256_unpackhi_epi64(t4, t6);
          const __m256i u6 = _mm256_unpacklo_epi64(t5, t7), u7 = _mm256_unpackhi_epi64(t5, t7);
          __m256i C[8];
          C[0] = _mm256_permute2x128_si256(u0, u4, 0x20);
          C[1] = _mm256_permute2x128_si256(u1, u5, 0x20);
          C[2] = _mm256_permute2x128_si256(u2, u6, 0x20);
          C[3] = _mm256_permute2x128_si256(u3, u7, 0x20);
          C[4] = _mm256_permute2x128_si256(u0, u4, 0x31);
          C[5] = _mm256_permute2x128_si256(u1, u5, 0x31);
          C[6] = _mm256_permute2x128_si256(u2, u6, 0x31);
          C[7] = _mm256_permute2x128_si256(u3, u7, 0x31);
          u64 qb;
          memcpy(&qb, (const u8*)Qw + (si >> 3), 8);
          qb >>= (si & 7);
          u32 q[NF];
          for (int k = 0; k < NF; k++) q[k] = (u32)_pext_u64(qb >> k, STRIDE);
          on_group(p, C, q);
          si += 8 * NF;
        } else if (!step()) {
          break;
        }
      }
    }
    if (!skip_open && (careful || last_window))
      while (si + NF < ns)
        if (!step()) break;
    if (skip_open) {
      ns = 0;
      Nw[0] = Nw[1] = Qw[0] = Qw[1] = 0;
      line_bad = false;
      continue;
    }
    if (!careful && !last_window) {
      // Fewer than eight lines' entries are left: all of them go to the front, to be joined by the next
      // window's (no block is flagged, so nothing has to be remembered about them).
      const u32 rest = ns - si;  // 1 .. 8 * NF
      u64 nbits, qbits;
      memcpy(&nbits, (const u8*)Nw + (si >> 3), 8);
      nbits >>= (si & 7);
      memcpy(&qbits, (const u8*)Qw + (si >> 3), 8);
      qbits >>= (si & 7);
      for (u32 k = 0; k < 64; k += 8)  // (forward, so the overlap is harmless)
        _mm256_storeu_si256((__m256i*)(Spos + k), _mm256_loadu_si256((const __m256i*)(Spos + si + k)));
      ns = rest;
      const u64 m = (1ULL << rest) - 1;
      Nw[0] = nbits & m;
      Nw[1] = 0;
      Qw[0] = qbits & m;
      Qw[1] = 0;
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
      on_vec(p, _mm256_load_si256((const __m256i*)st), _mm256_load_si256((const __m256i*)ln));
    }
  }
}

// The same with the fields in two arrays.
template <int NF, class F>
static inline __attribute__((always_inline)) void scan_chunk(const u8* begin, const u8* end, bool skip_header,
                                                             F&& on_line) {
  scan_lines<NF>(begin, end, skip_header, [&](const u8* base, __m256i st, __m256i ln) __attribute__((always_inline)) {
    alignas(32) u32 sta[8], lna[8];
    _mm256_store_si256((__m256i*)sta, st);
    _mm256_store_si256((__m256i*)lna, ln);
    // Hide where the pointers point, so the compiler reads the fields back from memory (one load each);
    // otherwise it extracts every element from the vector registers, which costs two to three times as
    // many instructions.
    const u32* stp = sta;
    const u32* lnp = lna;
    __asm__("" : "+r"(stp), "+r"(lnp));
    on_line(base, stp, lnp);
  });
}

}  // namespace scan
