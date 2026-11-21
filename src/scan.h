// CSV line scanner for the Shopping Sessions dialect.
//
// scan_chunk<NF>(begin, end, skip_header, on_line) calls on_line(fs, fl) for every well-formed line with
// exactly NF fields: fs[i] / fl[i] are the field's bytes with surrounding quotes removed. A line is
// well-formed when every field is either free of '"' or is '"' + bytes without '"' + '"' (length >= 2).
// Lines end at '\n' only; the last line may lack it. [begin, end) must start at a line start, and at least
// 64 bytes after `end` must be readable.
//
// Stage 1 (per 64-byte block): bit masks of ',', '\n', '"'. A quote is "regular" when it sits right after
// a separator (or the chunk start) or right before one (or the chunk end), and the quote parity since the
// line start is even at every separator. Blocks containing any irregular quote are flagged; lines that touch
// a flagged block are re-validated exactly (slow path). Comma and newline offsets are flattened into arrays.
// Stage 2 (per line): exactly NF-1 commas before the newline; fields split at the commas; a field is quoted
// iff it starts with '"' (regular quotes guarantee the closing one).
#pragma once
#include <immintrin.h>

#include <cstdint>
#include <cstring>

namespace scan {

using u8 = uint8_t;
using u32 = uint32_t;
using u64 = uint64_t;

// exact validation and split of one line [a, b) (no newline inside); returns field count (capped)
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

template <int NF, class F>
static void scan_chunk(const u8* begin, const u8* end, bool skip_header, F&& on_line) {
  const u8* p = begin;
  if (skip_header) {
    const u8* nl = (const u8*)memchr(p, '\n', (size_t)(end - p));
    if (!nl) return;
    p = nl + 1;
  }
  if (p >= end) return;
  const u32 total = (u32)(end - p);
  constexpr u32 WB = 64;  // blocks per window (one bit each in `badw`)
  constexpr u32 CAP = WB * 64 + 64;
  u32 Cpos[CAP + NF + 16];  // comma offsets (relative to p)
  u32 Npos[CAP + 16];       // newline offsets
  u32 nc = 0;               // commas buffered (carry-over of the open line first)
  bool line_bad = false;    // the open line touched a flagged block in an earlier window
  bool line_toomany = false;
  u32 lstart = 0;
  u32 carry = 0;            // quote parity since the start of the open line
  u64 prev_top = 1;         // was the byte before this block a separator (or chunk start)?
  const u8* fs[NF];
  u32 fl[NF];
  const __m256i vc = _mm256_set1_epi8(','), vn = _mm256_set1_epi8('\n'), vq = _mm256_set1_epi8('"');
  for (u32 w = 0; w < total; w += WB * 64) {
    const u32 wend = (total - w < WB * 64) ? total : w + WB * 64;
    u64 badw = 0;
    u32 nn = 0;
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
      if (rem < 64) {
        ends |= 1ULL << (rem - 1);  // the chunk end is a virtual separator
      } else if (rem == 64) {
        ends |= 1ULL << 63;
      } else {
        const u8 nx = blk[64];
        ends |= (u64)((nx == ',') | (nx == '\n')) << 63;
      }
      prev_top = sep >> 63;
      const u64 px = prefix_xor(mq) ^ (0 - (u64)carry);
      const u64 irregular = (mq & ~(starts | ends)) | (px & sep);
      badw |= (u64)(irregular != 0) << bi;
      if (mn) {
        const u32 last = 63 - (u32)__builtin_clzll(mn);
        carry = (u32)__builtin_popcountll(last == 63 ? 0 : (mq >> (last + 1))) & 1;
      } else {
        carry ^= (u32)__builtin_popcountll(mq) & 1;
      }
      // flatten commas and newlines
      {
        u64 m = mc;
        const u32 cnt = (u32)__builtin_popcountll(m);
        u32* o = Cpos + nc;
        for (u32 done = 0;; done += 8) {
#pragma GCC unroll 8
          for (int j = 0; j < 8; j++) {
            o[done + j] = off + (u32)_tzcnt_u64(m);
            m = _blsr_u64(m);
          }
          if (done + 8 >= cnt) break;
        }
        nc += cnt;
      }
      {
        u64 m = mn;
        const u32 cnt = (u32)__builtin_popcountll(m);
        u32* o = Npos + nn;
        for (u32 done = 0;; done += 4) {
#pragma GCC unroll 4
          for (int j = 0; j < 4; j++) {
            o[done + j] = off + (u32)_tzcnt_u64(m);
            m = _blsr_u64(m);
          }
          if (done + 4 >= cnt) break;
        }
        nn += cnt;
      }
    }
    // ---------------- stage 2
    u32 ci = 0;
    for (u32 li = 0; li < nn; li++) {
      const u32 nl = Npos[li];
      // block range of the line within this window
      const u32 b0 = (lstart > w) ? (lstart - w) >> 6 : 0;
      const u32 b1 = (nl - w) >> 6;
      const u64 span = (b1 >= 63 ? ~0ULL : ((2ULL << b1) - 1)) & ~((1ULL << b0) - 1);
      const bool touched_bad = line_bad || (badw & span);
      // commas of this line: Cpos[ci ..] while < nl
      bool shape = (ci + NF - 1 <= nc) && !line_toomany;
      if (shape) {
        if (NF > 1) shape = Cpos[ci + NF - 2] < nl;
        if (shape && ci + NF - 1 < nc) shape = Cpos[ci + NF - 1] > nl;
      }
      if (__builtin_expect(shape, 1)) {
        if (__builtin_expect(!touched_bad, 1)) {
          u32 a = lstart;
          for (int f = 0; f < NF; f++) {
            const u32 bnd = (f == NF - 1) ? nl : Cpos[ci + f];
            const u8* ptr = p + a;
            const u32 len = bnd - a;
            const u32 qd = (ptr[0] == '"') & (len != 0);
            fs[f] = ptr + qd;
            fl[f] = len - 2 * qd;
            a = bnd + 1;
          }
          on_line(fs, fl);
        } else {
          if (slow_line<NF>(p + lstart, p + nl, fs, fl)) on_line(fs, fl);
        }
        ci += NF - 1;
      } else {
        while (ci < nc && Cpos[ci] < nl) ci++;
      }
      lstart = nl + 1;
      line_bad = false;
      line_toomany = false;
    }
    // the open line: remember whether it touched a flagged block; carry its commas
    const u32 rest = nc - ci;
    if (lstart < wend) {
      const u32 b0 = (lstart > w) ? (lstart - w) >> 6 : 0;
      if (badw >> b0) line_bad = true;
    }
    if (rest >= (u32)NF) {
      line_toomany = true;
      nc = 0;
    } else {
      for (u32 r = 0; r < rest; r++) Cpos[r] = Cpos[ci + r];
      nc = rest;
    }
  }
  // last line without a newline
  if (lstart < total && slow_line<NF>(p + lstart, p + total, fs, fl)) on_line(fs, fl);
}

}  // namespace scan
