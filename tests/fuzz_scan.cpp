// Fuzz scan::scan_chunk against a byte-by-byte reference.
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include "../src/scan.h"
using namespace scan;
static uint64_t rng = 0x9E3779B97F4A7C15ULL;
static inline uint64_t nxt() { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng; }

template <int NF>
static void ref_scan(const u8* b, const u8* e, bool skip, std::vector<std::string>& out) {
  const u8* p = b;
  if (skip) { const u8* nl = (const u8*)memchr(p, '\n', e - p); if (!nl) return; p = nl + 1; }
  const u8* fs[NF]; u32 fl[NF];
  while (p < e) {
    const u8* nl = (const u8*)memchr(p, '\n', e - p);
    const u8* le = nl ? nl : e;
    if (slow_line<NF>(p, le, fs, fl)) { std::string r; for (int i = 0; i < NF; i++) { r.append((const char*)fs[i], fl[i]); r.push_back('|'); } out.push_back(r); }
    if (!nl) break;
    p = nl + 1;
  }
}

template <int NF>
static long run(long iters) {
  long bad = 0;
  std::vector<u8> buf;
  for (long it = 0; it < iters; it++) {
    size_t len;
    int mode = nxt() % 4;
    if (mode == 0) len = nxt() % 200;
    else if (mode == 1) len = nxt() % 9000;
    else if (mode == 2) len = nxt() % 40000;
    else len = 4096 * (1 + nxt() % 4) + (nxt() % 3) - 1;  // around window sizes
    buf.assign(len + 128, 0);
    // generate either random soup or near-valid lines
    size_t i = 0;
    int soup = nxt() % 6;
    while (i < len) {
      if (soup == 0) {
        const char* alph = "ab,,\"\n\r x";
        buf[i++] = alph[nxt() % 9];
      } else if (soup == 3) {
        // very long lines: separators and quotes with rare newlines (lines spanning many windows)
        int r = nxt() % 3000;
        buf[i++] = r == 0 ? '\n' : (r < 40 ? ',' : (r < 44 ? '"' : (u8)('a' + r % 26)));
      } else if (soup == 4) {
        // well-shaped lines with huge fields
        for (int f = 0; f < NF && i < len; f++) {
          if (f) buf[i++] = ',';
          bool q = nxt() % 4 == 0;
          size_t flen = nxt() % 8 ? nxt() % 10 : nxt() % 3000;
          if (q && i < len) buf[i++] = '"';
          for (size_t k = 0; k < flen && i < len; k++) buf[i++] = (u8)('a' + nxt() % 26);
          if (q && i < len) buf[i++] = '"';
        }
        if (i < len) buf[i++] = '\n';
      } else {
        // a line of NF fields, random quoting, occasional corruption
        int nf = NF + ((nxt() % 20 == 0) ? (int)(nxt() % 3) - 1 : 0);
        for (int f = 0; f < nf && i < len; f++) {
          if (f) buf[i++] = ',';
          bool q = nxt() % 5 == 0;
          int flen = nxt() % 12;
          if (nxt() % 50 == 0) flen = nxt() % 300;
          if (q && i < len) buf[i++] = '"';
          for (int k = 0; k < flen && i < len; k++) {
            int r = nxt() % 100;
            buf[i++] = r == 0 ? '"' : (r == 1 ? '\r' : (u8)('a' + r % 26));
          }
          if (q && i < len && nxt() % 30) buf[i++] = '"';
        }
        if (i < len) buf[i++] = (nxt() % 200 == 0) ? 'z' : '\n';
        if (i < len && nxt() % 100 == 0) buf[i++] = '\n';  // empty line
      }
    }
    for (size_t k = len; k < buf.size(); k++) buf[k] = (u8)(nxt() % 3 == 0 ? '"' : ',');  // garbage after end
    bool skip = nxt() % 2;
    std::vector<std::string> a, b;
    ref_scan<NF>(buf.data(), buf.data() + len, skip, a);
    scan_chunk<NF>(buf.data(), buf.data() + len, skip, [&](const u8* sb, const u32* fo, const u32* fl) {
      std::string r; for (int k = 0; k < NF; k++) { r.append((const char*)(sb + fo[k]), fl[k]); r.push_back('|'); } b.push_back(r); });
    if (a != b) {
      if (bad < 3) {
        fprintf(stderr, "NF=%d mismatch len=%zu skip=%d: ref %zu lines, fast %zu lines\n", NF, len, skip, a.size(), b.size());
        size_t m = std::min(a.size(), b.size());
        for (size_t k = 0; k < m; k++) if (a[k] != b[k]) { fprintf(stderr, "  first diff at %zu: ref [%s] fast [%s]\n", k, a[k].c_str(), b[k].c_str()); break; }
      }
      bad++;
    }
  }
  return bad;
}

int main(int argc, char** argv) {
  long iters = argc > 1 ? atol(argv[1]) : 20000;
  long b7 = run<7>(iters), b4 = run<4>(iters), b2 = run<2>(iters);
  printf("fuzz: NF7 %ld bad, NF4 %ld bad, NF2 %ld bad (of %ld each)\n", b7, b4, b2, iters);
  return (b7 || b4 || b2) ? 1 : 0;
}
