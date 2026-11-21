// probe2.h: measures the machine and the pipeline on it, and packs the results into ten short text fields.
// Built only with -DPROBE; nothing here is part of a scoring build.
#pragma once
#include <sched.h>
#include <stdarg.h>
#include <sys/statfs.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <cpuid.h>

namespace probe {

static constexpr int NSLOT = 10;
static std::string S[NSLOT];
static double tsc_hz = 0;
static double T[8];  // phase time stamps of the pipeline run in this process

static inline double wall() { return now_s(); }
static inline u64 tsc() { return __rdtsc(); }

__attribute__((format(printf, 2, 3))) static void put(int slot, const char* fmt, ...) {
  char buf[512];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof buf, fmt, ap);
  va_end(ap);
  if (!S[slot].empty()) S[slot] += ' ';
  S[slot] += buf;
}

static std::string slurp(const std::string& path, size_t maxb = 1 << 20) {
  std::string s;
  FILE* f = fopen(path.c_str(), "rb");
  if (!f) return s;
  char buf[4096];
  size_t n;
  while (s.size() < maxb && (n = fread(buf, 1, sizeof buf, f)) > 0) s.append(buf, n);
  fclose(f);
  return s;
}
static std::string word(const std::string& path) {  // first line, trimmed
  std::string s = slurp(path, 256);
  size_t e = s.find('\n');
  if (e != std::string::npos) s.resize(e);
  while (!s.empty() && s.back() == ' ') s.pop_back();
  return s;
}
static std::string bracketed(const std::string& path) {  // "always [madvise] never" -> "madvise"
  std::string s = slurp(path, 256);
  size_t a = s.find('['), b = s.find(']');
  if (a == std::string::npos || b == std::string::npos) return s.empty() ? "na" : "q";
  return s.substr(a + 1, b - a - 1);
}
static u64 meminfo_kb(const char* key) {
  std::string s = slurp("/proc/meminfo");
  size_t p = s.find(key);
  if (p == std::string::npos) return 0;
  return strtoull(s.c_str() + p + strlen(key) + 1, nullptr, 10);
}
static u64 anon_huge_kb() {
  std::string s = slurp("/proc/self/smaps_rollup");
  size_t p = s.find("AnonHugePages:");
  if (p == std::string::npos) return 0;
  return strtoull(s.c_str() + p + 14, nullptr, 10);
}

static void calibrate_tsc() {
  double t0 = wall();
  u64 c0 = tsc();
  while (wall() - t0 < 0.02) {
  }
  double t1 = wall();
  u64 c1 = tsc();
  tsc_hz = (double)(c1 - c0) / (t1 - t0);
}

// n iterations of a one-cycle loop; returns the core frequency in GHz
static NOINLINE double core_ghz(u64 n) {
  u64 k = n;
  u64 c0 = tsc();
  __asm__ volatile("1:\n\tdec %0\n\tjnz 1b" : "+r"(k) : : "cc");
  u64 c1 = tsc();
  return tsc_hz * (double)n / (double)(c1 - c0) / 1e9;
}

// ---- slot 0: what the machine is
static void identity() {
  unsigned a, b, c, d;
  unsigned fam = 0, model = 0, step = 0;
  if (__get_cpuid(1, &a, &b, &c, &d)) {
    step = a & 15;
    model = ((a >> 4) & 15) | ((a >> 12) & 0xF0);
    fam = ((a >> 8) & 15) + ((a >> 20) & 0xFF);
  }
  char brand[52] = {0};
  for (unsigned i = 0; i < 3; i++) {
    unsigned r[4] = {0, 0, 0, 0};
    __get_cpuid(0x80000002 + i, &r[0], &r[1], &r[2], &r[3]);
    memcpy(brand + 16 * i, r, 16);
  }
  std::string br;
  for (const char* p = brand; *p; p++)
    if (*p != ' ' && *p != '(' && *p != ')' && *p != '"') br += *p;
  br.erase(0, br.find_first_not_of(' '));
  // feature letters: F avx512f, B bw, L vl, N vnni, M vbmi, 2 vbmi2, A bitalg, P vpopcntdq, G gfni, E vaes, Q vpclmulqdq,
  // R fsrm, S erms, X amx-tile, H hypervisor
  std::string fl;
  unsigned b7 = 0, c7 = 0, d7 = 0, a7 = 0;
  __get_cpuid_count(7, 0, &a7, &b7, &c7, &d7);
  if (b7 & (1u << 16)) fl += 'F';
  if (b7 & (1u << 30)) fl += 'B';
  if (b7 & (1u << 31)) fl += 'L';
  if (c7 & (1u << 11)) fl += 'N';
  if (c7 & (1u << 1)) fl += 'M';
  if (c7 & (1u << 6)) fl += '2';
  if (c7 & (1u << 12)) fl += 'A';
  if (c7 & (1u << 14)) fl += 'P';
  if (c7 & (1u << 8)) fl += 'G';
  if (c7 & (1u << 9)) fl += 'E';
  if (c7 & (1u << 10)) fl += 'Q';
  if (d7 & (1u << 4)) fl += 'R';
  if (b7 & (1u << 9)) fl += 'S';
  if (d7 & (1u << 24)) fl += 'X';
  if (__get_cpuid(1, &a, &b, &c, &d) && (c & (1u << 31))) fl += 'H';
  put(0, "fms=%u.%u.%u %s fl=%s", fam, model, step, br.c_str(), fl.c_str());
  // caches (cpuid leaf 4)
  std::string caches;
  for (unsigned i = 0; i < 8; i++) {
    unsigned ea = 0, eb = 0, ec = 0, ed = 0;
    __get_cpuid_count(4, i, &ea, &eb, &ec, &ed);
    unsigned type = ea & 31;
    if (!type) break;
    if (type == 2) continue;  // instruction cache
    unsigned level = (ea >> 5) & 7, share = ((ea >> 14) & 0xFFF) + 1;
    u64 size = (u64)(((eb >> 22) & 0x3FF) + 1) * (((eb >> 12) & 0x3FF) + 1) * ((eb & 0xFFF) + 1) * ((u64)ec + 1);
    char t[64];
    snprintf(t, sizeof t, "l%u=%lluk/%u", level, (unsigned long long)(size >> 10), share);
    caches += (caches.empty() ? "" : ",");
    caches += t;
  }
  put(0, "%s", caches.c_str());
  cpu_set_t set;
  CPU_ZERO(&set);
  int naff = 0;
  std::string aff;
  if (sched_getaffinity(0, sizeof set, &set) == 0) {
    for (int i = 0; i < CPU_SETSIZE; i++)
      if (CPU_ISSET(i, &set)) {
        naff++;
        std::string sib = word("/sys/devices/system/cpu/cpu" + std::to_string(i) + "/topology/thread_siblings_list");
        if (naff <= 8) aff += (aff.empty() ? "" : ";") + std::to_string(i) + ":" + sib;
      }
  }
  put(0, "nc=%d/%ld sib=%s", naff, sysconf(_SC_NPROCESSORS_ONLN), aff.c_str());
  put(0, "thp=%s/%s", bracketed("/sys/kernel/mm/transparent_hugepage/enabled").c_str(),
      bracketed("/sys/kernel/mm/transparent_hugepage/defrag").c_str());
  struct utsname u;
  if (uname(&u) == 0) put(0, "k=%s", u.release);
  put(0, "mem=%llum free=%llum cach=%llum", (unsigned long long)(meminfo_kb("MemTotal") >> 10),
      (unsigned long long)(meminfo_kb("MemFree") >> 10), (unsigned long long)(meminfo_kb("Cached") >> 10));
  put(0, "cg=%s/%s", word("/sys/fs/cgroup/cpu.max").c_str(), word("/sys/fs/cgroup/memory.max").c_str());
  put(0, "ld=%s", word("/proc/loadavg").substr(0, 14).c_str());
}

// ---- slot 1: frequency, branches, threads
__attribute__((target("avx512f,avx512dq"))) static NOINLINE void burn_zmm_light(double seconds) {
  __m512i a = _mm512_set1_epi32(1), b = _mm512_set1_epi32(3);
  double t0 = wall();
  do {
    for (int i = 0; i < 20000; i++) {
      a = _mm512_add_epi32(a, b);
      a = _mm512_xor_si512(a, b);
    }
    __asm__ volatile("" : "+x"(a));
  } while (wall() - t0 < seconds);
}
__attribute__((target("avx512f,avx512dq"))) static NOINLINE void burn_zmm_heavy(double seconds) {
  __m512d a = _mm512_set1_pd(1.0000001), b = _mm512_set1_pd(0.9999999), c = a, d = b;
  double t0 = wall();
  do {
    for (int i = 0; i < 20000; i++) {
      a = _mm512_mul_pd(a, b);
      c = _mm512_mul_pd(c, d);
    }
    __asm__ volatile("" : "+x"(a), "+x"(c));
  } while (wall() - t0 < seconds);
}
static NOINLINE void burn_ymm_int_mul(double seconds) {  // what the timestamp kernels use, densely
  __m256i a = _mm256_set1_epi8(3), b = _mm256_set1_epi8(5);
  double t0 = wall();
  do {
    for (int i = 0; i < 20000; i++) {
      a = _mm256_maddubs_epi16(a, b);
      a = _mm256_madd_epi16(a, b);
      a = _mm256_or_si256(a, b);
    }
    __asm__ volatile("" : "+x"(a));
  } while (wall() - t0 < seconds);
}
static NOINLINE void burn_ymm_light(double seconds) {
  __m256i a = _mm256_set1_epi32(1), b = _mm256_set1_epi32(3);
  double t0 = wall();
  do {
    for (int i = 0; i < 20000; i++) {
      a = _mm256_add_epi32(a, b);
      a = _mm256_xor_si256(a, b);
    }
    __asm__ volatile("" : "+x"(a));
  } while (wall() - t0 < seconds);
}
static NOINLINE void fa(u64& x) { x = x * 3 + 1; }
static NOINLINE void fb(u64& x) { x = x * 5 + 7; }

static void frequency() {
  double g1 = core_ghz(20000000), g2 = core_ghz(20000000);
  put(1, "tsc=%.0f ghz=%.2f/%.2f", tsc_hz / 1e6, g1, g2);
  struct {
    const char* name;
    void (*fn)(double);
  } loads[] = {{"yl", burn_ymm_light}, {"ym", burn_ymm_int_mul}, {"zl", burn_zmm_light}, {"zh", burn_zmm_heavy}};
  for (auto& l : loads) {
    l.fn(0.03);
    double f0 = core_ghz(1000000);   // right after (a frequency license would still be held)
    double f2 = core_ghz(30000000);  // ~10 ms later
    put(1, "%s=%.2f>%.2f", l.name, f0, f2);
  }
  // all cores busy: frequency seen by one of them
  {
    std::atomic<bool> stop{false};
    std::vector<std::thread> th;
    for (int t = 0; t < NT - 1; t++)
      th.emplace_back([&] {
        volatile u64 x = 0;
        while (!stop.load(std::memory_order_relaxed)) x = x + 1;
      });
    burn_ymm_light(0.02);
    double f = core_ghz(30000000);
    stop = true;
    for (auto& t : th) t.join();
    put(1, "all=%.2f", f);
  }
  // branch mispredict cost
  {
    const u32 N = 1 << 20;
    std::vector<u8> r(N), z(N, 1);
    u64 s = 88172645463325252ULL;
    for (u32 i = 0; i < N; i++) {
      s ^= s << 13;
      s ^= s >> 7;
      s ^= s << 17;
      r[i] = (s >> 33) & 1;
    }
    auto run = [&](const std::vector<u8>& b) {
      u64 x = 1, best = ~0ULL;
      for (int rep = 0; rep < 5; rep++) {
        u64 c0 = tsc();
        for (u32 i = 0; i < N; i++) {
          if (b[i])
            fa(x);
          else
            fb(x);
        }
        u64 c1 = tsc();
        best = std::min(best, c1 - c0);
      }
      __asm__ volatile("" : "+r"(x));
      return (double)best / N;
    };
    double tp = run(z), tr = run(r);
    double ghz = core_ghz(5000000);
    put(1, "mp=%.1fcyc", (tr - tp) * 2 * (ghz * 1e9 / tsc_hz));
  }
}

static void scaling() {
  auto cpu_work = [](u64 iters) {
    u64 x = 1, acc = 0;
    for (u64 i = 0; i < iters; i++) {
      x = (x + i) * 0x9E3779B97F4A7C15ULL;
      x ^= x >> 29;
      acc += x >> 60;
      acc ^= _pext_u64(x, 0x7F7F7F7F7F7F7F7FULL);
    }
    __asm__ volatile("" : "+r"(acc));
  };
  const size_t NS = (size_t)1 << 22;  // 32 MB of u64 per thread: latency-bound work
  for (int mode = 0; mode < 2; mode++) {
    std::string line;
    double base = 0;
    for (int nt : {1, 2, 4, 8}) {
      if (nt > NT && nt != 1) continue;
      std::vector<std::vector<u64>> mem(mode ? nt : 0);
      if (mode)
        for (auto& m : mem) {
          m.resize(NS);
          for (size_t i = 0; i < NS; i++) m[i] = (i * 6364136223846793005ULL + 1442695040888963407ULL) & (NS - 1);
        }
      double t0 = wall();
      std::vector<std::thread> th;
      for (int t = 0; t < nt; t++)
        th.emplace_back([&, t]() {
          if (!mode) {
            cpu_work(20000000);
          } else {
            u64 j[8] = {1, 100003, 200003, 300017, 400037, 500011, 600011, 700003};
            const u64* a = mem[t].data();
            for (int k = 0; k < 300000; k++)
              for (int c = 0; c < 8; c++) j[c] = a[j[c]];
            volatile u64 s = j[0];
            (void)s;
          }
        });
      for (auto& t : th) t.join();
      double dt = wall() - t0;
      double thr = nt / dt;
      if (nt == 1) base = thr;
      char buf[64];
      if (nt == 1)
        snprintf(buf, sizeof buf, "%.0fms", dt * 1e3);
      else
        snprintf(buf, sizeof buf, ",%d:%.2f", nt, thr / base);
      line += buf;
    }
    put(1, "%s=%s", mode ? "sr" : "sc", line.c_str());
  }
  {
    double t0 = wall();
    for (int r = 0; r < 20; r++) {
      std::vector<std::thread> th;
      for (int t = 0; t < 7; t++) th.emplace_back([]() {});
      for (auto& t : th) t.join();
    }
    put(1, "thr7=%.0fus", (wall() - t0) * 1e6 / 20);
  }
}

// ---- slot 2: memory and files
static void memory() {
  const size_t SZ = (size_t)192 << 20;
  for (int huge = 1; huge >= 0; huge--) {
    u64 h0 = anon_huge_kb();
    u8* p = (u8*)mmap(nullptr, SZ, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (p == MAP_FAILED) return;
    madvise(p, SZ, huge ? MADV_HUGEPAGE : MADV_NOHUGEPAGE);
    double t1 = wall();
    for (size_t i = 0; i < SZ; i += 4096) p[i] = 1;
    double t2 = wall();
    u64 h1 = anon_huge_kb();
    u64 sum = 0;
    double t3 = wall();
    for (size_t i = 0; i < SZ; i += 8) {
      u64 v;
      memcpy(&v, p + i, 8);
      sum += v;
    }
    double t4 = wall();
    for (size_t i = 0; i < SZ; i += 32) _mm256_stream_si256((__m256i*)(p + i), _mm256_set1_epi8(1));
    _mm_sfence();
    double t5 = wall();
    const size_t NSL = (size_t)1 << 24;  // dependent random loads over 128 MB
    u64* a = (u64*)p;
    for (size_t i = 0; i < NSL; i++) a[i] = (i * 6364136223846793005ULL + 1442695040888963407ULL) & (NSL - 1);
    double t6 = wall();
    u64 i0 = 1;
    const int STEPS = 1 << 19;
    for (int k = 0; k < STEPS; k++) i0 = a[i0];
    double t7 = wall();
    u64 j[8] = {1, 1000003, 2000003, 3000017, 4000037, 5000011, 6000011, 7000003};
    for (int k = 0; k < STEPS; k++)
      for (int c = 0; c < 8; c++) j[c] = a[j[c]];
    double t8 = wall();
    sum += i0 + j[0] + j[7];
    __asm__ volatile("" : "+r"(sum));
    munmap(p, SZ);
    double t9 = wall();
    put(2, "%s:touch=%.0fns/pg,hp=%llum,rd=%.1fg,nt=%.1fg,rnd=%.0fns,rnd8=%.1fns,unmap=%.1fms", huge ? "H" : "4K",
        (t2 - t1) * 1e9 / (SZ / 4096), (unsigned long long)((h1 - h0) >> 10), SZ / (t4 - t3) / 1e9, SZ / (t5 - t4) / 1e9,
        (t7 - t6) * 1e9 / STEPS, (t8 - t7) * 1e9 / STEPS / 8, (t9 - t8) * 1e3);
  }
  // all threads touching fresh memory at once (64 MB each), both ways
  for (int huge = 1; huge >= 0; huge--) {
    const size_t SZT = (size_t)64 << 20;
    std::vector<u8*> ps(NT);
    for (auto& p : ps) {
      p = (u8*)mmap(nullptr, SZT, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
      madvise(p, SZT, huge ? MADV_HUGEPAGE : MADV_NOHUGEPAGE);
    }
    double t0 = wall();
    {
      std::vector<std::thread> th;
      for (int t = 0; t < NT; t++)
        th.emplace_back([&, t] {
          for (size_t i = 0; i < SZT; i += 4096) ps[t][i] = 1;
        });
      for (auto& t : th) t.join();
    }
    double t1 = wall();
    {  // every thread streaming into its own memory, then reading it back
      std::vector<std::thread> th;
      for (int t = 0; t < NT; t++)
        th.emplace_back([&, t] {
          for (size_t i = 0; i < SZT; i += 32) _mm256_stream_si256((__m256i*)(ps[t] + i), _mm256_set1_epi8(1));
          _mm_sfence();
        });
      for (auto& t : th) t.join();
    }
    double t2 = wall();
    {
      std::vector<std::thread> th;
      for (int t = 0; t < NT; t++)
        th.emplace_back([&, t] {
          u64 sum = 0;
          for (size_t i = 0; i < SZT; i += 8) {
            u64 v;
            memcpy(&v, ps[t] + i, 8);
            sum += v;
          }
          __asm__ volatile("" : "+r"(sum));
        });
      for (auto& t : th) t.join();
    }
    double t3 = wall();
    for (auto& p : ps) munmap(p, SZT);
    put(2, "%s=%.0fns/pg,nt=%.1fg,rd=%.1fg", huge ? "allH" : "all4K", (t1 - t0) * 1e9 / (SZT / 4096),
        (double)SZT * NT / (t2 - t1) / 1e9, (double)SZT * NT / (t3 - t2) / 1e9);
  }
}

static std::string largest_file(const std::string& dir, size_t& mx) {
  std::string best;
  mx = 0;
  for (auto& f : list_csv(dir)) {
    struct stat st;
    if (stat(f.c_str(), &st) == 0 && (size_t)st.st_size > mx) {
      mx = st.st_size;
      best = f;
    }
  }
  return best;
}

static void files(const std::string& data, const std::string& outdir) {
  size_t mx;
  std::string best = largest_file(data + "/events", mx);
  struct statfs sf;
  unsigned long dfs = 0, ofs = 0;
  if (statfs(data.c_str(), &sf) == 0) dfs = (unsigned long)sf.f_type;
  if (statfs(outdir.c_str(), &sf) == 0) ofs = (unsigned long)sf.f_type;
  put(2, "dfs=%lx ofs=%lx", dfs, ofs);
  if (best.empty()) return;
  {
    int fd = open(best.c_str(), O_RDONLY);
    u8* p = (u8*)mmap(nullptr, mx, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    // how much of it is in the page cache before we read anything
    size_t pages = (mx + 4095) / 4096, in = 0;
    std::vector<unsigned char> vec(pages);
    if (mincore(p, mx, vec.data()) == 0)
      for (unsigned char v : vec) in += v & 1;
    volatile u64 s = 0;
    double t1 = wall();
    for (size_t i = 0; i < mx; i += 4096) s = s + p[i];
    double t2 = wall();
    madvise(p, mx & ~(size_t)4095, MADV_DONTNEED);
    double t3 = wall();
    for (size_t i = 0; i < mx; i += 4096) s = s + p[i];
    double t4 = wall();
    munmap(p, mx);
    double t5 = wall();
    put(2, "f%zum:res=%.0f%%,map=%.0fns/pg,again=%.0f,dontneed=%.1fms,unmap=%.1fms", mx >> 20, 100.0 * in / pages,
        (t2 - t1) * 1e9 / pages, (t4 - t3) * 1e9 / pages, (t3 - t2) * 1e3, (t5 - t4) * 1e3);
  }
  {
    std::vector<u8> buf(1 << 20);
    double t0 = wall();
    int fd = open(best.c_str(), O_RDONLY);
    size_t tot = 0;
    ssize_t n;
    while ((n = read(fd, buf.data(), buf.size())) > 0) tot += n;
    close(fd);
    double t1 = wall();
    put(2, "read=%.1fg", tot / (t1 - t0) / 1e9);
  }
  {  // writing 64 MB to the output directory
    std::string path = outdir + "/.probe_tmp";
    std::vector<u8> buf(1 << 20, 7);
    double t0 = wall();
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
      size_t tot = 0;
      for (int i = 0; i < 64; i++) {
        ssize_t n = write(fd, buf.data(), buf.size());
        if (n > 0) tot += n;
      }
      double t1 = wall();
      close(fd);
      double t2 = wall();
      unlink(path.c_str());
      double t3 = wall();
      put(2, "ow=%.2fg,close=%.1fms,unlink=%.1fms", tot / (t1 - t0) / 1e9, (t2 - t1) * 1e3, (t3 - t2) * 1e3);
    } else {
      put(2, "ow=fail");
    }
  }
}

// ---- slot 7: separator scan of a whole file (masks + flattening the separator offsets), several ways
static NOINLINE u64 scan_avx2(const u8* p, size_t n, u32* out) {
  const __m256i vc = _mm256_set1_epi8(','), vn = _mm256_set1_epi8('\n'), vq = _mm256_set1_epi8('"');
  u64 total = 0;
  for (size_t off = 0; off + 64 <= n; off += 64) {
    const __m256i a = _mm256_loadu_si256((const __m256i*)(p + off)), b = _mm256_loadu_si256((const __m256i*)(p + off + 32));
    u64 mc = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vc)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vc)) << 32);
    u64 mn = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vn)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vn)) << 32);
    u64 mq = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, vq)) | ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, vq)) << 32);
    u64 m = mc | mn;
    const u32 cnt = (u32)__builtin_popcountll(m);
    u32* o = out + (total & 1023);
    for (u32 done = 0;; done += 8) {
      for (int j = 0; j < 8; j++) {
        o[done + j] = (u32)off + scan::tz64(m);
        m = _blsr_u64(m);
      }
      if (done + 8 >= cnt) break;
    }
    total += cnt + (mq & 1);
  }
  return total;
}
// 256-bit compares into mask registers, joined with kunpckdq
__attribute__((target("avx512f,avx512bw,avx512vl"))) static NOINLINE u64 scan_k256(const u8* p, size_t n, u32* out) {
  const __m256i vc = _mm256_set1_epi8(','), vn = _mm256_set1_epi8('\n'), vq = _mm256_set1_epi8('"');
  u64 total = 0;
  for (size_t off = 0; off + 64 <= n; off += 64) {
    const __m256i a = _mm256_loadu_si256((const __m256i*)(p + off)), b = _mm256_loadu_si256((const __m256i*)(p + off + 32));
    const u64 mc = _cvtmask64_u64(_mm512_kunpackd(_mm256_cmpeq_epi8_mask(b, vc), _mm256_cmpeq_epi8_mask(a, vc)));
    const u64 mn = _cvtmask64_u64(_mm512_kunpackd(_mm256_cmpeq_epi8_mask(b, vn), _mm256_cmpeq_epi8_mask(a, vn)));
    const u64 mq = _cvtmask64_u64(_mm512_kunpackd(_mm256_cmpeq_epi8_mask(b, vq), _mm256_cmpeq_epi8_mask(a, vq)));
    u64 m = mc | mn;
    const u32 cnt = (u32)__builtin_popcountll(m);
    u32* o = out + (total & 1023);
    for (u32 done = 0;; done += 8) {
      for (int j = 0; j < 8; j++) {
        o[done + j] = (u32)off + scan::tz64(m);
        m = _blsr_u64(m);
      }
      if (done + 8 >= cnt) break;
    }
    total += cnt + (mq & 1);
  }
  return total;
}
__attribute__((target("avx512f,avx512bw,avx512vl"))) static NOINLINE u64 scan_z_tz(const u8* p, size_t n, u32* out) {
  const __m512i vc = _mm512_set1_epi8(','), vn = _mm512_set1_epi8('\n'), vq = _mm512_set1_epi8('"');
  u64 total = 0;
  for (size_t off = 0; off + 64 <= n; off += 64) {
    const __m512i a = _mm512_loadu_si512((const void*)(p + off));
    u64 m = _mm512_cmpeq_epi8_mask(a, vc) | _mm512_cmpeq_epi8_mask(a, vn);
    const u64 mq = _mm512_cmpeq_epi8_mask(a, vq);
    const u32 cnt = (u32)__builtin_popcountll(m);
    u32* o = out + (total & 1023);
    for (u32 done = 0;; done += 8) {
      for (int j = 0; j < 8; j++) {
        o[done + j] = (u32)off + scan::tz64(m);
        m = _blsr_u64(m);
      }
      if (done + 8 >= cnt) break;
    }
    total += cnt + (mq & 1);
  }
  return total;
}
__attribute__((target("avx512f,avx512bw,avx512vl"))) static NOINLINE u64 scan_z_compressd(const u8* p, size_t n, u32* out) {
  const __m512i vc = _mm512_set1_epi8(','), vn = _mm512_set1_epi8('\n'), vq = _mm512_set1_epi8('"');
  const __m512i iota = _mm512_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
  u64 total = 0;
  for (size_t off = 0; off + 64 <= n; off += 64) {
    const __m512i a = _mm512_loadu_si512((const void*)(p + off));
    const u64 m = _mm512_cmpeq_epi8_mask(a, vc) | _mm512_cmpeq_epi8_mask(a, vn);
    const u64 mq = _mm512_cmpeq_epi8_mask(a, vq);
    u32* o = out + (total & 1023);
    __m512i base = _mm512_add_epi32(iota, _mm512_set1_epi32((int)off));
    for (int k = 0; k < 4; k++) {
      const __mmask16 mk = (__mmask16)(m >> (16 * k));
      _mm512_storeu_si512((void*)o, _mm512_maskz_compress_epi32(mk, base));
      o += __builtin_popcount(mk);
      base = _mm512_add_epi32(base, _mm512_set1_epi32(16));
    }
    total += (u32)__builtin_popcountll(m) + (mq & 1);
  }
  return total;
}
__attribute__((target("avx512f,avx512bw,avx512vl,avx512vbmi2"))) static NOINLINE u64 scan_z_compressb(const u8* p, size_t n, u32* out) {
  const __m512i vc = _mm512_set1_epi8(','), vn = _mm512_set1_epi8('\n'), vq = _mm512_set1_epi8('"');
  alignas(64) u8 io[64];
  for (int i = 0; i < 64; i++) io[i] = (u8)i;
  const __m512i iota = _mm512_load_si512((const void*)io);
  u64 total = 0;
  for (size_t off = 0; off + 64 <= n; off += 64) {
    const __m512i a = _mm512_loadu_si512((const void*)(p + off));
    const u64 m = _mm512_cmpeq_epi8_mask(a, vc) | _mm512_cmpeq_epi8_mask(a, vn);
    const u64 mq = _mm512_cmpeq_epi8_mask(a, vq);
    const __m512i idx = _mm512_maskz_compress_epi8(m, iota);  // block-relative offsets, bytes
    u32* o = out + (total & 1023);
    _mm512_storeu_si512((void*)o, _mm512_add_epi32(_mm512_cvtepu8_epi32(_mm512_castsi512_si128(idx)), _mm512_set1_epi32((int)off)));
    total += (u32)__builtin_popcountll(m) + (mq & 1);
  }
  return total;
}

static void scan_kernels(const std::string& data) {
  size_t mx;
  std::string best = largest_file(data + "/events", mx);
  if (best.empty()) return;
  MFile f = map_file(best);
  std::vector<u32> out(2048 + 128);
  const bool has512 = __builtin_cpu_supports("avx512bw"), has_vbmi2 = __builtin_cpu_supports("avx512vbmi2");
  struct K {
    const char* name;
    u64 (*fn)(const u8*, size_t, u32*);
    bool ok;
  } ks[] = {{"a2", scan_avx2, true},      {"k256", scan_k256, has512},        {"ztz", scan_z_tz, has512},
            {"zcd", scan_z_compressd, has512}, {"zcb", scan_z_compressb, has512 && has_vbmi2}, {"a2b", scan_avx2, true}};
  scan_avx2(f.data, f.size, out.data());  // warm the mapping
  put(7, "scan ns/blk@ghz");
  for (auto& k : ks) {
    if (!k.ok) {
      put(7, "%s=na", k.name);
      continue;
    }
    u64 best_t = ~0ULL;
    for (int r = 0; r < 6; r++) {
      u64 c0 = tsc();
      u64 tot = k.fn(f.data, f.size, out.data());
      u64 c1 = tsc();
      __asm__ volatile("" : "+r"(tot));
      best_t = std::min(best_t, c1 - c0);
    }
    double ghz = core_ghz(2000000);
    put(7, "%s=%.2f@%.2f", k.name, (double)best_t / f.size * 64 / tsc_hz * 1e9, ghz);
  }
}

// ---- child processes: a child fills its own slots and sends them to the parent through a pipe
static pid_t spawn(int& rfd, int& wfd) {
  int fds[2];
  rfd = wfd = -1;
  if (pipe(fds) != 0) return -1;
  pid_t pid = fork();
  if (pid == 0) {
    close(fds[0]);
    wfd = fds[1];
    for (auto& s : S) s.clear();
    return 0;
  }
  close(fds[1]);
  rfd = fds[0];
  return pid;
}
[[noreturn]] static void child_finish(int wfd) {
  std::string all;
  for (int i = 0; i < NSLOT; i++)
    if (!S[i].empty()) all += std::to_string(i) + "\t" + S[i] + "\n";
  char tbuf[64];
  snprintf(tbuf, sizeof tbuf, "T\t%.6f\n", wall());
  all += tbuf;
  size_t off = 0;
  while (off < all.size()) {
    ssize_t n = write(wfd, all.data() + off, all.size() - off);
    if (n <= 0) break;
    off += n;
  }
  close(wfd);
  _exit(0);
}
// Returns the time from the child's last word to its exit being seen (what the kernel needs to tear it down),
// in ms; -1 if the child did not finish properly.
static double collect(pid_t pid, int rfd) {
  if (pid <= 0) return -1;
  std::string got;
  char buf[4096];
  ssize_t n;
  while ((n = read(rfd, buf, sizeof buf)) > 0) got.append(buf, n);
  close(rfd);
  int st = 0;
  waitpid(pid, &st, 0);
  double t1 = wall();
  double child_end = 0;
  size_t pos = 0;
  while (pos < got.size()) {
    size_t e = got.find('\n', pos);
    if (e == std::string::npos) e = got.size();
    std::string line = got.substr(pos, e - pos);
    size_t tab = line.find('\t');
    if (tab != std::string::npos) {
      if (line[0] == 'T') {
        child_end = atof(line.c_str() + tab + 1);
      } else {
        int slot = atoi(line.c_str());
        if (slot >= 0 && slot < NSLOT) {
          if (!S[slot].empty()) S[slot] += ' ';
          S[slot] += line.substr(tab + 1);
        }
      }
    }
    pos = e + 1;
  }
  return (child_end > 0 && WIFEXITED(st) && WEXITSTATUS(st) == 0) ? (t1 - child_end) * 1e3 : -1;
}
template <class Fn>
static double in_child(Fn&& fn) {
  int rfd, wfd;
  pid_t pid = spawn(rfd, wfd);
  if (pid == 0) {
    fn();
    child_finish(wfd);
  }
  return collect(pid, rfd);
}

static void remove_tree(const std::string& dir) {  // two levels are enough for an output directory
  DIR* d = opendir(dir.c_str());
  if (!d) return;
  while (dirent* e = readdir(d)) {
    std::string n = e->d_name;
    if (n == "." || n == "..") continue;
    std::string p = dir + "/" + n;
    struct stat st;
    if (lstat(p.c_str(), &st) != 0) continue;
    if (S_ISDIR(st.st_mode))
      remove_tree(p);
    else
      unlink(p.c_str());
  }
  closedir(d);
  rmdir(dir.c_str());
}

// Phase report of the pipeline run that has just finished in this (child) process.
static void pipeline_report(int slot, const char* tag) {
  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);
  // T: 0 start, 1 links, 2 parse, 3 dedup, 4 person, 5 output
  put(slot, "%s:tot=%.1f,lk=%.1f,pa=%.1f,dd=%.1f,pe=%.1f,ou=%.1f", tag, (T[5] - T[0]) * 1e3, (T[1] - T[0]) * 1e3,
      (T[2] - T[1]) * 1e3, (T[3] - T[2]) * 1e3, (T[4] - T[3]) * 1e3, (T[5] - T[4]) * 1e3);
  std::string u = "u=", s = "s=";
  for (int i = 1; i < 6; i++) {
    char b[32];
    snprintf(b, sizeof b, "%s%.0f", i > 1 ? "/" : "", g_phase_cycles[i].load() / 1e3);
    u += b;
    snprintf(b, sizeof b, "%s%.0f", i > 1 ? "/" : "", g_phase_sys[i].load() / 1e3);
    s += b;
  }
  put(slot, "%s %s ru=%.0f/%.0f flt=%ld hp=%llum rss=%ldm cs=%ld/%ld", u.c_str(), s.c_str(),
      ru.ru_utime.tv_sec * 1e3 + ru.ru_utime.tv_usec / 1e3, ru.ru_stime.tv_sec * 1e3 + ru.ru_stime.tv_usec / 1e3, ru.ru_minflt,
      (unsigned long long)(anon_huge_kb() >> 10), ru.ru_maxrss / 1024, ru.ru_nvcsw, ru.ru_nivcsw);
}

}  // namespace probe
