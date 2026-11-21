// probe.h: prints what the machine looks like and how a few kernels behave on it. Built only with -DPROBE.
// Everything goes to stderr with a "PROBE" prefix. Nothing here touches the pipeline's results.
#pragma once
#include <sched.h>
#include <sys/utsname.h>

namespace probe {

static double tsc_hz = 0;

static inline double wall() { return now_s(); }

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

static void show(const char* label, const std::string& path, size_t maxb = 400) {
  std::string s = slurp(path, maxb);
  while (!s.empty() && (s.back() == '\n' || s.back() == ' ')) s.pop_back();
  for (auto& c : s)
    if (c == '\n') c = '|';
  fprintf(stderr, "PROBE %s: %s\n", label, s.empty() ? "(none)" : s.c_str());
}

static void grep_lines(const char* label, const std::string& path, std::initializer_list<const char*> keys, int max_each = 1) {
  std::string s = slurp(path);
  for (const char* k : keys) {
    int found = 0;
    size_t pos = 0;
    while (found < max_each) {
      size_t p = s.find(k, pos);
      if (p == std::string::npos) break;
      size_t b = s.rfind('\n', p);
      b = (b == std::string::npos) ? 0 : b + 1;
      size_t e = s.find('\n', p);
      if (e == std::string::npos) e = s.size();
      std::string line = s.substr(b, std::min<size_t>(e - b, 300));
      for (auto& c : line)
        if (c == '\t') c = ' ';
      fprintf(stderr, "PROBE %s: %s\n", label, line.c_str());
      found++;
      pos = e;
    }
  }
}

static void sysinfo(const std::string& data) {
  struct utsname u;
  if (uname(&u) == 0) fprintf(stderr, "PROBE uname: %s %s %s %s\n", u.sysname, u.release, u.version, u.machine);
  grep_lines("cpuinfo", "/proc/cpuinfo", {"model name", "cpu family", "model\t", "stepping", "microcode", "cache size", "bugs"});
  {
    std::string s = slurp("/proc/cpuinfo");
    size_t p = s.find("flags");
    if (p != std::string::npos) {
      size_t e = s.find('\n', p);
      std::string fl = " " + s.substr(p, e - p) + " ";
      std::string out;
      for (const char* k : {"avx2", "bmi2", "avx512f", "avx512bw", "avx512vl", "avx512_vnni", "avx512_vbmi", "avx512_vbmi2",
                            "avx512_bitalg", "avx512_vpopcntdq", "gfni", "vaes", "vpclmulqdq", "avx_vnni", "avx512_bf16",
                            "amx_tile", "fsrm", "erms", "md_clear", "hypervisor", "pdpe1gb", "rdt_a", "tsc_reliable",
                            "constant_tsc", "invpcid", "rtm", "hle"})
        if (fl.find(std::string(" ") + k + " ") != std::string::npos) out += std::string(k) + " ";
      fprintf(stderr, "PROBE flags: %s\n", out.c_str());
    }
    // MHz of every listed cpu
    std::string mhz;
    size_t pos = 0;
    int nproc = 0;
    while ((pos = s.find("cpu MHz", pos)) != std::string::npos) {
      size_t c = s.find(':', pos), e = s.find('\n', pos);
      if (nproc < 64) mhz += s.substr(c + 1, e - c - 1);
      nproc++;
      pos = e;
    }
    fprintf(stderr, "PROBE cpus listed: %d; MHz:%s\n", nproc, mhz.c_str());
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  std::string aff;
  int first = -1, naff = 0;
  if (sched_getaffinity(0, sizeof set, &set) == 0) {
    for (int i = 0; i < CPU_SETSIZE; i++)
      if (CPU_ISSET(i, &set)) {
        if (first < 0) first = i;
        naff++;
        std::string base = "/sys/devices/system/cpu/cpu" + std::to_string(i) + "/topology/";
        std::string sib = slurp(base + "thread_siblings_list", 64), core = slurp(base + "core_id", 16),
                    pkg = slurp(base + "physical_package_id", 16);
        for (std::string* x : {&sib, &core, &pkg})
          while (!x->empty() && x->back() == '\n') x->pop_back();
        aff += " " + std::to_string(i) + "(core " + core + " pkg " + pkg + " sib " + sib + ")";
      }
  }
  fprintf(stderr, "PROBE affinity: %d cpus:%s\n", naff, aff.c_str());
  fprintf(stderr, "PROBE sysconf: online %ld conf %ld hardware_concurrency %u\n", sysconf(_SC_NPROCESSORS_ONLN),
          sysconf(_SC_NPROCESSORS_CONF), std::thread::hardware_concurrency());
  if (first < 0) first = 0;
  for (int idx = 0; idx < 5; idx++) {
    std::string base = "/sys/devices/system/cpu/cpu" + std::to_string(first) + "/cache/index" + std::to_string(idx) + "/";
    std::string lvl = slurp(base + "level", 8), type = slurp(base + "type", 32), size = slurp(base + "size", 32),
                shared = slurp(base + "shared_cpu_list", 200), ways = slurp(base + "ways_of_associativity", 16);
    if (lvl.empty()) continue;
    for (std::string* x : {&lvl, &type, &size, &shared, &ways})
      while (!x->empty() && x->back() == '\n') x->pop_back();
    fprintf(stderr, "PROBE cache L%s %s %s ways %s shared_by %s\n", lvl.c_str(), type.c_str(), size.c_str(), ways.c_str(),
            shared.c_str());
  }
  show("thp enabled", "/sys/kernel/mm/transparent_hugepage/enabled");
  show("thp defrag", "/sys/kernel/mm/transparent_hugepage/defrag");
  show("thp khugepaged defrag", "/sys/kernel/mm/transparent_hugepage/khugepaged/defrag");
  show("thp use_zero_page", "/sys/kernel/mm/transparent_hugepage/use_zero_page");
  grep_lines("meminfo", "/proc/meminfo",
             {"MemTotal", "MemFree", "MemAvailable", "Cached:", "AnonHugePages", "HugePages_Total", "Hugepagesize", "Dirty:"});
  show("cgroup", "/proc/self/cgroup", 600);
  for (const char* f : {"/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/cpuset.cpus.effective", "/sys/fs/cgroup/memory.max",
                        "/sys/fs/cgroup/memory.high", "/sys/fs/cgroup/pids.max", "/sys/fs/cgroup/cpu.weight",
                        "/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "/sys/fs/cgroup/cpu/cpu.cfs_period_us",
                        "/sys/fs/cgroup/cpuset/cpuset.cpus", "/sys/fs/cgroup/memory/memory.limit_in_bytes",
                        "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpu/cpu.stat"})
    if (!slurp(f, 16).empty()) show(f, f, 300);
  {
    std::string s = slurp("/proc/self/mountinfo");
    size_t pos = 0;
    while (pos < s.size()) {
      size_t e = s.find('\n', pos);
      if (e == std::string::npos) e = s.size();
      std::string line = s.substr(pos, e - pos);
      // field 5 is the mount point
      size_t a = 0;
      for (int f = 0; f < 4 && a != std::string::npos; f++) a = line.find(' ', a + 1);
      if (a != std::string::npos) {
        size_t b = line.find(' ', a + 1);
        std::string mp = line.substr(a + 1, b - a - 1);
        if (mp == "/" || mp == "/data" || mp == "/output" || mp == "/scratch" || mp == "/tmp")
          fprintf(stderr, "PROBE mount: %.300s\n", line.c_str());
      }
      pos = e + 1;
    }
  }
  for (const char* k : {"NCPU", "MEM_GB", "DATA_DIR", "OUTPUT_DIR", "SCRATCH_DIR", "HOME", "PATH"})
    fprintf(stderr, "PROBE env %s=%s\n", k, getenv(k) ? getenv(k) : "(unset)");
  show("loadavg", "/proc/loadavg");
  show("uptime", "/proc/uptime");
  grep_lines("status", "/proc/self/status", {"Cpus_allowed_list", "Mems_allowed_list", "Seccomp", "NoNewPrivs", "Uid"});
  grep_lines("stat", "/proc/stat", {"procs_running", "procs_blocked"});
  show("vm overcommit", "/proc/sys/vm/overcommit_memory");
  show("vm max_map_count", "/proc/sys/vm/max_map_count");
  show("clocksource", "/sys/devices/system/clocksource/clocksource0/current_clocksource");
  for (const char* sub : {"events", "clicks", "links"}) {
    auto files = list_csv(data + "/" + sub);
    size_t tot = 0, mx = 0;
    for (auto& f : files) {
      struct stat st;
      if (stat(f.c_str(), &st) == 0) {
        tot += st.st_size;
        mx = std::max<size_t>(mx, st.st_size);
      }
    }
    fprintf(stderr, "PROBE data %s: %zu files, %zu bytes, largest %zu\n", sub, files.size(), tot, mx);
  }
}

// ---- timing helpers
static inline u64 tsc() { return __rdtsc(); }

static void calibrate_tsc() {
  double t0 = wall();
  u64 c0 = tsc();
  while (wall() - t0 < 0.02) {
  }
  double t1 = wall();
  u64 c1 = tsc();
  tsc_hz = (double)(c1 - c0) / (t1 - t0);
  fprintf(stderr, "PROBE tsc: %.1f MHz\n", tsc_hz / 1e6);
}

// n iterations of a one-cycle loop; returns the core frequency in GHz
static NOINLINE double core_ghz(u64 n) {
  u64 k = n;
  u64 c0 = tsc();
  __asm__ volatile("1:\n\tdec %0\n\tjnz 1b" : "+r"(k) : : "cc");
  u64 c1 = tsc();
  return tsc_hz * (double)n / (double)(c1 - c0) / 1e9;
}

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
static NOINLINE void burn_ymm_heavy(double seconds) {
  __m256d a = _mm256_set1_pd(1.0000001), b = _mm256_set1_pd(0.9999999), c = a, d = b;
  double t0 = wall();
  do {
    for (int i = 0; i < 20000; i++) {
      a = _mm256_mul_pd(a, b);
      c = _mm256_mul_pd(c, d);
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

static void frequency() {
  fprintf(stderr, "PROBE core GHz (1-cycle loop): %.3f %.3f %.3f\n", core_ghz(20000000), core_ghz(20000000), core_ghz(20000000));
  struct {
    const char* name;
    void (*fn)(double);
  } loads[] = {{"ymm light", burn_ymm_light}, {"ymm int-mul", burn_ymm_int_mul}, {"ymm fp-mul", burn_ymm_heavy},
               {"zmm light", burn_zmm_light}, {"zmm fp-mul", burn_zmm_heavy}};
  for (auto& l : loads) {
    l.fn(0.03);
    double f0 = core_ghz(1000000);   // right after (license still held)
    double f1 = core_ghz(1000000);
    double f2 = core_ghz(30000000);  // ~10 ms later
    double f3 = core_ghz(3000000);
    fprintf(stderr, "PROBE core GHz after 30 ms of %-11s: %.3f %.3f then %.3f %.3f\n", l.name, f0, f1, f2, f3);
  }
}

static NOINLINE void fa(u64& x) { x = x * 3 + 1; }
static NOINLINE void fb(u64& x) { x = x * 5 + 7; }
static void mispredict() {
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
  fprintf(stderr, "PROBE branch: predictable %.2f ticks/iter, random %.2f ticks/iter -> ~%.1f cycles per mispredict (core %.2f GHz)\n",
          tp, tr, (tr - tp) * 2 * (ghz * 1e9 / tsc_hz), ghz);
}

static u64 anon_huge_kb() {
  std::string s = slurp("/proc/self/smaps_rollup");
  size_t p = s.find("AnonHugePages:");
  if (p == std::string::npos) return 0;
  return strtoull(s.c_str() + p + 14, nullptr, 10);
}

static void memory() {
  const size_t SZ = (size_t)256 << 20;
  for (int huge = 1; huge >= 0; huge--) {
    u64 h0 = anon_huge_kb();
    double t0 = wall();
    u8* p = (u8*)mmap(nullptr, SZ, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (p == MAP_FAILED) {
      fprintf(stderr, "PROBE mem: mmap failed\n");
      return;
    }
    madvise(p, SZ, huge ? MADV_HUGEPAGE : MADV_NOHUGEPAGE);
    double t1 = wall();
    for (size_t i = 0; i < SZ; i += 4096) p[i] = 1;
    double t2 = wall();
    u64 h1 = anon_huge_kb();
    // sequential read
    u64 sum = 0;
    double t3 = wall();
    for (size_t i = 0; i < SZ; i += 8) {
      u64 v;
      memcpy(&v, p + i, 8);
      sum += v;
    }
    double t4 = wall();
    // non-temporal fill
    for (size_t i = 0; i < SZ; i += 32) _mm256_stream_si256((__m256i*)(p + i), _mm256_set1_epi8(1));
    _mm_sfence();
    double t5 = wall();
    // dependent random loads: i = a[i], a full-period LCG over 2^24 u64 slots (128 MB)
    const size_t NS = (size_t)1 << 24;
    u64* a = (u64*)p;
    for (size_t i = 0; i < NS; i++) a[i] = (i * 6364136223846793005ULL + 1442695040888963407ULL) & (NS - 1);
    double t6 = wall();
    u64 i0 = 1;
    const int STEPS = 1 << 20;
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
    fprintf(stderr,
            "PROBE mem %s: mmap %.3f ms, first touch 256MB %.1f ms (%.0f ns/4K page), AnonHuge +%llu MB, seq read %.2f GB/s, "
            "NT fill %.2f GB/s, random load %.1f ns, 8 parallel chains %.1f ns/load, munmap %.2f ms\n",
            huge ? "MADV_HUGEPAGE" : "MADV_NOHUGEPAGE", (t1 - t0) * 1e3, (t2 - t1) * 1e3, (t2 - t1) * 1e9 / (SZ / 4096),
            (unsigned long long)((h1 - h0) >> 10), SZ / (t4 - t3) / 1e9, SZ / (t5 - t4) / 1e9, (t7 - t6) * 1e9 / STEPS,
            (t8 - t7) * 1e9 / STEPS / 8, (t9 - t8) * 1e3);
  }
}

static void files(const std::string& data) {
  auto fl = list_csv(data + "/events");
  std::string best;
  size_t mx = 0;
  for (auto& f : fl) {
    struct stat st;
    if (stat(f.c_str(), &st) == 0 && (size_t)st.st_size > mx) {
      mx = st.st_size;
      best = f;
    }
  }
  if (best.empty()) return;
  for (int pass = 0; pass < 2; pass++) {
    double t0 = wall();
    int fd = open(best.c_str(), O_RDONLY);
    u8* p = (u8*)mmap(nullptr, mx, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    volatile u64 s = 0;
    double t1 = wall();
    for (size_t i = 0; i < mx; i += 4096) s += p[i];
    double t2 = wall();
    madvise(p, mx & ~(size_t)4095, MADV_DONTNEED);
    double t3 = wall();
    munmap(p, mx);
    double t4 = wall();
    fprintf(stderr, "PROBE file mmap pass %d (%zu MB): map %.3f ms, touch %.2f ms (%.0f ns/4K page), DONTNEED %.2f ms, munmap %.3f ms\n",
            pass, mx >> 20, (t1 - t0) * 1e3, (t2 - t1) * 1e3, (t2 - t1) * 1e9 / (mx / 4096 + 1), (t3 - t2) * 1e3, (t4 - t3) * 1e3);
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
    fprintf(stderr, "PROBE file read(): %.2f GB/s\n", tot / (t1 - t0) / 1e9);
  }
}

static void scaling() {
  // CPU-bound work per thread; throughput vs thread count shows what the hyperthreads add
  auto cpu_work = [](u64 iters) {
    u64 x = 1, acc = 0;
    for (u64 i = 0; i < iters; i++) {
      x = mix64(x + i);
      acc += x >> 60;
      acc ^= _pext_u64(x, 0x7F7F7F7F7F7F7F7FULL);
    }
    __asm__ volatile("" : "+r"(acc));
  };
  const size_t NS = (size_t)1 << 23;  // 64 MB of u64 per thread: latency-bound work
  for (int mode = 0; mode < 2; mode++) {
    std::string line;
    double base = 0;
    for (int nt : {1, 2, 4, 8}) {
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
            cpu_work(30000000);
          } else {
            u64 j[8] = {1, 100003, 200003, 300017, 400037, 500011, 600011, 700003};
            const u64* a = mem[t].data();
            for (int k = 0; k < 400000; k++)
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
      snprintf(buf, sizeof buf, " %dT %.2fx (%.0f ms)", nt, thr / base, dt * 1e3);
      line += buf;
    }
    fprintf(stderr, "PROBE scaling %s:%s\n", mode ? "random-load" : "cpu-bound", line.c_str());
  }
  {
    double t0 = wall();
    for (int r = 0; r < 20; r++) {
      std::vector<std::thread> th;
      for (int t = 0; t < 7; t++) th.emplace_back([]() {});
      for (auto& t : th) t.join();
    }
    fprintf(stderr, "PROBE threads: create+join 7 threads %.0f us\n", (wall() - t0) * 1e6 / 20);
  }
}

static void gather() {
  alignas(32) static u32 tab[4096];
  for (int i = 0; i < 4096; i++) tab[i] = i * 7;
  __m256i idx = _mm256_setr_epi32(1, 77, 513, 1029, 2047, 3001, 3777, 4095), acc = _mm256_setzero_si256();
  const int N = 2000000;
  u64 c0 = tsc();
  for (int i = 0; i < N; i++) {
    acc = _mm256_add_epi32(acc, _mm256_i32gather_epi32((const int*)tab, idx, 4));
    idx = _mm256_and_si256(_mm256_add_epi32(idx, _mm256_set1_epi32(13)), _mm256_set1_epi32(4095));
  }
  u64 c1 = tsc();
  __asm__ volatile("" : "+x"(acc));
  fprintf(stderr, "PROBE gather: %.1f ticks per 8-element vpgatherdd\n", (double)(c1 - c0) / N);
}

// Separator scan of a whole file (masks + flattening the separator offsets), four ways. The AVX-512 runs
// include whatever frequency change 512-bit registers cause on this machine.
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
        o[done + j] = (u32)off + (u32)_tzcnt_u64(m);
        m = _blsr_u64(m);
      }
      if (done + 8 >= cnt) break;
    }
    total += cnt + (mq & 1);
  }
  return total;
}
__attribute__((target("avx512f,avx512bw,avx512vl"))) static NOINLINE u64 scan_avx512_tz(const u8* p, size_t n, u32* out) {
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
        o[done + j] = (u32)off + (u32)_tzcnt_u64(m);
        m = _blsr_u64(m);
      }
      if (done + 8 >= cnt) break;
    }
    total += cnt + (mq & 1);
  }
  return total;
}
__attribute__((target("avx512f,avx512bw,avx512vl"))) static NOINLINE u64 scan_avx512_compressd(const u8* p, size_t n, u32* out) {
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
__attribute__((target("avx512f,avx512bw,avx512vl,avx512vbmi2"))) static NOINLINE u64 scan_avx512_compressb(const u8* p, size_t n, u32* out) {
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
    // widen the first 16 to 32 bits and add the block offset (enough for a normal block)
    _mm512_storeu_si512((void*)o, _mm512_add_epi32(_mm512_cvtepu8_epi32(_mm512_castsi512_si128(idx)), _mm512_set1_epi32((int)off)));
    total += (u32)__builtin_popcountll(m) + (mq & 1);
  }
  return total;
}

static void scan_kernels(const std::string& data) {
  auto fl = list_csv(data + "/events");
  std::string best;
  size_t mx = 0;
  for (auto& f : fl) {
    struct stat st;
    if (stat(f.c_str(), &st) == 0 && (size_t)st.st_size > mx) {
      mx = st.st_size;
      best = f;
    }
  }
  if (best.empty()) return;
  MFile f = map_file(best);
  std::vector<u32> out(2048 + 128);
  const bool has512 = __builtin_cpu_supports("avx512bw"), has_vbmi2 = __builtin_cpu_supports("avx512vbmi2");
  struct K {
    const char* name;
    u64 (*fn)(const u8*, size_t, u32*);
    bool ok;
  } ks[] = {{"avx2 + tzcnt", scan_avx2, true},
            {"avx512 masks + tzcnt", scan_avx512_tz, has512},
            {"avx512 + vpcompressd", scan_avx512_compressd, has512},
            {"avx512 + vpcompressb", scan_avx512_compressb, has512 && has_vbmi2},
            {"avx2 + tzcnt (again)", scan_avx2, true}};
  scan_avx2(f.data, f.size, out.data());  // warm the mapping
  for (auto& k : ks) {
    if (!k.ok) {
      fprintf(stderr, "PROBE scan %-22s: not supported\n", k.name);
      continue;
    }
    u64 best_t = ~0ULL, tot = 0;
    for (int r = 0; r < 6; r++) {
      u64 c0 = tsc();
      tot = k.fn(f.data, f.size, out.data());
      u64 c1 = tsc();
      best_t = std::min(best_t, c1 - c0);
    }
    double ghz = core_ghz(2000000);
    fprintf(stderr, "PROBE scan %-22s: %.3f ticks/byte = %.2f ns per 64-byte block (%llu seps), core %.3f GHz right after\n", k.name,
            (double)best_t / f.size, (double)best_t / f.size * 64 / tsc_hz * 1e9, (unsigned long long)tot, ghz);
  }
}

static void report(const std::string& data) {
  double t0 = wall();
  sysinfo(data);
  calibrate_tsc();
  frequency();
  mispredict();
  gather();
  memory();
  files(data);
  scan_kernels(data);
  scaling();
  fprintf(stderr, "PROBE report took %.2f s\n", wall() - t0);
}

}  // namespace probe
