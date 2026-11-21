// bench/driver.cpp: the scorer bench of branch c-bench. NEVER a scoring build.
//
// It writes the correct dev output (with build "a"), adds rows whose user_id starts with "!" so the dev check
// always rejects the submission, and uses that run to measure: it generates data sets with the organisers'
// generator on /scratch (20M and 60M events by default), runs every build of variants.txt on them in fresh
// processes, interleaved round by round, and reports wall time, CPU split, peak memory and route per build
// through the "!" rows, which the rejection message displays.
//
// Settings (environment, for local tests): BENCH_SIZES "20:5:3,60:15:3" = events M : clicks M : rounds per size,
// BENCH_SEED, BENCH_TMAX seconds after which no new run starts, BENCH_DIR the directory of the binaries.
#include <dirent.h>
#include <fcntl.h>
#include <ftw.h>
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "parquet.h"

extern char** environ;

static double now_s() {
  timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return (double)t.tv_sec + (double)t.tv_nsec * 1e-9;
}
static double g_t0 = 0;
static double el() { return now_s() - g_t0; }
static std::string env_or(const char* k, const char* d) {
  const char* v = getenv(k);
  return v && *v ? std::string(v) : std::string(d);
}
__attribute__((format(printf, 1, 2))) static std::string fmt(const char* f, ...) {
  char buf[2048];
  va_list ap;
  va_start(ap, f);
  vsnprintf(buf, sizeof buf, f, ap);
  va_end(ap);
  return buf;
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
static std::string first_line(const std::string& path) {
  std::string s = slurp(path, 512);
  size_t e = s.find('\n');
  if (e != std::string::npos) s.resize(e);
  return s;
}
static long long stat_field(const std::string& text, const char* key) {  // "key value" lines (memory.stat)
  size_t p = 0;
  const size_t kl = strlen(key);
  while ((p = text.find(key, p)) != std::string::npos) {
    if ((p == 0 || text[p - 1] == '\n') && p + kl < text.size() && text[p + kl] == ' ')
      return atoll(text.c_str() + p + kl + 1);
    p += kl;
  }
  return -1;
}

// ---- files ----
static int rm_cb(const char* p, const struct stat*, int, struct FTW*) { return remove(p) ? 0 : 0; }
static void rm_rf(const std::string& p) { nftw(p.c_str(), rm_cb, 64, FTW_DEPTH | FTW_PHYS); }
static long long g_du = 0;
static int du_cb(const char*, const struct stat* st, int type, struct FTW*) {
  if (type == FTW_F) g_du += st->st_size;
  return 0;
}
static long long du(const std::string& p) {
  g_du = 0;
  nftw(p.c_str(), du_cb, 64, FTW_PHYS);
  return g_du;
}

// num_rows of a Parquet file, from its footer (thrift compact FileMetaData, field 3). -1 if unreadable.
struct TR {
  const uint8_t* p;
  const uint8_t* e;
  bool bad = false;
  uint8_t byte() {
    if (p >= e) {
      bad = true;
      return 0;
    }
    return *p++;
  }
  uint64_t varint() {
    uint64_t v = 0;
    for (int s = 0; s < 64 && !bad; s += 7) {
      uint8_t b = byte();
      v |= (uint64_t)(b & 0x7F) << s;
      if (!(b & 0x80)) break;
    }
    return v;
  }
  int64_t zz() {
    uint64_t v = varint();
    return (int64_t)(v >> 1) ^ -(int64_t)(v & 1);
  }
  void skip(int t) {
    if (bad) return;
    switch (t) {
      case 1:
      case 2:
        break;
      case 3:
        byte();
        break;
      case 4:
      case 5:
      case 6:
        varint();
        break;
      case 7:
        p += 8;
        break;
      case 8: {
        uint64_t n = varint();
        p += n;
        break;
      }
      case 9:
      case 10: {
        uint8_t h = byte();
        uint64_t n = h >> 4;
        int et = h & 15;
        if (n == 15) n = varint();
        for (uint64_t i = 0; i < n && !bad; i++) {
          if (et == 1 || et == 2)
            byte();
          else
            skip(et);
        }
        break;
      }
      case 11: {
        uint64_t n = varint();
        if (n) {
          uint8_t kv = byte();
          for (uint64_t i = 0; i < n && !bad; i++) {
            skip(kv >> 4);
            skip(kv & 15);
          }
        }
        break;
      }
      case 12:
        skip_struct();
        break;
      default:
        bad = true;
    }
    if (p > e) bad = true;
  }
  void skip_struct() {
    while (!bad) {
      uint8_t h = byte();
      if (h == 0) return;
      if ((h >> 4) == 0) zz();
      skip(h & 15);
    }
  }
};
static long long parquet_rows(const std::string& path) {
  std::string s = slurp(path, 1u << 30);
  if (s.size() < 12 || memcmp(s.data() + s.size() - 4, "PAR1", 4) != 0) return -1;
  uint32_t fl;
  memcpy(&fl, s.data() + s.size() - 8, 4);
  if ((size_t)fl + 12 > s.size()) return -1;
  TR r{(const uint8_t*)s.data() + s.size() - 8 - fl, (const uint8_t*)s.data() + s.size() - 8};
  int16_t fid = 0;
  while (!r.bad) {
    uint8_t h = r.byte();
    if (h == 0) break;
    const int d = h >> 4, t = h & 15;
    fid = d ? (int16_t)(fid + d) : (int16_t)r.zz();
    if (fid == 3 && t == 6) return (long long)r.zz();
    r.skip(t);
  }
  return -1;
}
static long long table_rows(const std::string& dir) {
  DIR* d = opendir(dir.c_str());
  if (!d) return -1;
  long long n = 0;
  while (dirent* e = readdir(d)) {
    std::string nm = e->d_name;
    if (nm.empty() || nm[0] == '.' || nm[0] == '_' || nm.size() < 8 || nm.compare(nm.size() - 8, 8, ".parquet")) continue;
    long long r = parquet_rows(dir + "/" + nm);
    if (r < 0) {
      n = -1;
      break;
    }
    n += r;
  }
  closedir(d);
  return n;
}

// ---- child processes ----
struct Proc {
  int status = -1;
  double wall = 0;
  rusage ru{};
  std::string err;
  bool timed_out = false;
};
static Proc run_proc(const std::string& bin, const std::vector<std::string>& args, const std::vector<std::string>& setenv_,
                     double timeout_s) {
  Proc r;
  int fds[2];
  if (pipe2(fds, O_CLOEXEC) != 0) return r;
  std::vector<std::string> envs;
  for (char** e = environ; *e; e++) {
    std::string s(*e);
    bool over = false;
    for (auto& x : setenv_) {
      size_t q = x.find('=');
      if (q != std::string::npos && s.compare(0, q + 1, x, 0, q + 1) == 0) over = true;
    }
    if (!over) envs.push_back(s);
  }
  for (auto& x : setenv_) envs.push_back(x);
  std::vector<char*> av, ev;
  av.push_back((char*)bin.c_str());
  for (auto& a : args) av.push_back((char*)a.c_str());
  av.push_back(nullptr);
  for (auto& e : envs) ev.push_back((char*)e.c_str());
  ev.push_back(nullptr);
  const double t0 = now_s();
  const pid_t pid = fork();
  if (pid < 0) {
    close(fds[0]);
    close(fds[1]);
    return r;
  }
  if (pid == 0) {
    dup2(fds[1], 2);
    const int dn = open("/dev/null", O_RDWR);
    if (dn >= 0) {
      dup2(dn, 0);
      dup2(dn, 1);
    }
    execve(bin.c_str(), av.data(), ev.data());
    _exit(127);
  }
  close(fds[1]);
  char buf[8192];
  for (;;) {
    const double left = timeout_s - (now_s() - t0);
    if (left <= 0) {
      kill(pid, SIGKILL);
      r.timed_out = true;
      break;
    }
    pollfd pf{fds[0], POLLIN, 0};
    const int pr = poll(&pf, 1, (int)std::min(left * 1000.0, 1000.0) + 1);
    if (pr < 0 && errno == EINTR) continue;
    if (pr <= 0) continue;
    const ssize_t n = read(fds[0], buf, sizeof buf);
    if (n <= 0) break;
    if (r.err.size() < (1u << 20)) r.err.append(buf, (size_t)n);
  }
  close(fds[0]);
  int st = 0;
  while (wait4(pid, &st, 0, &r.ru) < 0 && errno == EINTR) {
  }
  r.wall = now_s() - t0;
  r.status = st;
  return r;
}
static double tv(const timeval& t) { return (double)t.tv_sec + (double)t.tv_usec * 1e-6; }

// ---- builds and runs ----
struct Variant {
  std::string tag, bin;
  std::vector<std::string> env;
};
struct Run {
  double wall = 0, user = 0, sys = 0, inproc = -1, ph[5] = {-1, -1, -1, -1, -1}, psys = -1;
  double tl[6] = {-1, -1, -1, -1, -1, -1};  // ms: files mapped, links, parse, scan, output written, end of run
  double thr_ms = -1;  // time the cgroup's CPU quota held the run back (cpu.stat throttled_usec, ms)
  long nthr = -1;      // and in how many periods
  long rss_mb = 0, minflt = 0;
  long long ud = -1, cd = -1;
  int status = 0;
  char route = '?';
  bool ok = false;
};
// ms of the log line "[ seconds] <key>...": the first such line, or the last one
static double log_at(const std::string& e, const char* key, bool last) {
  const std::string k = std::string("] ") + key;
  size_t p = last ? e.rfind(k) : e.find(k);
  if (p == std::string::npos) return -1;
  size_t b = e.rfind('[', p);
  return b == std::string::npos ? -1 : atof(e.c_str() + b + 1) * 1000.0;
}
static void parse_log(const std::string& e, Run& r) {
  r.tl[0] = log_at(e, "mapped", false);
  r.tl[1] = log_at(e, "links", true);
  r.tl[2] = log_at(e, "parsed", false);
  r.tl[3] = log_at(e, "dense scan done", false);
  r.tl[4] = log_at(e, "output written", false);
  {
    size_t a = e.find("ABS start "), z = e.find("ABS end ");
    if (a != std::string::npos && z != std::string::npos)
      r.tl[5] = (atof(e.c_str() + z + 8) - atof(e.c_str() + a + 10)) * 1000.0;
  }
  if (e.find("parsed (dense)") != std::string::npos)
    r.route = 'd';
  else if (e.find("parsed:") != std::string::npos)
    r.route = 'g';
  if (e.find("given up") != std::string::npos) r.route = 'f';
  size_t p = e.find("] output written");
  if (p != std::string::npos) {
    size_t b = e.rfind('[', p);
    if (b != std::string::npos) r.inproc = atof(e.c_str() + b + 1) * 1000.0;
  }
  p = e.find("user s: links");
  if (p != std::string::npos) {
    double a[5], tot = 0, st = 0, sp = 0;
    if (sscanf(e.c_str() + p, "user s: links %lf parse %lf dedup %lf person %lf output %lf | total %lf sys s: total %lf (parse %lf)",
               &a[0], &a[1], &a[2], &a[3], &a[4], &tot, &st, &sp) >= 5)
      for (int i = 0; i < 5; i++) r.ph[i] = a[i];
    size_t q = e.find("sys s: total", p);
    if (q != std::string::npos) r.psys = atof(e.c_str() + q + 12);
  }
}

static std::vector<Variant> load_variants(const std::string& dir) {
  std::vector<Variant> v;
  std::string s = slurp(dir + "/variants.txt");
  size_t a = 0;
  while (a < s.size()) {
    size_t b = s.find('\n', a);
    if (b == std::string::npos) b = s.size();
    std::string line = s.substr(a, b - a);
    a = b + 1;
    const size_t hash = line.find('#');  // a comment runs to the end of the line
    if (hash != std::string::npos) line.resize(hash);
    if (line.empty() || line[0] == '#') continue;
    std::vector<std::string> w;
    size_t i = 0;
    while (i < line.size()) {
      while (i < line.size() && line[i] == ' ') i++;
      size_t j = i;
      while (j < line.size() && line[j] != ' ') j++;
      if (j > i) w.push_back(line.substr(i, j - i));
      i = j;
    }
    if (w.size() < 2) continue;
    Variant x;
    x.tag = w[0];
    x.bin = w[1][0] == '/' ? w[1] : dir + "/" + w[1];
    for (size_t k = 2; k < w.size(); k++) x.env.push_back(w[k]);
    v.push_back(x);
  }
  return v;
}

static double median(std::vector<double> x) {
  if (x.empty()) return -1;
  std::sort(x.begin(), x.end());
  const size_t n = x.size();
  return n % 2 ? x[n / 2] : 0.5 * (x[n / 2 - 1] + x[n / 2]);
}

// ---- the report: rows of user_daily whose user_id starts with "!" ----
struct Row {
  std::string uid, cat;
  long long a = 0, b = 0, c = 0;
};
static int32_t g_day = 0;
static std::vector<std::string> tl_tags;  // builds whose phase timeline the report gives (BENCH_TIMELINE, comma-separated)
static bool write_report(const std::string& out, std::vector<Row> rows) {
  const std::string dir = out + "/user_daily";
  mkdir(dir.c_str(), 0777);
  std::vector<pq::Column> cols(6);
  cols[0].name = "user_id";
  cols[0].type = pq::T_BYTE_ARRAY;
  cols[0].is_string = true;
  cols[1].name = "day";
  cols[1].type = pq::T_INT32;
  cols[1].is_date = true;
  cols[2].name = "sessions";  // the same widths as the pipeline's own files: INT32, INT32, INT64
  cols[2].type = pq::T_INT32;
  cols[3].name = "events";
  cols[3].type = pq::T_INT32;
  cols[4].name = "revenue_cents";
  cols[5].name = "top_category";
  cols[5].type = pq::T_BYTE_ARRAY;
  cols[5].is_string = true;
  cols[5].optional = true;
  std::vector<uint8_t> def(rows.size() + 8, 1);
  pq::Buf lv;
  pq::encode_levels(lv, def.data(), rows.size());
  cols[5].pre.le32((uint32_t)lv.size());  // data page v1: the levels carry a 4-byte length
  cols[5].pre.bytes(lv.data(), lv.size());
  pq::Buf s0, s1, s2, s3, s4, s5;
  for (auto& r : rows) {
    pq::put_str(s0, r.uid.data(), (uint32_t)r.uid.size());
    s1.le32((uint32_t)g_day);
    const int32_t v0 = (int32_t)r.a, v1 = (int32_t)r.b;
    const int64_t v2 = r.c;
    s2.bytes(&v0, 4);
    s3.bytes(&v1, 4);
    s4.bytes(&v2, 8);
    pq::put_str(s5, r.cat.data(), (uint32_t)r.cat.size());
  }
  pq::Buf* bs[6] = {&s0, &s1, &s2, &s3, &s4, &s5};
  for (int i = 0; i < 6; i++) cols[i].body.push_back(pq::Piece{bs[i]->data(), bs[i]->size()});
  const std::string tmp = dir + "/.zz-bench.tmp", fin = dir + "/zz-bench.parquet";
  if (!pq::write_file(tmp, cols, (int64_t)rows.size())) return false;
  return rename(tmp.c_str(), fin.c_str()) == 0;
}

static int32_t day_of(const std::string& manifest) {  // days since 1970 of window_start's date
  size_t p = manifest.find("window_start");
  if (p == std::string::npos) return 20000;
  p = manifest.find('"', manifest.find(':', p));
  if (p == std::string::npos) return 20000;
  int y = 0, m = 0, d = 0;
  if (sscanf(manifest.c_str() + p + 1, "%d-%d-%d", &y, &m, &d) != 3) return 20000;
  y -= m <= 2;  // days_from_civil
  const int era = (y >= 0 ? y : y - 399) / 400;
  const unsigned yoe = (unsigned)(y - era * 400);
  const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + (int)doe - 719468;
}

// the word in [brackets] of a sysfs choice file ("always [madvise] never" -> "madvise")
static std::string chosen(const std::string& path) {
  const std::string s = first_line(path);
  size_t a = s.find('['), b = s.find(']');
  return a != std::string::npos && b != std::string::npos && b > a ? s.substr(a + 1, b - a - 1) : (s.empty() ? "-" : s);
}
// "fstype" or "fstype:huge=x" of the mount at path, from mountinfo
static std::string mount_of(const std::string& mi, const std::string& path) {
  size_t a = 0;
  while (a < mi.size()) {
    size_t b = mi.find('\n', a);
    if (b == std::string::npos) b = mi.size();
    const std::string l = mi.substr(a, b - a);
    a = b + 1;
    char root[512], mp[512];
    if (sscanf(l.c_str(), "%*s %*s %*s %511s %511s", root, mp) != 2 || path != mp) continue;
    size_t d = l.find(" - ");
    if (d == std::string::npos) continue;
    char fs[64], src[256], opts[1024];
    if (sscanf(l.c_str() + d + 3, "%63s %255s %1023s", fs, src, opts) < 3) continue;
    std::string r = fs;
    const char* h = strstr(opts, "huge=");
    if (h) r += ":" + std::string(h, strcspn(h, ","));
    return r;
  }
  return "none";
}
static std::string status_field(const char* key) {  // a line of /proc/self/status
  const std::string s = slurp("/proc/self/status");
  size_t p = s.find(key);
  if (p == std::string::npos) return "-";
  p += strlen(key);
  while (p < s.size() && (s[p] == ':' || s[p] == ' ' || s[p] == '\t')) p++;
  return s.substr(p, s.find('\n', p) - p);
}
static double mtime_s(const char* path) {
  struct stat st;
  return stat(path, &st) == 0 ? st.st_mtim.tv_sec + st.st_mtim.tv_nsec / 1e9 : -1;
}

int main() {
  g_t0 = now_s();
  std::string facts;  // facts about the machine and the container, for the last row
  {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    const double real0 = ts.tv_sec + ts.tv_nsec / 1e9;
    // how long after the container's files and cgroup were made this process started (ms)
    const double hn = mtime_s("/etc/hostname"), hs = mtime_s("/etc/hosts"), cg = mtime_s("/sys/fs/cgroup");
    facts += fmt(" up=hn%.0f,hs%.0f,cg%.0f", hn > 0 ? (real0 - hn) * 1e3 : -1, hs > 0 ? (real0 - hs) * 1e3 : -1,
                 cg > 0 ? (real0 - cg) * 1e3 : -1);
    const std::string mi = slurp("/proc/self/mountinfo");
    facts += fmt(" mnt=data:%s,scr:%s,out:%s,tmp:%s", mount_of(mi, "/data").c_str(), mount_of(mi, "/scratch").c_str(),
                 mount_of(mi, "/output").c_str(), mount_of(mi, "/tmp").c_str());
    facts += fmt(" thp=%s/%s/shm:%s", chosen("/sys/kernel/mm/transparent_hugepage/enabled").c_str(),
                 chosen("/sys/kernel/mm/transparent_hugepage/defrag").c_str(),
                 chosen("/sys/kernel/mm/transparent_hugepage/shmem_enabled").c_str());
    facts += fmt(" cpu=%s mem=%s nodes=%s", status_field("Cpus_allowed_list").c_str(),
                 status_field("Mems_allowed_list").c_str(), first_line("/sys/devices/system/node/online").c_str());
    const std::string cst = slurp("/sys/fs/cgroup/cpu.stat");
    facts += fmt(" burst=%s", first_line("/sys/fs/cgroup/cpu.max.burst").c_str());
    (void)cst;
  }
  const std::string data = env_or("DATA_DIR", "/data"), out = env_or("OUTPUT_DIR", "/output");
  const std::string scratch = env_or("SCRATCH_DIR", "/scratch"), dir = env_or("BENCH_DIR", "/usr/local/bench");
  const double tmax = atof(env_or("BENCH_TMAX", "150").c_str());
  const std::string seed = env_or("BENCH_SEED", "20261122");
  {
    const std::string t = env_or("BENCH_TIMELINE", "");
    size_t a = 0;
    while (a <= t.size()) {
      size_t b = t.find(',', a);
      if (b == std::string::npos) b = t.size();
      if (b > a) tl_tags.push_back(t.substr(a, b - a));
      a = b + 1;
    }
  }
  g_day = day_of(slurp(data + "/manifest.json"));
  std::vector<Variant> vs = load_variants(dir);
  std::string notes;

  // 1. the correct dev output, from build "a", as a plain run
  double dev_ms = -1;
  if (!vs.empty()) {
    Proc p = run_proc(vs[0].bin, {}, {}, 120);
    dev_ms = p.wall * 1000;
    if (p.status != 0) notes += fmt(" dev-exit=%d", p.status);
  }
  // 2. from here on the submission is rejected whatever happens
  write_report(out, {Row{"!0 bench started, no report written", "-", 0, 0, 0}});

  // 3. the machine
  cpu_set_t cs;
  int ncpu = 0;
  if (sched_getaffinity(0, sizeof cs, &cs) == 0) ncpu = CPU_COUNT(&cs);
  utsname un{};
  uname(&un);
  struct statvfs sv {};
  double scratch_gb = -1;
  if (statvfs(scratch.c_str(), &sv) == 0) scratch_gb = (double)sv.f_bavail * sv.f_frsize / 1e9;
  const std::string cgmax = first_line("/sys/fs/cgroup/memory.max"), cpumax = first_line("/sys/fs/cgroup/cpu.max");
  std::string env_s = fmt("cpus=%d k=%s scr=%.0fG cg=%s/%s dev=%.0fms nvar=%zu", ncpu, un.release, scratch_gb,
                          cpumax.c_str(), cgmax.c_str(), dev_ms, vs.size());

  // 4. sizes
  struct Size {
    double ev_m, ck_m;
    int rounds;
  };
  std::vector<Size> sizes;
  {
    std::string s = env_or("BENCH_SIZES", "20:5:3,60:15:3");
    size_t a = 0;
    while (a < s.size()) {
      size_t b = s.find(',', a);
      if (b == std::string::npos) b = s.size();
      Size z{0, 0, 1};
      if (sscanf(s.substr(a, b - a).c_str(), "%lf:%lf:%d", &z.ev_m, &z.ck_m, &z.rounds) >= 2) sizes.push_back(z);
      a = b + 1;
    }
  }
  std::vector<Row> rows;
  std::string gen_s;
  int sizeno = 0;
  for (const Size& z : sizes) {
    sizeno++;
    const std::string gdir = scratch + "/bench-gen", odir = scratch + "/bench-out";
    rm_rf(gdir);
    rm_rf(odir);
    const long long E = (long long)(z.ev_m * 1e6), C = (long long)(z.ck_m * 1e6);
    if (el() > tmax) {
      notes += fmt(" skip%.0fM", z.ev_m);
      continue;
    }
    Proc g = run_proc(dir + "/generate",
                      {"--seed", seed, "--events", std::to_string(E), "--clicks", std::to_string(C), "--output", gdir},
                      {}, std::max(10.0, tmax - el()));
    const long long bytes = du(gdir);
    const std::string mstat0 = slurp("/sys/fs/cgroup/memory.stat");
    // let the generator's dirty pages reach the disk, up to 20 s, so writeback does not run under the timings
    std::atomic<bool> synced{false};
    const double ts0 = now_s();
    {
      std::thread th([&] {
        int fd = open(gdir.c_str(), O_RDONLY | O_DIRECTORY);
        if (fd >= 0) {
          syncfs(fd);
          close(fd);
        }
        synced = true;
      });
      th.detach();
    }
    while (!synced && now_s() - ts0 < 20) usleep(20000);
    const double sync_s = now_s() - ts0;
    const long long dirty0 = stat_field(mstat0, "file_dirty");
    gen_s += fmt(" %.0fM:gen=%.1fs/%ldMB,%.2fGB,dirty=%lldMB,sync=%.1fs%s%s", z.ev_m, g.wall, g.ru.ru_maxrss / 1024,
                 bytes / 1e9, dirty0 >= 0 ? dirty0 >> 20 : -1, sync_s, synced ? "" : "+",
                 g.status ? fmt(",exit=%d", g.status).c_str() : "");
    if (g.status != 0 || bytes <= 0) continue;

    // interleaved rounds, the order rotated each round
    std::vector<std::vector<Run>> res(vs.size());
    for (int r = 0; r < z.rounds; r++) {
      for (size_t k = 0; k < vs.size(); k++) {
        const size_t vi = (k + (size_t)r) % vs.size();
        if (el() > tmax) {
          notes += fmt(" cut%.0fM.r%d", z.ev_m, r);
          r = z.rounds;
          break;
        }
        if (access(vs[vi].bin.c_str(), X_OK) != 0) continue;
        rm_rf(odir);
        mkdir(odir.c_str(), 0777);
        std::vector<std::string> e = {"DATA_DIR=" + gdir, "OUTPUT_DIR=" + odir, "SS_LOG=1"};
        for (auto& x : vs[vi].env) e.push_back(x);
        const std::string cs0 = slurp("/sys/fs/cgroup/cpu.stat");
        Proc p = run_proc(vs[vi].bin, {}, e, std::max(5.0, std::min(120.0, tmax + 60 - el())));
        const std::string cs1 = slurp("/sys/fs/cgroup/cpu.stat");
        Run u;
        u.status = p.status;
        u.wall = p.wall * 1000;
        u.user = tv(p.ru.ru_utime);
        u.sys = tv(p.ru.ru_stime);
        u.rss_mb = p.ru.ru_maxrss / 1024;
        u.minflt = p.ru.ru_minflt;
        parse_log(p.err, u);
        u.ud = table_rows(odir + "/user_daily");
        u.cd = table_rows(odir + "/campaign_daily");
        u.ok = p.status == 0 && !p.timed_out;
        {
          const long long t0 = stat_field(cs0, "throttled_usec"), t1 = stat_field(cs1, "throttled_usec");
          const long long n0 = stat_field(cs0, "nr_throttled"), n1 = stat_field(cs1, "nr_throttled");
          if (t0 >= 0 && t1 >= 0) u.thr_ms = (t1 - t0) / 1000.0;
          if (n0 >= 0 && n1 >= 0) u.nthr = (long)(n1 - n0);
        }
        res[vi].push_back(u);
      }
    }
    rm_rf(odir);
    rm_rf(gdir);

    // summary of this size: two text fields and the three medians of the first builds
    std::string a = fmt("!%d %.0fM", sizeno, z.ev_m), b, tlines;
    long long med3[3] = {0, 0, 0};
    long long ud0 = -2, cd0 = -2;
    bool agree = true;
    for (size_t vi = 0; vi < vs.size(); vi++) {
      if (access(vs[vi].bin.c_str(), X_OK) != 0) {
        a += fmt(" %s=missing", vs[vi].tag.c_str());
        continue;
      }
      std::vector<double> w, us, ss, ip, fl, th;
      long rss = 0;
      char route = '?';
      int bad = 0;
      std::vector<double> ph[5];
      for (auto& u : res[vi]) {
        if (!u.ok) {
          bad++;
          continue;
        }
        w.push_back(u.wall);
        us.push_back(u.user);
        ss.push_back(u.sys);
        if (u.inproc >= 0) ip.push_back(u.inproc);
        fl.push_back((double)u.minflt);
        if (u.thr_ms >= 0) th.push_back(u.thr_ms);
        rss = std::max(rss, u.rss_mb);
        route = u.route;
        if (u.ph[0] >= 0)
          for (int i = 0; i < 5; i++) ph[i].push_back(u.ph[i]);
        if (ud0 == -2) {
          ud0 = u.ud;
          cd0 = u.cd;
        } else if (u.ud != ud0 || u.cd != cd0)
          agree = false;
      }
      const double mw = median(w), mn = w.empty() ? -1 : *std::min_element(w.begin(), w.end());
      if (vi < 3) med3[vi] = (long long)mw;
      (void)mn;  // (bench 1 printed the best wall time too: a=512/498,...)
      (void)rss;  // (benches 1 to 5 printed the peak memory too, as 626M)
      a += fmt(" %s=%.0f,u%.2f,s%.2f,%c%s", vs[vi].tag.c_str(), mw, median(us), median(ss), route,
               bad ? fmt("!%d", bad).c_str() : (w.size() == (size_t)z.rounds ? "" : fmt("%zu", w.size()).c_str()));
      if (!th.empty() && *std::max_element(th.begin(), th.end()) > 0)  // throttled ms per run: median and most
        a += fmt(",T%.1f/%.1f", median(th), *std::max_element(th.begin(), th.end()));
      if (!ph[0].empty())  // CPU ms of links, parse, dedup or scan (the person phase and the output are a few ms)
        b += fmt("%s%s:%.0f+%.0f+%.0f", b.empty() ? "" : " ", vs[vi].tag.c_str(), median(ph[0]) * 1000,
                 median(ph[1]) * 1000, median(ph[2]) * 1000);
      if (!w.empty() && std::find(tl_tags.begin(), tl_tags.end(), vs[vi].tag) != tl_tags.end()) {
        // the timeline of the run with the median wall time
        const Run* best = nullptr;
        for (auto& u : res[vi])
          if (u.ok && (!best || std::fabs(u.wall - mw) < std::fabs(best->wall - mw))) best = &u;
        if (best)
          tlines += fmt(" %s@%.0fM:%.0f/%.0f/%.0f/%.0f/%.0f/%.0f", vs[vi].tag.c_str(), z.ev_m, best->tl[0], best->tl[1],
                        best->tl[2], best->tl[3], best->tl[4], best->tl[5]);
      }
    }
    a += fmt(" rows=%lld/%lld%s", ud0, cd0, agree ? "" : ",DIFFER");
    b += tlines;
    rows.push_back(Row{a, b.empty() ? "-" : b, med3[0], med3[1], med3[2]});
  }
  {  // the container's CPU throttling over the whole bench
    const std::string cst = slurp("/sys/fs/cgroup/cpu.stat");
    facts += fmt(" thr=%lld/%lldp,%.0fms", stat_field(cst, "nr_throttled"), stat_field(cst, "nr_periods"),
                 stat_field(cst, "throttled_usec") / 1000.0);
  }
  rows.push_back(Row{fmt("!%d %s%s tot=%.0fs", sizeno + 1, env_s.c_str(), notes.c_str(), el()),
                     (gen_s.empty() ? std::string("-") : gen_s.substr(1)) + facts,
                     (long long)dev_ms, ncpu, (long long)el()});
  if (!write_report(out, rows)) fprintf(stderr, "bench: report not written\n");
  for (auto& r : rows) fprintf(stderr, "BENCH %s || %s\n", r.uid.c_str(), r.cat.c_str());
  return 0;
}
