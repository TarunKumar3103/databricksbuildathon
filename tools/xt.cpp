// xt: run a command, report exec->exit wall time plus the child's own absolute timestamps (lines "ABS <name> <sec>")
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
static double now_s() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
int main(int argc, char** argv) {
  int pfd[2];
  if (pipe(pfd)) return 1;
  double t0 = now_s();
  pid_t pid = fork();
  if (pid == 0) {
    dup2(pfd[1], 2);
    close(pfd[0]);
    close(pfd[1]);
    int dn = open("/dev/null", 1);
    (void)dn;
    execv(argv[1], argv + 1);
    _exit(127);
  }
  close(pfd[1]);
  std::string err;
  char buf[4096];
  ssize_t n;
  while ((n = read(pfd[0], buf, sizeof buf)) > 0) err.append(buf, n);
  double t_eof = now_s();
  int st;
  struct rusage ru;
  wait4(pid, &st, 0, &ru);
  double t1 = now_s();
  double m0 = 0, m1 = 0;
  const char* p = strstr(err.c_str(), "ABS start ");
  if (p) m0 = atof(p + 10);
  p = strstr(err.c_str(), "ABS end ");
  if (p) m1 = atof(p + 8);
  printf("wall %.3f  startup %.4f  main %.3f  teardown %.4f (stderr eof at +%.4f)  user %.3f sys %.3f maxrss %ld MB minflt %ld\n", t1 - t0, m0 - t0, m1 - m0, t1 - m1,
         t_eof - m1, ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6, ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6, ru.ru_maxrss / 1024, ru.ru_minflt);
  if (getenv("XT_SHOW")) fputs(err.c_str(), stderr);
  return WIFEXITED(st) ? WEXITSTATUS(st) : 1;
}
