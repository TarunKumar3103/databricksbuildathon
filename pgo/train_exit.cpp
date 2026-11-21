// Linked only into the profile-training build. The pipeline ends with _exit(), which skips the
// handler that writes the profile, so the profile is written here first.
extern "C" int __llvm_profile_write_file(void);
extern "C" void __real__exit(int);
extern "C" void __wrap__exit(int code) {
  __llvm_profile_write_file();
  __real__exit(code);
}
