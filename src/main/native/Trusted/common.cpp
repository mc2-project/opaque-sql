#include "common.h"

#include "Enclave_t.h"
#include "util.h"

int printf(const char *fmt, ...) {
  char buf[BUFSIZ] = {'\0'};
  va_list ap;
  va_start(ap, fmt);
  int ret = vsnprintf(buf, BUFSIZ, fmt, ap);
  va_end(ap);
  ocall_print_string(buf);
  return ret;
}

void exit(int exit_code) {
  ocall_exit(exit_code);
}
