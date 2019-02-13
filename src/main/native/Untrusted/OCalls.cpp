#include "Enclave_u.h"

// This file contains definitions of ocalls declared in src/enclave/Enclave/Enclave.edl. These
// functions are invoked by the trusted code within enclave but execute in the untrusted region.

#include <cstdlib>

#include "SGXEnclave.h"

void ocall_print_string(const char *str) {
  // The SGX SDK will generate code that guarantees str is null-terminated
  printf("%s", str);
  fflush(stdout);
}

void unsafe_ocall_malloc(size_t size, uint8_t **ret) {
  *ret = static_cast<uint8_t *>(malloc(size));
}

void ocall_free(uint8_t *buf) {
  free(buf);
}

void ocall_exit(int exit_code) {
  std::exit(exit_code);
}

/**
 * Throw a Java exception with the specified message.
 *
 * This function is intended to be invoked from an ecall that was in turn invoked by a JNI method.
 * As a result of calling this function, the JNI method will throw a Java exception upon its return.
 *
 * Important: Note that this function will return to the caller. The exception is only thrown at the
 * end of the JNI method invocation.
 */
void ocall_throw(const char *message) {
  JNIEnv *env;
  jvm->AttachCurrentThread((void**)&env, nullptr);
  jclass exception = env->FindClass("edu/berkeley/cs/rise/opaque/OpaqueException");
  env->ThrowNew(exception, message);
}
