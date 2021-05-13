#include <jni.h>
#include <string>

#include "ServiceProvider.h"

#ifndef _Included_SP
#define _Included_SP
#ifdef __cplusplus
extern "C" {
#endif
JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_Init(JNIEnv *, jobject,
                                                                          jbyteArray, jstring);

JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_SPProcMsg0(JNIEnv *, jobject,
                                                                                jbyteArray);

JNIEXPORT jbyteArray JNICALL
Java_edu_berkeley_cs_rise_opaque_execution_SP_ProcessEnclaveReport(JNIEnv *, jobject, jbyteArray);

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_SPProcMsg3(JNIEnv *,
                                                                                      jobject,
                                                                                      jbyteArray);

JNIEXPORT jbyteArray JNICALL
Java_edu_berkeley_cs_rise_opaque_execution_SP_Decrypt(JNIEnv *, jobject, jstring);

// Python wrapper functions
ServiceProvider* sp_new(){ 
  const std::string id("client");
  return new ServiceProvider(id, false, false);
}

void
sp_init_wrapper(ServiceProvider * sp, uint8_t * provided_cert, size_t cert_len) {
  sp->init_wrapper(provided_cert, cert_len);
}

void
sp_process_enclave_report(ServiceProvider * sp, uint8_t * report, size_t * report_len, uint8_t ** ret_val, size_t * ret_len) { 
  sp->process_enclave_report_python_wrapper(report, report_len, ret_val, ret_len);
}

void
sp_decrypt(ServiceProvider * sp, char * cipher, size_t * cipher_len, uint8_t ** plain, size_t * plain_len) {
  sp->aes_gcm_decrypt(cipher, cipher_len, plain, plain_len);
}

void
sp_free_array(ServiceProvider *sp, uint8_t ** array) {
  sp->free_array(array);
}

void
sp_clean(ServiceProvider *sp) {
  sp->clean_up();
}

#ifdef __cplusplus
}
#endif
#endif
