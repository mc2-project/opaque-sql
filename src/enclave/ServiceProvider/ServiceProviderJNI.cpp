#include "SP.h"

#include <cstdint>
#include <cstdio>

#include "ServiceProvider.h"

/**
 * Throw a Java exception with the specified message.
 *
 * Important: Note that this function will return to the caller. The exception is only thrown at the
 * end of the JNI method invocation.
 */
void jni_throw(JNIEnv *env, const char *message) {
  jclass exception = env->FindClass("edu/berkeley/cs/rise/opaque/OpaqueException");
  env->ThrowNew(exception, message);
}

JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_Init(
  JNIEnv *env, jobject obj, jbyteArray shared_key, jstring intel_cert) {
  (void)env;
  (void)obj;

  jboolean if_copy = false;
  jbyte *shared_key_bytes = env->GetByteArrayElements(shared_key, &if_copy);

  const char *intel_cert_str = env->GetStringUTFChars(intel_cert, nullptr);
  try {
    service_provider.set_shared_key(reinterpret_cast<uint8_t *>(shared_key_bytes));
  } catch (const std::runtime_error &e) {
    jni_throw(env, e.what());
  }

  env->ReleaseByteArrayElements(shared_key, shared_key_bytes, 0);
  env->ReleaseStringUTFChars(intel_cert, intel_cert_str);
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_ProcessEnclaveReport(
  JNIEnv *env, jobject obj, jbyteArray report_msg_input) {
  (void)obj;

  jboolean if_copy = false;
  jbyte *report_msg_bytes = env->GetByteArrayElements(report_msg_input, &if_copy);
  oe_report_msg_t *report_msg = reinterpret_cast<oe_report_msg_t *>(report_msg_bytes);

  uint32_t shared_key_msg_size = 0;
  std::unique_ptr<oe_shared_key_msg_t> shared_key_msg;
  try {
    shared_key_msg = service_provider.process_enclave_report(report_msg, &shared_key_msg_size);
  } catch (const std::runtime_error &e) {
    jni_throw(env, e.what());
  }

  jbyteArray array_ret = env->NewByteArray(shared_key_msg_size);
  env->SetByteArrayRegion(array_ret, 0, shared_key_msg_size, reinterpret_cast<jbyte *>(shared_key_msg.get()));

  env->ReleaseByteArrayElements(report_msg_input, report_msg_bytes, 0);

  return array_ret;
}
