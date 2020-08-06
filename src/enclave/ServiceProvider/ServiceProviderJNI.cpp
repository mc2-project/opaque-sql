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
  JNIEnv *env, jobject obj, jbyteArray msg1_input) {
  (void)obj;

  jboolean if_copy = false;
  jbyte *msg1_bytes = env->GetByteArrayElements(msg1_input, &if_copy);
  oe_msg1_t *msg1 = reinterpret_cast<oe_msg1_t *>(msg1_bytes);

  uint32_t msg2_size = 0;
  std::unique_ptr<oe_msg2_t> msg2;
  try {
    msg2 = service_provider.process_enclave_report(msg1, &msg2_size);
  } catch (const std::runtime_error &e) {
    jni_throw(env, e.what());
  }

  jbyteArray array_ret = env->NewByteArray(msg2_size);
  env->SetByteArrayRegion(array_ret, 0, msg2_size, reinterpret_cast<jbyte *>(msg2.get()));

  env->ReleaseByteArrayElements(msg1_input, msg1_bytes, 0);

  return array_ret;
}
