#include <jni.h>

#ifndef _Included_SP
#define _Included_SP
#ifdef __cplusplus
extern "C" {
#endif
    // FIXME: remove last jbyteArray argument
  // JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_Init(
  //   JNIEnv *, jobject, jbyteArray, jstring, jstring, jbyteArray, jbyteArray);
    JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_Init(
      JNIEnv *, jobject, jbyteArray, jstring, jstring, jbyteArray);

  JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_SPProcMsg0(
    JNIEnv *, jobject, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_ProcessEnclaveReport(
    JNIEnv *, jobject, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SP_SPProcMsg3(
    JNIEnv *, jobject, jbyteArray);

#ifdef __cplusplus
}
#endif
#endif
