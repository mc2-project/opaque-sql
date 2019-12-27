#include <jni.h>

#ifndef _Included_SGXEnclave
#define _Included_SGXEnclave
#ifdef __cplusplus
extern "C" {
#endif
  JNIEXPORT jlong JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_StartEnclave(
    JNIEnv *, jobject, jstring);

  JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_StopEnclave(
    JNIEnv *, jobject, jlong);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Project(
    JNIEnv *, jobject, jlong, jbyteArray, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Filter(
    JNIEnv *, jobject, jlong, jbyteArray, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Encrypt(
    JNIEnv *, jobject, jlong, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Decrypt(
    JNIEnv *, jobject, jlong, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Sample(
    JNIEnv *, jobject, jlong, jbyteArray);

  JNIEXPORT jbyteArray JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_FindRangeBounds(
    JNIEnv *, jobject, jlong, jbyteArray, jint, jbyteArray);

  JNIEXPORT jobjectArray JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_PartitionForSort(
    JNIEnv *, jobject, jlong, jbyteArray, jint, jbyteArray, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ExternalSort(
    JNIEnv *, jobject, jlong, jbyteArray, jbyteArray);

  JNIEXPORT jbyteArray JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ScanCollectLastPrimary(
    JNIEnv *, jobject, jlong, jbyteArray, jbyteArray);

  JNIEXPORT jbyteArray JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousSortMergeJoin(
    JNIEnv *, jobject, jlong, jbyteArray, jbyteArray, jbyteArray);

  JNIEXPORT jobject JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousAggregateStep1(
    JNIEnv *, jobject, jlong, jbyteArray, jbyteArray);

  JNIEXPORT jbyteArray JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousAggregateStep2(
    JNIEnv *, jobject, jlong, jbyteArray, jbyteArray, jbyteArray, jbyteArray, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation1(
    JNIEnv *, jobject, jlong);

  JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation3(
    JNIEnv *, jobject, jlong, jbyteArray);

#ifdef __cplusplus
}
#endif
#endif
