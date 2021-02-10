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
<<<<<<< HEAD

  JNIEXPORT jbyteArray JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousSortMergeJoin(
    JNIEnv *, jobject, jlong, jbyteArray, jbyteArray);
=======

  JNIEXPORT jbyteArray JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ScanCollectLastPrimary(
    JNIEnv *, jobject, jlong, jbyteArray, jbyteArray);

  JNIEXPORT jbyteArray JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousSortMergeJoin(
    JNIEnv *, jobject, jlong, jbyteArray, jbyteArray, jbyteArray);
>>>>>>> a95f2c72af1e444b79a8dd7d71a11926c3435d4f

  JNIEXPORT jobject JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousAggregate(
    JNIEnv *, jobject, jlong, jbyteArray, jbyteArray, jboolean);

  JNIEXPORT jbyteArray JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_CountRowsPerPartition(
    JNIEnv *, jobject, jlong, jbyteArray);


  JNIEXPORT jbyteArray JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ComputeNumRowsPerPartition(
    JNIEnv *, jobject, jlong, jint, jbyteArray);

  JNIEXPORT jbyteArray JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_LocalLimit(
    JNIEnv *, jobject, jlong, jint, jbyteArray);

  JNIEXPORT jbyteArray JNICALL
  Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_LimitReturnRows(
    JNIEnv *, jobject, jlong, jlong, jbyteArray, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_GenerateReport(
    JNIEnv *, jobject, jlong);

  JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_FinishAttestation(
    JNIEnv *, jobject, jlong, jbyteArray);

#ifdef __cplusplus
}
#endif
#endif
