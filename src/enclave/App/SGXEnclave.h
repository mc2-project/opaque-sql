#include <jni.h>

#ifndef _Included_SGXEnclave
#define _Included_SGXEnclave
#ifdef __cplusplus
extern "C" {
#endif
  JNIEXPORT jlong JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_StartEnclave(
    JNIEnv *, jobject);

  JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_StopEnclave(
    JNIEnv *, jobject, jlong);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Project(
    JNIEnv *, jobject, jlong, jint, jint, jint, jbyteArray, jint);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Filter(
    JNIEnv *, jobject, jlong, jint, jint, jint, jbyteArray, jint, jobject);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Encrypt(
    JNIEnv *, jobject, jlong, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Decrypt(
    JNIEnv *, jobject, jlong, jbyteArray);

  JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Test(
    JNIEnv *, jobject, jlong);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ObliviousSort(
    JNIEnv *, jobject, jlong, jint, jbyteArray, jint, jint);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_AggregateStep1(
    JNIEnv *, jobject, jlong, jint, jint, jint, jbyteArray, jint);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ProcessBoundary(
    JNIEnv *, jobject, jlong, jint, jbyteArray, jint);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_AggregateStep2(
    JNIEnv *, jobject, jlong, jint, jint, jint, jbyteArray, jint, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_JoinSortPreprocess(
    JNIEnv *, jobject, jlong, jint, jint, jint, jbyteArray, jint, jbyteArray, jint);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ScanCollectLastPrimary(
    JNIEnv *, jobject, jlong, jint, jbyteArray, jint);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ProcessJoinBoundary(
    JNIEnv *, jobject, jlong, jint, jbyteArray, jint);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_SortMergeJoin(
    JNIEnv *, jobject, jlong, jint, jint, jint, jbyteArray, jint, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_EncryptAttribute(
    JNIEnv *, jobject, jlong, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_CreateBlock(
    JNIEnv *, jobject, jlong, jbyteArray, jint, jboolean);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_SplitBlock(
    JNIEnv *, jobject, jlong, jbyteArray, jint, jboolean);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Sample(
    JNIEnv *, jobject, jlong, jint, jint, jint, jbyteArray, jint, jobject);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_FindRangeBounds(
    JNIEnv *, jobject, jlong, jint, jint, jbyteArray, jint);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_PartitionForSort(
    JNIEnv *, jobject, jlong, jint, jint, jint, jint, jbyteArray, jint, jbyteArray, jintArray, jintArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ExternalSort(
    JNIEnv *, jobject, jlong, jint, jint, jint, jbyteArray, jint);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousAggregate(
    JNIEnv *, jobject, jlong, jint, jint, jint, jbyteArray, jint, jobject);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousSortMergeJoin(
    JNIEnv *, jobject, jlong, jint, jint, jint, jbyteArray, jint, jobject);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_EnclaveColumnSort(
    JNIEnv *, jobject, jlong, jint, jint, jint, jint, jbyteArray, jint, jint, jint, jint, jint, jint);

  JNIEXPORT jint JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_CountNumRows(
    JNIEnv *, jobject, jlong, jbyteArray);

  JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ColumnSortFilter(
    JNIEnv *env, jobject obj,
    jlong eid, jint op_code, jbyteArray input_rows, jint column, jint offset, jint num_rows,
    jobject num_output_rows_obj);
  
#ifdef __cplusplus
}
#endif
#endif
