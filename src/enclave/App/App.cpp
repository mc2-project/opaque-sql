/**
 *   Copyright(C) 2011-2015 Intel Corporation All Rights Reserved.
 *
 *   The source code, information  and  material ("Material") contained herein is
 *   owned  by Intel Corporation or its suppliers or licensors, and title to such
 *   Material remains  with Intel Corporation  or its suppliers or licensors. The
 *   Material  contains proprietary information  of  Intel or  its  suppliers and
 *   licensors. The  Material is protected by worldwide copyright laws and treaty
 *   provisions. No  part  of  the  Material  may  be  used,  copied, reproduced,
 *   modified, published, uploaded, posted, transmitted, distributed or disclosed
 *   in any way  without Intel's  prior  express written  permission. No  license
 *   under  any patent, copyright  or  other intellectual property rights  in the
 *   Material  is  granted  to  or  conferred  upon  you,  either  expressly,  by
 *   implication, inducement,  estoppel or  otherwise.  Any  license  under  such
 *   intellectual  property  rights must  be express  and  approved  by  Intel in
 *   writing.
 *
 *   *Third Party trademarks are the property of their respective owners.
 *
 *   Unless otherwise  agreed  by Intel  in writing, you may not remove  or alter
 *   this  notice or  any other notice embedded  in Materials by Intel or Intel's
 *   suppliers or licensors in any way.
 */

#include "SGXEnclave.h"

// MAX_PATH, getpwuid
#include <sys/types.h>
#ifdef _MSC_VER
# include <Shlobj.h>
#else
# include <unistd.h>
# include <pwd.h>
# define MAX_PATH FILENAME_MAX
#endif

#include <climits>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <openenclave/host.h>
#include <string>
#include <sys/time.h> // struct timeval
#include <time.h> // gettimeofday

#include "common.h"
#include "Enclave_u.h"
#include "Errlist.h"

#ifndef TRUE
# define TRUE 1
#endif

#ifndef FALSE
# define FALSE 0
#endif

#if defined(_MSC_VER)
# define TOKEN_FILENAME   "Enclave.token"
# define ENCLAVE_FILENAME "Enclave.signed.dll"
#elif defined(__GNUC__)
# define TOKEN_FILENAME   "enclave.token"
# define ENCLAVE_FILENAME "enclave.signed.so"
#endif

JavaVM* jvm;

/* Check error conditions for enclave operations */
std::string oe_error_message(oe_result_t ret) {
  size_t idx = 0;
  size_t ttl = sizeof(oe_errlist) / sizeof(oe_errlist[0]);

  for (idx = 0; idx < ttl; idx++) {
    if (ret == oe_errlist[idx].err) {
      std::string msg;
      msg.append("Error: ");
      msg.append(oe_errlist[idx].msg);
      if (NULL != oe_errlist[idx].sug) {
        msg.append(" Info: ");
        msg.append(oe_errlist[idx].sug);
      }
      return msg;
    }
  }

  // Format the status code as hex
  char buf[BUFSIZ] = {'\0'};
  snprintf(buf, BUFSIZ, "%#04x", ret);

  std::string msg;
  msg.append("Error code is ");
  msg.append(buf);
  msg.append(". Please refer to the \"OpenEnclave Documentation\" and/or \"Intel SGX SDK Developer Reference\" for "
             "more details.");
  return msg;
}

void oe_check(const char *description, oe_result_t ret) {
  if (ret != OE_OK) {
    std::string msg;
    msg.append(description);
    msg.append(" failed. ");
    msg.append(oe_error_message(ret));
    ocall_throw(msg.c_str());
  }
}

class scoped_timer {
public:
  scoped_timer(uint64_t *total_time) {
    this->total_time = total_time;
    struct timeval start;
    gettimeofday(&start, NULL);
    time_start = start.tv_sec * 1000000 + start.tv_usec;
  }

  ~scoped_timer() {
    struct timeval end;
    gettimeofday(&end, NULL);
    time_end = end.tv_sec * 1000000 + end.tv_usec;
    *total_time += time_end - time_start;
  }

  uint64_t * total_time;
  uint64_t time_start, time_end;
};

#if defined(PERF) || defined(DEBUG)
#define oe_check_and_time(description, op) do {        \
    printf("%s running...\n", description);             \
    uint64_t t_ = 0;                                    \
    oe_result_t ret_;                                  \
    {                                                   \
      scoped_timer timer_(&t_);                         \
      ret_ = op;                                        \
    }                                                   \
    double t_ms_ = ((double) t_) / 1000;                \
    if (ret_ != OE_OK) {                          \
      oe_check(description, ret_);                     \
    } else {                                            \
      printf("%s done (%f ms).\n", description, t_ms_); \
    }                                                   \
  } while (0)
#else
#define oe_check_and_time(description, op) oe_check(description, op)
#endif

/* OCall functions */
void ocall_print_string(const char *str)
{
  /* Proxy/Bridge will check the length and null-terminate
   * the input string to prevent buffer overflow.
   */
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
  JNIEnv* env;
  jvm->AttachCurrentThread((void**) &env, NULL);
  jclass exception = env->FindClass("edu/berkeley/cs/rise/opaque/OpaqueException");
  env->ThrowNew(exception, message);
}

JNIEXPORT jlong JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_StartEnclave(
  JNIEnv *env, jobject obj, jstring library_path) {
  (void)obj;

  env->GetJavaVM(&jvm);

  oe_enclave_t* enclave = nullptr;
  uint32_t flags = 0;

#ifdef SIMULATE
  flags |= OE_ENCLAVE_FLAG_SIMULATE;
#endif
  
  const char *library_path_str = env->GetStringUTFChars(library_path, nullptr);
  oe_check_and_time("StartEnclave",
                     oe_create_Enclave_enclave(
                       library_path_str, OE_ENCLAVE_TYPE_AUTO, flags, nullptr, 0, &enclave
                      )
                    );
  env->ReleaseStringUTFChars(library_path, library_path_str);
  long int enclavePtr = (long int)enclave;

  return enclavePtr;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_GenerateReport(
  JNIEnv *env, jobject obj, jlong eid) {
  (void)obj;
  (void)eid;

  uint8_t* report_msg = NULL;
  size_t report_msg_size = 0;

  oe_check_and_time("Generate enclave report",
                     ecall_generate_report((oe_enclave_t*)eid,
                                           &report_msg,
                                           &report_msg_size));

  // Allocate memory
  jbyteArray report_msg_bytes = env->NewByteArray(report_msg_size);
  env->SetByteArrayRegion(report_msg_bytes, 0, report_msg_size, reinterpret_cast<jbyte *>(report_msg));

  return report_msg_bytes;
}

JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_FinishAttestation(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray shared_key_msg_input) {
  (void)obj;

  jboolean if_copy = false;
  jbyte *shared_key_msg_bytes = env->GetByteArrayElements(shared_key_msg_input, &if_copy);
  uint32_t shared_key_msg_size = static_cast<uint32_t>(env->GetArrayLength(shared_key_msg_input));

  oe_check_and_time("Finish attestation",
                    ecall_finish_attestation((oe_enclave_t*)eid,
                                             reinterpret_cast<uint8_t *>(shared_key_msg_bytes),
                                             shared_key_msg_size));

  env->ReleaseByteArrayElements(shared_key_msg_input, shared_key_msg_bytes, 0);

}

JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_StopEnclave(
  JNIEnv *env, jobject obj, jlong eid) {
  (void)env;
  (void)obj;

  oe_check("StopEnclave", oe_terminate_enclave((oe_enclave_t*)eid));
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Project(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray project_list, jbyteArray input_rows) {
  (void)obj;

  jboolean if_copy;

  uint32_t project_list_length = (uint32_t) env->GetArrayLength(project_list);
  uint8_t *project_list_ptr = (uint8_t *) env->GetByteArrayElements(project_list, &if_copy);

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint8_t *output_rows = nullptr;
  size_t output_rows_length = 0;

  if (input_rows_ptr == nullptr) {
    ocall_throw("Project: JNI failed to get input byte array.");
  } else {
    oe_check_and_time("Project",
                       ecall_project(
                         (oe_enclave_t*)eid,
                         project_list_ptr, project_list_length,
                         input_rows_ptr, input_rows_length,
                         &output_rows, &output_rows_length));
  }

  env->ReleaseByteArrayElements(project_list, (jbyte *) project_list_ptr, 0);
  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  jbyteArray ret = env->NewByteArray(output_rows_length);
  env->SetByteArrayRegion(ret, 0, output_rows_length, (jbyte *) output_rows);
  free(output_rows);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Filter(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray condition, jbyteArray input_rows) {
  (void)obj;

  jboolean if_copy;

  size_t condition_length = (size_t) env->GetArrayLength(condition);
  uint8_t *condition_ptr = (uint8_t *) env->GetByteArrayElements(condition, &if_copy);

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint8_t *output_rows = nullptr;
  size_t output_rows_length = 0;

  if (input_rows_ptr == nullptr) {
    ocall_throw("Filter: JNI failed to get input byte array.");
  } else {
    oe_check_and_time("Filter",
                       ecall_filter(
                         (oe_enclave_t*)eid,
                         condition_ptr, condition_length,
                         input_rows_ptr, input_rows_length,
                         &output_rows, &output_rows_length));
  }

  env->ReleaseByteArrayElements(condition, (jbyte *) condition_ptr, 0);
  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  jbyteArray ret = env->NewByteArray(output_rows_length);
  env->SetByteArrayRegion(ret, 0, output_rows_length, (jbyte *) output_rows);
  free(output_rows);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Encrypt(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray plaintext) {
  (void)obj;

  uint32_t plength = (uint32_t) env->GetArrayLength(plaintext);
  jboolean if_copy = false;
  uint8_t *plaintext_ptr = (uint8_t *) env->GetByteArrayElements(plaintext, &if_copy);

  uint8_t *ciphertext_copy = nullptr;
  jsize clength = 0;

  if (plaintext_ptr == nullptr) {
    ocall_throw("Encrypt: JNI failed to get input byte array.");
  } else {
    clength = plength + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
    ciphertext_copy = new uint8_t[clength];

    oe_check("Encrypt",
              ecall_encrypt((oe_enclave_t*)eid, plaintext_ptr, plength, ciphertext_copy, (uint32_t) clength));
  }

  jbyteArray ciphertext = env->NewByteArray(clength);
  env->SetByteArrayRegion(ciphertext, 0, clength, (jbyte *) ciphertext_copy);

  env->ReleaseByteArrayElements(plaintext, (jbyte *) plaintext_ptr, 0);

  delete[] ciphertext_copy;

  return ciphertext;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Sample(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray input_rows) {
  (void)obj;

  jboolean if_copy;
  size_t input_rows_length = static_cast<size_t>(env->GetArrayLength(input_rows));
  uint8_t *input_rows_ptr = reinterpret_cast<uint8_t *>(
    env->GetByteArrayElements(input_rows, &if_copy));

  uint8_t *output_rows = nullptr;
  size_t output_rows_length = 0;

  if (input_rows_ptr == nullptr) {
    ocall_throw("Sample: JNI failed to get input byte array.");
  } else {
    oe_check_and_time("Sample",
                       ecall_sample(
                         (oe_enclave_t*)eid,
                         input_rows_ptr, input_rows_length,
                         &output_rows, &output_rows_length));
  }

  jbyteArray ret = env->NewByteArray(output_rows_length);
  env->SetByteArrayRegion(ret, 0, output_rows_length, (jbyte *) output_rows);
  free(output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_FindRangeBounds(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray sort_order, jint num_partitions,
  jbyteArray input_rows) {
  (void)obj;

  jboolean if_copy;

  size_t sort_order_length = static_cast<size_t>(env->GetArrayLength(sort_order));
  uint8_t *sort_order_ptr = reinterpret_cast<uint8_t *>(
    env->GetByteArrayElements(sort_order, &if_copy));

  size_t input_rows_length = static_cast<size_t>(env->GetArrayLength(input_rows));
  uint8_t *input_rows_ptr = reinterpret_cast<uint8_t *>(
    env->GetByteArrayElements(input_rows, &if_copy));

  uint8_t *output_rows = nullptr;
  size_t output_rows_length = 0;

  if (input_rows_ptr == nullptr) {
    ocall_throw("FindRangeBounds: JNI failed to get input byte array.");
  } else {
    oe_check_and_time("Find Range Bounds",
                       ecall_find_range_bounds(
                         (oe_enclave_t*)eid,
                         sort_order_ptr, sort_order_length,
                         num_partitions,
                         input_rows_ptr, input_rows_length,
                         &output_rows, &output_rows_length));
  }

  jbyteArray ret = env->NewByteArray(output_rows_length);
  env->SetByteArrayRegion(ret, 0, output_rows_length, reinterpret_cast<jbyte *>(output_rows));
  free(output_rows);

  env->ReleaseByteArrayElements(sort_order, reinterpret_cast<jbyte *>(sort_order_ptr), 0);
  env->ReleaseByteArrayElements(input_rows, reinterpret_cast<jbyte *>(input_rows_ptr), 0);

  return ret;
}

JNIEXPORT jobjectArray JNICALL
Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_PartitionForSort(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray sort_order, jint num_partitions,
  jbyteArray input_rows, jbyteArray boundary_rows) {
  (void)obj;

  jboolean if_copy;

  size_t sort_order_length = static_cast<size_t>(env->GetArrayLength(sort_order));
  uint8_t *sort_order_ptr = reinterpret_cast<uint8_t *>(
    env->GetByteArrayElements(sort_order, &if_copy));

  size_t input_rows_length = static_cast<size_t>(env->GetArrayLength(input_rows));
  uint8_t *input_rows_ptr = reinterpret_cast<uint8_t *>(
    env->GetByteArrayElements(input_rows, &if_copy));

  size_t boundary_rows_length = static_cast<size_t>(env->GetArrayLength(boundary_rows));
  uint8_t *boundary_rows_ptr = reinterpret_cast<uint8_t *>(
    env->GetByteArrayElements(boundary_rows, &if_copy));

  uint8_t **output_partitions = new uint8_t *[num_partitions];
  size_t *output_partition_lengths = new size_t[num_partitions];

  if (input_rows_ptr == nullptr) {
    ocall_throw("PartitionForSort: JNI failed to get input byte array.");
  } else {
    oe_check_and_time("Partition For Sort",
                       ecall_partition_for_sort(
                         (oe_enclave_t*)eid,
                         sort_order_ptr, sort_order_length,
                         num_partitions,
                         input_rows_ptr, input_rows_length,
                         boundary_rows_ptr, boundary_rows_length,
                         output_partitions, output_partition_lengths));
  }

  env->ReleaseByteArrayElements(sort_order, reinterpret_cast<jbyte *>(sort_order_ptr), 0);
  env->ReleaseByteArrayElements(input_rows, reinterpret_cast<jbyte *>(input_rows_ptr), 0);
  env->ReleaseByteArrayElements(boundary_rows, reinterpret_cast<jbyte *>(boundary_rows_ptr), 0);

  jobjectArray result = env->NewObjectArray(num_partitions,  env->FindClass("[B"), nullptr);
  for (jint i = 0; i < num_partitions; i++) {
    jbyteArray partition = env->NewByteArray(output_partition_lengths[i]);
    env->SetByteArrayRegion(partition, 0, output_partition_lengths[i],
                            reinterpret_cast<jbyte *>(output_partitions[i]));
    free(output_partitions[i]);
    env->SetObjectArrayElement(result, i, partition);
  }
  delete[] output_partitions;
  delete[] output_partition_lengths;

  return result;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ExternalSort(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray sort_order, jbyteArray input_rows) {
  (void)obj;

  jboolean if_copy;

  size_t sort_order_length = static_cast<size_t>(env->GetArrayLength(sort_order));
  uint8_t *sort_order_ptr = reinterpret_cast<uint8_t *>(
    env->GetByteArrayElements(sort_order, &if_copy));

  size_t input_rows_length = static_cast<size_t>(env->GetArrayLength(input_rows));
  uint8_t *input_rows_ptr = reinterpret_cast<uint8_t *>(
    env->GetByteArrayElements(input_rows, &if_copy));

  uint8_t *output_rows = nullptr;
  size_t output_rows_length = 0;

  if (input_rows_ptr == nullptr) {
    ocall_throw("ExternalSort: JNI failed to get input byte array.");
  } else {
    oe_check_and_time("External Non-Oblivious Sort",
                       ecall_external_sort((oe_enclave_t*)eid,
                                           sort_order_ptr, sort_order_length,
                                           input_rows_ptr, input_rows_length,
                                           &output_rows, &output_rows_length));
  }

  jbyteArray ret = env->NewByteArray(output_rows_length);
  env->SetByteArrayRegion(ret, 0, output_rows_length, reinterpret_cast<jbyte *>(output_rows));
  free(output_rows);

  env->ReleaseByteArrayElements(sort_order, reinterpret_cast<jbyte *>(sort_order_ptr), 0);
  env->ReleaseByteArrayElements(input_rows, reinterpret_cast<jbyte *>(input_rows_ptr), 0);

  return ret;
}

JNIEXPORT jbyteArray JNICALL
Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ScanCollectLastPrimary(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray join_expr, jbyteArray input_rows) {
  (void)obj;

  jboolean if_copy;

  uint32_t join_expr_length = (uint32_t) env->GetArrayLength(join_expr);
  uint8_t *join_expr_ptr = (uint8_t *) env->GetByteArrayElements(join_expr, &if_copy);

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint8_t *output_rows = nullptr;
  size_t output_rows_length = 0;

  if (input_rows_ptr == nullptr) {
    ocall_throw("ScanCollectLastPrimary: JNI failed to get input byte array.");
  } else {
    oe_check_and_time("Scan Collect Last Primary",
                       ecall_scan_collect_last_primary(
                         (oe_enclave_t*)eid,
                         join_expr_ptr, join_expr_length,
                         input_rows_ptr, input_rows_length,
                         &output_rows, &output_rows_length));
  }

  jbyteArray ret = env->NewByteArray(output_rows_length);
  env->SetByteArrayRegion(ret, 0, output_rows_length, (jbyte *) output_rows);
  free(output_rows);

  env->ReleaseByteArrayElements(join_expr, (jbyte *) join_expr_ptr, 0);
  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  return ret;
}

JNIEXPORT jbyteArray JNICALL
Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousSortMergeJoin(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray join_expr, jbyteArray input_rows,
  jbyteArray join_row) {
  (void)obj;

  jboolean if_copy;

  uint32_t join_expr_length = (uint32_t) env->GetArrayLength(join_expr);
  uint8_t *join_expr_ptr = (uint8_t *) env->GetByteArrayElements(join_expr, &if_copy);

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t join_row_length = (uint32_t) env->GetArrayLength(join_row);
  uint8_t *join_row_ptr = (uint8_t *) env->GetByteArrayElements(join_row, &if_copy);

  uint8_t *output_rows = nullptr;
  size_t output_rows_length = 0;

  if (input_rows_ptr == nullptr) {
    ocall_throw("NonObliviousSortMergeJoin: JNI failed to get input byte array.");
  } else {
    oe_check_and_time("Non-Oblivious Sort-Merge Join",
                       ecall_non_oblivious_sort_merge_join(
                         (oe_enclave_t*)eid,
                         join_expr_ptr, join_expr_length,
                         input_rows_ptr, input_rows_length,
                         join_row_ptr, join_row_length,
                         &output_rows, &output_rows_length));
  }
  
  jbyteArray ret = env->NewByteArray(output_rows_length);
  env->SetByteArrayRegion(ret, 0, output_rows_length, (jbyte *) output_rows);
  free(output_rows);

  env->ReleaseByteArrayElements(join_expr, (jbyte *) join_expr_ptr, 0);
  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);
  env->ReleaseByteArrayElements(join_row, (jbyte *) join_row_ptr, 0);

  return ret;
}

JNIEXPORT jobject JNICALL
Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousAggregate(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray agg_op, jbyteArray input_rows, jboolean isPartial) {
  (void)obj;

  jboolean if_copy;

  uint32_t agg_op_length = (uint32_t) env->GetArrayLength(agg_op);
  uint8_t *agg_op_ptr = (uint8_t *) env->GetByteArrayElements(agg_op, &if_copy);

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint8_t *output_rows = nullptr;
  size_t output_rows_length = 0;

  bool is_partial = (bool) isPartial;

  if (input_rows_ptr == nullptr) {
    ocall_throw("NonObliviousAggregate: JNI failed to get input byte array.");
  } else {
    oe_check_and_time("Non-Oblivious Aggregate",
                       ecall_non_oblivious_aggregate(
                         (oe_enclave_t*)eid,
                         agg_op_ptr, agg_op_length,
                         input_rows_ptr, input_rows_length,
                         &output_rows, &output_rows_length,
                         is_partial));
  }

  jbyteArray ret = env->NewByteArray(output_rows_length);
  env->SetByteArrayRegion(ret, 0, output_rows_length, (jbyte *) output_rows);
  free(output_rows);

  env->ReleaseByteArrayElements(agg_op, (jbyte *) agg_op_ptr, 0);
  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);
  
  return ret;
}

JNIEXPORT jbyteArray JNICALL
Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_CountRowsPerPartition(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray input_rows) {

    (void)obj;
    jboolean if_copy;
  
    uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
    uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

    uint8_t *output_rows = nullptr;
    size_t output_rows_length = 0;

    if (input_rows_ptr == nullptr) {
      ocall_throw("CountRowsPerPartition: JNI failed to get input byte array.");
    } else {
      oe_check_and_time("CountRowsPerPartition",
                        ecall_count_rows_per_partition((oe_enclave_t *) eid,
                                                       input_rows_ptr,
                                                       input_rows_length,
                                                       &output_rows,
                                                       &output_rows_length));
    }

    jbyteArray ret = env->NewByteArray(output_rows_length);
    env->SetByteArrayRegion(ret, 0, output_rows_length, (jbyte *) output_rows);
    free(output_rows);

    env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  return ret;
}

JNIEXPORT jbyteArray JNICALL
Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ComputeNumRowsPerPartition(
  JNIEnv *env, jobject obj, jlong eid, jint limit, jbyteArray input_rows) {

    (void)obj;
    jboolean if_copy;
  
    uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
    uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

    uint8_t *output_rows = nullptr;
    size_t output_rows_length = 0;

    if (input_rows_ptr == nullptr) {
      ocall_throw("ComputeNumRowsPerPartition: JNI failed to get input byte array.");
    } else {
      oe_check_and_time("ComputeNumRowsPerPartition",
                        ecall_compute_num_rows_per_partition((oe_enclave_t *) eid,
                                                             (uint32_t) limit,
                                                             input_rows_ptr,
                                                             input_rows_length,
                                                             &output_rows,
                                                             &output_rows_length));
    }

    jbyteArray ret = env->NewByteArray(output_rows_length);
    env->SetByteArrayRegion(ret, 0, output_rows_length, (jbyte *) output_rows);
    free(output_rows);

    env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  return ret;
}

JNIEXPORT jbyteArray JNICALL
Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_LocalLimit(
  JNIEnv *env, jobject obj, jlong eid, jint limit, jbyteArray input_rows) {

    (void)obj;
    jboolean if_copy;
  
    uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
    uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

    uint8_t *output_rows = nullptr;
    size_t output_rows_length = 0;

    if (input_rows_ptr == nullptr) {
      ocall_throw("LocalLimit: JNI failed to get input byte array.");
    } else {
      oe_check_and_time("LocalLimit",
                        ecall_local_limit((oe_enclave_t *) eid,
                                          limit,
                                          input_rows_ptr,
                                          input_rows_length,
                                          &output_rows,
                                          &output_rows_length));
    }

    jbyteArray ret = env->NewByteArray(output_rows_length);
    env->SetByteArrayRegion(ret, 0, output_rows_length, (jbyte *) output_rows);
    free(output_rows);

    env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  return ret;
}


JNIEXPORT jbyteArray JNICALL
Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_LimitReturnRows(
  JNIEnv *env, jobject obj, jlong eid, jlong partition_id, jbyteArray limits, jbyteArray input_rows) {

    (void)obj;
    jboolean if_copy;
  
    uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
    uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

    uint32_t limits_length = (uint32_t) env->GetArrayLength(limits);
    uint8_t *limits_ptr = (uint8_t *) env->GetByteArrayElements(limits, &if_copy);

    uint8_t *output_rows = nullptr;
    size_t output_rows_length = 0;

    if (input_rows_ptr == nullptr) {
      ocall_throw("LimitReturnRows: JNI failed to get input byte array.");
    } else {
      oe_check_and_time("LimitReturnRows",
                        ecall_limit_return_rows((oe_enclave_t *) eid,
                                                partition_id,
                                                limits_ptr,
                                                limits_length,
                                                input_rows_ptr,
                                                input_rows_length,
                                                &output_rows,
                                                &output_rows_length));
    }

    jbyteArray ret = env->NewByteArray(output_rows_length);
    env->SetByteArrayRegion(ret, 0, output_rows_length, (jbyte *) output_rows);
    free(output_rows);

    env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  return ret;
}
