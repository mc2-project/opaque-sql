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
#include <sys/time.h> // struct timeval
#include <time.h> // gettimeofday
#include <string>

#include <sgx_eid.h>     /* sgx_enclave_id_t */
#include <sgx_error.h>       /* sgx_status_t */
#include <sgx_uae_service.h>
#include <sgx_ukey_exchange.h>

#include "Enclave_u.h"

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

static sgx_ra_context_t context = INT_MAX;
JavaVM* jvm;

typedef struct _sgx_errlist_t {
  sgx_status_t err;
  const char *msg;
  const char *sug; /* Suggestion */
} sgx_errlist_t;

/* Error code returned by sgx_create_enclave */
static sgx_errlist_t sgx_errlist[] = {
  {
    SGX_ERROR_UNEXPECTED,
    "Unexpected error occurred.",
    NULL
  },
  {
    SGX_ERROR_INVALID_PARAMETER,
    "Invalid parameter.",
    NULL
  },
  {
    SGX_ERROR_OUT_OF_MEMORY,
    "Out of memory.",
    NULL
  },
  {
    SGX_ERROR_ENCLAVE_LOST,
    "Power transition occurred.",
    "Please refer to the sample \"PowerTransition\" for details."
  },
  {
    SGX_ERROR_INVALID_ENCLAVE,
    "Invalid enclave image.",
    NULL
  },
  {
    SGX_ERROR_INVALID_ENCLAVE_ID,
    "Invalid enclave identification.",
    NULL
  },
  {
    SGX_ERROR_INVALID_SIGNATURE,
    "Invalid enclave signature.",
    NULL
  },
  {
    SGX_ERROR_OUT_OF_EPC,
    "Out of EPC memory.",
    NULL
  },
  {
    SGX_ERROR_NO_DEVICE,
    "Invalid SGX device.",
    "Please make sure SGX module is enabled in the BIOS, and install SGX driver afterwards."
  },
  {
    SGX_ERROR_MEMORY_MAP_CONFLICT,
    "Memory map conflicted.",
    NULL
  },
  {
    SGX_ERROR_INVALID_METADATA,
    "Invalid enclave metadata.",
    NULL
  },
  {
    SGX_ERROR_DEVICE_BUSY,
    "SGX device was busy.",
    NULL
  },
  {
    SGX_ERROR_INVALID_VERSION,
    "Enclave version was invalid.",
    NULL
  },
  {
    SGX_ERROR_INVALID_ATTRIBUTE,
    "Enclave was not authorized.",
    NULL
  },
  {
    SGX_ERROR_ENCLAVE_FILE_ACCESS,
    "Can't open enclave file.",
    NULL
  },
};

/* Check error conditions for loading enclave */
std::string sgx_error_message(sgx_status_t ret) {
  size_t idx = 0;
  size_t ttl = sizeof(sgx_errlist) / sizeof(sgx_errlist[0]);

  for (idx = 0; idx < ttl; idx++) {
    if (ret == sgx_errlist[idx].err) {
      std::string msg;
      msg.append("Error: ");
      msg.append(sgx_errlist[idx].msg);
      if (NULL != sgx_errlist[idx].sug) {
        msg.append(" Info: ");
        msg.append(sgx_errlist[idx].sug);
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
  msg.append(". Please refer to the \"Intel SGX SDK Developer Reference\" for "
             "more details.");
  return msg;
}

void sgx_check(const char *description, sgx_status_t ret) {
  if (ret != SGX_SUCCESS) {
    std::string msg;
    msg.append(description);
    msg.append(" failed. ");
    msg.append(sgx_error_message(ret));
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
#define sgx_check_and_time(description, op) do {        \
    printf("%s running...\n", description);             \
    uint64_t t_ = 0;                                    \
    sgx_status_t ret_;                                  \
    {                                                   \
      scoped_timer timer_(&t_);                         \
      ret_ = op;                                        \
    }                                                   \
    double t_ms_ = ((double) t_) / 1000;                \
    if (ret_ != SGX_SUCCESS) {                          \
      sgx_check(description, ret_);                     \
    } else {                                            \
      printf("%s done (%f ms).\n", description, t_ms_); \
    }                                                   \
  } while (0)
#else
#define sgx_check_and_time(description, op) sgx_check(description, op)
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

  sgx_enclave_id_t eid;
  sgx_launch_token_t token = {0};
  int updated = 0;

  const char *library_path_str = env->GetStringUTFChars(library_path, nullptr);
  sgx_check_and_time("StartEnclave",
                     sgx_create_enclave(
                       library_path_str, SGX_DEBUG_FLAG, &token, &updated, &eid, nullptr));
  env->ReleaseStringUTFChars(library_path, library_path_str);

  return eid;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation0(
  JNIEnv *env, jobject obj, jlong eid) {
  (void)obj;

  sgx_status_t status;
  sgx_check_and_time("Initialize Remote Attestation",
                     ecall_enclave_init_ra(eid, &status, &context));
  sgx_check("Initialize Remote Attestation", status);

  uint32_t extended_epid_group_id = 0;
  sgx_check_and_time("Remote Attestation Step 0: Get Extended EPID Group ID",
                     sgx_get_extended_epid_group_id(&extended_epid_group_id));
  jbyteArray ret = env->NewByteArray(sizeof(uint32_t));
  env->SetByteArrayRegion(ret, 0, sizeof(uint32_t), (jbyte *) &extended_epid_group_id);
  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation1(
  JNIEnv *env, jobject obj, jlong eid) {
  (void)obj;

  sgx_ra_msg1_t msg1;
  sgx_check_and_time("Remote Attestation Step 1",
                     sgx_ra_get_msg1(context, eid, sgx_ra_get_ga, &msg1));
  jbyteArray array_ret = env->NewByteArray(sizeof(sgx_ra_msg1_t));
  env->SetByteArrayRegion(array_ret, 0, sizeof(sgx_ra_msg1_t), reinterpret_cast<jbyte *>(&msg1));
  return array_ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation2(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray msg2_input) {
  (void)obj;

  uint32_t msg2_size = static_cast<uint32_t>(env->GetArrayLength(msg2_input));
  jboolean if_copy = false;
  jbyte *msg2_bytes = env->GetByteArrayElements(msg2_input, &if_copy);
  sgx_ra_msg2_t *msg2 = reinterpret_cast<sgx_ra_msg2_t *>(msg2_bytes);

  uint32_t msg3_size = 0;
  sgx_ra_msg3_t *msg3 = nullptr;

  sgx_check_and_time("Remote Attestation Step 2",
                     sgx_ra_proc_msg2(context,
                                      eid,
                                      sgx_ra_proc_msg2_trusted,
                                      sgx_ra_get_msg3_trusted,
                                      msg2,
                                      msg2_size,
                                      &msg3,
                                      &msg3_size));

  jbyteArray ret = env->NewByteArray(msg3_size);
  env->SetByteArrayRegion(ret, 0, msg3_size, reinterpret_cast<jbyte *>(msg3));
  free(msg3);

  env->ReleaseByteArrayElements(msg2_input, msg2_bytes, 0);

  return ret;
}

JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation3(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray msg4_input) {
  (void)obj;

  jboolean if_copy = false;
  jbyte *msg4_bytes = env->GetByteArrayElements(msg4_input, &if_copy);
  uint32_t msg4_size = static_cast<uint32_t>(env->GetArrayLength(msg4_input));

  sgx_check_and_time("Remote Attestation Step 3",
                     ecall_ra_proc_msg4(eid,
                                        context,
                                        reinterpret_cast<uint8_t *>(msg4_bytes),
                                        msg4_size));

  env->ReleaseByteArrayElements(msg4_input, msg4_bytes, 0);

  ecall_enclave_ra_close(eid, context);
}

JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_StopEnclave(
  JNIEnv *env, jobject obj, jlong eid) {
  (void)env;
  (void)obj;

  sgx_check("StopEnclave", sgx_destroy_enclave(eid));
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
    sgx_check_and_time("Project",
                       ecall_project(
                         eid,
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
    sgx_check_and_time("Filter",
                       ecall_filter(
                         eid,
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

    sgx_check("Encrypt",
              ecall_encrypt(eid, plaintext_ptr, plength, ciphertext_copy, (uint32_t) clength));
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
    sgx_check_and_time("Sample",
                       ecall_sample(
                         eid,
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
    sgx_check_and_time("Find Range Bounds",
                       ecall_find_range_bounds(
                         eid,
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
    sgx_check_and_time("Partition For Sort",
                       ecall_partition_for_sort(
                         eid,
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
    sgx_check_and_time("External Non-Oblivious Sort",
                       ecall_external_sort(eid,
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
    sgx_check_and_time("Scan Collect Last Primary",
                       ecall_scan_collect_last_primary(
                         eid,
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
    sgx_check_and_time("Non-Oblivious Sort-Merge Join",
                       ecall_non_oblivious_sort_merge_join(
                         eid,
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
Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousAggregateStep1(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray agg_op, jbyteArray input_rows) {
  (void)obj;

  jboolean if_copy;

  uint32_t agg_op_length = (uint32_t) env->GetArrayLength(agg_op);
  uint8_t *agg_op_ptr = (uint8_t *) env->GetByteArrayElements(agg_op, &if_copy);

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint8_t *first_row = nullptr;
  size_t first_row_length = 0;

  uint8_t *last_group = nullptr;
  size_t last_group_length = 0;

  uint8_t *last_row = nullptr;
  size_t last_row_length = 0;

  if (input_rows_ptr == nullptr) {
    ocall_throw("NonObliviousAggregateStep1: JNI failed to get input byte array.");
  } else {
    sgx_check_and_time("Non-Oblivious Aggregate Step 1",
                       ecall_non_oblivious_aggregate_step1(
                         eid,
                         agg_op_ptr, agg_op_length,
                         input_rows_ptr, input_rows_length,
                         &first_row, &first_row_length,
                         &last_group, &last_group_length,
                         &last_row, &last_row_length));
  }

  jbyteArray first_row_array = env->NewByteArray(first_row_length);
  env->SetByteArrayRegion(first_row_array, 0, first_row_length, (jbyte *) first_row);
  free(first_row);

  jbyteArray last_group_array = env->NewByteArray(last_group_length);
  env->SetByteArrayRegion(last_group_array, 0, last_group_length, (jbyte *) last_group);
  free(last_group);

  jbyteArray last_row_array = env->NewByteArray(last_row_length);
  env->SetByteArrayRegion(last_row_array, 0, last_row_length, (jbyte *) last_row);
  free(last_row);

  env->ReleaseByteArrayElements(agg_op, (jbyte *) agg_op_ptr, 0);
  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  jclass tuple3_class = env->FindClass("scala/Tuple3");
  jobject ret = env->NewObject(
    tuple3_class,
    env->GetMethodID(tuple3_class, "<init>",
                     "(Ljava/lang/Object;Ljava/lang/Object;Ljava/lang/Object;)V"),
    first_row_array, last_group_array, last_row_array);

  return ret;
}

JNIEXPORT jbyteArray JNICALL
Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousAggregateStep2(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray agg_op, jbyteArray input_rows,
  jbyteArray next_partition_first_row, jbyteArray prev_partition_last_group,
  jbyteArray prev_partition_last_row) {
  (void)obj;

  jboolean if_copy;

  uint32_t agg_op_length = (uint32_t) env->GetArrayLength(agg_op);
  uint8_t *agg_op_ptr = (uint8_t *) env->GetByteArrayElements(agg_op, &if_copy);

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t next_partition_first_row_length =
    (uint32_t) env->GetArrayLength(next_partition_first_row);
  uint8_t *next_partition_first_row_ptr =
    (uint8_t *) env->GetByteArrayElements(next_partition_first_row, &if_copy);

  uint32_t prev_partition_last_group_length =
    (uint32_t) env->GetArrayLength(prev_partition_last_group);
  uint8_t *prev_partition_last_group_ptr =
    (uint8_t *) env->GetByteArrayElements(prev_partition_last_group, &if_copy);

  uint32_t prev_partition_last_row_length =
    (uint32_t) env->GetArrayLength(prev_partition_last_row);
  uint8_t *prev_partition_last_row_ptr =
    (uint8_t *) env->GetByteArrayElements(prev_partition_last_row, &if_copy);

  uint8_t *output_rows = nullptr;
  size_t output_rows_length = 0;

  if (input_rows_ptr == nullptr) {
    ocall_throw("NonObliviousAggregateStep2: JNI failed to get input byte array.");
  } else {
    sgx_check_and_time("Non-Oblivious Aggregate Step 2",
                       ecall_non_oblivious_aggregate_step2(
                         eid,
                         agg_op_ptr, agg_op_length,
                         input_rows_ptr, input_rows_length,
                         next_partition_first_row_ptr, next_partition_first_row_length,
                         prev_partition_last_group_ptr, prev_partition_last_group_length,
                         prev_partition_last_row_ptr, prev_partition_last_row_length,
                         &output_rows, &output_rows_length));
  }

  jbyteArray ret = env->NewByteArray(output_rows_length);
  env->SetByteArrayRegion(ret, 0, output_rows_length, (jbyte *) output_rows);
  free(output_rows);

  env->ReleaseByteArrayElements(agg_op, (jbyte *) agg_op_ptr, 0);
  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);
  env->ReleaseByteArrayElements(
    next_partition_first_row, (jbyte *) next_partition_first_row_ptr, 0);
  env->ReleaseByteArrayElements(
    prev_partition_last_group, (jbyte *) prev_partition_last_group_ptr, 0);
  env->ReleaseByteArrayElements(
    prev_partition_last_row, (jbyte *) prev_partition_last_row_ptr, 0);

  return ret;
}

JNIEXPORT jbyteArray JNICALL
Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_EnclaveColumnSort(
    JNIEnv *env, jobject obj, jlong eid, jbyteArray sort_order, jint round, jbyteArray input_rows,
    jint r, jint s, jint partition_index) {
  (void)obj;
  jboolean if_copy;

  uint32_t sort_order_length = static_cast<uint32_t>(env->GetArrayLength(sort_order));
  uint8_t *sort_order_ptr = reinterpret_cast<uint8_t *>(
      env->GetByteArrayElements(sort_order, &if_copy));

  uint32_t input_length = static_cast<uint32_t>(env->GetArrayLength(input_rows));
  uint8_t *input_rows_ptr = reinterpret_cast<uint8_t *>(
      env->GetByteArrayElements(input_rows, &if_copy));

  uint8_t *output_buffer = (uint8_t *) malloc(sizeof(uint8_t) * r);
  size_t output_buffer_size;
  switch (round) {
    case 0:
      sgx_check("Column Sort Pad", ecall_column_sort_pad(
          eid, input_rows_ptr, input_length, r, &output_buffer, &output_buffer_size));
      break;
    case 5:
      sgx_check("Column Sort Filter", ecall_column_sort_filter(
          eid, input_rows_ptr, input_length, &output_buffer, &output_buffer_size));
      break;
    default:
      sgx_check("Column Sort", ecall_column_sort(
          eid, round, sort_order_ptr, sort_order_length, input_rows_ptr, input_length,
          partition_index, r, s, &output_buffer, &output_buffer_size));
      break;
  }

  jbyteArray ret = env->NewByteArray(output_buffer_size);
  env->SetByteArrayRegion(ret, 0, output_buffer_size, (jbyte *) output_buffer);

  free(output_buffer);

  return ret;
}
