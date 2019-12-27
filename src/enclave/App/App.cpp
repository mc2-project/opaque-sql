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

//#include <sgx_eid.h>     /* sgx_enclave_id_t */
//#include <sgx_error.h>       /* sgx_status_t */
//#include <sgx_uae_service.h>
//#include <sgx_ukey_exchange.h>
#include "common.h"

#include <openenclave/host.h>

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

JavaVM* jvm;

typedef struct _oe_errlist_t {
  oe_result_t err;
  const char *msg;
  const char *sug; /* Suggestion */
} oe_errlist_t;

/* Error codes defined by oe_result_t */
static oe_errlist_t oe_errlist[] = {
    {
      OE_FAILURE,
      "The function failed (without a more specific error code)",
      NULL
    },
    {
      OE_BUFFER_TOO_SMALL,
      "One or more output buffer function parameters is too small.",
      NULL
    },
    {
      OE_INVALID_PARAMETER,
      "The function failed (without a more specific error code)",
      NULL
    },
    {
      OE_REENTRANT_ECALL,
      "One or more output buffer function parameters is too small.",
      NULL
    },
    {
      OE_OUT_OF_MEMORY,
      "The function is out of memory. This usually occurs when **malloc** or a related function returns null.",
      NULL
    },
    {
      OE_OUT_OF_THREADS,
      "The function is unable to bind the current host thread to an enclave thread. This occurs when the host performs an **ECALL** while all enclave threads are in use.",
      NULL
    },
    {
      OE_UNEXPECTED,
      "The function encountered an unexpected failure.",
      NULL
    },
    {
      OE_VERIFY_FAILED,
      "A cryptographic verification failed. Examples include: \n- enclave quote verification\n-public key signature verification\n- certificate chain verification",
      NULL
    },
    {
      OE_NOT_FOUND,
      "The function failed to find a resource. Examples of resources include files, directories, and functions (ECALL/OCALL), container elements.",
      NULL
    },
    {
      OE_INTEGER_OVERFLOW,
      "The function encountered an overflow in an integer operation, which can occur in arithmetic operations and cast operations.",
      NULL
    },
    {
      OE_PUBLIC_KEY_NOT_FOUND,
      "The certificate does not contain a public key.",
      NULL
    },
    {
      OE_OUT_OF_BOUNDS,
      "An integer index is outside the expected range. For example, an array index is greater than or equal to the array size.",
      NULL
    },
    {
      OE_OVERLAPPED_COPY,
      "The function prevented an attempt to perform an overlapped copy, where the source and destination buffers are overlapping.",
      NULL
    },
    {
      OE_CONSTRAINT_FAILED,
     "The function detected a constraint failure. A constraint restricts the value of a field, parameter, or variable. For example, the value of **day_of_the_week** must be between 1 and 7 inclusive.",
      NULL
    },
    {
      OE_IOCTL_FAILED,
      "An **IOCTL** operation failed. Open Enclave uses **IOCTL** operations to communicate with the Intel SGX driver.",
      NULL
    },
    {
      OE_UNSUPPORTED,
      "The given operation is unsupported, usually by a particular platform or environment.",
      NULL
    },
    {
      OE_READ_FAILED,
      "The function failed to read data from a device (such as a socket, or file).",
      NULL
    },
    {
      OE_SERVICE_UNAVAILABLE,
      "A software service is unavailable (such as the AESM service).",
      NULL
    },
    {
      OE_ENCLAVE_ABORTING,
      "The operation cannot be completed because the enclave is aborting.",
      NULL
    },
    {
      OE_ENCLAVE_ABORTED,
      "The operation cannot be completed because the enclave has already aborted.",
      NULL
    },
    {
      OE_PLATFORM_ERROR,
      "The underlying platform or hardware returned an error. For example, an SGX user-mode instruction failed.",
      NULL
    },
    {
      OE_INVALID_CPUSVN,
      "The given **CPUSVN** value is invalid. An SGX user-mode instruction may return this error.",
      NULL
    },
    {
      OE_INVALID_ISVSVN,
      "The given **ISVSNV** value is invalid. An SGX user-mode instruction may return this error.",
      NULL
    },
    {
      OE_INVALID_KEYNAME,
      "The given **key name** is invalid. An SGX user-mode instruction may return this error.",
      NULL
    },
    {
      OE_DEBUG_DOWNGRADE,
      "Attempted to create a debug enclave with an enclave image that does not allow it.",
      NULL
    },
    {
      OE_REPORT_PARSE_ERROR,
      "Failed to parse an enclave report.",
      NULL
    },
    {
      OE_MISSING_CERTIFICATE_CHAIN,
      "The certificate chain is not available or missing.",
      NULL
    },
    {
      OE_BUSY,
      "An operation cannot be performed beause the resource is busy. For example, a non-recursive mutex cannot be locked because it is already locked.",
      NULL
    },
    {
      OE_NOT_OWNER,
      "An operation cannot be performed because the requestor is not the owner of the resource. For example, a thread cannot lock a mutex because it is not the thread that acquired the mutex.",
      NULL
    },
    {
      OE_INVALID_SGX_CERTIFICATE_EXTENSIONS,
      "The certificate does not contain the expected SGX extensions.",
      NULL
    },
    {
      OE_MEMORY_LEAK,
      "A memory leak was detected during enclave termination.",
      NULL
    },
    {
      OE_BAD_ALIGNMENT,
      "The data is improperly aligned for the given operation. This may occur when an output buffer parameter is not suitably aligned for the data it will receive.",
      NULL
    },
    {
      OE_JSON_INFO_PARSE_ERROR,
      "Failed to parse the trusted computing base (TCB) revocation data or the QE Identity data for the enclave.",
      NULL
    },
    {
      OE_TCB_LEVEL_INVALID,
      "The level of the trusted computing base (TCB) is not up to date for report verification.",
      NULL
    },
    {
      OE_QUOTE_PROVIDER_LOAD_ERROR,
      "Failed to load the quote provider library used for quote generation and attestation.",
      NULL
    },
    {
      OE_QUOTE_PROVIDER_CALL_ERROR,
      "A call to the quote provider failed.",
      NULL
    },
    {
      OE_INVALID_REVOCATION_INFO,
      "The certificate revocation data for attesting the trusted computing base (TCB) is invalid for this enclave.",
      NULL
    },
    {
      OE_INVALID_UTC_DATE_TIME,
      "The given UTC date-time string or structure is invalid. This occurs when (1) an element is out of range (year, month, day, hours, minutes, seconds), or (2) the UTC date-time string is malformed.",
      NULL
    },
    {
      OE_INVALID_QE_IDENTITY_INFO,
      "The QE identity data is invalid.",
      NULL
    },
    {
      OE_UNSUPPORTED_ENCLAVE_IMAGE,
      "The enclave image contains unsupported constructs.",
      NULL
    }
    //,
    // {
    //   OE_VERIFY_CRL_EXPIRED,
    //   "The CRL for a certificate has expired.",
    //   NULL
    // },
    // {
    //   OE_VERIFY_CRL_MISSING,
    //   "The CRL for a certificate could not be found.",
    //   NULL
    // },
    // {
    //   OE_VERIFY_REVOKED,
    //   "The certificate or signature has been revoked.",
    //   NULL
    // },
    // {
    //   OE_VERIFY_FAILED_TO_FIND_VALIDITY_PERIOD,
    //   "Could not find a valid validity period.",
    //   NULL
    // },
    // {
    //   OE_CRYPTO_ERROR,
    //   "An underlying crypto provider returned an error.",
    //   NULL
    // },
    // {
    //   OE_INCORRECT_REPORT_SIZE,
    //   "OE report size does not match the expected size.",
    //   NULL
    // },
    // {
    //   OE_QUOTE_VERIFICATION_ERROR,
    //   "Quote verification error.",
    //   NULL
    // },
    // {
    //   OE_QUOTE_ENCLAVE_IDENTITY_VERIFICATION_FAILED,
    //   "Quote enclave identity verification failed.",
    //   NULL
    // },
    // {
    //   OE_QUOTE_ENCLAVE_IDENTITY_UNIQUEID_MISMATCH,
    //   "Unique id of the quoting enclave does not match expected value.",
    //   NULL
    // },
    // {
    //   QE_QUOTE_ENCLAVE_IDENTITY_PRODUCTID_MISMATCH,
    //   "Product id of the quoting enclave does not match expected value.",
    //   NULL
    // },
    // {
    //   OE_VERIFY_FAILED_AES_CMAC_MISMATCH,
    //   "AES CMAC of the report does not match the expected value.",
    //   NULL
    // }
    //,
    // {
    //   OE_CONTEXT_SWITCHLESS_OCALL_MISSED,
    //   "Failed to post a switchless call to host workers",
    //   NULL
    // },
    // {
    //   OE_THREAD_CREATE_ERROR,
    //   "Thread creation failed",
    //   NULL
    // },
    // {
    //   OE_THREAD_JOIN_ERROR,
    //   "Thread join failed.",
    //   NULL
    // }
};

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
  //int updated = 0;

  uint32_t flags = 0;
  const char* sgx_mode_env = std::getenv("SGX_MODE");
  if (strcmp(sgx_mode_env, "HW") != 0){
    flags |= OE_ENCLAVE_FLAG_SIMULATE;
  }

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

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation1(
  JNIEnv *env, jobject obj, jlong eid) {
  (void)obj;
  (void)eid;

  uint8_t* msg1 = NULL;
  size_t msg1_size = 0;

  oe_check_and_time("Remote Attestation Step 1.2",
                     ecall_oe_proc_msg1((oe_enclave_t*)eid,
                                        &msg1,
                                        &msg1_size));

  // Allocate memory
  jbyteArray msg1_bytes = env->NewByteArray(msg1_size);
  env->SetByteArrayRegion(msg1_bytes, 0, msg1_size, reinterpret_cast<jbyte *>(msg1));

  return msg1_bytes;
}

JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation3(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray msg4_input) {
  (void)obj;

  jboolean if_copy = false;
  jbyte *msg4_bytes = env->GetByteArrayElements(msg4_input, &if_copy);
  uint32_t msg4_size = static_cast<uint32_t>(env->GetArrayLength(msg4_input));

  oe_check_and_time("Remote Attestation Step 3",
                     ecall_ra_proc_msg4((oe_enclave_t*)eid,
                                        reinterpret_cast<uint8_t *>(msg4_bytes),
                                        msg4_size));

  env->ReleaseByteArrayElements(msg4_input, msg4_bytes, 0);

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
    oe_check_and_time("Non-Oblivious Aggregate Step 1",
                       ecall_non_oblivious_aggregate_step1(
                         (oe_enclave_t*)eid,
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
    oe_check_and_time("Non-Oblivious Aggregate Step 2",
                       ecall_non_oblivious_aggregate_step2(
                         (oe_enclave_t*)eid,
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
