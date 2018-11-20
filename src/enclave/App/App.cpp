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

#include <sgx_eid.h>     /* sgx_enclave_id_t */
#include <sgx_error.h>       /* sgx_status_t */
#include <sgx_uae_service.h>
#include <sgx_ukey_exchange.h>

#include "Enclave_u.h"
#include "service_provider.h"

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

/* Global EID shared by multiple threads */
sgx_enclave_id_t global_eid = 0;

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

  {
    SGX_SUCCESS,
    "SGX call success",
    NULL
  },
};

/* Check error conditions for loading enclave */
void print_error_message(sgx_status_t ret)
{
  size_t idx = 0;
  size_t ttl = sizeof sgx_errlist/sizeof sgx_errlist[0];

  for (idx = 0; idx < ttl; idx++) {
    if(ret == sgx_errlist[idx].err) {
      if(NULL != sgx_errlist[idx].sug)
        printf("Info: %s\n", sgx_errlist[idx].sug);
      printf("Error: %s\n", sgx_errlist[idx].msg);
      break;
    }
  }

  if (idx == ttl)
    printf("Error: Unexpected error occurred.\n");
}

void sgx_check_quiet(const char* message, sgx_status_t ret)
{
  if (ret != SGX_SUCCESS) {
    printf("%s failed\n", message);
    print_error_message(ret);
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
#define sgx_check(message, op) do {                     \
    printf("%s running...\n", message);                 \
    uint64_t t_ = 0;                                    \
    sgx_status_t ret_;                                  \
    {                                                   \
      scoped_timer timer_(&t_);                         \
      ret_ = op;                                        \
    }                                                   \
    double t_ms_ = ((double) t_) / 1000;                \
    if (ret_ != SGX_SUCCESS) {                          \
      printf("%s failed (%f ms)\n", message, t_ms_);    \
      print_error_message(ret_);                        \
    } else {                                            \
      printf("%s done (%f ms).\n", message, t_ms_);     \
    }                                                   \
  } while (0)
#else
#define sgx_check(message, op) sgx_check_quiet(message, op)
#endif

/* Initialize the enclave:
 *   Step 1: retrive the launch token saved by last transaction
 *   Step 2: call sgx_create_enclave to initialize an enclave instance
 *   Step 3: save the launch token if it is updated
 */
int initialize_enclave(void)
{
  char token_path[MAX_PATH] = {'\0'};
  sgx_launch_token_t token = {0};
  sgx_status_t ret = SGX_ERROR_UNEXPECTED;
  int updated = 0;

  /* Step 1: retrive the launch token saved by last transaction */
#ifdef _MSC_VER
  /* try to get the token saved in CSIDL_LOCAL_APPDATA */
  if (S_OK != SHGetFolderPathA(NULL, CSIDL_LOCAL_APPDATA, NULL, 0, token_path)) {
    strncpy_s(token_path, _countof(token_path), TOKEN_FILENAME, sizeof(TOKEN_FILENAME));
  } else {
    strncat_s(token_path, _countof(token_path), "\\" TOKEN_FILENAME, sizeof(TOKEN_FILENAME)+2);
  }

  /* open the token file */
  HANDLE token_handler = CreateFileA(token_path, GENERIC_READ|GENERIC_WRITE, FILE_SHARE_READ, NULL, OPEN_ALWAYS, NULL, NULL);
  if (token_handler == INVALID_HANDLE_VALUE) {
    printf("Warning: Failed to create/open the launch token file \"%s\".\n", token_path);
  } else {
    /* read the token from saved file */
    DWORD read_num = 0;
    ReadFile(token_handler, token, sizeof(sgx_launch_token_t), &read_num, NULL);
    if (read_num != 0 && read_num != sizeof(sgx_launch_token_t)) {
      /* if token is invalid, clear the buffer */
      memset(&token, 0x0, sizeof(sgx_launch_token_t));
      printf("Warning: Invalid launch token read from \"%s\".\n", token_path);
    }
  }
#else /* __GNUC__ */
  /* try to get the token saved in $HOME */
  const char *home_dir = getpwuid(getuid())->pw_dir;

  if (home_dir != NULL &&
      (strlen(home_dir)+strlen("/")+sizeof(TOKEN_FILENAME)+1) <= MAX_PATH) {
    /* compose the token path */
    strncpy(token_path, home_dir, strlen(home_dir));
    strncat(token_path, "/", strlen("/"));
    strncat(token_path, TOKEN_FILENAME, sizeof(TOKEN_FILENAME)+1);
  } else {
    /* if token path is too long or $HOME is NULL */
    strncpy(token_path, TOKEN_FILENAME, sizeof(TOKEN_FILENAME));
  }

  FILE *fp = fopen(token_path, "rb");
  if (fp == NULL && (fp = fopen(token_path, "wb")) == NULL) {
    printf("Warning: Failed to create/open the launch token file \"%s\".\n", token_path);
  }

  if (fp != NULL) {
    /* read the token from saved file */
    size_t read_num = fread(token, 1, sizeof(sgx_launch_token_t), fp);
    if (read_num != 0 && read_num != sizeof(sgx_launch_token_t)) {
      /* if token is invalid, clear the buffer */
      memset(&token, 0x0, sizeof(sgx_launch_token_t));
      printf("Warning: Invalid launch token read from \"%s\".\n", token_path);
    }
  }
#endif
  /* Step 2: call sgx_create_enclave to initialize an enclave instance */
  /* Debug Support: set 2nd parameter to 1 */
  ret = sgx_create_enclave(ENCLAVE_FILENAME, SGX_DEBUG_FLAG, &token, &updated, &global_eid, NULL);
  if (ret != SGX_SUCCESS) {
    print_error_message(ret);
#ifdef _MSC_VER
    if (token_handler != INVALID_HANDLE_VALUE)
      CloseHandle(token_handler);
#else
    if (fp != NULL) fclose(fp);
#endif
    return -1;
  }

  /* Step 3: save the launch token if it is updated */
#ifdef _MSC_VER
  if (updated == FALSE || token_handler == INVALID_HANDLE_VALUE) {
    /* if the token is not updated, or file handler is invalid, do not perform saving */
    if (token_handler != INVALID_HANDLE_VALUE)
      CloseHandle(token_handler);
    return 0;
  }

  /* flush the file cache */
  FlushFileBuffers(token_handler);
  /* set access offset to the begin of the file */
  SetFilePointer(token_handler, 0, NULL, FILE_BEGIN);

  /* write back the token */
  DWORD write_num = 0;
  WriteFile(token_handler, token, sizeof(sgx_launch_token_t), &write_num, NULL);
  if (write_num != sizeof(sgx_launch_token_t))
    printf("Warning: Failed to save launch token to \"%s\".\n", token_path);
  CloseHandle(token_handler);
#else /* __GNUC__ */
  if (updated == FALSE || fp == NULL) {
    /* if the token is not updated, or file handler is invalid, do not perform saving */
    if (fp != NULL) fclose(fp);
    return 0;
  }

  /* reopen the file with write capablity */
  fp = freopen(token_path, "wb", fp);
  if (fp == NULL) return 0;
  size_t write_num = fwrite(token, 1, sizeof(sgx_launch_token_t), fp);
  if (write_num != sizeof(sgx_launch_token_t))
    printf("Warning: Failed to save launch token to \"%s\".\n", token_path);
  fclose(fp);
#endif
  return 0;
}

/* OCall functions */
void ocall_print_string(const char *str)
{
  /* Proxy/Bridge will check the length and null-terminate
   * the input string to prevent buffer overflow.
   */
  printf("%s", str);
  fflush(stdout);
}

void ocall_malloc(size_t size, uint8_t **ret) {
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

#if defined(_MSC_VER)
/* query and enable SGX device*/
int query_sgx_status()
{
  sgx_device_status_t sgx_device_status;
  sgx_status_t sgx_ret = sgx_enable_device(&sgx_device_status);
  if (sgx_ret != SGX_SUCCESS) {
    printf("Failed to get SGX device status.\n");
    return -1;
  }
  else {
    switch (sgx_device_status) {
    case SGX_ENABLED:
      return 0;
    case SGX_DISABLED_REBOOT_REQUIRED:
      printf("SGX device has been enabled. Please reboot your machine.\n");
      return -1;
    case SGX_DISABLED_LEGACY_OS:
      printf("SGX device can't be enabled on an OS that doesn't support EFI interface.\n");
      return -1;
    case SGX_DISABLED:
      printf("SGX device not found.\n");
      return -1;
    default:
      printf("Unexpected error.\n");
      return -1;
    }
  }
}
#endif


JNIEXPORT jlong JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_StartEnclave(
  JNIEnv *env, jobject obj, jstring library_path) {
  (void)env;
  (void)obj;

  env->GetJavaVM(&jvm);

  sgx_enclave_id_t eid;
  sgx_launch_token_t token = {0};
  int updated = 0;

  const char *library_path_str = env->GetStringUTFChars(library_path, nullptr);
  sgx_check("StartEnclave",
            sgx_create_enclave(
              library_path_str, SGX_DEBUG_FLAG, &token, &updated, &eid, nullptr));
  env->ReleaseStringUTFChars(library_path, library_path_str);

  return eid;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation0(
  JNIEnv *env, jobject obj) {

  (void)env;
  (void)obj;

  // in the first step of the remote attestation, generate message 1 to send to the client
  int ret = 0;

  // Preparation for remote attestation by configuring extended epid group id
  // This is Intel's group signature scheme for trusted hardware
  // It keeps the machine anonymous while allowing the client to use a single public verification key to verify

  uint32_t extended_epid_group_id = 0;
  ret = sgx_get_extended_epid_group_id(&extended_epid_group_id);
  if (SGX_SUCCESS != (sgx_status_t)ret) {
    fprintf(stdout, "\nError, call sgx_get_extended_epid_group_id fail [%s].", __FUNCTION__);
    jbyteArray array_ret = env->NewByteArray(0);
    return array_ret;
  }

#ifdef DEBUG
  fprintf(stdout, "\nCall sgx_get_extended_epid_group_id success.");
#endif

  // The ISV application sends msg0 to the SP.
  // The ISV decides whether to support this extended epid group id.
#ifdef DEBUG
  fprintf(stdout, "\nSending msg0 to remote attestation service provider.\n");
#endif

  jbyteArray array_ret = env->NewByteArray(sizeof(uint32_t));
  env->SetByteArrayRegion(array_ret, 0, sizeof(uint32_t), (jbyte *) &extended_epid_group_id);

  return array_ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation1(
    JNIEnv *env, jobject obj,
    jlong eid) {

  (void)env;
  (void)obj;
  (void)eid;

  // Remote attestation will be initiated when the ISV server challenges the ISV
  // app or if the ISV app detects it doesn't have the credentials
  // (shared secret) from a previous attestation required for secure
  // communication with the server.

  int ret = 0;
  int enclave_lost_retry_time = 2;
  sgx_status_t status;

  // Ideally, this check would be around the full attestation flow.
  do {
    ret = ecall_enclave_init_ra(eid,
                                &status,
                                false,
                                &context);
  } while (SGX_ERROR_ENCLAVE_LOST == ret && enclave_lost_retry_time--);

  if (status != SGX_SUCCESS) {
    printf("[RemoteAttestation1] enclave_init_ra's status is %u\n", (uint32_t) status);
    std::exit(1);
  }

  uint8_t *msg1 = (uint8_t *) malloc(sizeof(sgx_ra_msg1_t));

#ifdef DEBUG
  printf("[RemoteAttestation1] context is %u, eid: %u\n", (uint32_t) context, (uint32_t) eid);
#endif

  ret = sgx_ra_get_msg1(context, eid, sgx_ra_get_ga, (sgx_ra_msg1_t*) msg1);

  if(SGX_SUCCESS != ret) {
    ret = -1;
    fprintf(stdout, "\nError, call sgx_ra_get_msg1 fail [%s].", __FUNCTION__);
    jbyteArray array_ret = env->NewByteArray(0);
    return array_ret;
  } else {
#ifdef DEBUG
    fprintf(stdout, "\nCall sgx_ra_get_msg1 success.\n");
    fprintf(stdout, "\nMSG1 body generated -\n");
    PRINT_BYTE_ARRAY(stdout, msg1, sizeof(sgx_ra_msg1_t));
#endif
  }

  // The ISV application sends msg1 to the SP to get msg2,
  // msg2 needs to be freed when no longer needed.
  // The ISV decides whether to use linkable or unlinkable signatures.
#ifdef DEBUG
  fprintf(stdout, "\nSending msg1 to remote attestation service provider."
          "Expecting msg2 back.\n");
#endif

  jbyteArray array_ret = env->NewByteArray(sizeof(sgx_ra_msg1_t));
  env->SetByteArrayRegion(array_ret, 0, sizeof(sgx_ra_msg1_t), (jbyte *) msg1);

  free(msg1);

  return array_ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation2(
    JNIEnv *env, jobject obj,
    jlong eid,
    jbyteArray msg2_input) {

  (void)env;
  (void)obj;

  int ret = 0;
  //sgx_ra_context_t context = INT_MAX;

  (void)ret;
  (void)eid;
  // Successfully sent msg1 and received a msg2 back.
  // Time now to check msg2.

  //uint32_t input_len = (uint32_t) env->GetArrayLength(msg2_input);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(msg2_input, &if_copy);
  sgx_ra_msg2_t* p_msg2_body = (sgx_ra_msg2_t*)(ptr);

#ifdef DEBUG
  printf("Printing p_msg2_body\n");
  PRINT_BYTE_ARRAY(stdout, p_msg2_body, sizeof(sgx_ra_msg2_t));
#endif

  uint32_t msg3_size = 0;
  sgx_ra_msg3_t *msg3 = NULL;

  // The ISV app now calls uKE sgx_ra_proc_msg2,
  // The ISV app is responsible for freeing the returned p_msg3!
#ifdef DEBUG
  printf("[RemoteAttestation2] context is %u, eid: %u\n", (uint32_t) context, (uint32_t) eid);
#endif
  ret = sgx_ra_proc_msg2(context,
                         eid,
                         sgx_ra_proc_msg2_trusted,
                         sgx_ra_get_msg3_trusted,
                         p_msg2_body,
                         sizeof(sgx_ra_msg2_t),
                         &msg3,
                         &msg3_size);

  if (!msg3) {
    fprintf(stdout, "\nError, call sgx_ra_proc_msg2 fail. msg3 = 0x%p [%s].\n", msg3, __FUNCTION__);
    print_error_message((sgx_status_t) ret);
    jbyteArray array_ret = env->NewByteArray(0);
    return array_ret;
  }

  if(SGX_SUCCESS != (sgx_status_t)ret) {
    fprintf(stdout, "\nError, call sgx_ra_proc_msg2 fail. "
            "ret = 0x%08x [%s].\n", ret, __FUNCTION__);
    print_error_message((sgx_status_t) ret);
    jbyteArray array_ret = env->NewByteArray(0);
    return array_ret;
  } else {
#ifdef DEBUG
    fprintf(stdout, "\nCall sgx_ra_proc_msg2 success.\n");
#endif
  }

  jbyteArray array_ret = env->NewByteArray(msg3_size);
  env->SetByteArrayRegion(array_ret, 0, msg3_size, (jbyte *) msg3);

  free(msg3);
  return array_ret;
}


JNIEXPORT void JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation3(
    JNIEnv *env, jobject obj,
    jlong eid,
    jbyteArray att_result_input) {

  (void)env;
  (void)obj;

#ifdef DEBUG
  printf("RemoteAttestation3 called\n");
#endif

  sgx_status_t status = SGX_SUCCESS;
  //uint32_t input_len = (uint32_t) env->GetArrayLength(att_result_input);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(att_result_input, &if_copy);

  ra_samp_response_header_t *att_result_full = (ra_samp_response_header_t *)(ptr);
  sample_ra_att_result_msg_t *att_result = (sample_ra_att_result_msg_t *) att_result_full->body;

#ifdef DEBUG
  printf("[RemoteAttestation3] att_result's size is %u\n", att_result_full->size);
#endif

  // Check the MAC using MK on the attestation result message.
  // The format of the attestation result message is ISV specific.
  // This is a simple form for demonstration. In a real product,
  // the ISV may want to communicate more information.
  int ret = 0;
  ret = ecall_verify_att_result_mac(eid,
                                    &status,
                                    context,
                                    (uint8_t*)&att_result->platform_info_blob,
                                    sizeof(ias_platform_info_blob_t),
                                    (uint8_t*)&att_result->mac,
                                    sizeof(sgx_mac_t));

  if((SGX_SUCCESS != ret) || (SGX_SUCCESS != status)) {
    fprintf(stdout, "\nError: INTEGRITY FAILED - attestation result message MK based cmac failed in [%s], status is %u", __FUNCTION__, (uint32_t) status);
    return ;
  }

  bool attestation_passed = true;
  // Check the attestation result for pass or fail.
  // Whether attestation passes or fails is a decision made by the ISV Server.
  // When the ISV server decides to trust the enclave, then it will return success.
  // When the ISV server decided to not trust the enclave, then it will return failure.
  if (0 != att_result_full->status[0] || 0 != att_result_full->status[1]) {
    fprintf(stdout, "\nError, attestation result message MK based cmac "
            "failed in [%s].", __FUNCTION__);
    attestation_passed = false;
  }

  // The attestation result message should contain a field for the Platform
  // Info Blob (PIB).  The PIB is returned by attestation server in the attestation report.
  // It is not returned in all cases, but when it is, the ISV app
  // should pass it to the blob analysis API called sgx_report_attestation_status()
  // along with the trust decision from the ISV server.
  // The ISV application will take action based on the update_info.
  // returned in update_info by the API.
  // This call is stubbed out for the sample.
  //
  // sgx_update_info_bit_t update_info;
  // ret = sgx_report_attestation_status(
  //     &p_att_result_msg_body->platform_info_blob,
  //     attestation_passed ? 0 : 1, &update_info);

  // Get the shared secret sent by the server using SK (if attestation
  // passed)
#ifdef DEBUG
  printf("[RemoteAttestation3] %u\n", attestation_passed);
#endif
  if (attestation_passed) {
    ret = ecall_put_secret_data(eid,
                                &status,
                                context,
                                att_result->secret.payload,
                                att_result->secret.payload_size,
                                att_result->secret.payload_tag);

    if((SGX_SUCCESS != ret)  || (SGX_SUCCESS != status)) {
      fprintf(stdout, "\nError, attestation result message secret "
              "using SK based AESGCM failed in [%s]. ret = "
              "0x%0x. status = 0x%0x", __FUNCTION__, ret,
              status);
      return ;
    }
  }

  fprintf(stdout, "\nSecret successfully received from server.");
  fprintf(stdout, "\nRemote attestation success!\n");

#ifdef DEBUG
  fprintf(stdout, "Destroying the key exchange context\n");
#endif
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

  uint8_t *output_rows;
  size_t output_rows_length;

  sgx_check("Project",
            ecall_project(
              eid,
              project_list_ptr, project_list_length,
              input_rows_ptr, input_rows_length,
              &output_rows, &output_rows_length));

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

  uint8_t *output_rows;
  size_t output_rows_length;

  sgx_check("Filter",
            ecall_filter(
              eid,
              condition_ptr, condition_length,
              input_rows_ptr, input_rows_length,
              &output_rows, &output_rows_length));

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
  jbyte *ptr = env->GetByteArrayElements(plaintext, &if_copy);

  uint8_t *plaintext_ptr = (uint8_t *) ptr;

  const jsize clength = plength + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
  jbyteArray ciphertext = env->NewByteArray(clength);

  uint8_t *ciphertext_copy = new uint8_t[clength];

  sgx_check_quiet(
    "Encrypt", ecall_encrypt(eid, plaintext_ptr, plength, ciphertext_copy, (uint32_t) clength));

  env->SetByteArrayRegion(ciphertext, 0, clength, (jbyte *) ciphertext_copy);

  env->ReleaseByteArrayElements(plaintext, ptr, 0);

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

  uint8_t *output_rows;
  size_t output_rows_length;

  sgx_check("Sample",
            ecall_sample(
              eid,
              input_rows_ptr, input_rows_length,
              &output_rows, &output_rows_length));

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

  uint8_t *output_rows;
  size_t output_rows_length;

  sgx_check("Find Range Bounds",
            ecall_find_range_bounds(
              eid,
              sort_order_ptr, sort_order_length,
              num_partitions,
              input_rows_ptr, input_rows_length,
              &output_rows, &output_rows_length));

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

  sgx_check("Partition For Sort",
            ecall_partition_for_sort(
              eid,
              sort_order_ptr, sort_order_length,
              num_partitions,
              input_rows_ptr, input_rows_length,
              boundary_rows_ptr, boundary_rows_length,
              output_partitions, output_partition_lengths));

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

  uint8_t *output_rows;
  size_t output_rows_length;

  sgx_check("External non-oblivious sort",
            ecall_external_sort(eid,
                                sort_order_ptr, sort_order_length,
                                input_rows_ptr, input_rows_length,
                                &output_rows, &output_rows_length));

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

  uint8_t *output_rows;
  size_t output_rows_length;

  sgx_check("Scan Collect Last Primary",
            ecall_scan_collect_last_primary(
              eid,
              join_expr_ptr, join_expr_length,
              input_rows_ptr, input_rows_length,
              &output_rows, &output_rows_length));

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

  uint8_t *output_rows;
  size_t output_rows_length;

  sgx_check("Non-oblivious SortMergeJoin",
            ecall_non_oblivious_sort_merge_join(
              eid,
              join_expr_ptr, join_expr_length,
              input_rows_ptr, input_rows_length,
              join_row_ptr, join_row_length,
              &output_rows, &output_rows_length));
  
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

  uint8_t *first_row;
  size_t first_row_length;

  uint8_t *last_group;
  size_t last_group_length;

  uint8_t *last_row;
  size_t last_row_length;

  sgx_check("Non-Oblivious Aggregate Step 1",
            ecall_non_oblivious_aggregate_step1(
              eid,
              agg_op_ptr, agg_op_length,
              input_rows_ptr, input_rows_length,
              &first_row, &first_row_length,
              &last_group, &last_group_length,
              &last_row, &last_row_length));

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

  uint8_t *output_rows;
  size_t output_rows_length;

  sgx_check("Non-Oblivious Aggregate Step 2",
            ecall_non_oblivious_aggregate_step2(
              eid,
              agg_op_ptr, agg_op_length,
              input_rows_ptr, input_rows_length,
              next_partition_first_row_ptr, next_partition_first_row_length,
              prev_partition_last_group_ptr, prev_partition_last_group_length,
              prev_partition_last_row_ptr, prev_partition_last_row_length,
              &output_rows, &output_rows_length));

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

/* application entry */
//SGX_CDECL
int SGX_CDECL main(int argc, char *argv[])
{
  (void)(argc);
  (void)(argv);

#if defined(_MSC_VER)
  if (query_sgx_status() < 0) {
    /* either SGX is disabled, or a reboot is required to enable SGX */
    printf("Enter a character before exit ...\n");
    getchar();
    return -1;
  }
#endif

  /* Initialize the enclave */
  if(initialize_enclave() < 0){
    printf("Enter a character before exit ...\n");
    getchar();
    return -1;
  }

  /* Destroy the enclave */
  sgx_destroy_enclave(global_eid);

  printf("Info: SampleEnclave successfully returned.\n");

  return 0;
}
