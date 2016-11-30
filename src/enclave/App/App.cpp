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

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstdlib>
#include <sys/time.h>
#include <time.h>
#include <vector>

#ifdef _MSC_VER
# include <Shlobj.h>
#else
# include <unistd.h>
# include <pwd.h>
# define MAX_PATH FILENAME_MAX
#endif

#include "sgx_trts.h"
#include "sgx_urts.h"
#include "App.h"
#include "Enclave_u.h"
#include "sgx_tcrypto.h"
#include "sgx_ukey_exchange.h"
#include "define.h"
#include "common.h"

#include "SGXEnclave.h"
#include "sample_messages.h"    // this is for debugging remote attestation only
#include "service_provider.h"

static sgx_ra_context_t context = INT_MAX;

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

uint8_t* msg1_samples[] = { msg1_sample1, msg1_sample2 };
uint8_t* msg2_samples[] = { msg2_sample1, msg2_sample2 };
uint8_t* msg3_samples[] = { msg3_sample1, msg3_sample2 };
uint8_t* attestation_msg_samples[] = { attestation_msg_sample1, attestation_msg_sample2};


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
  JNIEnv *env, jobject obj) {
  (void)env;
  (void)obj;

  sgx_enclave_id_t eid;
  sgx_launch_token_t token = {0};
  int updated = 0;

  sgx_check("StartEnclave",
            sgx_create_enclave(
              std::getenv("LIBENCLAVESIGNED_PATH"), SGX_DEBUG_FLAG, &token, &updated, &eid, NULL));

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
    assert(false);
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
  printf("[RemoteAttestation2] context is %u, eid: %u\n", (uint32_t) context, (uint32_t) eid);
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

  printf("[RemoteAttestation3] att_result's size is %u\n", att_result_full->size);

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

// read a chunk of buffer from the scala program

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Project(
  JNIEnv *env, jobject obj, jlong eid, jint index, jint num_part,
  jint op_code, jbyteArray input_rows, jint num_rows) {
  (void)obj;

  jboolean if_copy;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t output_rows_length = block_size_upper_bound(num_rows);
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);

  uint32_t actual_output_rows_length = 0;

  sgx_check("Project",
            ecall_project(
              eid, index, num_part,
              op_code, input_rows_ptr, input_rows_length, num_rows, output_rows,
              output_rows_length, &actual_output_rows_length));

  jbyteArray ret = env->NewByteArray(actual_output_rows_length);
  env->SetByteArrayRegion(ret, 0, actual_output_rows_length, (jbyte *) output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Filter(
  JNIEnv *env, jobject obj, jlong eid,
  jint index, jint num_part,
  jint op_code, jbyteArray input_rows, jint num_rows,
  jobject num_output_rows_obj) {
  (void)obj;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  jboolean if_copy;
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t output_rows_length = block_size_upper_bound(num_rows);
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);

  uint32_t actual_output_rows_length = 0;
  uint32_t num_output_rows = 0;

  sgx_check("Filter",
            ecall_filter(
              eid,
              index, num_part,
              op_code, input_rows_ptr, input_rows_length, num_rows, output_rows,
              output_rows_length, &actual_output_rows_length, &num_output_rows));

  jbyteArray ret = env->NewByteArray(actual_output_rows_length);
  env->SetByteArrayRegion(ret, 0, actual_output_rows_length, (jbyte *) output_rows);

  jclass num_output_rows_class = env->GetObjectClass(num_output_rows_obj);
  jfieldID field_id = env->GetFieldID(num_output_rows_class, "value", "I");
  env->SetIntField(num_output_rows_obj, field_id, num_output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  return ret;
}


JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Encrypt(
  JNIEnv *env,
  jobject obj,
  jlong eid,
  jbyteArray plaintext) {
  (void)obj;

  uint32_t plength = (uint32_t) env->GetArrayLength(plaintext);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(plaintext, &if_copy);

  uint8_t *plaintext_ptr = (uint8_t *) ptr;

  const jsize clength = plength + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
  jbyteArray ciphertext = env->NewByteArray(clength);

  uint8_t ciphertext_copy[2048];

  sgx_check_quiet(
    "Encrypt", ecall_encrypt(eid, plaintext_ptr, plength, ciphertext_copy, (uint32_t) clength));

  env->SetByteArrayRegion(ciphertext, 0, clength, (jbyte *) ciphertext_copy);

  env->ReleaseByteArrayElements(plaintext, ptr, 0);

  return ciphertext;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Decrypt(
  JNIEnv *env,
  jobject obj,
  jlong eid,
  jbyteArray ciphertext) {
  (void)obj;

  uint32_t clength = (uint32_t) env->GetArrayLength(ciphertext);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(ciphertext, &if_copy);

  uint8_t *ciphertext_ptr = (uint8_t *) ptr;

  const jsize plength = clength - SGX_AESGCM_IV_SIZE - SGX_AESGCM_MAC_SIZE;
  jbyteArray plaintext = env->NewByteArray(plength);

  uint8_t plaintext_copy[2048];

  sgx_check_quiet(
    "Decrypt", ecall_decrypt(eid, ciphertext_ptr, clength, plaintext_copy, (uint32_t) plength));

  env->SetByteArrayRegion(plaintext, 0, plength, (jbyte *) plaintext_copy);

  env->ReleaseByteArrayElements(ciphertext, ptr, 0);

  return plaintext;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_EncryptAttribute(
  JNIEnv *env,
  jobject obj,
  jlong eid,
  jbyteArray plaintext) {
  (void)obj;

  uint32_t plength = (uint32_t) env->GetArrayLength(plaintext);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(plaintext, &if_copy);

  uint8_t *plaintext_ptr = (uint8_t *) ptr;

  uint32_t ciphertext_length = 4 + ENC_HEADER_SIZE + HEADER_SIZE + ATTRIBUTE_UPPER_BOUND;
  uint8_t *ciphertext_copy = (uint8_t *) malloc(ciphertext_length);

  uint32_t actual_size = 0;

  sgx_check_quiet(
    "EncryptAttribute",
    ecall_encrypt_attribute(eid, plaintext_ptr, plength,
                            ciphertext_copy, (uint32_t) ciphertext_length,
                            &actual_size));

  jbyteArray ciphertext = env->NewByteArray(actual_size - 4);
  env->SetByteArrayRegion(ciphertext, 0, actual_size - 4, (jbyte *) (ciphertext_copy + 4));

  env->ReleaseByteArrayElements(plaintext, ptr, 0);

  free(ciphertext_copy);

  return ciphertext;
}

JNIEXPORT void JNICALL SGX_CDECL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Test(JNIEnv *env, jobject obj, jlong eid) {
  (void)env;
  (void)obj;
  (void)eid;
  printf("Test!\n");
}

/**
 * Divide the input rows into a number of buffers of bounded size. The input will be divided at
 * block boundaries. The buffers and their respective sizes will be written into buffer_list and
 * buffers_num_rows.
 */
uint32_t split_rows(uint8_t *input, uint32_t input_len, uint32_t num_rows,
                    std::vector<uint8_t *> &buffer_list, std::vector<uint32_t> &buffers_num_rows,
                    uint32_t *row_upper_bound) {
  (void)num_rows;

  BlockReader r(input, input_len);
  uint8_t *cur_block;
  uint32_t cur_block_size;
  uint32_t cur_block_num_rows;
  uint32_t total_rows = 0;
  r.read(&cur_block, &cur_block_size, &cur_block_num_rows, row_upper_bound);
  while (cur_block != NULL) {
    uint8_t *cur_buffer = cur_block;
    uint32_t cur_buffer_size = cur_block_size;
    uint32_t cur_buffer_num_rows = cur_block_num_rows;
    uint32_t cur_row_upper_bound;

    r.read(&cur_block, &cur_block_size, &cur_block_num_rows, &cur_row_upper_bound);
    while (cur_buffer_size + cur_block_size <= MAX_SORT_BUFFER && cur_block != NULL) {
      cur_buffer_size += cur_block_size;
      cur_buffer_num_rows += cur_block_num_rows;
      *row_upper_bound =
        cur_row_upper_bound > *row_upper_bound ? cur_row_upper_bound : *row_upper_bound;

      r.read(&cur_block, &cur_block_size, &cur_block_num_rows, &cur_row_upper_bound);
    }

    buffer_list.push_back(cur_buffer);
    buffers_num_rows.push_back(cur_buffer_num_rows);
    total_rows += cur_buffer_num_rows;
    debug("split_rows: Buffer %lu: %d bytes, %d rows\n",
          buffer_list.size(), cur_buffer_size, cur_buffer_num_rows);
  }
  // Sentinel pointer to the end of the input
  buffer_list.push_back(input + input_len);

  perf("split_rows: Input (%d bytes, %d rows) split into %lu buffers, row upper bound %d\n",
       input_len, num_rows, buffer_list.size() - 1, *row_upper_bound);

  return total_rows;
}

// the op_code allows the internal sort code to decide which comparator to use
// assume that the elements are of equal size!
JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ObliviousSort(JNIEnv *env,
                                                                                jobject obj,
                                                                                jlong eid,
                                                                                jint op_code,
                                                                                jbyteArray input,
                                                                                jint offset,
                                                                                jint num_items) {
  (void)obj;
  (void)offset;

  uint32_t input_len = (uint32_t) env->GetArrayLength(input);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(input, &if_copy);

  uint8_t *input_copy = (uint8_t *) malloc(input_len);

  for (uint32_t i = 0; i < input_len; i++) {
    input_copy[i] = *(ptr + i);
  }

  std::vector<uint8_t *> buffer_list;
  std::vector<uint32_t> num_rows;
  uint32_t row_upper_bound = 0;
  split_rows(input_copy, input_len, num_items, buffer_list, num_rows, &row_upper_bound);

  sgx_check("External Oblivious Sort",
            ecall_external_oblivious_sort(
              eid, op_code, buffer_list.size() - 1, buffer_list.data(), num_rows.data(),
              row_upper_bound));

  jbyteArray ret = env->NewByteArray(input_len);
  env->SetByteArrayRegion(ret, 0, input_len, (jbyte *) input_copy);

  env->ReleaseByteArrayElements(input, ptr, 0);

  free(input_copy);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_AggregateStep1(
  JNIEnv *env,
  jobject obj,
  jlong eid,
  jint index,
  jint num_part,
  jint op_code,
  jbyteArray input_rows,
  jint num_rows) {
  (void)obj;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  jboolean if_copy;
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t actual_size = 0;

  uint32_t output_rows_length = 2048 + 12 + 16 + 2048 + 2048;
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);

  sgx_check("Aggregate step 1",
            ecall_aggregate_step1(eid, index, num_part,
              op_code,
              input_rows_ptr, input_rows_length,
              num_rows,
              output_rows, output_rows_length,
              &actual_size));

  assert(output_rows_length >= actual_size);

  jbyteArray ret = env->NewByteArray(actual_size);
  env->SetByteArrayRegion(ret, 0, actual_size, (jbyte *) output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ProcessBoundary(
  JNIEnv *env,
  jobject obj,
  jlong eid,
  jint op_code,
  jbyteArray rows,
  jint num_rows) {
  (void)obj;

  jboolean if_copy;

  uint32_t rows_length = (uint32_t) env->GetArrayLength(rows);
  uint8_t *rows_ptr = (uint8_t *) env->GetByteArrayElements(rows, &if_copy);


  // output rows length should be input_rows length + num_rows * PARTIAL_AGG_UPPER_BOUND
  uint32_t single_row_size = 4 + 4 + ENC_HEADER_SIZE + AGG_UPPER_BOUND; // +4 for DAG task ID information
  single_row_size += 4 + ENC_HEADER_SIZE + ROW_UPPER_BOUND;
  uint32_t out_agg_rows_length = single_row_size * num_rows;

  uint8_t *out_agg_rows = (uint8_t *) malloc(out_agg_rows_length);
  uint32_t actual_out_agg_rows_size = 0;

  sgx_check("ProcessBoundary",
            ecall_process_boundary_records(
              eid, op_code,
              rows_ptr, rows_length,
              num_rows,
              out_agg_rows, out_agg_rows_length,
              &actual_out_agg_rows_size));

  jbyteArray ret = env->NewByteArray(actual_out_agg_rows_size);
  env->SetByteArrayRegion(ret, 0, actual_out_agg_rows_size, (jbyte *) out_agg_rows);

  env->ReleaseByteArrayElements(rows, (jbyte *) rows_ptr, 0);

  free(out_agg_rows);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_AggregateStep2(
  JNIEnv *env, jobject obj, jlong eid,
  jint index, jint num_part,
  jint op_code, jbyteArray input_rows, jint num_rows,
  jbyteArray boundary_info_row) {
  (void)obj;

  jboolean if_copy;
  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);
  uint32_t boundary_info_row_length = (uint32_t) env->GetArrayLength(boundary_info_row);
  uint8_t *boundary_info_row_ptr =
    (uint8_t *) env->GetByteArrayElements(boundary_info_row, &if_copy);

  uint32_t actual_size = 0;

  uint32_t output_rows_length = block_size_upper_bound(num_rows);
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);

  sgx_check("Aggregate step 2",
            ecall_aggregate_step2(
              eid,
              index, num_part,
              op_code,
              input_rows_ptr, input_rows_length,
              num_rows,
              boundary_info_row_ptr, boundary_info_row_length,
              output_rows, output_rows_length,
              &actual_size));

  jbyteArray ret = env->NewByteArray(actual_size);
  env->SetByteArrayRegion(ret, 0, actual_size, (jbyte *) output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  return ret;
}

void print_bytes_(uint8_t *ptr, uint32_t len) {
  for (uint32_t i = 0; i < len; i++) {
    printf("%u", *(ptr + i));
    printf(" - ");
  }

  printf("\n");
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_JoinSortPreprocess(
  JNIEnv *env,
  jobject obj,
  jlong eid,
  jint index,
  jint num_part,
  jint op_code,
  jbyteArray primary_rows,
  jint num_primary_rows,
  jbyteArray foreign_rows,
  jint num_foreign_rows) {
  (void)obj;

  jboolean if_copy;

  uint32_t primary_rows_length = (uint32_t) env->GetArrayLength(primary_rows);
  uint8_t *primary_rows_ptr = (uint8_t *) env->GetByteArrayElements(primary_rows, &if_copy);

  uint32_t foreign_rows_length = (uint32_t) env->GetArrayLength(foreign_rows);
  uint8_t *foreign_rows_ptr = (uint8_t *) env->GetByteArrayElements(foreign_rows, &if_copy);

  uint32_t output_rows_length = block_size_upper_bound(num_primary_rows + num_foreign_rows);
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);
  uint8_t *output_rows_ptr = output_rows;

  uint32_t actual_output_len = 0;

  sgx_check("JoinSortPreprocess",
            ecall_join_sort_preprocess(
              eid,
              index, num_part,
              op_code,
              primary_rows_ptr, primary_rows_length, num_primary_rows,
              foreign_rows_ptr, foreign_rows_length, num_foreign_rows,
              output_rows_ptr, output_rows_length, &actual_output_len));


  jbyteArray ret = env->NewByteArray(actual_output_len);
  env->SetByteArrayRegion(ret, 0, actual_output_len, (jbyte *) output_rows);

  env->ReleaseByteArrayElements(primary_rows, (jbyte *) primary_rows_ptr, 0);
  env->ReleaseByteArrayElements(foreign_rows, (jbyte *) foreign_rows_ptr, 0);

  free(output_rows);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ProcessJoinBoundary(
  JNIEnv *env, jobject obj, jlong eid, jint op_code, jbyteArray input_rows, jint num_rows) {
  (void)obj;

  jboolean if_copy;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t single_row_length = 4 + 4 + ENC_ROW_UPPER_BOUND;
  uint32_t output_rows_length = single_row_length * num_rows;
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);
  uint8_t *output_rows_ptr = output_rows;
  uint32_t actual_output_length = 0;

  sgx_check("ProcessJoinBoundary",
            ecall_process_join_boundary(
              eid, op_code, input_rows_ptr, input_rows_length, num_rows,
              output_rows_ptr, output_rows_length, &actual_output_length));

  jbyteArray ret = env->NewByteArray(actual_output_length);
  env->SetByteArrayRegion(ret, 0, actual_output_length, (jbyte *) output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ScanCollectLastPrimary(
  JNIEnv *env,
  jobject obj,
  jlong eid,
  jint op_code,
  jbyteArray input_rows,
  jint num_rows) {
  (void)obj;

  jboolean if_copy;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t output_length = 4 + ENC_ROW_UPPER_BOUND;
  uint8_t *output = (uint8_t *) malloc(output_length);

  uint32_t actual_output_len = 0;

  sgx_check("ScanCollectLastPrimary",
            ecall_scan_collect_last_primary(
              eid,
              op_code,
              input_rows_ptr, input_rows_length,
              num_rows,
              output, output_length, &actual_output_len));

  jbyteArray ret = env->NewByteArray(actual_output_len);
  env->SetByteArrayRegion(ret, 0, actual_output_len, (jbyte *) output);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output);

  return ret;
}


JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_SortMergeJoin(
  JNIEnv *env,
  jobject obj,
  jlong eid,
  jint index,
  jint num_part,
  jint op_code,
  jbyteArray input_rows,
  jint num_rows,
  jbyteArray join_row) {
  (void)obj;

  jboolean if_copy;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t output_length = block_size_upper_bound(num_rows);
  uint8_t *output = (uint8_t *) malloc(output_length);

  uint32_t join_row_length = (uint32_t) env->GetArrayLength(join_row);
  uint8_t *join_row_ptr = (uint8_t *) env->GetByteArrayElements(join_row, &if_copy);

  uint32_t actual_output_length = 0;

  sgx_check("SortMergeJoin",
            ecall_sort_merge_join(
              eid,
              index, num_part,
              op_code,
              input_rows_ptr, input_rows_length,
              num_rows,
              join_row_ptr, join_row_length,
              output, output_length, &actual_output_length));
  
  jbyteArray ret = env->NewByteArray(actual_output_length);
  env->SetByteArrayRegion(ret, 0, actual_output_length, (jbyte *) output);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output);

  return ret;

}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_CreateBlock(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray rows, jint num_rows,
  jboolean rows_are_join_rows) {
  (void)obj;

  uint32_t rows_len = (uint32_t) env->GetArrayLength(rows);
  jboolean if_copy = false;
  uint8_t *rows_ptr = (uint8_t *) env->GetByteArrayElements(rows, &if_copy);

  uint32_t block_len = block_size_upper_bound(num_rows);
  uint8_t *block = (uint8_t *) malloc(block_len);

  debug("CreateBlock: num_rows=%d, rows_len=%d, block_len=%d\n", num_rows, rows_len, block_len);

  uint32_t actual_size = 0;

  sgx_check(
    "CreateBlock",
    ecall_create_block(eid, rows_ptr, rows_len, num_rows, rows_are_join_rows,
                       block, block_len, &actual_size));

  debug("CreateBlock: actual_size=%d\n", actual_size);

  jbyteArray result = env->NewByteArray(actual_size);
  env->SetByteArrayRegion(result, 0, actual_size, (jbyte *) block);

  env->ReleaseByteArrayElements(rows, (jbyte *) rows_ptr, 0);

  free(block);

  return result;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_SplitBlock(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray block, jint num_rows,
  jboolean rows_are_join_rows) {
  (void)obj;

  uint32_t block_len = (uint32_t) env->GetArrayLength(block);
  jboolean if_copy = false;
  uint8_t *block_ptr = (uint8_t *) env->GetByteArrayElements(block, &if_copy);

  uint32_t rows_len = num_rows * (4 + ENC_ROW_UPPER_BOUND);
  uint8_t *rows = (uint8_t *) malloc(rows_len);

  debug("SplitBlock: num_rows=%d, block_len=%d, rows_len=%d\n", num_rows, block_len, rows_len);

  uint32_t actual_size = 0;

  sgx_check(
    "SplitBlock",
    ecall_split_block(eid, block_ptr, block_len,
                      rows, rows_len, num_rows, rows_are_join_rows, &actual_size));

  debug("SplitBlock: actual_size=%d\n", actual_size);

  jbyteArray result = env->NewByteArray(actual_size);
  env->SetByteArrayRegion(result, 0, actual_size, (jbyte *) rows);

  env->ReleaseByteArrayElements(block, (jbyte *) block_ptr, 0);

  free(rows);

  return result;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_Sample(
  JNIEnv *env, jobject obj, jlong eid,
  jint index, jint num_part,
  jint op_code, jbyteArray input, jint num_rows,
  jobject num_output_rows_obj) {
  (void)obj;

  uint32_t input_length = (uint32_t) env->GetArrayLength(input);
  jboolean if_copy;
  uint8_t *input_ptr = (uint8_t *) env->GetByteArrayElements(input, &if_copy);

  uint32_t output_length = block_size_upper_bound(num_rows);
  uint8_t *output = (uint8_t *) malloc(output_length);

  uint32_t actual_output_length = 0;
  uint32_t num_output_rows = 0;

  sgx_check("Sample",
            ecall_sample(
              eid,
              index, num_part,
              op_code, input_ptr, input_length, num_rows, output,
              &actual_output_length, &num_output_rows));

  jbyteArray ret = env->NewByteArray(actual_output_length);
  env->SetByteArrayRegion(ret, 0, actual_output_length, (jbyte *) output);

  jclass num_output_rows_class = env->GetObjectClass(num_output_rows_obj);
  jfieldID field_id = env->GetFieldID(num_output_rows_class, "value", "I");
  env->SetIntField(num_output_rows_obj, field_id, num_output_rows);

  env->ReleaseByteArrayElements(input, (jbyte *) input_ptr, 0);

  free(output);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_FindRangeBounds(
  JNIEnv *env, jobject obj, jlong eid, jint op_code, jint num_partitions, jbyteArray input,
  jint num_rows) {
  (void)obj;

  uint32_t input_len = (uint32_t) env->GetArrayLength(input);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(input, &if_copy);

  uint8_t *input_copy = (uint8_t *) malloc(input_len);
  uint8_t *scratch = (uint8_t *) malloc(input_len);

  for (uint32_t i = 0; i < input_len; i++) {
    input_copy[i] = *(ptr + i);
  }

  std::vector<uint8_t *> buffer_list;
  std::vector<uint32_t> buffer_num_rows;
  uint32_t row_upper_bound = 0;
  split_rows(input_copy, input_len, num_rows, buffer_list, buffer_num_rows, &row_upper_bound);

  uint8_t *output = (uint8_t *) malloc(input_len);
  uint32_t actual_output_length = 0;
  sgx_check("Find Range Bounds",
            ecall_find_range_bounds(
              eid, op_code, num_partitions, buffer_list.size() - 1, buffer_list.data(),
              buffer_num_rows.data(), row_upper_bound, output, &actual_output_length, scratch));

  jbyteArray ret = env->NewByteArray(actual_output_length);
  env->SetByteArrayRegion(ret, 0, actual_output_length, (jbyte *) output);

  env->ReleaseByteArrayElements(input, ptr, 0);

  free(input_copy);
  free(scratch);
  free(output);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_PartitionForSort(
  JNIEnv *env, jobject obj, jlong eid,
  jint index, jint num_part,
  jint op_code, jint num_partitions, jbyteArray input,
  jint num_rows, jbyteArray boundary_rows, jintArray offsets, jintArray rows_per_partition) {
  (void) obj;

  uint32_t input_len = (uint32_t) env->GetArrayLength(input);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(input, &if_copy);

  uint8_t *boundary_rows_ptr = (uint8_t *) env->GetByteArrayElements(boundary_rows, &if_copy);
  uint32_t boundary_rows_len = (uint32_t) env->GetArrayLength(boundary_rows);

  uint8_t *input_copy = (uint8_t *) malloc(input_len);
  uint8_t *scratch = (uint8_t *) malloc(input_len);

  for (uint32_t i = 0; i < input_len; i++) {
    input_copy[i] = *(ptr + i);
  }

  std::vector<uint8_t *> buffer_list;
  std::vector<uint32_t> buffer_num_rows;
  uint32_t row_upper_bound = 0;
  split_rows(input_copy, input_len, num_rows, buffer_list, buffer_num_rows, &row_upper_bound);

  uint32_t output_len = 2 * input_len; // need extra space for the increased number of block
                                       // headers. TODO: use block_size_upper_bound
  uint8_t *output = (uint8_t *) malloc(output_len);
  uint8_t **output_partition_ptrs =
    (uint8_t **) malloc((num_partitions + 1) * sizeof(uint8_t *));
  uint32_t *output_partition_num_rows = (uint32_t *) malloc(num_partitions * sizeof(uint32_t));
  sgx_check("Partition For Sort",
            ecall_partition_for_sort(
              eid,
              index, num_part,
              op_code, num_partitions, buffer_list.size() - 1, buffer_list.data(),
              buffer_num_rows.data(), row_upper_bound,
              boundary_rows_ptr, boundary_rows_len,
              output, output_partition_ptrs,
              output_partition_num_rows, scratch));

  jint *offsets_ptr = env->GetIntArrayElements(offsets, &if_copy);
  jint *rows_per_partition_ptr = env->GetIntArrayElements(rows_per_partition, &if_copy);
  for (jint i = 0; i < num_partitions; i++) {
    offsets_ptr[i] = output_partition_ptrs[i] - output;
    rows_per_partition_ptr[i] = output_partition_num_rows[i];
  }
  offsets_ptr[num_partitions] = output_partition_ptrs[num_partitions] - output;

  jbyteArray result = env->NewByteArray(output_len);
  env->SetByteArrayRegion(result, 0, output_len, (jbyte *) output);

  env->ReleaseByteArrayElements(input, ptr, 0);
  env->ReleaseIntArrayElements(offsets, offsets_ptr, 0);
  env->ReleaseIntArrayElements(rows_per_partition, rows_per_partition_ptr, 0);

  free(input_copy);
  free(scratch);
  free(output);
  free(output_partition_ptrs);
  free(output_partition_num_rows);

  return result;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ExternalSort(
    JNIEnv *env,
    jobject obj,
    jlong eid,
    jint index,
    jint num_part,
    jint op_code,
    jbyteArray input,
    jint num_items) {
  (void)obj;

  if (num_items == 0) {
    jbyteArray ret = env->NewByteArray(0);
    return ret;
  }

  uint32_t input_len = (uint32_t) env->GetArrayLength(input);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(input, &if_copy);

  uint8_t *input_copy = (uint8_t *) malloc(input_len);
  uint8_t *scratch = (uint8_t *) malloc(input_len);

  for (uint32_t i = 0; i < input_len; i++) {
    input_copy[i] = *(ptr + i);
  }

  std::vector<uint8_t *> buffer_list;
  std::vector<uint32_t> num_rows;
  uint32_t row_upper_bound = 0;
  split_rows(input_copy, input_len, num_items, buffer_list, num_rows, &row_upper_bound);

  sgx_check("External non-oblivious sort",
            ecall_external_sort(eid,
                                index, num_part,
								op_code,
                                buffer_list.size() - 1,
								buffer_list.data(),
                                num_rows.data(),
								row_upper_bound,
                                scratch));

  jbyteArray ret = env->NewByteArray(input_len);
  env->SetByteArrayRegion(ret, 0, input_len, (jbyte *) input_copy);

  env->ReleaseByteArrayElements(input, ptr, 0);

  free(input_copy);
  free(scratch);

  return ret;
}


JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_ColumnSortFilter(
  JNIEnv *env, jobject obj,
  jlong eid, jint op_code, jbyteArray input_rows, jint column, jint offset,
  jint num_rows, jobject num_output_rows_obj) {
  (void)obj;

  uint32_t input_len = (uint32_t) env->GetArrayLength(input_rows);
  jboolean if_copy = false;
  jbyte *input_rows_ptr = env->GetByteArrayElements(input_rows, &if_copy);

  uint8_t *input_copy = (uint8_t *) malloc(input_len);
  uint8_t *input_copy_ptr = input_copy;

  for (uint32_t i = 0; i < input_len; i++) {
    input_copy[i] = *(input_rows_ptr + i);
  }

  std::vector<uint8_t *> buffer_list;
  std::vector<uint32_t> buffer_num_rows;
  uint32_t row_upper_bound = 0;
  uint32_t total_rows = split_rows(input_copy, input_len, num_rows, buffer_list, buffer_num_rows, &row_upper_bound);

  uint8_t *output_rows = (uint8_t *) malloc(input_len);
  uint32_t output_rows_size = 0;
  uint32_t num_output_rows = 0;

  ecall_column_sort_filter(
    eid, op_code, input_copy_ptr, input_len, (uint32_t) column, (uint32_t) offset,
    total_rows, row_upper_bound, output_rows, &output_rows_size, &num_output_rows);

  jclass num_output_rows_class = env->GetObjectClass(num_output_rows_obj);
  jfieldID field_id = env->GetFieldID(num_output_rows_class, "value", "I");
  env->SetIntField(num_output_rows_obj, field_id, num_output_rows);

  if (num_output_rows == 0) {
    jbyteArray ret = env->NewByteArray(0);
    free(output_rows);
    free(input_copy);

    return ret;
  }
  jbyteArray ret = env->NewByteArray(output_rows_size);
  env->SetByteArrayRegion(ret, 0, output_rows_size, (jbyte *) output_rows);

  free(output_rows);
  free(input_copy);

  return ret;
}

uint32_t est(uint32_t per_column_data_size) {

  uint32_t max_block_size = MAX_BLOCK_SIZE - (16 + 12 + 16);

  uint32_t per_column_blocks = (per_column_data_size % max_block_size == 0) ? per_column_data_size / max_block_size + 1 : per_column_data_size / max_block_size + 2;
  if (per_column_data_size == 0) {
    per_column_blocks = 1;
  }
  uint32_t estimated_column_size = per_column_blocks * (16 + 12 + 16) + per_column_data_size;

  return 4 + 4 + estimated_column_size;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_EnclaveColumnSort(
    JNIEnv *env,
    jobject obj,
    jlong eid,
    jint index,
    jint number_part,
    jint op_code,
    jint round,
    jbyteArray input,
    jint r,
    jint s,
    jint column_input,
    jint current_part,
    jint num_part,
    jint offset) {

  (void)obj;
  (void)current_part;
  (void)num_part;

  uint32_t input_len = (uint32_t) env->GetArrayLength(input);

  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(input, &if_copy);

  uint8_t *input_copy = (uint8_t *) malloc(input_len);

  for (uint32_t i = 0; i < input_len; i++) {
    input_copy[i] = *(ptr + i);
  }

  // Divide the input into buffers of bounded size. Each buffer will contain one or more blocks.
  std::vector<uint8_t *> buffer_list;
  std::vector<uint32_t> num_rows;
  uint32_t row_upper_bound = 0;

  BlockReader reader(input_copy, input_len);
  uint8_t *cur_block;
  uint32_t cur_block_size;
  uint32_t cur_block_num_rows;
  uint32_t total_rows = 0;
  uint32_t num_blocks = 0;
  reader.read(&cur_block, &cur_block_size, &cur_block_num_rows, &row_upper_bound);
  while (cur_block != NULL) {
    uint8_t *cur_buffer = cur_block;
    uint32_t cur_buffer_size = cur_block_size;
    uint32_t cur_buffer_num_rows = cur_block_num_rows;
    uint32_t cur_row_upper_bound;

    reader.read(&cur_block, &cur_block_size, &cur_block_num_rows, &cur_row_upper_bound);
    while (cur_buffer_size + cur_block_size <= MAX_SORT_BUFFER && cur_block != NULL) {
      cur_buffer_size += cur_block_size;
      cur_buffer_num_rows += cur_block_num_rows;
      row_upper_bound =
        cur_row_upper_bound > row_upper_bound ? cur_row_upper_bound : row_upper_bound;

      reader.read(&cur_block, &cur_block_size, &cur_block_num_rows, &cur_row_upper_bound);
      num_blocks++;
    }

    buffer_list.push_back(cur_buffer);
    num_rows.push_back(cur_buffer_num_rows);
    total_rows += cur_buffer_num_rows;
    debug("ColumnSort: Buffer %lu: %d bytes, %d rows\n",
          buffer_list.size(), cur_buffer_size, cur_buffer_num_rows);
  }
  // Sentinel pointer to the end of the input
  buffer_list.push_back(input_copy + input_len);

  // TODO: current allocation very inefficient, change this
  uint32_t per_column_data_size = r * row_upper_bound;
  uint32_t per_column_blocks = (per_column_data_size % MAX_BLOCK_SIZE == 0) ? per_column_data_size / MAX_BLOCK_SIZE : per_column_data_size / MAX_BLOCK_SIZE + 1;
  uint32_t estimated_column_size = per_column_blocks * (16 + 12 + 16) + per_column_data_size;

  uint32_t total_data_size = (total_rows / s + 1) * row_upper_bound;
  uint32_t per_shuffle_column_size = r / s * row_upper_bound;
  uint32_t per_half_column_size = (r ) * row_upper_bound;

  (void) estimated_column_size;
  (void) total_data_size;
  (void) per_shuffle_column_size;
  (void) per_half_column_size;

  //printf("ColumnSort[%u] total_rows: %u, total_data_size: %u, buffer_list.size: %lu, r is %u, s is %u\n", round, total_rows, total_data_size, buffer_list.size(), r, s);

  // construct total of s output buffers
  uint8_t **output_buffers = (uint8_t **) malloc(sizeof(uint8_t *) * s);
  uint32_t *output_buffer_sizes = (uint32_t *) malloc(sizeof(uint32_t) * ((uint32_t) s));
  // for (uint32_t i = 0; i < (uint32_t) s; i++) {
  //   if (estimated_column_size > 0) {
  //     output_buffers[i] = (uint8_t *) malloc(estimated_column_size);
  //   } else {
  //     output_buffers[i] = NULL;
  //   }
  //   output_buffer_sizes[i] = 0;
  // }

  uint32_t column = (uint32_t) column_input;
  for (uint32_t i = 0; i < (uint32_t) s; i++) {
    if (round == 0) {
      output_buffers[i] = (uint8_t *) malloc(est(total_data_size));
      //printf("ColumnSort[%u] column %u allocating %u bytes\n", round, i, est(total_data_size));
    } else if (round == 1) {
      if (i == 0) {
        output_buffers[i] = (uint8_t *) malloc(est(per_column_data_size));
        //printf("ColumnSort[%u] column is %u, column %u allocating %u bytes\n", round, column, i, est(per_column_data_size));
      } else {
        output_buffers[i] = (uint8_t *) malloc(est(0));
        //printf("ColumnSort[%u] column is %u, column %u allocating %u bytes\n", round, column, i, est(0));
      }
    } else if (round == 2 || round == 3) {
      output_buffers[i] = (uint8_t *) malloc(est(per_shuffle_column_size));
      //printf("ColumnSort[%u] column is %u, column %u allocating %u bytes\n", round, column, i, est(per_shuffle_column_size * 2));
    } else if (round == 4) {
      if (i + 1 == column || i  == ((column) % s)) {
        output_buffers[i] = (uint8_t *) malloc(est(per_half_column_size));
        //printf("ColumnSort[%u] column is %u, column %u allocating %u bytes\n", round, column, i+1, est(per_half_column_size));
      } else {
        output_buffers[i] = (uint8_t *) malloc(est(0));
        //printf("ColumnSort[%u] column is %u, column %u allocating %u bytes\n", round, column, i+1, est(0));
      }
    } else if (round == 5) {
      if (i + 1 == column || (i + 1) % s == column - 1) {
        output_buffers[i] = (uint8_t *) malloc(est(per_half_column_size));
        //printf("ColumnSort[%u] column is %u, column %u allocating %u bytes\n", round, column, i+1, est(per_half_column_size));
      } else {
        output_buffers[i] = (uint8_t *) malloc(est(0));
      }
    } else {
      //printf("Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_EnclaveColumnSort: Should not be here!");
      assert(false);
    }
  }

  if (round == 0) {
    ecall_column_sort_preprocess(eid,
                                 op_code, input_copy, input_len, total_rows, row_upper_bound,
                                 (uint32_t) offset, r, s, output_buffers, output_buffer_sizes);
  } else if (round == 1) {
    uint32_t output_rows_size = 0;
    ecall_column_sort_padding(eid,
                              op_code, input_copy, input_len, total_rows, row_upper_bound,
                              r, s, output_buffers[0], &output_rows_size);

    jbyteArray ret = env->NewByteArray(output_rows_size);
    env->SetByteArrayRegion(ret, 0, output_rows_size, (jbyte *) output_buffers[0]);
    env->ReleaseByteArrayElements(input, ptr, 0);

    free(input_copy);
    for (uint32_t i = 0; i < (uint32_t) s; i++) {
      if (output_buffers[i] != NULL) {
        free(output_buffers[i]);
      }
    }
    free(output_buffers);

    return ret;
  } else {
    ecall_column_sort(eid,
                      index, number_part,
                      op_code, round-1, input_copy, input_len, num_rows.data(),
                      buffer_list.data(), buffer_list.size() - 1, row_upper_bound,
                      column, r, s,
                      output_buffers, output_buffer_sizes);
  }

  // serialize the buffers onto one big buffer
  uint32_t final_size = 0;
  for (uint32_t i = 0; i < (uint32_t) s; i++) {
	final_size += output_buffer_sizes[i] + 4 + 4; // column ID, of column length
  }

  uint8_t *output = (uint8_t *) malloc(final_size);
  uint8_t *output_ptr = output;

  for (uint32_t i = 0; i < (uint32_t) s; i++) {
    *((uint32_t *) output_ptr) = i + 1;
    output_ptr += 4;
    *((uint32_t *) output_ptr) = output_buffer_sizes[i];
    output_ptr += 4;
    //printf("ColumnSort: outputting column %u size %u, max is %u\n", i+1, output_buffer_sizes[i], r*row_upper_bound);
    memcpy(output_ptr, output_buffers[i], output_buffer_sizes[i]);
    output_ptr += output_buffer_sizes[i];
  }

  jbyteArray ret = env->NewByteArray(final_size);
  env->SetByteArrayRegion(ret, 0, final_size, (jbyte *) output);

  env->ReleaseByteArrayElements(input, ptr, 0);

  free(input_copy);
  for (uint32_t i = 0; i < (uint32_t) s; i++) {
    if (output_buffers[i] != NULL) {
      free(output_buffers[i]);
    }
  }
  free(output_buffers);
  free(output);
  free(output_buffer_sizes);

  return ret;
  
}

JNIEXPORT jint JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_CountNumRows(JNIEnv *env, jobject obj, jlong eid, jbyteArray input_rows) {

  (void)obj;
  (void)eid;
  (void)input_rows;
  (void)env;

  uint32_t num_rows = 0;
  jboolean if_copy = false;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  ecall_count_rows(eid, input_rows_ptr, input_rows_length, &num_rows);

  return (jint) num_rows;
}

JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousAggregate(
  JNIEnv *env, jobject obj, jlong eid,
  jint index, jint num_part,
  jint op_code, jbyteArray input_rows, jint num_rows,
  jobject num_output_rows_obj) {
  (void)obj;

  jboolean if_copy;
  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t actual_size = 0;

  uint32_t output_rows_length = block_size_upper_bound(num_rows);
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);
  uint32_t num_output_rows = 0;

  sgx_check("Non-oblivious aggregation",
            ecall_non_oblivious_aggregate(eid,
                                          index, num_part,
                                          op_code,
										  input_rows_ptr, input_rows_length,
										  num_rows,
										  output_rows, output_rows_length,
                                          &actual_size, &num_output_rows));

  jbyteArray ret = env->NewByteArray(actual_size);
  env->SetByteArrayRegion(ret, 0, actual_size, (jbyte *) output_rows);

  jclass num_output_rows_class = env->GetObjectClass(num_output_rows_obj);
  jfieldID field_id = env->GetFieldID(num_output_rows_class, "value", "I");
  env->SetIntField(num_output_rows_obj, field_id, num_output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  return ret;
}


JNIEXPORT jbyteArray JNICALL Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_NonObliviousSortMergeJoin(
  JNIEnv *env, jobject obj, jlong eid,
  jint index, jint num_part,
  jint op_code, jbyteArray input_rows, jint num_rows,
  jobject num_output_rows_obj) {
  (void)obj;

  jboolean if_copy;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t output_length = block_size_upper_bound(num_rows);
  uint8_t *output = (uint8_t *) malloc(output_length);

  uint32_t actual_output_length = 0;
  uint32_t num_output_rows = 0;

  sgx_check("Non-oblivious SortMergeJoin",
            ecall_non_oblivious_sort_merge_join(eid,
                                                index, num_part,
												op_code,
												input_rows_ptr, input_rows_length,
												num_rows,
												output, output_length,
                                                &actual_output_length, &num_output_rows));
  
  jbyteArray ret = env->NewByteArray(actual_output_length);
  env->SetByteArrayRegion(ret, 0, actual_output_length, (jbyte *) output);

  jclass num_output_rows_class = env->GetObjectClass(num_output_rows_obj);
  jfieldID field_id = env->GetFieldID(num_output_rows_class, "value", "I");
  env->SetIntField(num_output_rows_obj, field_id, num_output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output);

  return ret;

}

// test functionality only
void test_oblivious_swap() {
  char string1[11] = "hello12345";
  char string2[11] = "hello56789";

  uint8_t data1[10];
  uint8_t data2[10];

  uint8_t *data1_ptr = data1;
  uint8_t *data2_ptr = data2;

  memcpy(data1_ptr, string1, 10);
  memcpy(data2_ptr, string2, 10);

  printf("string1: %s\t string2: %s\n", (char *) data1, (char *) data2);

  ecall_oblivious_swap(global_eid, data1_ptr, data2_ptr, 10);

  printf("string1: %s\t string2: %s\n", (char *) data1, (char *) data2);
}

void test_ra() {

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

  //test_oblivious_swap();

  /* Destroy the enclave */
  sgx_destroy_enclave(global_eid);

  printf("Info: SampleEnclave successfully returned.\n");

  return 0;
}
