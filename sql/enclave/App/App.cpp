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
#include "sgx_status.h"
#include "App.h"
#include "Enclave_u.h"
#include "org_apache_spark_sql_SGXEnclave.h"
#include "sgx_tcrypto.h"
#include "define.h"
#include "common.h"

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


JNIEXPORT jlong JNICALL Java_org_apache_spark_sql_SGXEnclave_StartEnclave(
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


JNIEXPORT void JNICALL Java_org_apache_spark_sql_SGXEnclave_StopEnclave(
  JNIEnv *env, jobject obj, jlong eid) {
  (void)env;
  (void)obj;

  sgx_check("StopEnclave", sgx_destroy_enclave(eid));
}

// read a chunk of buffer from the scala program

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_Project(
  JNIEnv *env, jobject obj, jlong eid, jint op_code, jbyteArray input_rows, jint num_rows) {
  (void)obj;

  jboolean if_copy;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t output_rows_length = block_size_upper_bound(num_rows);
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);

  uint32_t actual_output_rows_length = 0;

  sgx_check("Project",
            ecall_project(
              eid, op_code, input_rows_ptr, input_rows_length, num_rows, output_rows,
              output_rows_length, &actual_output_rows_length));

  jbyteArray ret = env->NewByteArray(actual_output_rows_length);
  env->SetByteArrayRegion(ret, 0, actual_output_rows_length, (jbyte *) output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_Filter(
  JNIEnv *env, jobject obj, jlong eid, jint op_code, jbyteArray input_rows, jint num_rows,
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
              eid, op_code, input_rows_ptr, input_rows_length, num_rows, output_rows,
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


JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_Encrypt(
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

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_Decrypt(
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

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_EncryptAttribute(
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

JNIEXPORT void JNICALL SGX_CDECL Java_org_apache_spark_sql_SGXEnclave_Test(JNIEnv *env, jobject obj, jlong eid) {
  (void)env;
  (void)obj;
  (void)eid;
  printf("Test!\n");
}


// the op_code allows the internal sort code to decide which comparator to use
// assume that the elements are of equal size!
JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_ObliviousSort(JNIEnv *env,
                                                                                jobject obj,
                                                                                jlong eid,
                                                                                jint op_code,
                                                                                jbyteArray input,
                                                                                jint offset,
                                                                                jint num_items) {
  (void)obj;
  (void)offset;

  if (num_items == 0) {
    jbyteArray ret = env->NewByteArray(0);
    return ret;
  }

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

  BlockReader r(input_copy, input_len);
  uint32_t row_upper_bound = r.get_row_upper_bound();
  uint8_t *cur_block;
  uint32_t cur_block_size;
  uint32_t cur_block_num_rows;
  uint32_t total_rows = 0;
  r.read(&cur_block, &cur_block_size, &cur_block_num_rows);
  while (cur_block != NULL) {
    uint8_t *cur_buffer = cur_block;
    uint32_t cur_buffer_size = cur_block_size;
    uint32_t cur_buffer_num_rows = cur_block_num_rows;

    r.read(&cur_block, &cur_block_size, &cur_block_num_rows);
    while (cur_buffer_size + cur_block_size <= MAX_SORT_BUFFER && cur_block != NULL) {
      cur_buffer_size += cur_block_size;
      cur_buffer_num_rows += cur_block_num_rows;

      r.read(&cur_block, &cur_block_size, &cur_block_num_rows);
    }

    buffer_list.push_back(cur_buffer);
    num_rows.push_back(cur_buffer_num_rows);
    total_rows += cur_buffer_num_rows;
    debug("ObliviousSort: Buffer %lu: %d bytes, %d rows\n",
          buffer_list.size(), cur_buffer_size, cur_buffer_num_rows);
  }

  perf("ObliviousSort: Input (%d bytes, %d rows) split into %lu buffers with %d rows\n",
       input_len, num_items, buffer_list.size(), total_rows);

  sgx_check("External Oblivious Sort",
            ecall_external_oblivious_sort(
              eid, op_code, buffer_list.size(), buffer_list.data(), num_rows.data(),
              row_upper_bound));

  jbyteArray ret = env->NewByteArray(input_len);
  env->SetByteArrayRegion(ret, 0, input_len, (jbyte *) input_copy);

  env->ReleaseByteArrayElements(input, ptr, 0);

  free(input_copy);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_AggregateStep1(
  JNIEnv *env,
  jobject obj,
  jlong eid,
  jint op_code,
  jbyteArray input_rows,
  jint num_rows) {
  (void)obj;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  jboolean if_copy;
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  if (num_rows == 0) {
    jbyteArray ret = env->NewByteArray(0);
    env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);
    return ret;
  }

  uint32_t actual_size = 0;

  uint32_t output_rows_length = 2048 + 12 + 16 + 2048;
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);

  sgx_check("Aggregate step 1",
            ecall_aggregate_step1(
              eid, op_code,
              input_rows_ptr, input_rows_length,
              num_rows,
              output_rows, output_rows_length,
              &actual_size));

  jbyteArray ret = env->NewByteArray(actual_size);
  env->SetByteArrayRegion(ret, 0, actual_size, (jbyte *) output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_ProcessBoundary(
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
  uint32_t single_row_size = 4 + ENC_HEADER_SIZE + AGG_UPPER_BOUND;
  single_row_size += ENC_HEADER_SIZE + ROW_UPPER_BOUND;
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

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_AggregateStep2(
  JNIEnv *env, jobject obj, jlong eid, jint op_code, jbyteArray input_rows, jint num_rows,
  jbyteArray boundary_info_row) {
  (void)obj;

  jboolean if_copy;
  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);
  uint32_t boundary_info_row_length = (uint32_t) env->GetArrayLength(boundary_info_row);
  uint8_t *boundary_info_row_ptr =
    (uint8_t *) env->GetByteArrayElements(boundary_info_row, &if_copy);

  if (num_rows == 0) {
    jbyteArray ret = env->NewByteArray(0);
    env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);
    env->ReleaseByteArrayElements(boundary_info_row, (jbyte *) boundary_info_row_ptr, 0);
    return ret;
  }

  uint32_t actual_size = 0;

  uint32_t output_rows_length = block_size_upper_bound(num_rows);
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);

  sgx_check("Aggregate step 2",
            ecall_aggregate_step2(
              eid, op_code,
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

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_JoinSortPreprocess(
  JNIEnv *env,
  jobject obj,
  jlong eid,
  jint op_code,
  jbyteArray enc_table_id,
  jbyteArray input_rows,
  jint num_rows) {
  (void)obj;

  jboolean if_copy;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t output_rows_length = block_size_upper_bound(num_rows);
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);
  uint8_t *output_rows_ptr = output_rows;

  uint8_t *enc_table_id_ptr = (uint8_t *) env->GetByteArrayElements(enc_table_id, &if_copy);

  uint32_t actual_output_len = 0;

  sgx_check("JoinSortPreprocess",
            ecall_join_sort_preprocess(
              eid,
              op_code,
              enc_table_id_ptr,
              input_rows_ptr, input_rows_length,
              num_rows,
              output_rows_ptr, output_rows_length, &actual_output_len));


  jbyteArray ret = env->NewByteArray(actual_output_len);
  env->SetByteArrayRegion(ret, 0, actual_output_len, (jbyte *) output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_ProcessJoinBoundary(
  JNIEnv *env, jobject obj, jlong eid, jint op_code, jbyteArray input_rows, jint num_rows) {
  (void)obj;

  jboolean if_copy;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t single_row_length = ENC_HEADER_SIZE + JOIN_ROW_UPPER_BOUND;
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

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_ScanCollectLastPrimary(
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

  uint32_t output_length = ENC_HEADER_SIZE + JOIN_ROW_UPPER_BOUND;
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


JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_SortMergeJoin(
  JNIEnv *env,
  jobject obj,
  jlong eid,
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

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_CreateBlock(
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

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_SplitBlock(
  JNIEnv *env, jobject obj, jlong eid, jbyteArray block, jint num_rows,
  jboolean rows_are_join_rows) {
  (void)obj;

  uint32_t block_len = (uint32_t) env->GetArrayLength(block);
  jboolean if_copy = false;
  uint8_t *block_ptr = (uint8_t *) env->GetByteArrayElements(block, &if_copy);

  uint32_t rows_len = num_rows * ENC_ROW_UPPER_BOUND;
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

uint32_t enc_size(uint32_t len) {
  return len + ENC_HEADER_SIZE;
}

uint32_t format_encrypt_row(uint8_t *row, uint32_t index, uint32_t num_cols) {
  static char chars[5] = {'A', 'B', 'C', 'D', 'E'};
  uint8_t temp[1024];

  uint8_t *row_ptr = row;

  *( (uint32_t *) row_ptr) = num_cols;
  row_ptr += 4;

  // [int][string][int]
  *temp = INT;
  *( (uint32_t *) (temp + 1)) = 4;
  *( (uint32_t *) (temp + 5)) = index;

  *( (uint32_t *) row_ptr) = enc_size(1 + 4 + 4);
  row_ptr += 4;
  ecall_encrypt(global_eid, temp, (1 + 4 + 4), row_ptr, enc_size(1 + 4 + 4));
  row_ptr += enc_size(1 + 4 + 4);

  *temp = INT;
  *( (uint32_t *) (temp + 1)) = 4;
  *( (uint32_t *) (temp + 5)) = (uint32_t) (rand());
  //printf("rand is %u\n", *( (uint32_t *) (temp + 5)));

  *( (uint32_t *) row_ptr) = enc_size(1 + 4 + 4);
  row_ptr += 4;
  ecall_encrypt(global_eid, temp, (1 + 4 + 4), row_ptr, enc_size(1 + 4 + 4));
  row_ptr += enc_size(1 + 4 + 4);

  *temp = STRING;
  *( (uint32_t *) (temp + 1)) = 1;
  *((char *) (temp + 5)) = chars[index % 5];

  *( (uint32_t *) row_ptr) = enc_size(HEADER_SIZE + STRING_UPPER_BOUND);
  row_ptr += 4;
  ecall_encrypt(global_eid, temp, (HEADER_SIZE + STRING_UPPER_BOUND), row_ptr, enc_size(HEADER_SIZE + STRING_UPPER_BOUND));
  row_ptr += enc_size(HEADER_SIZE + STRING_UPPER_BOUND);

  return (row_ptr - row);
}

void decrypt_and_print(uint8_t *row, uint32_t num_rows, uint32_t cols) {
  uint8_t temp[1024];
  uint8_t *ptr = row;

  for (uint32_t i = 0; i < num_rows; i++) {
    //printf("Row -- num_cols is %u\n", *( (uint32_t *) ptr));
    ptr += 4;

    for (uint32_t j = 0; j < cols; j++) {
      uint32_t enc_len = *( (uint32_t *) ptr);
      ptr += 4;
      ecall_decrypt(global_eid, ptr, enc_len, temp, enc_len - ENC_HEADER_SIZE);

      if (false) {
        uint8_t *value_ptr = temp;
        uint8_t attr_type = *value_ptr;
        uint32_t attr_len = *( (uint32_t *) (value_ptr + 1));
        printf("[attr: type is %u, attr_len is %u; ", attr_type, attr_len);
        if (attr_type == 1) {
          printf("Attr: %u]\n", *( (uint32_t *) (value_ptr + 1 + 4)));
        } else if (attr_type == 2) {
          printf("Attr: %.*s]\n", attr_len, (char *) (value_ptr + 1 + 4));
        }
      }

      ptr += enc_len;
    }
  }
}

void test_encryption_perf(int argc, char *argv[]) {

  if (argc < 2) {
    printf("Usage: ./app big_size small_size\n");
	assert(false);
  }

  const uint32_t big_size = atoi(argv[1]); // 256 * 1024 * 1024;
  const uint32_t small_size = atoi(argv[2]); // 1 * 1024;
  // const uint32_t big_size = 256 * 1024 * 1024;
  // const uint32_t small_size = 256 * 1024 * 1024;
  const uint32_t num_blocks = big_size / small_size;
  uint8_t *ciphertext = (uint8_t *)malloc(big_size + num_blocks * ENC_HEADER_SIZE);
  uint8_t *plaintext = (uint8_t *)malloc(big_size);

  uint8_t *ciphertext_ptr = ciphertext;

  uint64_t t;

  // Warmup
  ecall_encrypt(global_eid, plaintext, big_size, ciphertext, big_size + ENC_HEADER_SIZE);

  t = 0;
  {
    scoped_timer timer(&t);
    for (uint32_t i = 0; i < num_blocks; i++) {
      ecall_encrypt(global_eid, plaintext, small_size, ciphertext_ptr,
                    small_size + ENC_HEADER_SIZE);
      ciphertext_ptr += small_size + ENC_HEADER_SIZE;
    }
  }
  double small_time_ms = ((double) t) / 1000;


  t = 0;
  {
    scoped_timer timer(&t);
    ecall_encrypt(global_eid, plaintext, big_size, ciphertext, big_size + ENC_HEADER_SIZE);
  }
  double big_time_ms = ((double) t) / 1000;

  printf("big %d %d %f\n", big_size, small_size, big_time_ms);

  printf("small %d %d %f\n", big_size, small_size, small_time_ms);

  free(ciphertext);
  free(plaintext);
}

void test_stream_encryption() {
  ecall_stream_encryption_test(global_eid);
}

/* Application entry */
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

  test_stream_encryption();
  
  /* Destroy the enclave */
  sgx_destroy_enclave(global_eid);

  printf("Info: SampleEnclave successfully returned.\n");

  return 0;
}
