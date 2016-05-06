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


JNIEXPORT jlong JNICALL Java_org_apache_spark_sql_SGXEnclave_StartEnclave(JNIEnv *env, jobject obj) {
  // if (initialize_enclave() < 0) {
  //   printf("Enclave intiailization failed!\n");
  // }

  sgx_enclave_id_t eid;
  sgx_status_t ret = SGX_SUCCESS;
  sgx_launch_token_t token = {0};
  int updated = 0;

  ret = sgx_create_enclave(std::getenv("LIBENCLAVESIGNED_PATH"), SGX_DEBUG_FLAG, &token, &updated, &eid, NULL);

  if (ret != SGX_SUCCESS) {
    print_error_message(ret);
  }
  
  return eid;
}


JNIEXPORT void JNICALL Java_org_apache_spark_sql_SGXEnclave_StopEnclave(JNIEnv *env, jobject obj, jlong eid) {
  if (SGX_SUCCESS != sgx_destroy_enclave(eid)) {
    printf("Enclave destruction failure\n");
  }
}

// read a chunk of buffer from the scala program

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_Project(
  JNIEnv *env, jobject obj, jlong eid, jint op_code, jbyteArray input_rows, jint num_rows) {

  jboolean if_copy;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t output_rows_length = (ENC_HEADER_SIZE + ROW_UPPER_BOUND) * num_rows;
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);

  uint32_t actual_output_rows_length = 0;

  ecall_project(
    eid, op_code, input_rows_ptr, input_rows_length, num_rows, output_rows, output_rows_length,
    &actual_output_rows_length);

  jbyteArray ret = env->NewByteArray(actual_output_rows_length);
  env->SetByteArrayRegion(ret, 0, actual_output_rows_length, (jbyte *) output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  return ret;
}

JNIEXPORT jboolean JNICALL Java_org_apache_spark_sql_SGXEnclave_Filter(JNIEnv *env, 
								       jobject obj, 
								       jlong eid, 
								       jint op_code, 
								       jbyteArray row) {


  const jsize length = env->GetArrayLength(row);
  jboolean if_copy = false;
  jbyte *row_ptr = env->GetByteArrayElements(row, &if_copy);

  // printf("Row's length is %u\n", length);

  int ret = 0;
  sgx_status_t status = ecall_filter_single_row(eid, &ret, op_code, (uint8_t *) row_ptr, (uint32_t) length);
  if (status != SGX_SUCCESS) {
    printf("filter_single_row() not successful!\n");
  }

  env->ReleaseByteArrayElements(row, row_ptr, 0);

  return (ret == 1);
}


JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_Encrypt(JNIEnv *env, 
									  jobject obj, 
									  jlong eid, 
									  jbyteArray plaintext) {

  uint32_t plength = (uint32_t) env->GetArrayLength(plaintext);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(plaintext, &if_copy);
  
  uint8_t *plaintext_ptr = (uint8_t *) ptr;

  const jsize clength = plength + SGX_AESGCM_IV_SIZE + SGX_AESGCM_MAC_SIZE;
  jbyteArray ciphertext = env->NewByteArray(clength);
  uint8_t *ciphertext_ptr = (uint8_t *) env->GetByteArrayElements(plaintext, &if_copy);

  uint8_t ciphertext_copy[2048];
  
  //printf("Encrypt(): plength is %u\n", plength);

  ecall_encrypt(eid, plaintext_ptr, plength, ciphertext_copy, (uint32_t) clength);

  env->SetByteArrayRegion(ciphertext, 0, clength, (jbyte *) ciphertext_copy);

  env->ReleaseByteArrayElements(plaintext, ptr, 0);

  return ciphertext;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_Decrypt(JNIEnv *env, 
									  jobject obj, 
									  jlong eid, 
									  jbyteArray ciphertext) {

  uint32_t clength = (uint32_t) env->GetArrayLength(ciphertext);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(ciphertext, &if_copy);
  
  uint8_t *ciphertext_ptr = (uint8_t *) ptr;
 
  const jsize plength = clength - SGX_AESGCM_IV_SIZE - SGX_AESGCM_MAC_SIZE;
  jbyteArray plaintext = env->NewByteArray(plength);

  uint8_t plaintext_copy[2048];
  
  ecall_decrypt(eid, ciphertext_ptr, clength, plaintext_copy, (uint32_t) plength);

  env->SetByteArrayRegion(plaintext, 0, plength, (jbyte *) plaintext_copy);

  env->ReleaseByteArrayElements(ciphertext, ptr, 0);

  return plaintext;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_EncryptAttribute(JNIEnv *env, 
										   jobject obj, 
										   jlong eid, 
										   jbyteArray plaintext) {
  
  uint32_t plength = (uint32_t) env->GetArrayLength(plaintext);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(plaintext, &if_copy);
  
  uint8_t *plaintext_ptr = (uint8_t *) ptr;

  uint32_t ciphertext_length = 4 + ENC_HEADER_SIZE + HEADER_SIZE + ATTRIBUTE_UPPER_BOUND;
  uint8_t *ciphertext_copy = (uint8_t *) malloc(ciphertext_length);

  uint32_t actual_size = 0;
  
  ecall_encrypt_attribute(eid, plaintext_ptr, plength,
			  ciphertext_copy, (uint32_t) ciphertext_length,
			  &actual_size);

  //printf("actual size is %u, type is %u\n", actual_size, *plaintext_ptr);

  jbyteArray ciphertext = env->NewByteArray(actual_size - 4);
  env->SetByteArrayRegion(ciphertext, 0, actual_size - 4, (jbyte *) (ciphertext_copy + 4));

  env->ReleaseByteArrayElements(plaintext, ptr, 0);

  free(ciphertext_copy);

  return ciphertext;
}

JNIEXPORT void JNICALL SGX_CDECL Java_org_apache_spark_sql_SGXEnclave_Test(JNIEnv *env, jobject obj, jlong eid) {
  int input = 0;
  //ecall_test_int(eid, &input);
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
  
  uint32_t input_len = (uint32_t) env->GetArrayLength(input);
  jboolean if_copy = false;
  jbyte *ptr = env->GetByteArrayElements(input, &if_copy);

  uint8_t *input_copy = (uint8_t *) malloc(input_len);
  uint8_t *scratch = (uint8_t *) malloc(num_items * (ENC_HEADER_SIZE + ROW_UPPER_BOUND));


  for (int i = 0; i < input_len; i++) {
    input_copy[i] = *(ptr + i);
    //printf("input_copy is %u\n", input_copy[i]);
  }

  //printf("input len is %u, MAX_SORT_BUFFER is %u\n", input_len, MAX_SORT_BUFFER);

  if (input_len < MAX_SINGLE_SORT_BUFFER) {

    uint8_t *buffer_list[1] = {input_copy};
    uint32_t buffer_sizes[1] = {input_len};
    uint32_t num_rows[1];
    num_rows[0] = (uint32_t) num_items;

    uint64_t t = 0;
    {
      scoped_timer timer(&t);
      sgx_status_t status = ecall_external_oblivious_sort(eid, op_code, 1, buffer_list, buffer_sizes, num_rows, scratch);
    }

    double t_ms = ((double) t) / 1000;
    printf("Sorting %u items, input_len is %u; sorting took %f ms\n", num_items, input_len, t_ms);
    //printf("Sort took %f ms\n", t_ms);
	
  } else {

    // try to split the input into partitions if it's too big
    uint32_t element_size = input_len / num_items;
    uint32_t elements_per_part = PAR_MAX_ELEMENTS;
    
    uint32_t num_part = num_items / elements_per_part;
    if (input_len % elements_per_part != 0) {
      num_part += 1;
    }
    
    //printf("input_len is %u, element_size is %u, num part is %u, num_items: %u, elements_per_part: %u\n", input_len, element_size, num_part, num_items, elements_per_part);

    uint8_t **buffer_list = (uint8_t **) malloc(sizeof(uint8_t *) * num_part);
    uint32_t *buffer_sizes = (uint32_t *) malloc(sizeof(uint32_t) * num_part);
    uint32_t *num_rows = (uint32_t *) malloc(sizeof(uint32_t) * num_part);
    
    uint8_t *input_ptr = input_copy;
    
    for (uint32_t i = 0 ; i < num_part; i++) {
      buffer_list[i] = input_ptr;
      if (i == num_part - 1) {
	num_rows[i] = num_items - elements_per_part * i;
	buffer_sizes[i] = input_len - (input_ptr - input_copy);
      } else {
	num_rows[i] = elements_per_part;
	buffer_sizes[i] = elements_per_part * element_size;
      }
      
      input_ptr += buffer_sizes[i];
    }

    uint64_t t = 0;
    {    
      scoped_timer timer(&t);
      sgx_status_t status = ecall_external_oblivious_sort(eid, op_code, num_part,
							  buffer_list, buffer_sizes, num_rows, scratch);
    }

    double t_ms = ((double) t) / 1000;
    printf("Sorting %u items, input_len is %u; sorting took %f ms\n", num_items, input_len, t_ms);
    //printf("Sort took %f ms\n", t_ms);

    free(buffer_list);
    free(buffer_sizes);
    free(num_rows);
  }

  jbyteArray ret = env->NewByteArray(input_len);
  env->SetByteArrayRegion(ret, 0, input_len, (jbyte *) input_copy);

  env->ReleaseByteArrayElements(input, ptr, 0);

  free(input_copy);
  free(scratch);
  
  return ret;
}


JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_RandomID(JNIEnv *env,
									   jobject obj,
									   jlong eid) {
  
  // size should be SGX
  const uint32_t random_id_length = ENC_HEADER_SIZE + HEADER_SIZE + 4;
  jbyteArray ret = env->NewByteArray(ENC_HEADER_SIZE + HEADER_SIZE + 4);
  jboolean if_copy;
  jbyte *ptr = env->GetByteArrayElements(ret, &if_copy);

  uint8_t buf[random_id_length];
  ecall_random_id(eid, buf, random_id_length);

  env->SetByteArrayRegion(ret, 0, random_id_length, (jbyte *) buf);

  return ret;

}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_Aggregate(JNIEnv *env, 
									    jobject obj, 
									    jlong eid,
									    jint op_code,
									    jbyteArray input_rows,
									    jint num_rows,
									    jbyteArray agg_row) {

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  jboolean if_copy;
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t agg_row_length = (uint32_t) env->GetArrayLength(agg_row);
  uint8_t *agg_row_ptr = (uint8_t *) env->GetByteArrayElements(agg_row, &if_copy);
  
  uint32_t actual_size = 0;
  int flag;
  if (op_code == OP_GROUPBY_COL2_SUM_COL3_STEP1 ||
      op_code == OP_GROUPBY_COL1_SUM_COL2_STEP1 ||
      op_code == OP_GROUPBY_COL1_AVG_COL2_SUM_COL3_STEP1) {
    flag = 1;
  } else if (op_code == OP_GROUPBY_COL2_SUM_COL3_STEP2 ||
             op_code == OP_GROUPBY_COL1_SUM_COL2_STEP2 ||
             op_code == OP_GROUPBY_COL1_AVG_COL2_SUM_COL3_STEP2) {
    flag = 2;
  } else {
    printf("Aggregate: unknown opcode %d\n", op_code);
    assert(false);
  }

  // output rows length should be input_rows length + num_rows * PARTIAL_AGG_UPPER_BOUND
  
  uint32_t output_rows_length = 2048 + 12 + 16 + 2048;
  uint8_t *output_rows = NULL;

  if (flag == 1) {
    output_rows = (uint8_t *) malloc(output_rows_length);
  } else {
    // TODO: change this hard-coded buffer
    uint32_t real_size = 4 + 12 + 16 + 4 + 4 + 2048 + 128;
    output_rows_length = num_rows  * real_size;
    output_rows = (uint8_t *) malloc(4 + output_rows_length);
  }

  uint64_t t = 0;
  {
    scoped_timer timer(&t);
    ecall_scan_aggregation_count_distinct(eid, op_code,
					  input_rows_ptr, input_rows_length,
					  num_rows,
					  agg_row_ptr, agg_row_length,
					  output_rows + 4, output_rows_length,
					  &actual_size,
					  flag,
					  (uint32_t *) output_rows);
  }

  double t_ms = ((double) t) / 1000;
  printf("Enclave aggregation took %f ms\n", t_ms);
  
  // printf("alloc size is %u, actual_size is %u, num_rows is %u\n", output_rows_length, actual_size, num_rows);

  jbyteArray ret = env->NewByteArray(actual_size);
  env->SetByteArrayRegion(ret, 0, actual_size, (jbyte *) (output_rows + 4));

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  return ret;
}

void print_bytes_(uint8_t *ptr, uint32_t len) {
  
  for (int i = 0; i < len; i++) {
    printf("%u", *(ptr + i));
    printf(" - ");
  }

  printf("\n");
}


JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_ProcessBoundary(JNIEnv *env, 
										  jobject obj, 
										  jlong eid,
										  jint op_code,
										  jbyteArray rows,
										  jint num_rows) {
  
  jboolean if_copy;
  
  uint32_t rows_length = (uint32_t) env->GetArrayLength(rows);
  uint8_t *rows_ptr = (uint8_t *) env->GetByteArrayElements(rows, &if_copy);
  
  
  // output rows length should be input_rows length + num_rows * PARTIAL_AGG_UPPER_BOUND
  //uint32_t real_size = 4 + 12 + 16 + 4 + 4 + 2048 + 128;
  uint32_t single_row_size = 4 + ENC_HEADER_SIZE + AGG_UPPER_BOUND;
  single_row_size += ENC_HEADER_SIZE + ROW_UPPER_BOUND;
  uint32_t out_agg_rows_length = single_row_size * num_rows;
  // printf("single row size is %u\n", single_row_size);
  
  uint8_t *out_agg_rows = (uint8_t *) malloc(out_agg_rows_length);
  uint32_t actual_out_agg_rows_size = 0;
  
  ecall_process_boundary_records(eid, op_code,
  								 rows_ptr, rows_length,
  								 num_rows,
  								 out_agg_rows, out_agg_rows_length,
								 &actual_out_agg_rows_size);

  jbyteArray ret = env->NewByteArray(actual_out_agg_rows_size);
  env->SetByteArrayRegion(ret, 0, actual_out_agg_rows_size, (jbyte *) out_agg_rows);

  env->ReleaseByteArrayElements(rows, (jbyte *) rows_ptr, 0);

  free(out_agg_rows);

  return ret;
}



JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_FinalAggregation(JNIEnv *env, 
																				   jobject obj, 
																				   jlong eid,
																				   jint op_code,
																				   jbyteArray rows,
																				   jint num_rows) {
  /*
  
  jboolean if_copy;
  
  uint32_t rows_length = (uint32_t) env->GetArrayLength(rows);
  uint8_t *rows_ptr = (uint8_t *) env->GetByteArrayElements(rows, &if_copy);
  
  
  // output rows length should be input_rows length + num_rows * PARTIAL_AGG_UPPER_BOUND
  uint32_t real_size = ENC_HEADER_SIZE + AGG_UPPER_BOUND;
  uint32_t out_agg_rows_length = real_size * num_rows;
  
  uint8_t *out_agg_rows = (uint8_t *) malloc(out_agg_rows_length);
  
  ecall_process_boundary_records(eid, op_code,
  								 rows_ptr, rows_length,
  								 num_rows,
  								 out_agg_rows, out_agg_rows_length);

  jbyteArray ret = env->NewByteArray(out_agg_rows_length);
  env->SetByteArrayRegion(ret, 0, out_agg_rows_length, (jbyte *) out_agg_rows);

  env->ReleaseByteArrayElements(rows, (jbyte *) rows_ptr, 0);

  free(out_agg_rows);

  return ret;
  */
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_JoinSortPreprocess(JNIEnv *env, 
										     jobject obj, 
										     jlong eid,
										     jint op_code,
										     jbyteArray enc_table_id,
										     jbyteArray input_rows,
										     jint num_rows) {
  jboolean if_copy;
  
  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);
  uint32_t row_length = 0;
  
  uint32_t single_row_length = ENC_HEADER_SIZE + JOIN_ROW_UPPER_BOUND;
  uint32_t output_rows_length = single_row_length * num_rows;
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);
  uint8_t *output_rows_ptr = output_rows;

  uint8_t *enc_table_id_ptr = (uint8_t *) env->GetByteArrayElements(enc_table_id, &if_copy);

  // try to call on each row individually

  //printf("Preprocess 1, num_rows is %u\n", num_rows);

  ecall_join_sort_preprocess(eid,
			     op_code,
			     enc_table_id_ptr, 
			     input_rows_ptr, input_rows_length,
			     num_rows, 
			     output_rows_ptr, output_rows_length);

   
  jbyteArray ret = env->NewByteArray(output_rows_length);
  env->SetByteArrayRegion(ret, 0, output_rows_length, (jbyte *) output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  //printf("Preprocess 2\n");

  return ret;  
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_ProcessJoinBoundary(
  JNIEnv *env, jobject obj, jlong eid, jint op_code, jbyteArray input_rows, jint num_rows) {

  jboolean if_copy;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);
  uint32_t row_length = 0;

  uint32_t single_row_length = ENC_HEADER_SIZE + JOIN_ROW_UPPER_BOUND;
  uint32_t output_rows_length = single_row_length * num_rows;
  uint8_t *output_rows = (uint8_t *) malloc(output_rows_length);
  uint8_t *output_rows_ptr = output_rows;
  uint32_t actual_output_length = 0;

  ecall_process_join_boundary(eid, op_code, input_rows_ptr, input_rows_length, num_rows,
                              output_rows_ptr, output_rows_length, &actual_output_length);

  jbyteArray ret = env->NewByteArray(actual_output_length);
  env->SetByteArrayRegion(ret, 0, actual_output_length, (jbyte *) output_rows);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output_rows);

  return ret;
}

JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_ScanCollectLastPrimary(JNIEnv *env, 
																						 jobject obj, 
																						 jlong eid,
																						 jint op_code,
																						 jbyteArray input_rows,
																						 jint num_rows) {
  jboolean if_copy;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t output_length = ENC_HEADER_SIZE + JOIN_ROW_UPPER_BOUND;
  uint8_t *output = (uint8_t *) malloc(output_length);

  //printf("scan_collect start\n");
  
  ecall_scan_collect_last_primary(eid,
								  op_code,
								  input_rows_ptr, input_rows_length,
  								  num_rows,
  								  output, output_length);

  jbyteArray ret = env->NewByteArray(output_length);
  env->SetByteArrayRegion(ret, 0, output_length, (jbyte *) output);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output);

  //printf("scan_collect done\n");

  return ret;
}


JNIEXPORT jbyteArray JNICALL Java_org_apache_spark_sql_SGXEnclave_SortMergeJoin(JNIEnv *env, 
										jobject obj, 
										jlong eid,
										jint op_code,
										jbyteArray input_rows,
                                                                                jint num_rows,
                                                                                jbyteArray join_row) {

  jboolean if_copy;

  uint32_t input_rows_length = (uint32_t) env->GetArrayLength(input_rows);
  uint8_t *input_rows_ptr = (uint8_t *) env->GetByteArrayElements(input_rows, &if_copy);

  uint32_t output_length = (ENC_HEADER_SIZE + 2 * JOIN_ROW_UPPER_BOUND) * num_rows;
  uint8_t *output = (uint8_t *) malloc(output_length);

  uint32_t join_row_length = (uint32_t) env->GetArrayLength(join_row);
  uint8_t *join_row_ptr = (uint8_t *) env->GetByteArrayElements(join_row, &if_copy);

  uint32_t actual_output_length = 0;

  ecall_sort_merge_join(eid,
						op_code,
						input_rows_ptr, input_rows_length,
						num_rows,
                        join_row_ptr, join_row_length,
                        output, output_length, &actual_output_length);

  jbyteArray ret = env->NewByteArray(actual_output_length);
  env->SetByteArrayRegion(ret, 0, actual_output_length, (jbyte *) output);

  env->ReleaseByteArrayElements(input_rows, (jbyte *) input_rows_ptr, 0);

  free(output);

  return ret;
	
}

uint32_t enc_size(uint32_t len) {
  return len + ENC_HEADER_SIZE;
}

uint32_t format_encrypt_row(uint8_t *row, uint32_t index, uint32_t num_cols) {
  static char chars[5] = {'A', 'B', 'C', 'D', 'E'};
  uint8_t temp[1024];

  uint8_t *row_ptr = row;
  uint32_t len = 0;

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

uint32_t format_row(uint8_t *row, uint32_t index, uint32_t num_cols) {
  static char chars[3] = {'A', 'B', 'C'};
  uint8_t temp[1024];

  uint8_t *row_ptr = row;
  uint32_t len = 0;

  *( (uint32_t *) row_ptr) = num_cols;
  row_ptr += 4;
  
  // [int][string][int]
  *row_ptr = INT;
  *( (uint32_t *) (row_ptr + 1)) = 4;
  *( (uint32_t *) (row_ptr + 5)) = index;
  row_ptr += 1 + 4 + 4;  
  
  *row_ptr = INT;
  *( (uint32_t *) (row_ptr + 1)) = 4;
  *( (uint32_t *) (row_ptr + 5)) = (uint32_t ) (rand());
  row_ptr += 1 + 4 + 4;

  *row_ptr = STRING;
  *( (uint32_t *) (row_ptr + 1)) = 1;
  *((char *) (row_ptr + 5)) = chars[index % 3];
  row_ptr += HEADER_SIZE + STRING_UPPER_BOUND;


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

void test_enclave_sort() {
  // use op_code = OP_SORT_COL2

  srand(time(NULL));

  int op_code = OP_SORT_COL2;
  uint32_t total_num_rows = 250 * 1024;
  uint32_t num_cols = 3;
  // [int][string][int]
  uint32_t single_row_size = 4 + num_cols * 4 + enc_size(HEADER_SIZE + 4) * 2 + enc_size(HEADER_SIZE + STRING_UPPER_BOUND);
  uint32_t single_row_plaintext_size = 4 + num_cols * 4 + (HEADER_SIZE + 4) * 2 + (HEADER_SIZE + STRING_UPPER_BOUND);
  printf("num items: %u, single_row_size is %u, total data sorted: %u\n", total_num_rows, single_row_size, total_num_rows * single_row_size);
  uint8_t *input_rows = (uint8_t *) malloc(single_row_size * total_num_rows);
  
  uint8_t *enc_data = (uint8_t *) malloc(ROW_UPPER_BOUND * total_num_rows + ENC_HEADER_SIZE * total_num_rows);
  uint32_t actual_size = 0;

  uint64_t t = 0;

  uint8_t *input_rows_ptr = input_rows;
  uint32_t offset = 0;

  {
    scoped_timer timer(&t);
    for (uint32_t i = 0; i < total_num_rows; i++) {
      offset = format_encrypt_row(input_rows_ptr, i, num_cols);
      input_rows_ptr += offset;
    }
  }

  double t_ms = ((double) t) / 1000;
  printf("Encryption took %f ms\n", t_ms);
  
  printf("Encryption done\n");
  // split the input rows into 64 partitions of (1024 * 4) rows
  if (total_num_rows * ROW_UPPER_BOUND < MAX_SINGLE_SORT_BUFFER) {
    printf("Single round sort called\n");

    const uint32_t num_part = 1;
    uint8_t *buffer_list[1];
    uint32_t buffer_sizes[1];
    uint32_t num_rows[1];

    buffer_list[0] = input_rows;
    buffer_sizes[0] = single_row_size * total_num_rows;
    num_rows[0] = total_num_rows;

    t = 0;
    {
      scoped_timer timer(&t);
      sgx_status_t status = ecall_external_oblivious_sort(global_eid, op_code,
							  num_part,
							  buffer_list, buffer_sizes, num_rows,
							  enc_data);
      print_error_message(status);
    }
  
    t_ms = ((double) t) / 1000;
    printf("Sort took %f ms\n", t_ms);

  
  } else {
    printf("Multi-round sort called\n");

    const uint32_t num_part = total_num_rows / PAR_MAX_ELEMENTS + 1;

    uint8_t *buffer_list[num_part];
    uint32_t buffer_sizes[num_part];
    uint32_t num_rows[num_part];

    input_rows_ptr = input_rows;

    for (uint32_t i = 0 ; i < num_part; i++) {
      buffer_list[i] = input_rows_ptr;
      if (i == num_part - 1) {
	num_rows[i] = total_num_rows - PAR_MAX_ELEMENTS * i;
	buffer_sizes[i] = single_row_size * total_num_rows - (input_rows_ptr - input_rows);
      } else {
	num_rows[i] = PAR_MAX_ELEMENTS;
	buffer_sizes[i] = PAR_MAX_ELEMENTS * single_row_size;
      }
    
      input_rows_ptr += buffer_sizes[i];
    }

    t = 0;
    {
      scoped_timer timer(&t);
      sgx_status_t status = ecall_external_oblivious_sort(global_eid, op_code,
							  num_part,
							  buffer_list, buffer_sizes, num_rows,
							  enc_data);
      print_error_message(status);
    }
  
    t_ms = ((double) t) / 1000;
    printf("Sort took %f ms\n", t_ms);
  }

  decrypt_and_print(input_rows, total_num_rows, num_cols);

  free(enc_data);
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
    }[
#endif 
    
    /* Initialize the enclave */
    if(initialize_enclave() < 0){
        printf("Enter a character before exit ...\n");
        getchar();
        return -1; 
    }

	// sgx_status_t status = ecall_test(global_eid);
	// print_error_message(status);
	
	test_enclave_sort();

    /* Destroy the enclave */
    sgx_destroy_enclave(global_eid);
    
    printf("Info: SampleEnclave successfully returned.\n");

    return 0;
}


