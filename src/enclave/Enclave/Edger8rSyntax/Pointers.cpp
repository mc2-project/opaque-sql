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

/* Test Pointer Auttributes */

#include <sys/types.h>
#include <string.h>

#include "sgx_trts.h"
#include "../Enclave.h"
#include "Enclave_t.h"

/* checksum_internal:
 *   get simple checksum of input buffer and length
 */
int32_t checksum_internal(char *buf, size_t count)
{
    register int32_t sum = 0;
    int16_t *ptr = (int16_t *)buf;

    /* Main summing loop */
    while(count > 1) {
        sum = sum + *ptr++;
        count = count - 2;
    }

    /* Add left-over byte, if any */
    if (count > 0)
        sum = sum + *((char *)ptr);

	return ~sum;
}

/* ecall_pointer_user_check, ecall_pointer_in, ecall_pointer_out, ecall_pointer_in_out:
 *   The root ECALLs to test [in], [out], [user_check] attributes.
 */
size_t ecall_pointer_user_check(void *val, size_t sz)
{
    /* check if the buffer is allocated outside */
    if (sgx_is_outside_enclave(val, sz) != 1)
        abort();

    char tmp[100] = {0};
    size_t len = sz>100?100:sz;
    
    /* copy the memory into the enclave to make sure 'val' 
     * is not being changed in checksum_internal() */
    memcpy(tmp, val, len);
    
    int32_t sum = checksum_internal((char *)tmp, len);
    printf("Checksum(0x%p, %zu) = 0x%x\n", 
            val, len, sum);
    
    /* modify outside memory directly */
    memcpy(val, "SGX_SUCCESS", len>12?12:len);

	return len;
}

/* ecall_pointer_in:
 *   the buffer of val is copied to the enclave.
 */

void ecall_pointer_in(int *val)
{
    if (sgx_is_within_enclave(val, sizeof(int)) != 1)
        abort();
    *val = 1234;
}

/* ecall_pointer_out:
 *   the buffer of val is copied to the untrusted side.
 */
void ecall_pointer_out(int *val)
{
    if (sgx_is_within_enclave(val, sizeof(int)) != 1)
        abort();
    assert(*val == 0);
    *val = 1234;
}

/* ecall_pointer_in_out:
 * the buffer of val is double-copied.
 */
void ecall_pointer_in_out(int *val)
{
    if (sgx_is_within_enclave(val, sizeof(int)) != 1)
        abort();
    *val = 1234;
}

/* ocall_pointer_attr:
 *   The root ECALL that test OCALL [in], [out], [user_check].
 */
void ocall_pointer_attr(void)
{
    sgx_status_t ret = SGX_ERROR_UNEXPECTED;

    int val = 0;
    ret = ocall_pointer_user_check(&val);
    if (ret != SGX_SUCCESS)
        abort();

    val = 0;
    ret = ocall_pointer_in(&val);
    if (ret != SGX_SUCCESS)
        abort();
    assert(val == 0);

    val = 0;
    ret = ocall_pointer_out(&val);
    if (ret != SGX_SUCCESS)
        abort();
    assert(val == 1234);

    val = 0;
    ret = ocall_pointer_in_out(&val);
    if (ret != SGX_SUCCESS)
        abort();
    assert(val == 1234);

    return;
}

/* ecall_pointer_string:
 *   [string] defines a string.
 */
void ecall_pointer_string(char *str)
{
    strncpy(str, "0987654321", strlen(str));
}

/* ecall_pointer_string_const:
 *   const [string] defines a string that cannot be modified.
 */
void ecall_pointer_string_const(const char *str)
{
    char* temp = new char[strlen(str)];
    strncpy(temp, str, strlen(str));
    delete []temp;
}

/* ecall_pointer_size:
 *   'len' needs to be specified to tell Edger8r the length of 'str'.
 */
void ecall_pointer_size(void *ptr, size_t len)
{
    strncpy((char*)ptr, "0987654321", len);
}

/* ecall_pointer_count:
 *   'cnt' needs to be specified to tell Edger8r the number of elements in 'arr'.
 */
void ecall_pointer_count(int *arr, int cnt)
{
    for (int i = (cnt - 1); i >= 0; i--)
        arr[i] = (cnt - 1 - i);
}

/* ecall_pointer_isptr_readonly:
 *   'buf' is user defined type, shall be tagged with [isptr].
 *   if it's not writable, [readonly] shall be specified. 
 */
void ecall_pointer_isptr_readonly(buffer_t buf, size_t len)
{
    strncpy((char*)buf, "0987654321", len);
}

/* get_buffer_len:
 *   get the length of input buffer 'buf'.
 */
size_t get_buffer_len(const char* buf)
{
    (void)buf;
    return 10*sizeof(int);
}

/* ecall_pointer_sizefunc:
 *   call get_buffer_len to determin the length of 'buf'.
 */
void ecall_pointer_sizefunc(char *buf)
{
    int *tmp = (int*)buf;
    for (int i = 0; i < 10; i++) {
        assert(tmp[i] == 0);
        tmp[i] = i;
    }
}
