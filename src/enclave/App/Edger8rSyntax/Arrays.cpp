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

#include "../App.h"
#include "Enclave_u.h"

/* edger8r_array_attributes:
 *   Invokes ECALLs declared with array attributes.
 */
void edger8r_array_attributes(void)
{
    sgx_status_t ret = SGX_ERROR_UNEXPECTED;

    /* user_check */
    int arr1[4] = {0, 1, 2, 3};
    ret = ecall_array_user_check(global_eid, arr1);
    if (ret != SGX_SUCCESS)
        abort();

    /* make sure arr1 is changed */
    for (int i = 0; i < 4; i++)
        assert(arr1[i] == (3 - i));

    /* in */
    int arr2[4] = {0, 1, 2, 3};
    ret = ecall_array_in(global_eid, arr2);
    if (ret != SGX_SUCCESS)
        abort();
    
    /* arr2 is not changed */
    for (int i = 0; i < 4; i++)
        assert(arr2[i] == i);
    
    /* out */
    int arr3[4] = {0, 1, 2, 3};
    ret = ecall_array_out(global_eid, arr3);
    if (ret != SGX_SUCCESS)
        abort();
    
    /* arr3 is changed */
    for (int i = 0; i < 4; i++)
        assert(arr3[i] == (3 - i));
    
    /* in, out */
    int arr4[4] = {0, 1, 2, 3};
    ret = ecall_array_in_out(global_eid, arr4);
    if (ret != SGX_SUCCESS)
        abort();
    
    /* arr4 is changed */
    for (int i = 0; i < 4; i++)
        assert(arr4[i] == (3 - i));
    
    /* isary */
    array_t arr5 = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    ret = ecall_array_isary(global_eid, arr5);
    if (ret != SGX_SUCCESS)
        abort();
    
    /* arr5 is changed */
    for (int i = 0; i < 10; i++)
        assert(arr5[i] == (9 - i));
}
