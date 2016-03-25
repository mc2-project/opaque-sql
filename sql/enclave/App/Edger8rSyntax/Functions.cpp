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

/* No need to implement memccpy here! */

/* edger8r_function_attributes:
 *   Invokes ECALL declared with calling convention attributes.
 *   Invokes ECALL declared with [public].
 */
void edger8r_function_attributes(void)
{
    sgx_status_t ret = SGX_ERROR_UNEXPECTED;

    ret = ecall_function_calling_convs(global_eid);
    if (ret != SGX_SUCCESS)
        abort();
    
    ret = ecall_function_public(global_eid);
    if (ret != SGX_SUCCESS)
        abort();
    
    /* user shall not invoke private function here */
    int runned = 0;
    ret = ecall_function_private(global_eid, &runned);
    if (ret != SGX_ERROR_ECALL_NOT_ALLOWED || runned != 0)
        abort();
}

/* ocall_function_allow:
 *   The OCALL invokes the [allow]ed ECALL 'edger8r_private'.
 */
void ocall_function_allow(void)
{
    int runned = 0;
    sgx_status_t ret = SGX_ERROR_UNEXPECTED;
    
    ret = ecall_function_private(global_eid, &runned);
    if (ret != SGX_SUCCESS || runned != 1)
        abort();
}
