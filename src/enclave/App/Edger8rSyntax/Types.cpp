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

/* edger8r_type_attributes:
 *   Invokes ECALLs declared with basic types.
 */
void edger8r_type_attributes(void)
{
    sgx_status_t ret = SGX_ERROR_UNEXPECTED;

    ret = ecall_type_char(global_eid, (char)0x12);
    if (ret != SGX_SUCCESS)
        abort();

    ret = ecall_type_int(global_eid, (int)1234);
    if (ret != SGX_SUCCESS)
        abort();

    ret = ecall_type_float(global_eid, (float)1234.0);
    if (ret != SGX_SUCCESS)
        abort();

    ret = ecall_type_double(global_eid, (double)1234.5678);
    if (ret != SGX_SUCCESS)
        abort();

    ret = ecall_type_size_t(global_eid, (size_t)12345678);
    if (ret != SGX_SUCCESS)
        abort();

    ret = ecall_type_wchar_t(global_eid, (wchar_t)0x1234);
    if (ret != SGX_SUCCESS)
        abort();

    struct struct_foo_t g = {1234, 5678};
    ret = ecall_type_struct(global_eid, g);
    if (ret != SGX_SUCCESS)
        abort();
    
    union union_foo_t val = {0};
    ret = ecall_type_enum_union(global_eid, ENUM_FOO_0, &val);
    if (ret != SGX_SUCCESS)
        abort();
    assert(val.union_foo_0 == 2);
}
