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

/* Test Basic Types */

#include "sgx_trts.h"
#include "../Enclave.h"
#include "Enclave_t.h"
#include <limits>
#include <cmath>

/* used to eliminate `unused variable' warning */
#define UNUSED(val) (void)(val)

#define ULP 2

/* used to compare double variables in order to avoid compile warnings */
bool  almost_equal(double x, double y)
{
    /* the machine epsilon has to be scaled to the magnitude of the larger value
       and multiplied by the desired precision in ULPs (units in the last place) */
    return std::abs(x-y) <= std::numeric_limits<double>::epsilon() * std::abs(x+y) * ULP;
}

/* used to compare double variables in order to avoid compile warnings */
bool  almost_equal(float x, float y)
{
    /* the machine epsilon has to be scaled to the magnitude of the larger value
       and multiplied by the desired precision in ULPs (units in the last place) */
    return std::abs(x-y) <= std::numeric_limits<float>::epsilon() * std::abs(x+y) * ULP;
}

/* ecall_type_char:
 *   [char] value passed by App.
 */
void ecall_type_char(char val)
{
    assert(val == 0x12);
#ifndef DEBUG
    UNUSED(val);
#endif
}

/* ecall_type_int:
 *   [int] value passed by App.
 */
void ecall_type_int(int val)
{
    assert(val == 1234);
#ifndef DEBUG
    UNUSED(val);
#endif
}

/* ecall_type_float:
 *   [float] value passed by App.
 */
void ecall_type_float(float val)
{
    assert(almost_equal(val, (float)1234.0));
#ifndef DEBUG
    UNUSED(val);
#endif
}

/* ecall_type_double:
 *   [double] value passed by App.
 */
void ecall_type_double(double val)
{
    assert(almost_equal(val, (double)1234.5678));
#ifndef DEBUG
    UNUSED(val);
#endif
}

/* ecall_type_size_t:
 *   [size_t] value passed by App.
 */
void ecall_type_size_t(size_t val)
{
    assert(val == (size_t)12345678);
#ifndef DEBUG
    UNUSED(val);
#endif
}

/* ecall_type_wchar_t:
 *   [wchar_t] value passed by App.
 */
void ecall_type_wchar_t(wchar_t val)
{
    assert(val == (wchar_t)0x1234);
#ifndef DEBUG
    UNUSED(val);
#endif
}

/* ecall_type_struct:
 *   struct_foo_t is defined in EDL and can be used in ECALL.
 */
void ecall_type_struct(struct struct_foo_t val)
{
    assert(val.struct_foo_0 == 1234);
    assert(val.struct_foo_1 == 5678);
#ifndef DEBUG
    UNUSED(val);
#endif
}

/*
 * ecall_type_enum_union:
 *   enum_foo_t/union_foo_t is defined in EDL 
 *   and can be used in ECALL.
 */
void ecall_type_enum_union(enum enum_foo_t val1, union union_foo_t *val2)
{
    if (sgx_is_outside_enclave(val2, sizeof(union union_foo_t)) != 1)
        abort();
    val2->union_foo_0 = 1;
    val2->union_foo_1 = 2; /* overwrite union_foo_0 */
    assert(val1 == ENUM_FOO_0);
#ifndef DEBUG
    UNUSED(val1);
#endif
}
