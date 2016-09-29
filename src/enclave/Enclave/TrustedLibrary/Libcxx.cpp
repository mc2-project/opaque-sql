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

#include <cstdlib>
#include <string>

#include "../Enclave.h"
#include "Enclave_t.h"

/*
 * ecall_exception:
 *   throw/catch C++ exception inside the enclave.
 */

void ecall_exception(void)
{
    std::string foo = "foo";
    try {
        throw std::runtime_error(foo);
    }
    catch (std::runtime_error const& e) {
        assert( foo == e.what() );
        std::runtime_error clone("");
        clone = e;
        assert(foo == clone.what() );
    }
    catch (...) {
        assert( false );
    }
}

#include <map>
#include <algorithm>

using namespace std;

/*
 * ecall_map:
 *   Utilize STL <map> in the enclave.
 */
void ecall_map(void)
{
    typedef map<char, int, less<char> > map_t;
    typedef map_t::value_type map_value;
    map_t m;

    m.insert(map_value('a', 1));
    m.insert(map_value('b', 2));
    m.insert(map_value('c', 3));
    m.insert(map_value('d', 4));

    assert(m['a'] == 1);
    assert(m['b'] == 2);
    assert(m['c'] == 3);
    assert(m['d'] == 4);

    assert(m.find('e') == m.end());
    
    return;
}
