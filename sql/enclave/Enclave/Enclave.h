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

#ifndef _ENCLAVE_H_
#define _ENCLAVE_H_

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <stdint.h>

#include "util.h"
#include "Aggregate.h"
#include "Crypto.h"
#include "Join.h"

#if defined(__cplusplus)
extern "C" {
#endif

  //void printf(const char *fmt, ...);
  int ecall_filter_single_row(int op_code, uint8_t *row, uint32_t length);
  void ecall_encrypt(uint8_t *plaintext, uint32_t plaintext_length,
		     uint8_t *ciphertext, uint32_t cipher_length);
  
  void ecall_decrypt(uint8_t *ciphertext, 
		     uint32_t cipher_length,
		     uint8_t *plaintext,
		     uint32_t plaintext_length);

  void ecall_random_id(uint8_t *ptr, uint32_t length);
  void ecall_oblivious_sort(int op_code, uint8_t *input, uint32_t buffer_length,
							int low_idx, uint32_t list_length);

#if defined(__cplusplus)
}
#endif

#endif /* !_ENCLAVE_H_ */
