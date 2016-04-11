#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include "sgx_trts.h"
#include "math.h"
#include "Crypto.h"

#ifndef JOIN_H
#define JOIN_H

void sort_merge_join(int op_code,
					 uint8_t *input_rows, uint32_t input_rows_length,
					 uint32_t num_rows,
					 uint8_t *output, uint32_t output_length,
					 uint8_t *enc_table_p, uint8_t *enc_table_f);

#endif
