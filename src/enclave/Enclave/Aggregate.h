#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>

#include "Enclave.h"
#include "Enclave_t.h"  /* print_string */
#include "sgx_trts.h"
#include "Crypto.h"
#include "util.h"
#include "EncryptedDAG.h"

#ifndef AGGREGATE_H
#define AGGREGATE_H

template<typename AggregatorType>
void non_oblivious_aggregate(Verify *verify_set,
                             uint8_t *input_rows, uint32_t input_rows_length,
							 uint32_t num_rows,
							 uint8_t *output_rows, uint32_t output_rows_length,
                             uint32_t *actual_output_rows_length,
                             uint32_t *num_output_rows);

#include "Aggregate.tcc"

#endif // AGGREGATE_H
