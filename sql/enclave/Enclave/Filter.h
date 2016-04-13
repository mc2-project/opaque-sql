#include <stdarg.h>
#include <stdio.h>      /* vsnprintf */
#include <stdint.h>
#include <assert.h>

#include "sgx_trts.h"
#include "math.h"
#include "Crypto.h"
#include "util.h"
#include "InternalTypes.h"

#ifndef FILTER_H
#define FILTER_H

int ecall_filter_single_row(int op_code, uint8_t *row, uint32_t length);

#endif
