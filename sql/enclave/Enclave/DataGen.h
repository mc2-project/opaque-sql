#include "NewInternalTypes.h"
#include "common.h"

#ifndef _DATAGEN_H_
#define _DATAGEN_H_


uint32_t generate_encrypted_block(uint32_t num_cols, uint8_t *column_types,
				  uint32_t num_rows,
				  uint8_t *output_buffer,
				  uint8_t type);

uint32_t generate_encrypted_block_with_opcode(uint32_t num_cols, uint8_t *column_types,
					      uint32_t num_rows,
					      uint8_t *output_buffer,
					      uint8_t type,
					      uint32_t opcode);

#endif
