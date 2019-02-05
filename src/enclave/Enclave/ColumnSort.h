using namespace edu::berkeley::cs::rise::opaque;

void shift_up(uint8_t *input_rows, size_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_rows_length);

void shift_down(uint8_t *input_rows, size_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_rows_length);

void transpose(uint8_t *input_rows, size_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_rows_length);

void untranspose(uint8_t *input_rows, size_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_rows_length);