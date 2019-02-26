/**
 * Sort an arbitrary number of encrypted input rows by decrypting a limited number of rows at a time
 * into enclave memory, sorting them using quicksort, and re-encrypting them to untrusted memory.
 * The granularity of decryption is a tuix::EncryptedBlock, which should fit entirely in enclave
 * memory.
 */
void oblivious_sort(uint8_t *sort_order, uint32_t sort_order_length,
                   uint8_t *input_rows, uint32_t input_rows_length,
                   uint8_t **output_row, uint32_t *output_row_length);
