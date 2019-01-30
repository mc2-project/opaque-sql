using namespace edu::berkeley::cs::rise::opaque;

void shift_up(uint8_t *input_rows, size_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_rows_length) {

  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  uint32_t top_destination =
      (partition_idx == 0) ? num_partitions - 1 : partition_idx - 1;
  uint32_t bottom_destination = partition_idx;

  uint32_t i = 0;
  uint32_t n = r.num_rows();
  assert(n % 2 == 0);

  bool top_written = false, bottom_written = false;

  while (r.has_next()) {
    const tuix::Row *row = r.next();
    w.write(row);

    if (i + 1 == n / 2) {
        w.write_shuffle_output(w.write_encrypted_blocks(), top_destination);
        top_written = true;
    }
    if (i == n - 1) {
        w.write_shuffle_output(w.write_encrypted_blocks(), bottom_destination);
        bottom_written = true;
    }

    i++;
  }

  if (!top_written) {
      w.write_shuffle_output(w.write_encrypted_blocks(), top_destination);
  }
  if (!bottom_written) {
      w.write_shuffle_output(w.write_encrypted_blocks(), bottom_destination);
  }

  w.finish(w.write_shuffle_outputs());
  *output_rows = w.output_buffer().release();
  *output_rows_length = w.output_size();
}
