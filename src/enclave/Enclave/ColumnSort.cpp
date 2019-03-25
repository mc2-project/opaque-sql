#include "ColumnSort.h"
#include "Flatbuffers.h"
#include <vector>

void shift_up(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_row, size_t *output_row_size) {

  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  uint32_t top_destination =
    (partition_idx == 0) ? 0 : partition_idx - 1;
  uint32_t bottom_destination = (partition_idx == 0) ? num_partitions - 1 : partition_idx;

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
  *output_row = w.output_buffer().release();
  *output_row_size = w.output_size();
}

void shift_down(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_row, size_t *output_row_size) {
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  uint32_t top_destination = partition_idx;
  uint32_t bottom_destination = (partition_idx == num_partitions - 1) ? 0 : partition_idx + 1;


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
  *output_row = w.output_buffer().release();
  *output_row_size = w.output_size();
}

void transpose(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_row, size_t *output_row_size) {
  (void)partition_idx;
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);

  std::vector<std::unique_ptr<FlatbuffersRowWriter>> ws(num_partitions);

  // for (uint32_t k = 0; k < num_partitions; k++) {
  //   ws.emplace_back(std::unique_ptr<FlatbuffersRowWriter>(
  //     new FlatbuffersRowWriter()));
  // } 
  uint32_t i = 0;

  while (r.has_next()) {
    const tuix::Row *row = r.next();
    printf("looking for this index: %i\n", i % num_partitions);
    FlatbuffersRowWriter* rw = ws[i % num_partitions].get();
    rw->write(row);
    printf("wrote row\n");
    i++;
  }
  printf("wrote to all corresponding row writers");

  FlatbuffersRowWriter shuffle_output_writer;
  for (uint32_t j = 0; j < ws.size(); j++) {
    ws[j]->write_shuffle_output(ws[j]->write_encrypted_blocks(), j);
    std::unique_ptr<uint8_t, decltype(&ocall_free)> out_buffer = ws[j]->output_buffer();

    ShuffleOutputReader sor(out_buffer.get(), ws[j]->output_size());
    shuffle_output_writer.append_shuffle_output(sor.get());
  }
  printf("created shuffle outputs");
  shuffle_output_writer.finish(shuffle_output_writer.write_shuffle_outputs());
  *output_row = shuffle_output_writer.output_buffer().release();
  *output_row_size = shuffle_output_writer.output_size();
}

void untranspose(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_row, size_t *output_row_size) {
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  uint32_t n = r.num_rows();
  assert(n % 2 == 0);

  uint32_t row = 1;
  uint32_t col = partition_idx + 1;
  uint32_t idx = 0;
  uint32_t dst_column = 0;
  uint32_t dst_partition_idx = 0;
  uint32_t prev_dst_partition_idx = 0;

  while (r.has_next()) {
    const tuix::Row *in_row = r.next();
    w.write(in_row);

    idx = (row - 1) * num_partitions + col;
    dst_column = (idx - 1) / n + 1;
    dst_partition_idx = dst_column - 1;

    if (dst_partition_idx != prev_dst_partition_idx) {
      // Rows are going to a different partition
      w.write_shuffle_output(w.write_encrypted_blocks(), prev_dst_partition_idx);
    }

    prev_dst_partition_idx = dst_partition_idx;
    row++;
  }

  w.finish(w.write_shuffle_outputs());
  *output_row = w.output_buffer().release();
  *output_row_size = w.output_size(); 
}

void column_sort_pad(uint8_t *input_rows,
                         uint32_t input_rows_length,
                         uint32_t rows_per_partition,
                         uint8_t **output_row,
                         size_t *output_row_size) {
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;
  uint32_t num_rows = r.num_rows();

  const tuix::Row *row = NULL;

  while (r.has_next()) {
    row = r.next();
    w.write(row);
  }

  uint32_t num_dummies = rows_per_partition - num_rows;
  for (uint32_t i = 0; i < num_dummies; i++) {
    w.write_dummy_row(row);
  } 

  w.finish(w.write_encrypted_blocks());
  *output_row = w.output_buffer().release();
  *output_row_size = w.output_size(); 

}

void column_sort_filter(uint8_t *input_rows,
                         uint32_t input_rows_length,
                         uint8_t **output_row,
                         size_t *output_row_size) {
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  const tuix::Row *row = NULL;

  while (r.has_next()) {
    row = r.next();
    if (!row->is_dummy()) {
      w.write(row);
    }
  }

  w.finish(w.write_encrypted_blocks());
  *output_row = w.output_buffer().release();
  *output_row_size = w.output_size(); 
}



