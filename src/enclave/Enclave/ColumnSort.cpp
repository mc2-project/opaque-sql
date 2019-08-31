#include "ColumnSort.h"

#include <vector>

#include "ExpressionEvaluation.h"
#include "FlatbuffersReaders.h"
#include "FlatbuffersWriters.h"

/*
* Split each partition in half and shift each half "upwards"
* The top half of each partition goes to the previous partition
* The bottom half of each partition stays in the same partition
* Number of input rows must be even
*/
void shift_up(uint8_t *input_rows, uint32_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_row, size_t *output_row_size) {
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  ShuffleOutputWriter w;

  uint32_t top_destination;
  uint32_t bottom_destination;

  if (num_partitions == 1) {
    top_destination = partition_idx;
    bottom_destination = partition_idx;
  } else {
    top_destination = (partition_idx == 0) ? 0 : partition_idx - 1;
    bottom_destination = (partition_idx == 0) ? num_partitions - 1 : partition_idx;
  }

  uint32_t i = 0;
  uint32_t n = r.num_rows();

  assert(n % 2 == 0);

  bool top_written = false, bottom_written = false;
  while (r.has_next()) {
    const tuix::Row *row = r.next();
    w.append(row);
    if (i + 1 == n / 2) {
      w.finish_shuffle_output(top_destination);
      top_written = true;
    }
    if (i == n - 1) {
      w.finish_shuffle_output(bottom_destination);
      bottom_written = true;
    }

    i++;
  }

  if (!top_written) {
    w.finish_shuffle_output(top_destination);
  }
  if (!bottom_written) {
    w.finish_shuffle_output(bottom_destination);
  }

  w.output_buffer(output_row, output_row_size);
}

/*
* Split each partition in half and shift each half "downwards"
* The top half of each partition stays in the same partition
* The bottom half of each partition goes to the following partition
* The bottom half of the last partition loops around to the first partition
* Number of input rows must be even
*/ 
void shift_down(uint8_t *input_rows, uint32_t input_rows_length,
                uint32_t partition_idx, uint32_t num_partitions,
                uint8_t **output_row, size_t *output_row_size) {
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  ShuffleOutputWriter w;

  uint32_t top_destination;
  uint32_t bottom_destination;

  if (num_partitions == 1) {
    top_destination = partition_idx;
    bottom_destination = partition_idx;
  } else {
    top_destination = partition_idx;
    bottom_destination = (partition_idx == num_partitions - 1) ? 0 : partition_idx + 1;
  }
  
  uint32_t i = 0;
  uint32_t n = r.num_rows();
  assert(n % 2 == 0);
  bool top_written = false, bottom_written = false;
  while (r.has_next()) {
    const tuix::Row *row = r.next();
    w.append(row);
    if (i + 1 == n / 2) {
      w.finish_shuffle_output(top_destination);
      top_written = true;
    }
    if (i == n - 1) {
      w.finish_shuffle_output(bottom_destination);
      bottom_written = true;
    }
    i++;
  }

  if (!top_written) {
    w.finish_shuffle_output(top_destination);
  }
  if (!bottom_written) {
    w.finish_shuffle_output(bottom_destination);
  }

  w.output_buffer(output_row, output_row_size);
}

/*
* Read vertically, write horizontally
* i.e. for a matrix
*   1 2 3
*   4 5 6
*   7 8 9
* 1 goes to partition 1, 4 goes to partition 2, 7 goes to partition 3
* 2 goes to partition 1, 5 goes to partition 2, 8 goes to partition 3, etc
* Number of input rows must be even
*/
void transpose(uint8_t *input_rows, uint32_t input_rows_length,
               uint32_t partition_idx, uint32_t num_partitions,
               uint8_t **output_row, size_t *output_row_size) {
  (void)partition_idx;

  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  std::vector<RowWriter> ws(num_partitions);

  uint32_t i = 0;
  while (r.has_next()) {
    const tuix::Row *row = r.next();
    ws[i % num_partitions].append(row);
    i++;
  }

  ShuffleOutputWriter shuffle_output_writer;
  for (uint32_t j = 0; j < num_partitions; j++) {
    UntrustedBufferRef<tuix::EncryptedBlocks> partition = ws[j].output_buffer();
    RowReader partition_reader(partition.view());
    while (partition_reader.has_next()) {
      shuffle_output_writer.append(partition_reader.next());
    }
    shuffle_output_writer.finish_shuffle_output(j);
  }
  shuffle_output_writer.output_buffer(output_row, output_row_size);
}


/*
* Read horizontally, write vertically
* i.e. for a matrix
*   1 2 3
*   4 5 6
*   7 8 9
* 1 goes to partition 1, 2 goes to partition 1, 3 goes to partition 1
* 4 goes to partition 2, 5 goes to partition 2, 6 goes to partition 2, etc
* Number of input rows must be even
*/
void untranspose(uint8_t *input_rows, uint32_t input_rows_length,
                 uint32_t partition_idx, uint32_t num_partitions,
                 uint8_t **output_row, size_t *output_row_size) {
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  ShuffleOutputWriter w;

  uint32_t n = r.num_rows();
  assert(n % 2 == 0);

  uint32_t row = 1;
  uint32_t col = partition_idx + 1;
  uint32_t idx = 0;
  uint32_t dst_column = 0;
  uint32_t dst_partition_idx = 0;
  uint32_t prev_dst_partition_idx = 0;

  while (r.has_next()) {
    idx = (row - 1) * num_partitions + col;
    dst_column = (idx - 1) / n + 1;
    dst_partition_idx = dst_column - 1;

    if (dst_partition_idx != prev_dst_partition_idx) {
      // Rows are going to a different partition
      w.finish_shuffle_output(prev_dst_partition_idx);
    }

    const tuix::Row *in_row = r.next();
    w.append(in_row);

    prev_dst_partition_idx = dst_partition_idx;
    row++;
  }

  // Write shuffle output for the last chunk of rows
  w.finish_shuffle_output(prev_dst_partition_idx);

  w.output_buffer(output_row, output_row_size);
}

/*
* Pad with dummy rows so that each partition has the same number of rows
*/
void column_sort_pad(uint8_t *input_rows, uint32_t input_rows_length, uint32_t rows_per_partition,
                     uint8_t **output_row, size_t *output_row_size) {
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter w;
  uint32_t num_rows = r.num_rows();

  const tuix::Row *row = nullptr;

  while (r.has_next()) {
    row = r.next();
    w.append(row);
  }

  uint32_t num_dummies = rows_per_partition - num_rows;
  for (uint32_t i = 0; i < num_dummies; i++) {
    w.append_as_dummy(row);
  } 

  w.output_buffer(output_row, output_row_size);
}

/*
* Remove all dummy rows that were added during padding from each partition 
*/
void column_sort_filter(uint8_t *input_rows, uint32_t input_rows_length,
                        uint8_t **output_row, size_t *output_row_size) {
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter w;

  while (r.has_next()) {
    const tuix::Row *row = r.next();
    if (!row->is_dummy()) {
      w.append(row);
    } 
  }

  w.output_buffer(output_row, output_row_size);
}



