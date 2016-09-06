#ifndef COLUMN_SORT_H
#define COLUMN_SORT_H

#include "util.h"
#include "NewInternalTypes.h"
#include "EncryptedDAG.h"

// splits a sequence of rows (in block format) into several streams
template<typename RecordType>
void transpose(Verify *verify_set,
               uint8_t *input_rows,
			   uint32_t num_rows,
			   uint32_t row_upper_bound,
			   uint32_t column,
			   uint32_t r,
			   uint32_t s, //  r * s is the total number of items being sorted
			   uint8_t **output_buffers,
			   uint32_t *output_buffer_sizes) {

  uint32_t new_row = 0;
  (void)new_row;
  (void)row_upper_bound;
  uint32_t new_column = column;
  uint32_t idx = 0;

  RowReader reader(input_rows, verify_set);
  RowWriter **writers = (RowWriter **) malloc(sizeof(RowWriter *) * s);
  // create s number of rowwriters
  for (uint32_t i = 0; i < s; i++) {
    writers[i] = new RowWriter(output_buffers[i], row_upper_bound);
    writers[i]->set_self_task_id(verify_set->get_self_task_id());
  }
  RecordType record;

  for (uint32_t row = 1; row <= num_rows; row++) {
	idx = (column - 1) * r + row;
	new_row = (idx - 1) / s + 1;
	new_column = (idx + s - 1) % s + 1;

	//printf("[tranpose] idx: %u, (%u, %u) -> (%u, %u)\n", idx, column, row, new_column, new_row);

	reader.read(&record);
	writers[new_column - 1]->write(&record);
  }

  for (uint32_t i = 0; i < s; i++) {
	writers[i]->close();
	output_buffer_sizes[i] = writers[i]->bytes_written();
    delete writers[i];
  }

  free(writers);
}


template<typename RecordType>
void untranspose(Verify *verify_set,
                 uint8_t *input_rows,
				 uint32_t num_rows,
				 uint32_t row_upper_bound,
				 uint32_t column,
				 uint32_t r,
				 uint32_t s, //  r * s is the total number of items being sorted
				 uint8_t **output_buffers,
				 uint32_t *output_buffer_sizes) {
  
  uint32_t new_row = 0;
  (void)new_row;
  (void)row_upper_bound;
  uint32_t new_column = column;
  uint32_t idx = 0;

  RowReader reader(input_rows, verify_set);
  RowWriter **writers = (RowWriter **) malloc(sizeof(RowWriter *) * s);
  // create s number of rowwriters
  for (uint32_t i = 0; i < s; i++) {
    writers[i] = new RowWriter(output_buffers[i], row_upper_bound);
    writers[i]->set_self_task_id(verify_set->get_self_task_id());
  }
  RecordType record;

  for (uint32_t row = 1; row <= num_rows; row++) {
	idx = (row - 1) * s + column;
	new_row = (idx + r - 1) % r + 1;
	new_column = (idx - 1) / r + 1;

	//printf("[untranpose] idx: %u, (%u, %u) -> (%u, %u)\n", idx, column, row, new_column, new_row);

	reader.read(&record);
	writers[new_column - 1]->write(&record);
  }

  for (uint32_t i = 0; i < s; i++) {
	writers[i]->close();
	output_buffer_sizes[i] = writers[i]->bytes_written();
    delete writers[i];
  }

  free(writers);  
  
}


template<typename RecordType>
void shiftdown(Verify *verify_set,
               uint8_t *input_rows,
			   uint32_t num_rows,
			   uint32_t row_upper_bound,
			   uint32_t column,
			   uint32_t r,
			   uint32_t s, //  r * s is the total number of items being sorted
			   uint8_t **output_buffers,
			   uint32_t *output_buffer_sizes) {
  
  uint32_t new_row = 0;
  (void)new_row;
  (void)row_upper_bound;
  uint32_t new_column = column;
  uint32_t idx = 0;
  uint32_t new_idx = 0;

  RowReader reader(input_rows, verify_set);
  RowWriter **writers = (RowWriter **) malloc(sizeof(RowWriter *) * s);
  // create s number of rowwriters
  for (uint32_t i = 0; i < s; i++) {
    writers[i] = new RowWriter(output_buffers[i], row_upper_bound);
    writers[i]->set_self_task_id(verify_set->get_self_task_id());
  }
  RecordType record;

  for (uint32_t row = 1; row <= num_rows; row++) {
	idx = (column - 1) * r + row;
	new_idx = (idx + r / 2 - 1) % (r * s) + 1;
	new_row = (new_idx + r - 1) % r + 1;
	new_column = (new_idx - 1) / r + 1;

	reader.read(&record);
	writers[new_column - 1]->write(&record);
	//printf("[shiftdown] idx: %u, new_idx: %u, (%u, %u) -> (%u, %u)\n", idx, new_idx, column, row, new_column, new_row);
  }

  for (uint32_t i = 0; i < s; i++) {
	writers[i]->close();
	output_buffer_sizes[i] = writers[i]->bytes_written();
    delete writers[i];
  }

  free(writers);    
  
}


template<typename RecordType>
void shiftup(Verify *verify_set,
             uint8_t *input_rows,
			 uint32_t num_rows,
			 uint32_t row_upper_bound,
			 uint32_t column,
			 uint32_t r,
			 uint32_t s, //  r * s is the total number of items being sorted
			 uint8_t **output_buffers,
			 uint32_t *output_buffer_sizes) {
  
  uint32_t new_row = 0;
  (void)new_row;
  (void)row_upper_bound;
  uint32_t new_column = column;
  uint32_t idx = 0;
  uint32_t new_idx = 0;

  RowReader reader(input_rows, verify_set);
  RowWriter **writers = (RowWriter **) malloc(sizeof(RowWriter *) * s);
  // create s number of rowwriters
  for (uint32_t i = 0; i < s; i++) {
    writers[i] = new RowWriter(output_buffers[i], row_upper_bound);
    writers[i]->set_self_task_id(verify_set->get_self_task_id());
  }
  RecordType record;

  for (uint32_t row = 1; row <= num_rows; row++) {

	if (column == 1) {
	  
	  if (row > r / 2) {
		new_row = row;
		new_column = s;
	  } else {
		new_row = row;
		new_column = column;
	  }
	  
	} else {
	  idx = (column - 1) * r + row;
      new_idx = (idx + r * s - r / 2 - 1) % (r * s) + 1;
      new_row = (new_idx + r - 1) % r + 1;
	  new_column = (new_idx - 1) / r + 1;
	}

	//printf("[shiftup] idx: %u, new_idx: %u, (%u, %u) -> (%u, %u)\n", idx, new_idx, column, row, new_column, new_row);

	reader.read(&record);
	writers[new_column - 1]->write(&record);
  }

  for (uint32_t i = 0; i < s; i++) {
	writers[i]->close();
	output_buffer_sizes[i] = writers[i]->bytes_written();
    delete writers[i];
  }

  free(writers);
  
}


template<typename RecordType>
void write_dummy(int op_code, uint32_t num_rows, RowWriter *writer, RecordType *last_row) {
  (void)op_code;
  (void)num_rows;
  (void)writer;
  (void)last_row;

  (void)op_code;
  last_row->mark_dummy();
  for (uint32_t i = 0; i < num_rows; i++) {
    writer->write(last_row);
  }
}

template<typename RecordType>
void column_sort_preprocess(int op_code,
                            Verify *verify_set,
                            uint8_t *input_rows,
							uint32_t num_rows,
                            uint32_t row_upper_bound,
                            uint32_t offset,
							uint32_t r,
                            uint32_t s, //  r * s is the total number of items being sorted
							uint8_t **output_buffers,
                            uint32_t *output_buffer_sizes) {

  (void)op_code;
  (void)row_upper_bound;
  (void)r;

  // using the index offset, we could re-map each row to its new column number
  RowReader reader(input_rows, verify_set);
  RecordType row;

  RowWriter **writers = (RowWriter **) malloc(sizeof(RowWriter *) * s);
  // create s number of rowwriters
  for (uint32_t i = 0; i < s; i++) {
    writers[i] = new RowWriter(output_buffers[i], row_upper_bound);
    writers[i]->set_self_task_id(verify_set->get_self_task_id());
  }

  uint32_t index = 0;
  uint32_t new_column = 0;

  for (uint32_t i = 0; i < num_rows; i++) {
    reader.read(&row);
    index = offset + i;
    new_column = (index % s) + 1;
    writers[new_column - 1]->write(&row);
  }

  for (uint32_t i = 0; i < s; i++) {
    writers[i]->close();
    output_buffer_sizes[i] = writers[i]->bytes_written();
    //printf("column_sort_preprocess: bytes_written for column %u is %u\n", i+1, output_buffer_sizes[i]);
  }

  for (uint32_t i = 0; i < s; i++) {
	delete writers[i];
  }
  free(writers);
}

// handle padding after the preprocess step
template<typename RecordType>
void column_sort_padding(int op_code,
                         Verify *verify_set,
                         uint8_t *input_rows,
                         uint32_t num_rows,
                         uint32_t row_upper_bound,
                         uint32_t r,
                         uint32_t s,
                         uint8_t *output_rows,
                         uint32_t *output_rows_size) {
  (void)row_upper_bound;
  (void)s;

  RowReader reader(input_rows, verify_set);
  RecordType row;
  RowWriter writer(output_rows, row_upper_bound);
  writer.set_self_task_id(verify_set->get_self_task_id());

  for (uint32_t i = 0; i < num_rows; i++) {
    reader.read(&row);
    writer.write(&row);
  }

  // handles padding
  write_dummy<RecordType>(op_code, r - num_rows, &writer, &row);
  writer.close();
  *output_rows_size = writer.bytes_written();

  //printf("column_sort_padding: padding %u rows, from %u to %u, total bytes written: %u\n", r - num_rows, num_rows, r, writer.bytes_written());
}

void count_rows(uint8_t *input_rows,
                uint32_t buffer_size,
                uint32_t *output_rows);

template<typename RecordType>
bool is_dummy(RecordType *row) {
  (void)row;
  return false;
}

// filter out the padded rows
template<typename RecordType>
void column_sort_filter(int op_code,
                        Verify *verify_set,
                        uint8_t *input_rows,
                        uint32_t column,
                        uint32_t offset,
                        uint32_t num_rows,
                        uint32_t row_upper_bound,
                        uint8_t *output_rows,
                        uint32_t *output_rows_size,
                        uint32_t *num_output_rows) {

  (void)op_code;
  (void)row_upper_bound;

  RowReader r(input_rows, verify_set);
  RowWriter w(output_rows);
  w.set_self_task_id(verify_set->get_self_task_id());
  RecordType cur;

  uint32_t num_output_rows_result = 0;
  uint32_t index = 0;

  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&cur);
    index = (column - 1) * num_rows + i;
    if (index < offset) {
      w.write(&cur);
      num_output_rows_result++;
    }
  }

  w.close();
  *output_rows_size = w.bytes_written();
  *num_output_rows = num_output_rows_result;
}

#endif
