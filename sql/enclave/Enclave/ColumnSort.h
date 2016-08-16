#include "util.h"

// splits a sequence of rows (in block format) into several streams
template<typename RecordType>
void transpose(uint8_t *input_rows,
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

  RowReader reader(input_rows);
  RowWriter **writers = (RowWriter **) malloc(sizeof(RowWriter *) * s);
  // create s number of rowwriters
  for (uint32_t i = 0; i < s; i++) {
	writers[i] = new RowWriter(output_buffers[i]);
  }
  RecordType record;

  for (uint32_t row = 1; row <= num_rows; row++) {
	idx = (column - 1) * r + row;
	new_row = (idx - 1) / s + 1;
	new_column = (idx + s - 1) % s + 1;

	printf("[tranpose] idx: %u, (%u, %u) -> (%u, %u)\n", idx, column, row, new_column, new_row);

	reader.read(&record);
	writers[new_column - 1]->write(&record);
  }

  for (uint32_t i = 0; i < s; i++) {
	writers[i]->close();
	output_buffer_sizes[i] = writers[i]->bytes_written();
	free(writers[i]);
  }

  free(writers);
}


template<typename RecordType>
void untranspose(uint8_t *input_rows,
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

  RowReader reader(input_rows);
  RowWriter **writers = (RowWriter **) malloc(sizeof(RowWriter *) * s);
  // create s number of rowwriters
  for (uint32_t i = 0; i < s; i++) {
	writers[i] = new RowWriter(output_buffers[i]);
  }
  RecordType record;

  for (uint32_t row = 1; row <= num_rows; row++) {
	idx = (row - 1) * s + column;
	new_row = (idx + r - 1) % r + 1;
	new_column = (idx - 1) / r + 1;

	printf("[untranpose] idx: %u, (%u, %u) -> (%u, %u)\n", idx, column, row, new_column, new_row);

	reader.read(&record);
	writers[new_column - 1]->write(&record);
  }

  for (uint32_t i = 0; i < s; i++) {
	writers[i]->close();
	output_buffer_sizes[i] = writers[i]->bytes_written();
	free(writers[i]);
  }

  free(writers);  
  
}


template<typename RecordType>
void shiftdown(uint8_t *input_rows,
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

  RowReader reader(input_rows);
  RowWriter **writers = (RowWriter **) malloc(sizeof(RowWriter *) * s);
  // create s number of rowwriters
  for (uint32_t i = 0; i < s; i++) {
	writers[i] = new RowWriter(output_buffers[i]);
  }
  RecordType record;

  for (uint32_t row = 1; row <= num_rows; row++) {
	idx = (column - 1) * r + row;
	new_idx = (idx + r / 2 - 1) % (r * s) + 1;
	new_row = (new_idx + r - 1) % r + 1;
	new_column = (new_idx - 1) / r + 1;

	reader.read(&record);
	writers[new_column - 1]->write(&record);
	printf("[shiftdown] idx: %u, new_idx: %u, (%u, %u) -> (%u, %u)\n", idx, new_idx, column, row, new_column, new_row);
  }

  for (uint32_t i = 0; i < s; i++) {
	writers[i]->close();
	output_buffer_sizes[i] = writers[i]->bytes_written();
	free(writers[i]);
  }

  free(writers);    
  
}


template<typename RecordType>
void shiftup(uint8_t *input_rows,
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

  RowReader reader(input_rows);
  RowWriter **writers = (RowWriter **) malloc(sizeof(RowWriter *) * s);
  // create s number of rowwriters
  for (uint32_t i = 0; i < s; i++) {
	writers[i] = new RowWriter(output_buffers[i]);
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

	printf("[shiftup] idx: %u, new_idx: %u, (%u, %u) -> (%u, %u)\n", idx, new_idx, column, row, new_column, new_row);

	reader.read(&record);
	writers[new_column - 1]->write(&record);
  }

  for (uint32_t i = 0; i < s; i++) {
	writers[i]->close();
	output_buffer_sizes[i] = writers[i]->bytes_written();
	free(writers[i]);
  }

  free(writers);
  
}
