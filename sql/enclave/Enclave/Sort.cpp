#include "Sort.h"


uint8_t *flip(uint8_t *current,
			  uint8_t *v1, uint8_t *v2) {
  if (current == v1) {
	return v2;
  }
  return v1;
}

// can stream read a sequence of encrypted buffers
class StreamReader {

public:
  StreamReader() {
	reader = NULL;
	num_bufs = 0;
	current_buf = 0;
	rows_left = 0;
  }

  ~StreamReader() {
	if (reader != NULL) {
	  delete reader;
	}
  }

  void add_buf(uint8_t *buf, uint32_t num_rows) {
	this->num_rows[num_bufs] = num_rows;
	buffers[num_bufs++] = buf;
  }

  void init() {
	reader = new StreamRowReader(buffers[0]);
	current_buf = 0;
	rows_left = num_rows[0];
  }

  template<typename RecordType>
  bool read(SortPointer<RecordType> *ptr, int op_code) {
	if (rows_left == 0) {
	  ++current_buf;
	  if (current_buf >= num_bufs) {
		return false;
	  }
	  
	  reader->reset_block(buffers[current_buf]);
	  rows_left = num_rows[current_buf];
	}

	reader->read<RecordType>(ptr, op_code);
	--rows_left;
	return true;
  }

  StreamRowReader *reader;
  uint8_t *buffers[1024];
  uint32_t num_rows[1024];
  uint32_t num_bufs;
  uint32_t current_buf;
  uint32_t rows_left;
};


// can stream write infinite number of rows, but will write them out in blocks of a certain size
// TODO: must make sure that the stream writes out in blocks
class StreamWriter {
  
public:
  StreamWriter() {
	writer = NULL;
	output_buf = NULL;
	row_upper_bound = 0;
	num_buffers = 0;
	total_rows = 0;
	avg_rows = 0;
	current_buf = 0;
	current_num_rows = 0;
	bytes_written_ = 0;
	rows_written_ = 0;
  }

  ~StreamWriter() {
	if (writer != NULL) {
	  delete writer;
	}
  }

  void init(uint8_t *output_buf, uint32_t row_upper_bound,
			uint32_t num_buffers, uint32_t total_rows) {
	// the writer must be periodically reset so that the rows are split into num_buffers blocks
	writer = new StreamRowWriter(output_buf);
	this->output_buf = output_buf;
	this->row_upper_bound = row_upper_bound;
	this->num_buffers = num_buffers;
	this->total_rows = total_rows;
	this->avg_rows = total_rows / num_buffers;
  }

  template<typename RecordType>
  void write(SortPointer<RecordType> *sort_ptr) {
	if (current_num_rows == avg_rows && current_buf < num_buffers - 1) {
	  writer->close();
	  bytes_written_ += writer->bytes_written();
	  delete writer;
	  writer = NULL;
	  writer = new StreamRowWriter(output_buf + bytes_written_);
	  current_num_rows = 0;
	  ++current_buf;
	}
	writer->write<RecordType>(sort_ptr);
	++current_num_rows;
	++rows_written_;
  }

  void close() {
	writer->close();
	bytes_written_ += writer->bytes_written();
	printf("[StreamWriter::close] bytes_written_ is %u, rows written is %u\n", bytes_written_, rows_written_);
  }

  uint32_t bytes_written() {
   	return bytes_written_;
  }
  
  StreamRowWriter *writer;
  uint8_t *output_buf;
  uint32_t current_num_rows;
  uint32_t row_upper_bound;
  uint32_t current_buf;
  uint32_t num_buffers;
  uint32_t total_rows;
  uint32_t avg_rows;
  uint32_t bytes_written_;
  uint32_t rows_written_;
};


template<typename RecordType>
uint32_t external_merge_helper(int op_code,
							   SortPointer<RecordType> *records,
							   StreamReader *readers,
							   uint32_t num_readers,
							   uint8_t *output_buffer,
							   uint32_t row_upper_bound) {
  
  // number of sort pointers should equal to num_readers

  std::vector<HeapSortItem<SortPointer<RecordType> > *> heap;

  StreamWriter *writer = new StreamWriter;
  uint32_t total_rows = 0;
  uint32_t num_buffers = 0;

  for (uint32_t i = 0; i < num_readers; i++) {
	num_buffers += readers[i].num_bufs;
	for (uint32_t j = 0; j < readers[i].num_bufs; j++) {
	  //printf("[external_merge_helper] reader %u buffer %u has rows %u\n", i, j, readers[i].num_rows[j]);
	  total_rows += readers[i].num_rows[j];
	}
  }

  printf("[external_merge_helper] num_readers: %u, total_rows: %u, num_buffers: %u\n", num_readers, total_rows, num_buffers);

  writer->init(output_buffer, row_upper_bound, num_buffers, total_rows);
  
  for (uint32_t i = 0; i < num_readers; i++) {
	HeapSortItem<SortPointer<RecordType> > *v = new HeapSortItem<SortPointer<RecordType> >();
	v->index = i;
	v->v = &(records[i]);
	readers[i].read(v->v, op_code);
	heap.push_back(v);
  }

  for (uint32_t i = 0; i < total_rows; i++) {
	std::make_heap(heap.begin(), heap.end(),
				   [op_code](const HeapSortItem<SortPointer<RecordType> > *a,
							 const HeapSortItem<SortPointer<RecordType> > *b) {
					 return !(a->v)->less_than(b->v, op_code, NULL);
				   });
	
	HeapSortItem<SortPointer<RecordType> > *value = heap.front();
	
	// DEBUG
	// SortPointer<RecordType> *temp_value = reinterpret_cast<SortPointer<RecordType> *> (value->v);
	// printf("Writing out the following row from with index %u\n", value->index);
	// temp_value->print();
   	
	writer->write(value->v);

	value = heap.front();
	std::pop_heap(heap.begin(), heap.end());
	heap.pop_back();

	bool if_done = readers[value->index].read(value->v, op_code);

	if (if_done) {
	  heap.push_back(value);
	  std::push_heap(heap.begin(), heap.end());
	} else {
	  delete value;
	}
  }
  
  writer->close();
  uint32_t bytes_written = writer->bytes_written();

  delete writer;
  
  return bytes_written;
}


template<typename RecordType>
uint32_t external_merge(int op_code,
						uint32_t num_streams,
						std::vector<BlockReader> &blocks,
						uint32_t offset,
						uint8_t *output_buffers,
						SortPointer<RecordType> *sort_ptrs,
						uint32_t row_upper_bound) {
  
  // allocate num_streams number of row readers
  // each stream reader could contain multiple buffers!
  StreamReader *readers = new StreamReader[num_streams];

  uint8_t *block_out = NULL;
  uint32_t len_out = 0;
  uint32_t num_rows_out = 0;
  uint32_t rows_upper_bound_out = 0;

  for (uint32_t i = 0; i < num_streams; i++) {

	while (true) {
	  
	  blocks[i + offset].read(&block_out, &len_out, &num_rows_out, &rows_upper_bound_out);
	  printf("[external_merge] len_out: %u, num_rows_out: %u, rows_upper_bound_out %u\n", len_out, num_rows_out, rows_upper_bound_out);
	  
	  if (block_out == NULL) {
		//printf("[external_merge] offset %u has null block\n", i);
		break;
	  }
		  
	  readers[i].add_buf(block_out, num_rows_out);
	}

	readers[i].init();
  }

  // put the first row of each stream in a heap
  uint32_t bytes_written = external_merge_helper<RecordType>(op_code,
															 sort_ptrs,
															 readers,
															 num_streams,
															 output_buffers,
															 row_upper_bound);

  delete[] readers;
  return bytes_written;
}


// this is a non-oblivious sort that uses external sort
template<typename RecordType>
void external_sort(int op_code,
				   uint32_t num_buffers,
				   uint8_t **buffer_list,
				   uint32_t *num_rows,
				   uint32_t row_upper_bound,
				   uint8_t *scratch) {

  printf("External sort called\n");

  uint32_t max_num_rows = 0;
  uint32_t total_num_rows = 0;
  
  for (uint32_t i = 0; i < num_buffers; i++) {
    if (max_num_rows < num_rows[i]) {
      max_num_rows = num_rows[i];
    }
	total_num_rows += num_rows[i];
  }

  uint32_t max_list_length = max_num_rows * 2;

  // Actual record data, in arbitrary and unchanging order
  RecordType *data = (RecordType *) malloc(max_list_length * sizeof(RecordType));
  
  for (uint32_t i = 0; i < max_list_length; i++) {
    new(&data[i]) RecordType(row_upper_bound);
  }

  // Pointers to the record data. Only the pointers will be sorted, not the records themselves
  SortPointer<RecordType> *sort_ptrs = new SortPointer<RecordType>[max_list_length];
  for (uint32_t i = 0; i < max_list_length; i++) {
    sort_ptrs[i].init(&data[i]);
  }

  uint32_t num_comparisons = 0;
  uint32_t num_deep_comparisons = 0;

  if (num_buffers == 1) {
    debug("Sorting single buffer with %d rows, opcode %d\n", num_rows[0], op_code);
    sort_single_buffer(op_code, buffer_list[0], num_rows[0], sort_ptrs, max_list_length,
                       row_upper_bound, &num_comparisons, &num_deep_comparisons);
  } else {
    // Sort each buffer individually and write it out
    for (uint32_t i = 0; i < num_buffers; i++) {
      debug("Sorting buffer %d with %d rows, opcode %d\n", i, num_rows[i], op_code);
      sort_single_buffer(op_code, buffer_list[i], num_rows[i], sort_ptrs, max_list_length,
                         row_upper_bound, &num_comparisons, &num_deep_comparisons);
    }

	printf("Sort single buffers done, total num buffers: %u, total_num_rows: %u\n", num_buffers, total_num_rows);

	// now, merge these together in rounds
	uint32_t max_num_streams = 40;
	uint32_t num_streams = num_buffers < max_num_streams ? num_buffers : max_num_streams;

	
	uint8_t *output_buffers = buffer_list[0];
	uint8_t *output_buffers_ptr = output_buffers;

	std::vector<BlockReader> read_buffers;
	std::vector<BlockReader> write_buffers;

	for (uint32_t i = 0; i < num_buffers; i++) {
	  uint32_t buffer_length = *((uint32_t *) (buffer_list[i])) + 12;
	  write_buffers.push_back(BlockReader(buffer_list[i], buffer_length));
	}

	while (write_buffers.size() > 1) {

	  // copy the merge_buffers into scratch
	  uint8_t *scratch_ptr = scratch;

	  read_buffers.clear();
	  for (uint32_t i = 0; i < write_buffers.size(); i++) {
		// find this block's length, and num rows
		memcpy(scratch_ptr, write_buffers[i].get_buf(), write_buffers[i].get_len());
		read_buffers.push_back(BlockReader(scratch_ptr, write_buffers[i].get_len()));
		scratch_ptr += read_buffers[i].get_len();
	  }
		 
	  write_buffers.clear();

	  output_buffers_ptr = output_buffers;
	  num_streams = max_num_streams;

	  printf("read_buffers size is %u, write_buffers size is %u\n", read_buffers.size(), write_buffers.size());
	  
	  for (uint32_t i = 0; i < read_buffers.size(); i += max_num_streams) {

		printf("Round %u\n", i);

		if (read_buffers.size() - i == 1) {
		  memcpy(output_buffers_ptr, read_buffers[i].get_buf(), read_buffers[i].get_len());
		  write_buffers.push_back(BlockReader(output_buffers_ptr, read_buffers[i].get_len()));
		} else {
		  if (read_buffers.size() - i < max_num_streams) {
			num_streams = read_buffers.size() - i;
		  }
		
		  // fill the merge buffers, etc
		  uint32_t output_buffers_len = external_merge<RecordType>(op_code, num_streams, 
																   read_buffers, i,
																   output_buffers_ptr,
																   sort_ptrs, 
																   row_upper_bound);

		  write_buffers.push_back(BlockReader(output_buffers_ptr, output_buffers_len));
		  output_buffers_ptr += output_buffers_len;
		  
		}
		
	  }

	  RowReader r(buffer_list[0]);
	  NewRecord row;
	  for (uint32_t i = 0; i < total_num_rows; i++) {
		r.read(&row);
	  }
	  
	}


  }

  delete [] sort_ptrs;
  
}


// given a block of encrypted data, output a smaller block of sampled data
template<typename RecordType>
void sample(uint8_t *input_rows,
			uint8_t *output_rows,
			uint32_t *output_rows_size) {
  
  // sample ~5% of the rows
  uint32_t num_output_rows_ = 0;
  unsigned char buf[2];
  uint16_t *buf_ptr = (uint16_t *) buf;

  RowReader row_reader(input_rows);
  uint32_t row_upper_bound = *( (uint32_t *) (input_rows + 8));
  uint32_t num_rows = *( (uint32_t *) (input_rows + 4));
	
  RowWriter row_writer(output_rows, row_upper_bound);

  RecordType row;
  for (uint32_t i = 0; i < num_rows; i++) {
	row_reader.read(&row);
	
	// call the internal random generator
	sgx_read_rand(buf, 2);

	if (*buf_ptr <= 3276) {
	  row_writer.write(&row);
	  ++num_output_rows_;
	} 
  }

  row_writer.close();
  *output_rows_size = row_writer.bytes_written();
  
}


template<typename RecordType>
void find_range_bounds(int op_code, 
					   uint32_t num_buffers,
					   uint8_t *input_rows,
					   uint32_t input_rows_len,
					   uint8_t *scratch,
					   uint8_t *output_rows,
					   uint32_t *output_rows_len) {
  
  // first, sort these rows, then divide into num_buffers for range boundaries
  
  BlockReader r(input_rows, input_rows_len);

  uint32_t len_out = 0;
  uint8_t **buffer_list = (uint8_t **) malloc(num_buffers * sizeof(uint8_t *));
  uint32_t *num_rows = (uint32_t *) malloc(num_buffers * sizeof(uint32_t));
  uint32_t row_upper_bound = 0;

  for (uint32_t i = 0; i < num_buffers; i++) {
	r.read(buffer_list + i, &len_out, num_rows + i, &row_upper_bound);
  }
  
  external_sort<RecordType>(op_code, num_buffers, buffer_list, num_rows, row_upper_bound, scratch);
  
  uint32_t total_num_rows = 0;
  for (uint32_t i = 0; i < num_buffers; i++) {
	total_num_rows += num_rows[i];
  }
  
  uint32_t num_rows_per_part = total_num_rows / num_buffers;

  StreamRowReader row_reader(input_rows);
  StreamRowWriter row_writer(output_rows);

  RecordType row;

  uint32_t current_rows_in_part = 0;

  printf("[find_range_bounds] total_num_rows is %u, num_rows_per_part is %u\n", total_num_rows, num_rows_per_part);

  // each partition should get all of the range boundaries
  for (uint32_t i = 0; i < total_num_rows; i++) {
	row_reader.read(&row);
	if (current_rows_in_part == num_rows_per_part || i == total_num_rows - 1) {
	  // output this row
	  row_writer.write(&row);
	  current_rows_in_part = 0;
	} else {
	  ++current_rows_in_part;
	}
  }

  row_writer.close();
  *output_rows_len = row_writer.bytes_written();

  free(buffer_list);
  free(num_rows);
}

// non-oblivoius distributed sort on a single partition
template<typename RecordType>
void sort_partition(int op_code,
					uint32_t num_buffers,
					uint8_t *input_rows,
					uint32_t input_rows_len,
					uint8_t *boundary_rows,
					uint8_t num_partitions,
                    uint8_t *output,
                    uint32_t *output_partitions_num_rows,
					uint8_t **output_stream_list,
					uint8_t *scratch) {

  // sort locally, then look at the boundary rows to output a number of streams
  BlockReader r(input_rows, input_rows_len);

  uint32_t len_out = 0;
  uint8_t **buffer_list = (uint8_t **) malloc(num_buffers * sizeof(uint8_t *));
  uint32_t *num_rows = (uint32_t *) malloc(num_buffers * sizeof(uint32_t));
  uint32_t row_upper_bound = 0;
  uint32_t total_rows = 0;

  for (uint32_t i = 0; i < num_buffers; i++) {
	r.read(buffer_list + i, &len_out, num_rows + i, &row_upper_bound);
	total_rows += num_rows[i];
  }
  
  external_sort<RecordType>(op_code, num_buffers, buffer_list, num_rows, row_upper_bound, scratch);

  printf("[sort_partition] external sort done, total num rows is %u\n", total_rows);

  RecordType row;
  RecordType boundary_row;
  uint32_t stream = 0;

  StreamRowReader reader(input_rows);
  StreamRowReader boundary_reader(boundary_rows);
  boundary_reader.read(&boundary_row);

  StreamRowWriter writer(output);
  uint32_t offset = 0;
  uint32_t cur_num_rows = 0;
  output_stream_list[stream] = output + offset;

  // for each row, compare with the boundary rows
  for (uint32_t i = 0; i < total_rows; i++) {
	reader.read(&row);
	
	// compare currently read row & boundary_row
    if (!row.less_than(&boundary_row, op_code) && stream < num_partitions) {
	  //printf("[sort_partition] stream is %u\n", stream);
	  
	  writer.close();
	  offset = writer.bytes_written();
      output_partitions_num_rows[stream] = cur_num_rows;
      cur_num_rows = 0;
	  ++stream;

	  output_stream_list[stream] = output + offset;

      if (stream < num_partitions) {
		boundary_reader.read(&boundary_row);
	  }
	}
	
    writer.write(&row);
    ++cur_num_rows;
  }

  printf("[sort_partition] final stream is %u\n", stream);
  
  writer.close();

  free(buffer_list);
  free(num_rows);
}

template<typename RecordType>
void sort(int op_code,
		  uint32_t num_buffers,
		  uint8_t *input_rows,
		  uint32_t input_rows_len,
		  uint8_t *scratch) {

  BlockReader r(input_rows, input_rows_len);
  
  uint32_t len_out = 0;
  uint8_t **buffer_list = (uint8_t **) malloc(num_buffers * sizeof(uint8_t *));
  uint32_t *num_rows = (uint32_t *) malloc(num_buffers * sizeof(uint32_t));
  uint32_t row_upper_bound = 0;
  
  for (uint32_t i = 0; i < num_buffers; i++) {
	r.read(buffer_list + i, &len_out, num_rows + i, &row_upper_bound);
  }
  
  external_sort<RecordType>(op_code, num_buffers, buffer_list, num_rows, row_upper_bound, scratch);

  free(buffer_list);
  free(num_rows);
}

template void external_sort<NewRecord>(int op_code,
									   uint32_t num_buffers,
									   uint8_t **buffer_list,
									   uint32_t *num_rows,
									   uint32_t row_upper_bound,
									   uint8_t *scratch);

template void external_sort<NewJoinRecord>(int op_code,
										   uint32_t num_buffers,
										   uint8_t **buffer_list,
										   uint32_t *num_rows,
										   uint32_t row_upper_bound,
										   uint8_t *scratch);

template void sample<NewRecord>(uint8_t *input_rows, uint8_t *output_rows, uint32_t *output_rows_len);
template void sample<NewJoinRecord>(uint8_t *input_rows, uint8_t *output_rows, uint32_t *output_rows_len);

template void find_range_bounds<NewRecord>(int op_code, 
										   uint32_t num_buffers,
										   uint8_t *input_rows,
										   uint32_t input_rows_len,
										   uint8_t *scratch,
										   uint8_t *output_rows,
										   uint32_t *output_rows_len);

template void find_range_bounds<NewJoinRecord>(int op_code, 
											   uint32_t num_buffers,
											   uint8_t *input_rows,
											   uint32_t input_rows_len,
											   uint8_t *scratch,
											   uint8_t *output_rows,
											   uint32_t *output_rows_len);

template void sort_partition<NewRecord>(int op_code,
										uint32_t num_buffers,
										uint8_t *input_rows,
										uint32_t input_rows_len,
										uint8_t *boundary_rows,
										uint8_t num_partitions,
										uint8_t *output,
                                        uint32_t *output_partitions_num_rows,
										uint8_t **output_stream_list,
										uint8_t *scratch);

template void sort_partition<NewJoinRecord>(int op_code,
											uint32_t num_buffers,
											uint8_t *input_rows,
											uint32_t input_rows_len,
											uint8_t *boundary_rows,
											uint8_t num_partitions,
											uint8_t *output,
                                            uint32_t *output_partitions_num_rows,
											uint8_t **output_stream_list,
											uint8_t *scratch);
