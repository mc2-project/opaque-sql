#include "Sort.h"

#include <algorithm>
#include <memory>
#include <queue>

template<typename RecordType>
class MergeItem {
 public:
  SortPointer<RecordType> v;
  uint32_t reader_idx;
};

template<typename RecordType>
void external_merge(int op_code,
                    std::vector<uint8_t *> &runs,
                    uint32_t run_start,
                    uint32_t num_runs,
                    SortPointer<RecordType> *sort_ptrs,
                    uint32_t sort_ptrs_len,
                    uint8_t *scratch,
                    uint32_t *num_comparisons,
                    uint32_t *num_deep_comparisons) {

  check(sort_ptrs_len >= num_runs,
        "external_merge: sort_ptrs is not large enough (%d vs %d)\n", sort_ptrs_len, num_runs);

  std::vector<StreamRowReader> readers;
  for (uint32_t i = 0; i < num_runs; i++) {
    readers.push_back(StreamRowReader(runs[run_start + i], runs[run_start + i + 1]));
  }

  auto compare = [op_code, num_comparisons, num_deep_comparisons](const MergeItem<RecordType> &a,
                                                                  const MergeItem<RecordType> &b) {
    (*num_comparisons)++;
    return b.v.less_than(&a.v, op_code, num_deep_comparisons);
  };
  std::priority_queue<MergeItem<RecordType>, std::vector<MergeItem<RecordType>>, decltype(compare)>
    queue(compare);
  for (uint32_t i = 0; i < num_runs; i++) {
    MergeItem<RecordType> item;
    item.v = sort_ptrs[i];
    readers[i].read(&item.v, op_code);
    item.reader_idx = i;
    queue.push(item);
  }

  // Sort the runs into scratch
  RowWriter w(scratch);
  while (!queue.empty()) {
    MergeItem<RecordType> item = queue.top();
    queue.pop();
    w.write(&item.v);
    // Read another row from the same run that this one came from
    if (readers[item.reader_idx].has_next()) {
      readers[item.reader_idx].read(&item.v, op_code);
      queue.push(item);
    }
  }
  w.close();

  // Overwrite the runs with scratch, merging them into one big run
  memcpy(runs[run_start], scratch, w.bytes_written());
}

template<typename RecordType>
void external_sort(int op_code,
				   uint32_t num_buffers,
				   uint8_t **buffer_list,
                   uint32_t *num_rows,
				   uint32_t row_upper_bound,
				   uint8_t *scratch) {

  // Maximum number of rows we will need to store in memory at a time: the contents of the largest
  // buffer
  uint32_t max_num_rows = 0;
  for (uint32_t i = 0; i < num_buffers; i++) {
    if (max_num_rows < num_rows[i]) {
      max_num_rows = num_rows[i];
    }
  }
  uint32_t max_list_length = std::max(max_num_rows, MAX_NUM_STREAMS);

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

  uint32_t num_comparisons = 0, num_deep_comparisons = 0;

  // Sort each buffer individually
  for (uint32_t i = 0; i < num_buffers; i++) {
    debug("Sorting buffer %d with %d rows, opcode %d\n", i, num_rows[i], op_code);
    sort_single_buffer(op_code, buffer_list[i], num_rows[i], sort_ptrs, max_list_length,
                       row_upper_bound, &num_comparisons, &num_deep_comparisons);
  }

  // Each buffer now forms a sorted run. Keep a pointer to the beginning of each run, plus a
  // sentinel pointer to the end of the last run
  std::vector<uint8_t *> runs(buffer_list, buffer_list + num_buffers + 1);

  // Merge sorted runs, merging up to MAX_NUM_STREAMS runs at a time
  while (runs.size() - 1 > 1) {
    perf("external_sort: Merging %d runs, up to %d at a time\n",
         runs.size() - 1, MAX_NUM_STREAMS);

    std::vector<uint8_t *> new_runs;
    for (uint32_t run_start = 0; run_start < runs.size() - 1; run_start += MAX_NUM_STREAMS) {
      uint32_t num_runs =
        std::min(MAX_NUM_STREAMS, static_cast<uint32_t>(runs.size() - 1) - run_start);
      debug("external_sort: Merging buffers %d-%d\n", run_start, run_start + num_runs - 1);

      external_merge<RecordType>(
        op_code, runs, run_start, num_runs, sort_ptrs, max_list_length, scratch, &num_comparisons,
        &num_deep_comparisons);

      new_runs.push_back(runs[run_start]);
    }
    new_runs.push_back(runs[runs.size() - 1]); // copy over the sentinel pointer

    runs = new_runs;
  }

  perf("external_sort: %d comparisons, %d deep comparisons\n",
       num_comparisons, num_deep_comparisons);

  delete[] sort_ptrs;
  for (uint32_t i = 0; i < max_list_length; i++) {
    data[i].~RecordType();
  }
  free(data);
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

  // TODO: split(input_rows, &num_buffers);
  
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
