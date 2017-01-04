#include "Sort.h"

#include <algorithm>
#include <memory>
#include <queue>

#include "ExpressionEvaluation.h"

template<typename RecordType>
class MergeItem {
 public:
  SortPointer<RecordType> v;
  uint32_t reader_idx;
};

template<typename RecordType>
void external_merge(int op_code,
                    Verify *verify_set,
                    std::vector<uint8_t *> &runs,
                    uint32_t run_start,
                    uint32_t num_runs,
                    SortPointer<RecordType> *sort_ptrs,
                    uint32_t sort_ptrs_len,
                    uint32_t row_upper_bound,
                    uint8_t *scratch,
                    uint32_t *num_comparisons,
                    uint32_t *num_deep_comparisons) {

  check(sort_ptrs_len >= num_runs,
        "external_merge: sort_ptrs is not large enough (%d vs %d)\n", sort_ptrs_len, num_runs);

  std::vector<StreamRowReader *> readers;
  for (uint32_t i = 0; i < num_runs; i++) {
    readers.push_back(new StreamRowReader(runs[run_start + i], runs[run_start + i + 1]));
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
    readers[i]->read(&item.v, op_code);
    item.reader_idx = i;
    queue.push(item);
  }

  // Sort the runs into scratch
  RowWriter w(scratch, row_upper_bound);
  w.set_self_task_id(verify_set->get_self_task_id());
  while (!queue.empty()) {
    MergeItem<RecordType> item = queue.top();
    queue.pop();
    w.write(&item.v);

    // Read another row from the same run that this one came from
    if (readers[item.reader_idx]->has_next()) {
      readers[item.reader_idx]->read(&item.v, op_code);
      queue.push(item);
    }
  }
  w.close();

  // Overwrite the runs with scratch, merging them into one big run
  memcpy(runs[run_start], scratch, w.bytes_written());

  for (uint32_t i = 0; i < num_runs; i++) {
    delete readers[i];
  }
}

void sort_single_encrypted_block(
  FlatbuffersRowWriter &w,
  const tuix::EncryptedBlock *block,
  FlatbuffersSortOrderEvaluator &sort_eval) {

  EncryptedBlockToRowReader r(block);
  std::vector<const tuix::Row *> sort_ptrs(r.begin(), r.end());

  std::sort(
    sort_ptrs.begin(), sort_ptrs.end(),
    [sort_eval](const tuix::Row *a,
                const tuix::Row *b) {
      return sort_eval.less_than(a, b);
    });

  for (auto it = sort_ptrs.begin(); it != sort_ptrs.end(); ++it) {
    w.write(*it);
  }
}

void external_sort(uint8_t *sort_order, size_t sort_order_length,
                   uint8_t *input_rows, size_t input_rows_length,
                   uint8_t **output_rows, size_t *output_rows_length) {

  flatbuffers::Verifier v(sort_order, sort_order_length);
  check(v.VerifyBuffer<tuix::SortExpr>(nullptr),
        "Corrupt SortExpr %p of length %d\n", sort_order, sort_order_length);

  FlatbuffersSortOrderEvaluator sort_eval(flatbuffers::GetRoot<tuix::SortExpr>(sort_order));

  // 1. Sort each EncryptedBlock individually by decrypting it, sorting within the enclave, and
  // re-encrypting to a different buffer.

  EncryptedBlocksToEncryptedBlockReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;
  uint32_t i = 0;
  std::vector<uint32_t> runs;
  for (auto it = r.begin(); it != r.end(); ++it, ++i) {
    debug("Sorting buffer %d with %d rows\n", i, it->num_rows());

    sort_single_encrypted_block(w, *it, sort_eval);
    runs.push_back(i);
  }

  w.close();
  *output_rows = w.output_buffer();
  *output_rows_length = w.output_size();

  // TODO
  // 2. Merge sorted runs. Initially each buffer forms a sorted run. We merge B runs at a time by
  // decrypting an EncryptedBlock from each one, merging them within the enclave using a priority
  // queue, and re-encrypting to a different buffer.

  // const uint32_t B = 2;

  // Maximum number of rows we will need to store in memory at a time: the contents of the largest
  // buffer

  // uint32_t max_rows_per_block = 0;
  // for (uint32_t i = 0; i < num_buffers; i++) {
  //   if (max_num_rows < num_rows[i]) {
  //     max_num_rows = num_rows[i];
  //   }
  // }
  // uint32_t max_list_length = std::max(max_num_rows, B);

  // Pointers to the record data. Only the pointers will be sorted, not the records themselves
  // std::vector<FlatbuffersSortPointer> sort_ptrs(MAX_ROWS_PER_ENCRYPTEDBLOCK);


  // Each buffer now forms a sorted run. Keep a pointer to the beginning of each run, plus a
  // sentinel pointer to the end of the last run
  /*
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

      external_merge<RecordType>(op_code, verify_set,
                                 runs, run_start, num_runs, sort_ptrs, max_list_length, row_upper_bound, scratch,
                                 &num_comparisons, &num_deep_comparisons);

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
  */
}

template<typename RecordType>
void sample(Verify *verify_set,
            uint8_t *input_rows,
            uint32_t input_rows_len,
            uint32_t num_rows,
			uint8_t *output_rows,
            uint32_t *output_rows_size,
            uint32_t *num_output_rows) {

  uint32_t row_upper_bound = 0;
  {
    BlockReader b(input_rows, input_rows_len);
    uint8_t *block;
    uint32_t len, num_rows, result;
    b.read(&block, &len, &num_rows, &result);
    if (block == NULL) {
      *output_rows_size = 0;
      return;
    } else {
      row_upper_bound = result;
    }
  }
  
  // Sample ~5% of the rows or 1000 rows, whichever is greater
  unsigned char buf[2];
  uint16_t *buf_ptr = (uint16_t *) buf;

  uint16_t sampling_ratio;
  if (num_rows > 1000 * 20) {
    sampling_ratio = 3276; // 5% of 2^16
  } else {
    sampling_ratio = 16383;
  }

  RowReader r(input_rows, input_rows + input_rows_len, verify_set);
  RowWriter w(output_rows, row_upper_bound);
  w.set_self_task_id(verify_set->get_self_task_id());
  RecordType row;
  uint32_t num_output_rows_result = 0;
  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&row);
    sgx_read_rand(buf, 2);
    if (*buf_ptr <= sampling_ratio) {
      w.write(&row);
      num_output_rows_result++;
    }
  }

  w.close();
  *output_rows_size = w.bytes_written();
  *num_output_rows = num_output_rows_result;
}

template<typename RecordType>
void find_range_bounds(int op_code,
                       Verify *verify_set,
                       uint32_t num_partitions,
					   uint32_t num_buffers,
                       uint8_t **buffer_list,
                       uint32_t *num_rows,
                       uint32_t row_upper_bound,
                       uint8_t *output_rows,
                       uint32_t *output_rows_len,
                       uint8_t *scratch) {

  // Sort the input rows
  check(false, "not implemented\n");
  (void)op_code;
  (void)scratch;
  //external_sort<RecordType>(op_code, verify_set, num_buffers, buffer_list, num_rows, row_upper_bound, scratch);

  // Split them into one range per partition
  uint32_t total_num_rows = 0;
  for (uint32_t i = 0; i < num_buffers; i++) {
    total_num_rows += num_rows[i];
  }
  uint32_t num_rows_per_part = total_num_rows / num_partitions;

  RowReader r(buffer_list[0], buffer_list[num_buffers]);
  RowWriter w(output_rows, row_upper_bound);
  w.set_self_task_id(verify_set->get_self_task_id());
  RecordType row;
  uint32_t current_rows_in_part = 0;
  for (uint32_t i = 0; i < total_num_rows; i++) {
    r.read(&row);
    if (current_rows_in_part == num_rows_per_part) {
      w.write(&row);
      current_rows_in_part = 0;
	} else {
	  ++current_rows_in_part;
	}
  }

  w.close();
  *output_rows_len = w.bytes_written();
}

template<typename RecordType>
void partition_for_sort(int op_code,
                        Verify *verify_set,
                        uint8_t num_partitions,
                        uint32_t num_buffers,
                        uint8_t **buffer_list,
                        uint32_t *num_rows,
                        uint32_t row_upper_bound,
                        uint8_t *boundary_rows,
                        uint32_t boundary_rows_len,
                        uint8_t *output,
                        uint8_t **output_partition_ptrs,
                        uint32_t *output_partition_num_rows,
                        uint8_t *scratch) {

  // Sort the input rows
  check(false, "not implemented\n");
  (void)scratch;
  // external_sort<RecordType>(op_code, verify_set, num_buffers, buffer_list, num_rows, row_upper_bound, scratch);

  uint32_t total_num_rows = 0;
  for (uint32_t i = 0; i < num_buffers; i++) {
    total_num_rows += num_rows[i];
  }

  // Scan through the sorted input rows and copy them to the output, marking the beginning of each
  // range with a pointer. A range contains all rows greater than or equal to one boundary row and
  // less than the next boundary row. The first range contains all rows less than the first boundary
  // row, and the last range contains all rows greater than or equal to the last boundary row.
  RowReader r(buffer_list[0], buffer_list[num_buffers]);
  RowReader b(boundary_rows, boundary_rows + boundary_rows_len, verify_set);
  RowWriter w(output, row_upper_bound);
  w.set_self_task_id(verify_set->get_self_task_id());
  RecordType row;
  RecordType boundary_row;
  uint32_t out_idx = 0;
  uint32_t cur_num_rows = 0;

  output_partition_ptrs[out_idx] = output;
  b.read(&boundary_row);

  for (uint32_t i = 0; i < total_num_rows; i++) {
    r.read(&row);

    // The new row falls outside the current range, so we start a new range
    if (!row.less_than(&boundary_row, op_code) && out_idx < num_partitions - 1) {
      // Record num_rows for the old range
      output_partition_num_rows[out_idx] = cur_num_rows; cur_num_rows = 0;

      out_idx++;

      // Record the beginning of the new range
      w.finish_block();
      output_partition_ptrs[out_idx] = output + w.bytes_written();

      if (out_idx < num_partitions - 1) {
        b.read(&boundary_row);
      }
	}
	
    w.write(&row);
    ++cur_num_rows;
  }

  // Record num_rows for the last range
  output_partition_num_rows[num_partitions - 1] = cur_num_rows;

  w.close();
  // Write the sentinel pointer to the end of the last range
  output_partition_ptrs[num_partitions] = output + w.bytes_written();
}

template void sample<NewRecord>(
  Verify *verify_set,
  uint8_t *input_rows,
  uint32_t input_rows_len,
  uint32_t num_rows,
  uint8_t *output_rows,
  uint32_t *output_rows_size,
  uint32_t *num_output_rows);

template void sample<NewJoinRecord>(
  Verify *verify_set,
  uint8_t *input_rows,
  uint32_t input_rows_len,
  uint32_t num_rows,
  uint8_t *output_rows,
  uint32_t *output_rows_size,
  uint32_t *num_output_rows);

template void find_range_bounds<NewRecord>(
  int op_code,
  Verify *verify_set,
  uint32_t num_partitions,
  uint32_t num_buffers,
  uint8_t **buffer_list,
  uint32_t *num_rows,
  uint32_t row_upper_bound,
  uint8_t *output_rows,
  uint32_t *output_rows_len,
  uint8_t *scratch);

template void find_range_bounds<NewJoinRecord>(
  int op_code,
  Verify *verify_set,
  uint32_t num_partitions,
  uint32_t num_buffers,
  uint8_t **buffer_list,
  uint32_t *num_rows,
  uint32_t row_upper_bound,
  uint8_t *output_rows,
  uint32_t *output_rows_len,
  uint8_t *scratch);

template void partition_for_sort<NewRecord>(
  int op_code,
  Verify *verify_set,
  uint8_t num_partitions,
  uint32_t num_buffers,
  uint8_t **buffer_list,
  uint32_t *num_rows,
  uint32_t row_upper_bound,
  uint8_t *boundary_rows,
  uint32_t boundary_rows_len,
  uint8_t *output,
  uint8_t **output_partition_ptrs,
  uint32_t *output_partition_num_rows,
  uint8_t *scratch);

template void partition_for_sort<NewJoinRecord>(
  int op_code,
  Verify *verify_set,
  uint8_t num_partitions,
  uint32_t num_buffers,
  uint8_t **buffer_list,
  uint32_t *num_rows,
  uint32_t row_upper_bound,
  uint8_t *boundary_rows,
  uint32_t boundary_rows_len,
  uint8_t *output,
  uint8_t **output_partition_ptrs,
  uint32_t *output_partition_num_rows,
  uint8_t *scratch);
