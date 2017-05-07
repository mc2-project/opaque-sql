#include "Sort.h"

#include <algorithm>
#include <memory>
#include <queue>

#include "ExpressionEvaluation.h"

class MergeItem {
 public:
  const tuix::Row *v;
  uint32_t run_idx;
};

flatbuffers::Offset<tuix::EncryptedBlocks> external_merge(
  SortedRunsReader &r,
  uint32_t run_start,
  uint32_t num_runs,
  FlatbuffersRowWriter &w,
  FlatbuffersSortOrderEvaluator &sort_eval) {

  // Maintain a priority queue with one row per run
  auto compare = [&sort_eval](const MergeItem &a, const MergeItem &b) {
    return sort_eval.less_than(b.v, a.v);
  };
  std::priority_queue<MergeItem, std::vector<MergeItem>, decltype(compare)>
    queue(compare);

  // Initialize the priority queue with the first row from each run
  for (uint32_t i = run_start; i < run_start + num_runs; i++) {
    debug("external_merge: Read first row from run %d\n", i);
    MergeItem item;
    item.v = r.next_from_run(i);
    item.run_idx = i;
    queue.push(item);
  }

  // Merge the runs using the priority queue
  while (!queue.empty()) {
    MergeItem item = queue.top();
    queue.pop();
    printf("merge: write row ");
    print(item.v);
    w.write(item.v);

    // Read another row from the same run that this one came from
    if (r.run_has_next(item.run_idx)) {
      item.v = r.next_from_run(item.run_idx);
      queue.push(item);
    }
  }
  return w.write_encrypted_blocks();
}

flatbuffers::Offset<tuix::EncryptedBlocks> sort_single_encrypted_block(
  FlatbuffersRowWriter &w,
  const tuix::EncryptedBlock *block,
  FlatbuffersSortOrderEvaluator &sort_eval) {

  EncryptedBlockToRowReader r;
  r.reset(block);
  std::vector<const tuix::Row *> sort_ptrs(r.begin(), r.end());

  std::sort(
    sort_ptrs.begin(), sort_ptrs.end(),
    [&sort_eval](const tuix::Row *a, const tuix::Row *b) {
      return sort_eval.less_than(a, b);
    });

  for (auto it = sort_ptrs.begin(); it != sort_ptrs.end(); ++it) {
    w.write(*it);
  }
  return w.write_encrypted_blocks();
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
  FlatbuffersRowWriter w;
  {
    EncryptedBlocksToEncryptedBlockReader r(input_rows, input_rows_length);
    std::vector<flatbuffers::Offset<tuix::EncryptedBlocks>> runs;
    uint32_t i = 0;
    for (auto it = r.begin(); it != r.end(); ++it, ++i) {
      debug("Sorting buffer %d with %d rows\n", i, it->num_rows());
      runs.push_back(sort_single_encrypted_block(w, *it, sort_eval));
    }
    w.finish(w.write_sorted_runs(runs));
  }

  // 2. Merge sorted runs. Initially each buffer forms a sorted run. We merge B runs at a time by
  // decrypting an EncryptedBlock from each one, merging them within the enclave using a priority
  // queue, and re-encrypting to a different buffer.
  SortedRunsReader r(w.output_buffer(), w.output_size());
  while (r.num_runs() > 1) {
    debug("external_sort: Merging %d runs, up to %d at a time\n",
         r.num_runs(), MAX_NUM_STREAMS);

    w.clear();
    std::vector<flatbuffers::Offset<tuix::EncryptedBlocks>> runs;
    for (uint32_t run_start = 0; run_start < r.num_runs(); run_start += MAX_NUM_STREAMS) {
      uint32_t num_runs =
        std::min(MAX_NUM_STREAMS, static_cast<uint32_t>(r.num_runs()) - run_start);
      debug("external_sort: Merging buffers %d-%d\n", run_start, run_start + num_runs - 1);

      runs.push_back(external_merge(r, run_start, num_runs, w, sort_eval));
    }

    if (runs.size() > 1) {
      w.finish(w.write_sorted_runs(runs));
      r.reset(w.output_buffer(), w.output_size());
    } else {
      // Done merging. Return the single remaining sorted run.
      w.finish(runs[0]);
      *output_rows = w.output_buffer();
      *output_rows_length = w.output_size();
      return;
    }
  }
}

void sample(uint8_t *input_rows, size_t input_rows_length,
			uint8_t **output_rows, size_t *output_rows_length, uint32_t *num_output_rows) {
  uint32_t num_input_rows; // TODO: calculate this

  // Sample ~5% of the rows or 1000 rows, whichever is greater
  uint16_t sampling_ratio;
  if (num_input_rows > 1000 * 20) {
    sampling_ratio = 3276; // 5% of 2^16
  } else {
    sampling_ratio = 16383;
  }

  EncryptedBlocksToRowReader r(input_rows, input_rows_len);
  FlatbuffersRowWriter w;
  uint8_t rand[2];

  while (r.has_next()) {
    const tuix::Row *row = r.next();

    sgx_read_rand(rand, 2);
    if (*reinterpret_cast<uint16_t *>(rand) <= sampling_ratio) {
      w.write(row);
    }
  }

  w.finish(w.write_encrypted_blocks());
  *output_rows = w.output_buffer();
  *output_rows_length = w.output_size();
  *num_output_rows = w.output_num_rows();
}

void find_range_bounds(uint8_t *sort_order, size_t sort_order_length,
                       uint32_t num_partitions,
                       uint8_t *input_rows, size_t input_rows_length,
                       uint8_t **output_rows, size_t *output_rows_length) {
  // Sort the input rows
  external_sort(sort_order, sort_order_length,
                input_rows, input_rows_length,
                output_rows, output_rows_length);

  // Split them into one range per partition
  uint32_t num_input_rows; // TODO: calculate this
  uint32_t num_rows_per_part = num_input_rows / num_partitions;

  EncryptedBlocksToRowReader r(input_rows, input_rows_len);
  FlatbuffersRowWriter w;
  uint32_t current_rows_in_part = 0;
  while (r.has_next()) {
    const tuix::Row *row = r.next();
    if (current_rows_in_part == num_rows_per_part) {
      w.write(row);
      current_rows_in_part = 0;
	} else {
	  ++current_rows_in_part;
	}
  }

  w.finish(w.write_encrypted_blocks());
  *output_rows = w.output_buffer();
  *output_rows_length = w.output_size();
  *num_output_rows = w.output_num_rows();
}

template<typename RecordType>
void partition_for_sort(uint8_t *sort_order, size_t sort_order_length,
                        uint8_t *input_rows, size_t input_rows_length,
                        uint8_t *boundary_rows, uint32_t boundary_rows_length,
                        uint8_t **output_rows, size_t *output_rows_length) {

  // Sort the input rows
  external_sort(sort_order, sort_order_length,
                input_rows, input_rows_length,
                output_rows, output_rows_length);

  // Scan through the input rows and copy each to the appropriate output partition specified by the
  // ranges encoded in the given boundary_rows. A range contains all rows greater than or equal to
  // one boundary row and less than the next boundary row. The first range contains all rows less
  // than the first boundary row, and the last range contains all rows greater than or equal to the
  // last boundary row.
  EncryptedBlocksToRowReader r(input_rows, input_rows_len);
  FlatbuffersRowWriter w;
  std::vector<flatbuffers::Offset<tuix::EncryptedBlocks>> output_runs;

  EncryptedBlocksToRowReader b(boundary_rows, boundary_rows_len);
  const tuix::Row *b_start = nullptr;
  const tuix::Row *b_end = r.next();

  FlatbuffersSortOrderEvaluator sort_eval(sort_order, sort_order_length);

  while (r.has_next()) {
    const tuix::Row *row = r.next();

    // Advance boundary rows
    while (b_end != nullptr && !sort_eval.less_than(row, b_end)) {
      b_start = b_end;
      b_end = b.has_next() ? b.next() : nullptr;
      output_runs.push_back(w.write_encrypted_blocks());
    }

    w.write(row);
  }
  output_runs.push_back(w.write_encrypted_blocks());

  w.finish(w.write_sorted_runs(output_runs));
  *output_rows = w.output_buffer();
  *output_rows_length = w.output_size();
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
  uint32_t *output_partition_num_rows);

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
  uint32_t *output_partition_num_rows);
