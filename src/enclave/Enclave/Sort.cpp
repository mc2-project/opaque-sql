#include "Sort.h"
#include "Random.h"

#include <algorithm>
#include <queue>

#include "ExpressionEvaluation.h"
#include "FlatbuffersReaders.h"
#include "FlatbuffersWriters.h"


class MergeItem {
 public:
  const tuix::Row *v;
  uint32_t run_idx;
};

void external_merge(
  SortedRunsReader &r,
  uint32_t run_start,
  uint32_t num_runs,
  SortedRunsWriter &w,
  FlatbuffersSortOrderEvaluator &sort_eval,
  std::string curr_ecall) {

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
    w.append(item.v);

    // Read another row from the same run that this one came from
    if (r.run_has_next(item.run_idx)) {
      item.v = r.next_from_run(item.run_idx);
      queue.push(item);
    }
  }
  w.finish_run(curr_ecall);
}

void sort_single_encrypted_block(
  SortedRunsWriter &w,
  const tuix::EncryptedBlock *block,
  FlatbuffersSortOrderEvaluator &sort_eval,
  std::string curr_ecall) {

  debug("Sort Single Encrypted Block called\n");
  EncryptedBlockToRowReader r;
  r.reset(block);
  std::vector<const tuix::Row *> sort_ptrs(r.begin(), r.end());

  std::sort(
    sort_ptrs.begin(), sort_ptrs.end(),
    [&sort_eval](const tuix::Row *a, const tuix::Row *b) {
      return sort_eval.less_than(a, b);
    });

  for (auto it = sort_ptrs.begin(); it != sort_ptrs.end(); ++it) {
    w.append(*it);
  }
  w.finish_run(curr_ecall);
}

void external_sort(uint8_t *sort_order, size_t sort_order_length,
                   uint8_t *input_rows, size_t input_rows_length,
                   uint8_t **output_rows, size_t *output_rows_length,
                   std::string curr_ecall) {
  // curr_ecall defaults to std::string("externalSort")
  FlatbuffersSortOrderEvaluator sort_eval(sort_order, sort_order_length);

  // 1. Sort each EncryptedBlock individually by decrypting it, sorting within the enclave, and
  // re-encrypting to a different buffer.
  SortedRunsWriter w;
  {
    EncryptedBlocksToEncryptedBlockReader r(
      BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
    uint32_t i = 0;
    for (auto it = r.begin(); it != r.end(); ++it, ++i) {
      debug("Sorting buffer %d with %d rows\n", i, it->num_rows());
      sort_single_encrypted_block(w, *it, sort_eval, curr_ecall);
    }

    if (w.num_runs() <= 1) {
      // Only 0 or 1 runs, so we are done - no need to merge runs
      w.as_row_writer()->output_buffer(output_rows, output_rows_length, curr_ecall);
      return;
    }
  }

  // 2. Merge sorted runs. Initially each buffer forms a sorted run. We merge B runs at a time by
  // decrypting an EncryptedBlock from each one, merging them within the enclave using a priority
  // queue, and re-encrypting to a different buffer.

  debug("Merging sorted runs\n");
  auto runs_buf = w.output_buffer();
  SortedRunsReader r(runs_buf.view(), false);
  while (r.num_runs() > 1) {
    debug("external_sort: Merging %d runs, up to %d at a time\n",
         r.num_runs(), MAX_NUM_STREAMS);

    w.clear();
    for (uint32_t run_start = 0; run_start < r.num_runs(); run_start += MAX_NUM_STREAMS) {
      uint32_t num_runs =
        std::min(MAX_NUM_STREAMS, static_cast<uint32_t>(r.num_runs()) - run_start);
      debug("external_sort: Merging buffers %d-%d\n", run_start, run_start + num_runs - 1);

      external_merge(r, run_start, num_runs, w, sort_eval, curr_ecall);
    }

    if (w.num_runs() > 1) {
      runs_buf = w.output_buffer();
      r.reset(runs_buf.view(), false);
    } else {
      // Done merging. Return the single remaining sorted run.
      w.as_row_writer()->output_buffer(output_rows, output_rows_length, curr_ecall);
      return;
    }
  }
}

void sample(uint8_t *input_rows, size_t input_rows_length,
			uint8_t **output_rows, size_t *output_rows_length) {
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter w;

  // Sample ~5% of the rows or 1000 rows, whichever is greater
  uint16_t sampling_ratio;
  if (r.num_rows() > 1000 * 20) {
    sampling_ratio = 3276; // 5% of 2^16
  } else {
    sampling_ratio = 16383;
  }

  while (r.has_next()) {
    const tuix::Row *row = r.next();

    uint16_t rand;
    mbedtls_read_rand(reinterpret_cast<unsigned char*>(&rand), 2);
    if (rand <= sampling_ratio) {
      w.append(row);
    }
  }

  w.output_buffer(output_rows, output_rows_length, std::string("sample"));
}

void find_range_bounds(uint8_t *sort_order, size_t sort_order_length,
                       uint32_t num_partitions,
                       uint8_t *input_rows, size_t input_rows_length,
                       uint8_t **output_rows, size_t *output_rows_length) {
  // Sort the input rows
  uint8_t *sorted_rows;
  size_t sorted_rows_length;

  EnclaveContext::getInstance().set_append_mac(false);
  // Passing in the empty string as the curr_ecall means that the resulting blocks in sorted_rows won't have any log metadata associated with it
  external_sort(sort_order, sort_order_length,
                input_rows, input_rows_length,
                &sorted_rows, &sorted_rows_length,
                std::string(""));

  // Split them into one range per partition
  EnclaveContext::getInstance().set_append_mac(true);
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(sorted_rows, sorted_rows_length));
  RowWriter w;
  uint32_t num_rows_per_part = r.num_rows() / num_partitions;
  uint32_t current_rows_in_part = 0;
  while (r.has_next()) {
    const tuix::Row *row = r.next();
    if (current_rows_in_part == num_rows_per_part) {
      w.append(row);
      current_rows_in_part = 0;
	} else {
	  ++current_rows_in_part;
	}
  }

  w.output_buffer(output_rows, output_rows_length, std::string("findRangeBounds"));

  ocall_free(sorted_rows);
}

void partition_for_sort(uint8_t *sort_order, size_t sort_order_length,
                        uint32_t num_partitions,
                        uint8_t *input_rows, size_t input_rows_length,
                        uint8_t *boundary_rows, size_t boundary_rows_length,
                        uint8_t **output_partition_ptrs, size_t *output_partition_lengths) {
  // Sort the input rows
  uint8_t *sorted_rows;
  size_t sorted_rows_length;
  EnclaveContext::getInstance().set_append_mac(false);
  external_sort(sort_order, sort_order_length,
                input_rows, input_rows_length,
                &sorted_rows, &sorted_rows_length,
                std::string(""));

  // Scan through the input rows and copy each to the appropriate output partition specified by the
  // ranges encoded in the given boundary_rows. A range contains all rows greater than or equal to
  // one boundary row and less than the next boundary row. The first range contains all rows less
  // than the first boundary row, and the last range contains all rows greater than or equal to the
  // last boundary row.
  FlatbuffersSortOrderEvaluator sort_eval(sort_order, sort_order_length);
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(sorted_rows, sorted_rows_length));
  RowWriter w;
  uint32_t output_partition_idx = 0;

  RowReader b(BufferRefView<tuix::EncryptedBlocks>(boundary_rows, boundary_rows_length));
  // Invariant: b_upper is the first boundary row strictly greater than the current range, or
  // nullptr if we are in the last range
  FlatbuffersTemporaryRow b_upper(b.has_next() ? b.next() : nullptr);

  EnclaveContext::getInstance().set_append_mac(true);
  while (r.has_next()) {
    const tuix::Row *row = r.next();

    // Advance boundary rows to maintain the invariant on b_upper
    while (b_upper.get() != nullptr && !sort_eval.less_than(row, b_upper.get())) {
      b_upper.set(b.has_next() ? b.next() : nullptr);

      // Write out the newly-finished partition
      w.output_buffer(
        &output_partition_ptrs[output_partition_idx],
        &output_partition_lengths[output_partition_idx], std::string("partitionForSort"));
      w.clear();
      output_partition_idx++;
    }

    w.append(row);
  }

  // Write out the final partition. If there were fewer boundary rows than expected output
  // partitions, write out enough empty partitions to ensure the expected number of output
  // partitions.
  while (output_partition_idx < num_partitions) {
    w.output_buffer(
      &output_partition_ptrs[output_partition_idx],
      &output_partition_lengths[output_partition_idx], std::string("partitionForSort"));
    w.clear();
    output_partition_idx++;
  }

  ocall_free(sorted_rows);
}
