#include "ObliviousSort.h"

#include "Sort.h"

#include "util.h"

#include <algorithm>
#include <queue>

#include "ExpressionEvaluation.h"

class MergeItem {
 public:
  const tuix::Row *v;
  uint32_t run_idx;
};

// Convenient way to bundle a buffer and its length, to pass it around
// typed by its root.
// A BufferRef does not own its buffer.
// Based on flatbuffers::BufferRef, but with unique_ptr over ocall'd memory instead of raw pointers.
template<typename T> struct BufferRef {
    BufferRef() : buf(nullptr), len(0) {}
    BufferRef(std::unique_ptr<uint8_t, decltype(&ocall_free)> _buf, uint32_t _len)
        : buf(_buf), len(_len) {}

    const T *GetRoot() const { return flatbuffers::GetRoot<T>(buf.get()); }

    std::unique_ptr<uint8_t, decltype(&ocall_free)> buf;
    uint32_t len;
};

void oblivious_merge(FlatbuffersSortOrderEvaluator &sort_eval, const tuix::EncryptedBlocks *block1, const tuix::EncryptedBlocks *block2, FlatbuffersRowWriter *w1, FlatbuffersRowWriter *w2) {
  auto compare = [&sort_eval](const MergeItem &a, const MergeItem &b) {
    return sort_eval.less_than(b.v, a.v);
  };
  std::priority_queue<MergeItem, std::vector<MergeItem>, decltype(compare)>
    queue(compare);
    MergeItem item;
    EncryptedBlocksToRowReader r1(block1);
    uint32_t r1_rows = r1.num_rows();
    EncryptedBlocksToRowReader r2(block2);
    item.v = r1.next();
    item.run_idx = 0;
    queue.push(item);
    item.v = r2.next();
    item.run_idx = 1;
    queue.push(item);

  // Merge the runs using the priority queue
  uint32_t i = 0;
  while (!queue.empty()) {
    MergeItem item = queue.top();
    queue.pop();
    if (i < r1_rows) {
      w1->write(item.v);
    }
    else {
      w2->write(item.v);
    }
    // Read another row from the same run that this one came from
    EncryptedBlocksToRowReader *r_next = (item.run_idx == 0) ? &r1 : &r2;
    if (r_next->has_next()) {
      item.v = r_next->next();
      queue.push(item);
    }
    i++;
  }
}

void oblivious_sort(uint8_t *sort_order, size_t sort_order_length,
                   uint8_t *input_rows, size_t input_rows_length,
                   uint8_t **output_rows, size_t *output_rows_length) {
  FlatbuffersSortOrderEvaluator sort_eval(sort_order, sort_order_length);

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
    if (runs.size() > 1) {
      w.finish(w.write_sorted_runs(runs));
    } else if (runs.size() == 1) {
      w.finish(runs[0]);
      *output_rows = w.output_buffer().release();
      *output_rows_length = w.output_size();
      return;
    } else {
      w.finish(w.write_encrypted_blocks());
      *output_rows = w.output_buffer().release();
      *output_rows_length = w.output_size();
      return;
    }
  }

  // 2. Merge sorted runs. Initially each buffer forms a sorted run. We merge B runs at a time by
  // decrypting an EncryptedBlock from each one, merging them within the enclave using a priority
  // queue, and re-encrypting to a different buffer.
  auto runs_buf = w.output_buffer();
  auto runs_len = w.output_size();
  SortedRunsReader r(runs_buf.get(), runs_len);
  std::vector<BufferRef<tuix::EncryptedBlocks>> blocks; //TODO initialize blocks
  int len = blocks.size();
  int log_len = log_2(len) + 1;
  int offset = 0;
  // Merge sorted buffers pairwise
  for (int stage = 1; stage <= log_len; stage++) {
    for (int stage_i = stage; stage_i >= 1; stage_i--) {
      int part_size = pow_2(stage_i);
      int part_size_half = part_size / 2;
      for (int i = offset; i <= (offset + len - 1); i += part_size) {
        for (int j = 1; j <= part_size_half; j++) {
          int idx = i + j - 1;
          int pair_idx = i + part_size - j;
          if (pair_idx < offset + len) {
            debug("Merging buffers %d and %d with %d, %d rows\n",
                  idx, pair_idx, num_rows[idx], num_rows[pair_idx]);
            FlatbuffersRowWriter w1, w2;
            oblivious_merge(sort_eval, blocks[idx].GetRoot(), blocks[pair_idx].GetRoot(),
                            &w1, &w2);
            blocks[idx].buf = w1.output_buffer();
            blocks[idx].len = w1.output_size();
            blocks[pair_idx].buf = w2.output_buffer();
            blocks[pair_idx].len = w2.output_size();
          }
        }
      }
    }
  }
  w.clear();
  for (auto& block : blocks) {
    w.write(block.GetRoot());
  }  
  w.finish(w.write_encrypted_blocks());
  *output_rows = w.output_buffer().release();
  *output_rows_length = w.output_size();
}
