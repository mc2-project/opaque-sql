// -*- mode: C++ -*-

#include "ObliviousSort.h"

#include <algorithm>

#include "common.h"
#include "util.h"

int log_2(int value);
int pow_2(int value);

template<typename RecordType>
uint32_t sort_single_buffer(
  int op_code, Verify *verify_set,
  uint8_t *buffer, uint8_t *buffer_end, 
  uint8_t *write_buffer,
  uint32_t num_rows, SortPointer<RecordType> *sort_ptrs,
  uint32_t sort_ptrs_len, uint32_t row_upper_bound, uint32_t *num_comparisons,
  uint32_t *num_deep_comparisons) {

  check(sort_ptrs_len >= num_rows,
        "sort_single_buffer: sort_ptrs is not large enough (%d vs %d)\n", sort_ptrs_len, num_rows);

  RowReader r(buffer, buffer_end, verify_set);
  for (uint32_t i = 0; i < num_rows; i++) {
    r.read(&sort_ptrs[i], op_code);
  }

  std::sort(
    sort_ptrs, sort_ptrs + num_rows,
    [op_code, num_comparisons, num_deep_comparisons](const SortPointer<RecordType> &a,
                                                     const SortPointer<RecordType> &b) {
      (*num_comparisons)++;
      return a.less_than(&b, op_code, num_deep_comparisons);
    });

  RowWriter w(write_buffer, row_upper_bound);
  w.set_self_task_id(verify_set->get_self_task_id());
  for (uint32_t i = 0; i < num_rows; i++) {
    w.write(&sort_ptrs[i]);
  }
  w.close();
  printf("[%s] bytes read: %u, bytes_written is %u, buffer + byteswritten: %p\n", __FUNCTION__, (uint32_t)(buffer_end - buffer), w.bytes_written(), buffer+w.bytes_written());
  return w.bytes_written();
}

template<typename RecordType>
void merge(int op_code, Verify *verify_set,
           uint8_t *buffer1, uint8_t *buffer1_end, uint32_t buffer1_rows,
           uint8_t *buffer2, uint8_t *buffer2_end, uint32_t buffer2_rows,
           SortPointer<RecordType> *sort_ptrs, uint32_t sort_ptrs_len, uint32_t row_upper_bound,
           uint32_t *num_comparisons, uint32_t *num_deep_comparisons) {

  check(sort_ptrs_len >= buffer1_rows + buffer2_rows,
        "merge: sort_ptrs is not large enough (%d vs %d)\n",
        sort_ptrs_len, buffer1_rows + buffer2_rows);

  struct BufferVars {
    BufferVars(uint8_t *buffer, uint8_t* buffer_end)
      : r(buffer, buffer_end), rows_read(0), rec(), ptr(), ptr_is_empty(true) {
      ptr.init(&rec);
    }
    RowReader r;
    uint32_t rows_read;
    RecordType rec;
    SortPointer<RecordType> ptr;
    bool ptr_is_empty;
  } b1(buffer1, buffer1_end), b2(buffer2, buffer2_end);

  for (uint32_t i = 0; i < buffer1_rows + buffer2_rows; i++) {
    // Fill ptr1 and ptr2
    if (b1.ptr_is_empty && b1.rows_read < buffer1_rows) {
      b1.r.read(&b1.ptr, op_code);
      b1.ptr_is_empty = false;
      b1.rows_read++;
    }
    if (b2.ptr_is_empty && b2.rows_read < buffer2_rows) {
      b2.r.read(&b2.ptr, op_code);
      b2.ptr_is_empty = false;
      b2.rows_read++;
    }

    // Write out the smaller one and clear it
    if (!b1.ptr_is_empty && !b2.ptr_is_empty) {
      (*num_comparisons)++;
      if (b1.ptr.less_than(&b2.ptr, op_code, num_deep_comparisons)) {
        sort_ptrs[i].set(&b1.ptr);
        b1.ptr_is_empty = true;
      } else {
        sort_ptrs[i].set(&b2.ptr);
        b2.ptr_is_empty = true;
      }
    } else if (!b1.ptr_is_empty) {
      sort_ptrs[i].set(&b1.ptr);
      b1.ptr_is_empty = true;
    } else if (!b2.ptr_is_empty) {
      sort_ptrs[i].set(&b2.ptr);
      b2.ptr_is_empty = true;
    } else {
      printf("merge: Violated assumptions - input exhausted before output full\n");
      assert(false);
    }
  }

  check(b1.ptr_is_empty && b2.ptr_is_empty,
        "merge: Violated assumptions - output is full but input remains\n");

  // Write the merged result back, splitting it across buffers 1 and 2.
  // Note: RowWriter must ensure that all subsets of n rows have the same size, otherwise the
  // buffers may overrun their boundaries. For example, suppose each group of the same characters
  // represents a row. If we merge two buffers [aaaaac] and [bbbddd] to form [aaaaabbbcddd], there
  // is no way to split the merged result into two buffers of identical size.
  RowWriter w1(buffer1, row_upper_bound);
  w1.set_self_task_id(verify_set->get_self_task_id());
  for (uint32_t r = 0; r < buffer1_rows; r++) {
    w1.write(&sort_ptrs[r]);
  }
  w1.close();

  RowWriter w2(buffer2, row_upper_bound);
  w2.set_self_task_id(verify_set->get_self_task_id());
  for (uint32_t r = buffer1_rows; r < buffer1_rows + buffer2_rows; r++) {
    w2.write(&sort_ptrs[r]);
  }
  w2.close();
}

template<typename RecordType>
void external_oblivious_sort(int op_code,
                             Verify *verify_set,
                             uint32_t num_buffers,
                             uint8_t **buffer_list,
                             uint32_t *num_rows,
                             uint32_t row_upper_bound) {
  int len = num_buffers;
  int log_len = log_2(len) + 1;
  int offset = 0;

  perf("external_oblivious_sort: Sorting %d buffers in %d rounds\n", num_buffers, log_len);

  // Maximum number of rows we will need to store in memory at a time: the contents of two buffers
  // (for merging)
  uint32_t max_num_rows = 0;
  for (uint32_t i = 0; i < num_buffers; i++) {
    if (max_num_rows < num_rows[i]) {
      max_num_rows = num_rows[i];
    }
  }
  uint32_t max_list_length = max_num_rows * 2;

  // Actual record data, in arbitrary and unchanging order
  RecordType *data = (RecordType *) malloc(max_list_length * sizeof(RecordType));
  for (uint32_t i = 0; i < max_list_length; i++) {
    new(&data[i]) RecordType(row_upper_bound);
  }
  perf("external_oblivious_sort: data occupies %d bytes in enclave memory\n",
       max_list_length * (sizeof(RecordType) + row_upper_bound));

  // Pointers to the record data. Only the pointers will be sorted, not the records themselves
  SortPointer<RecordType> *sort_ptrs = new SortPointer<RecordType>[max_list_length];
  for (uint32_t i = 0; i < max_list_length; i++) {
    sort_ptrs[i].init(&data[i]);
  }
  perf("external_oblivious_sort: sort_ptrs occupies %d bytes in enclave memory\n",
       max_list_length * sizeof(SortPointer<RecordType>));

  uint32_t num_comparisons = 0, num_deep_comparisons = 0;

  if (num_buffers == 1) {
    debug("Sorting single buffer with %d rows, opcode %d\n", num_rows[0], op_code);
    sort_single_buffer(op_code, verify_set,
                       buffer_list[0], buffer_list[1], buffer_list[0],
					   num_rows[0],
                       sort_ptrs, max_list_length, row_upper_bound,
                       &num_comparisons, &num_deep_comparisons);
  } else {
    // Sort each buffer individually
    for (uint32_t i = 0; i < num_buffers; i++) {
      debug("[%s] Sorting buffer %d with %d rows, opcode %d\n", __FUNCTION__, i, num_rows[i], op_code);
      sort_single_buffer(op_code, verify_set,
                         buffer_list[i], buffer_list[i + 1], buffer_list[i],
						 num_rows[i],
                         sort_ptrs, max_list_length, row_upper_bound,
                         &num_comparisons, &num_deep_comparisons);
    }

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
              merge(op_code, verify_set,
                    buffer_list[idx], buffer_list[idx + 1], num_rows[idx],
                    buffer_list[pair_idx], buffer_list[pair_idx + 1], num_rows[pair_idx],
                    sort_ptrs, max_list_length, row_upper_bound,
                    &num_comparisons, &num_deep_comparisons);
            }
          }
        }
      }
    }
  }

  perf("external_oblivious_sort: %d comparisons, %d deep comparisons\n",
       num_comparisons, num_deep_comparisons);

  delete[] sort_ptrs;
  for (uint32_t i = 0; i < max_list_length; i++) {
    data[i].~RecordType();
  }
  free(data);
}
