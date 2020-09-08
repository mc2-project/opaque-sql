#include "Flatbuffers.h"

#ifndef FLATBUFFERS_WRITERS_H
#define FLATBUFFERS_WRITERS_H

using namespace edu::berkeley::cs::rise::opaque;

class UntrustedMemoryAllocator : public flatbuffers::Allocator {
public:
  virtual uint8_t *allocate(size_t size) {
    uint8_t *result = nullptr;
    ocall_malloc(size, &result);
    return result;
  }
  virtual void deallocate(uint8_t *p, size_t size) {
    (void)size;
    ocall_free(p);
  }
};

/** Append-only container for rows wrapped in tuix::EncryptedBlocks. */
class RowWriter {
public:
  RowWriter()
    : builder(), rows_vector(), total_num_rows(0), untrusted_alloc(),
      enc_block_builder(1024, &untrusted_alloc), finished(false) {}

  void clear();

  /** Append the given Row. */
  void append(const tuix::Row *row);

  /** Append the given `Field`s as a Row. */
  void append(const std::vector<const tuix::Field *> &row_fields);

  /** Concatenate the fields of the two given `Row`s and append the resulting single Row. */
  void append(const tuix::Row *row1, const tuix::Row *row2);

  /** Expose the stored rows as a buffer. */
  UntrustedBufferRef<tuix::EncryptedBlocks> output_buffer(std::string ecall);

  /** Expose the stored rows as a buffer. The caller takes ownership of the resulting buffer. */
  void output_buffer(uint8_t **output_rows, size_t *output_rows_length, std::string ecall);

  /** Count how many rows have been appended. */
  uint32_t num_rows();

private:
  void maybe_finish_block();
  void finish_block();
  flatbuffers::Offset<tuix::EncryptedBlocks> finish_blocks(std::string curr_ecall);

  flatbuffers::FlatBufferBuilder builder;
  std::vector<flatbuffers::Offset<tuix::Row>> rows_vector;
  uint32_t total_num_rows;

  // For writing the resulting EncryptedBlocks
  UntrustedMemoryAllocator untrusted_alloc;
  flatbuffers::FlatBufferBuilder enc_block_builder;
  std::vector<flatbuffers::Offset<tuix::EncryptedBlock>> enc_block_vector;

  bool finished;

  friend class SortedRunsWriter;
};

/** Append-only container for rows wrapped in tuix::SortedRuns. */
class SortedRunsWriter {
public:
  SortedRunsWriter() : container() {}

  void clear();

  /** Append the given Row. */
  void append(const tuix::Row *row);

  /** Append the given `Field`s as a Row. */
  void append(const std::vector<const tuix::Field *> &row_fields);

  /** Concatenate the fields of the two given `Row`s and append the resulting single Row. */
  void append(const tuix::Row *row1, const tuix::Row *row2);

  /**
   * Wrap all rows written since the last call to this method into a single sorted run.
   */
  void finish_run(std::string ecall);

  /** Count how many runs have been written (i.e., how many times `finish_run` has been called). */
  uint32_t num_runs();

  /** Expose the stored runs as a buffer. */
  UntrustedBufferRef<tuix::SortedRuns> output_buffer();

  /**
   * If there is only one run, expose it as as a RowWriter. This object retains ownership of the
   * returned pointer.
   */
  RowWriter *as_row_writer();

private:
  RowWriter container;
  std::vector<flatbuffers::Offset<tuix::EncryptedBlocks>> runs;
};

#endif
