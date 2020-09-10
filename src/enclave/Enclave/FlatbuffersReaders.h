#include "Flatbuffers.h"
#include "IntegrityUtils.h"

#ifndef FLATBUFFERS_READERS_H
#define FLATBUFFERS_READERS_H

using namespace edu::berkeley::cs::rise::opaque;

/**
 * A reader for Row objects within an EncryptedBlock object that provides both iterator-based and
 * range-style interfaces.
 */
class EncryptedBlockToRowReader {
public:
  EncryptedBlockToRowReader() : rows(nullptr), initialized(false) {}

  void reset(const tuix::EncryptedBlock *encrypted_block);

  bool has_next() {
    return initialized && row_idx < rows->rows()->size();
  }

  const tuix::Row *next() {
    return rows->rows()->Get(row_idx++);
  }

  flatbuffers::Vector<flatbuffers::Offset<tuix::Row>>::const_iterator begin() {
    return rows->rows()->begin();
  }

  flatbuffers::Vector<flatbuffers::Offset<tuix::Row>>::const_iterator end() {
    return rows->rows()->end();
  }

private:
  std::unique_ptr<uint8_t> rows_buf;
  const tuix::Rows *rows;
  uint32_t row_idx;
  bool initialized;
};

/** An iterator-style reader for Rows organized into EncryptedBlocks. */
class RowReader {
public:
  RowReader(BufferRefView<tuix::EncryptedBlocks> buf);
  RowReader(const tuix::EncryptedBlocks *encrypted_blocks, bool log_init=true);

  void reset(BufferRefView<tuix::EncryptedBlocks> buf);
  void reset(const tuix::EncryptedBlocks *encrypted_blocks, bool log_init=true);

  uint32_t num_rows();
  bool has_next();
  /** Access the next Row. Invalidates any previously-returned Row pointers. */
  const tuix::Row *next();

private:
  void init_block_reader();

  const tuix::EncryptedBlocks *encrypted_blocks;
  uint32_t block_idx;
  EncryptedBlockToRowReader block_reader;
};

/**
 * A reader for Rows organized into sorted runs.
 *
 * Different runs can be read independently. Within a run, access is performed using an
 * iterator-style sequential interface.
 */
class SortedRunsReader {
public:
  SortedRunsReader(BufferRefView<tuix::SortedRuns> buf, bool log_init=true);

  void reset(BufferRefView<tuix::SortedRuns> buf, bool log_init=true);

  uint32_t num_runs();
  bool run_has_next(uint32_t run_idx);
  /**
   * Access the next Row from the given run. Invalidates any previously-returned Row pointers from
   * the same run.
   */
  const tuix::Row *next_from_run(uint32_t run_idx);

private:
  const tuix::SortedRuns *sorted_runs;
  std::vector<RowReader> run_readers;
};

/** A range-style reader for EncryptedBlock objects within an EncryptedBlocks object. */
class EncryptedBlocksToEncryptedBlockReader {
public:
  EncryptedBlocksToEncryptedBlockReader(BufferRefView<tuix::EncryptedBlocks> buf) {
    buf.verify();
    encrypted_blocks = buf.root();
    init_log(encrypted_blocks);
  }
  flatbuffers::Vector<flatbuffers::Offset<tuix::EncryptedBlock>>::const_iterator begin() {
    return encrypted_blocks->blocks()->begin();
  }
  flatbuffers::Vector<flatbuffers::Offset<tuix::EncryptedBlock>>::const_iterator end() {
    return encrypted_blocks->blocks()->end();
  }

private:
  const tuix::EncryptedBlocks *encrypted_blocks;
};


#endif
