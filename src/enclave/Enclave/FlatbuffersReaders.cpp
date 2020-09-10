#include "FlatbuffersReaders.h"
#include "../Common/mCrypto.h"
#include "../Common/common.h"

void EncryptedBlockToRowReader::reset(const tuix::EncryptedBlock *encrypted_block) {
  uint32_t num_rows = encrypted_block->num_rows();

  // Decrypt encrypted block here
  const size_t rows_len = dec_size(encrypted_block->enc_rows()->size());
  rows_buf.reset(new uint8_t[rows_len]);
  // Decrypt one encrypted block at a time
  decrypt(encrypted_block->enc_rows()->data(), encrypted_block->enc_rows()->size(),
          rows_buf.get());
  BufferRefView<tuix::Rows> buf(rows_buf.get(), rows_len);
  buf.verify();

  rows = buf.root();
  if (rows->rows()->size() != num_rows) {
    throw std::runtime_error(
      std::string("EncryptedBlock claimed to contain ")
      + std::to_string(num_rows)
      + std::string("rows but actually contains ")
      + std::to_string(rows->rows()->size())
      + std::string(" rows"));
  }

  row_idx = 0;
  initialized = true;
}

RowReader::RowReader(BufferRefView<tuix::EncryptedBlocks> buf) {
  reset(buf);
}

RowReader::RowReader(const tuix::EncryptedBlocks *encrypted_blocks, bool log_init) {
  reset(encrypted_blocks, log_init);
}

void RowReader::reset(BufferRefView<tuix::EncryptedBlocks> buf) {
  buf.verify();
  reset(buf.root());
}

void RowReader::reset(const tuix::EncryptedBlocks *encrypted_blocks, bool log_init) {
  this->encrypted_blocks = encrypted_blocks;
  if (log_init) {
    init_log(encrypted_blocks);
  }

  block_idx = 0;
  init_block_reader();
}

uint32_t RowReader::num_rows() {
  uint32_t result = 0;
  for (auto it = encrypted_blocks->blocks()->begin();
       it != encrypted_blocks->blocks()->end(); ++it) {
    result += it->num_rows();
  }
  return result;
}

bool RowReader::has_next() {
  return block_reader.has_next() || block_idx + 1 < encrypted_blocks->blocks()->size();
}

const tuix::Row *RowReader::next() {
  // Note: this will invalidate any pointers returned by previous invocations of this method
  if (!block_reader.has_next()) {
    assert(block_idx + 1 < encrypted_blocks->blocks()->size());
    block_idx++;
    init_block_reader();
  }

  return block_reader.next();
}

void RowReader::init_block_reader() {
  if (block_idx < encrypted_blocks->blocks()->size()) {
    block_reader.reset(encrypted_blocks->blocks()->Get(block_idx));
  }
}

SortedRunsReader::SortedRunsReader(BufferRefView<tuix::SortedRuns> buf, bool log_init) {
  reset(buf, log_init);
}

void SortedRunsReader::reset(BufferRefView<tuix::SortedRuns> buf, bool log_init) {
  buf.verify();
  sorted_runs = buf.root();
  run_readers.clear();
  for (auto it = sorted_runs->runs()->begin(); it != sorted_runs->runs()->end(); ++it) {
    run_readers.push_back(RowReader(*it, log_init));
  }
}

uint32_t SortedRunsReader::num_runs() {
  return sorted_runs->runs()->size();
}

bool SortedRunsReader::run_has_next(uint32_t run_idx) {
  return run_readers[run_idx].has_next();
}

const tuix::Row *SortedRunsReader::next_from_run(uint32_t run_idx) {
  return run_readers[run_idx].next();
}
