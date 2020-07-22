#include "FlatbuffersWriters.h"
#include "EnclaveContext.h"
#include <iostream>

void RowWriter::clear() {
  builder.Clear();
  rows_vector.clear();
  total_num_rows = 0;
  enc_block_builder.Clear();
  log_entry_builder.Clear();
  log_entry_chain_builder.Clear();
  enc_block_vector.clear();
  finished = false;
}

void RowWriter::append(const tuix::Row *row) {
  rows_vector.push_back(flatbuffers_copy(row, builder));
  total_num_rows++;
  maybe_finish_block();
}

void RowWriter::append(const std::vector<const tuix::Field *> &row_fields) {
  flatbuffers::uoffset_t num_fields = row_fields.size();
  std::vector<flatbuffers::Offset<tuix::Field>> field_values(num_fields);
  for (flatbuffers::uoffset_t i = 0; i < num_fields; i++) {
    field_values[i] = flatbuffers_copy<tuix::Field>(row_fields[i], builder);
  }
  rows_vector.push_back(tuix::CreateRowDirect(builder, &field_values));
  total_num_rows++;
  maybe_finish_block();
}

void RowWriter::append(const tuix::Row *row1, const tuix::Row *row2) {
  flatbuffers::uoffset_t num_fields = row1->field_values()->size() + row2->field_values()->size();
  std::vector<flatbuffers::Offset<tuix::Field>> field_values(num_fields);
  flatbuffers::uoffset_t i = 0;
  for (auto it = row1->field_values()->begin(); it != row1->field_values()->end(); ++it, ++i) {
    field_values[i] = flatbuffers_copy<tuix::Field>(*it, builder);
  }
  for (auto it = row2->field_values()->begin(); it != row2->field_values()->end(); ++it, ++i) {
    field_values[i] = flatbuffers_copy<tuix::Field>(*it, builder);
  }
  rows_vector.push_back(tuix::CreateRowDirect(builder, &field_values));
  total_num_rows++;
  maybe_finish_block();
}

UntrustedBufferRef<tuix::EncryptedBlocks> RowWriter::output_buffer() {
  if (!finished) {
    finish_blocks();
  }

  // Allocate enc block builder's buffer size outside enclave
  uint8_t *buf_ptr;
  ocall_malloc(enc_block_builder.GetSize(), &buf_ptr);

  // Copy the buffer to untrusted memory
  std::unique_ptr<uint8_t, decltype(&ocall_free)> buf(buf_ptr, &ocall_free);
  memcpy(buf.get(), enc_block_builder.GetBufferPointer(), enc_block_builder.GetSize());

  // Create an UntrustedBufferRef out of the untrusted memory
  UntrustedBufferRef<tuix::EncryptedBlocks> buffer(
    std::move(buf), enc_block_builder.GetSize());

  return buffer;
}

void RowWriter::output_buffer(uint8_t **output_rows, size_t *output_rows_length, std::string ecall) {
  EnclaveContext::getInstance().set_log_entry_ecall(ecall);

  // Get the UntrustedBufferRef
  auto result = output_buffer();

  // output rows is a reference to encrypted blocks in untrusted memory
  *output_rows = result.buf.release();
  *output_rows_length = result.len;

}

uint32_t RowWriter::num_rows() {
  return total_num_rows;
}


void RowWriter::maybe_finish_block() {
  if (builder.GetSize() >= MAX_BLOCK_SIZE) {
    finish_block();
  }
}

void RowWriter::finish_block() {
  // Serialize the rows
  builder.Finish(tuix::CreateRowsDirect(builder, &rows_vector));
  size_t enc_rows_len = enc_size(builder.GetSize());

  // Allocate space for block in untrusted memory
  uint8_t *enc_rows_ptr = nullptr;
  ocall_malloc(enc_rows_len, &enc_rows_ptr);

  // Encrypt the serialized rows and push the ciphertext to untrusted memory
  std::unique_ptr<uint8_t, decltype(&ocall_free)> enc_rows(enc_rows_ptr, &ocall_free);
  encrypt(builder.GetBufferPointer(), builder.GetSize(), enc_rows.get());

  // Add each EncryptedBlock's MAC to the log entry so that next partition can check it
  uint8_t mac[SGX_AESGCM_MAC_SIZE];
  memcpy(mac, enc_rows.get() + SGX_AESGCM_IV_SIZE + builder.GetSize(), SGX_AESGCM_MAC_SIZE);
  EnclaveContext::getInstance().add_mac_to_mac_lst(mac);

  // Add the offset to enc_block_vector
  enc_block_vector.push_back(
      // Create offset into enc_block_builder where the entire EncryptedBlock is
    tuix::CreateEncryptedBlock(
      enc_block_builder,
      rows_vector.size(),
      // Create offset into enc_block_builder to find serialized rows
      enc_block_builder.CreateVector(enc_rows.get(), enc_rows_len)));

  // Clear the entire row FlatBufferBuilder
  builder.Clear();
  rows_vector.clear();
}

flatbuffers::Offset<tuix::EncryptedBlocks> RowWriter::finish_blocks() {
  if (rows_vector.size() > 0) {
    finish_block();
  }

  std::string curr_ecall = EnclaveContext::getInstance().get_log_entry_ecall();
  std::cout << "Finishing the Encrypted Blocks for ecall: " << EnclaveContext::getInstance().get_log_entry_ecall() << std::endl;
  int job_id = EnclaveContext::getInstance().get_job_id();

  size_t mac_lst_len = EnclaveContext::getInstance().get_mac_lst_len();
  uint8_t mac_lst[mac_lst_len * SGX_AESGCM_MAC_SIZE];
  EnclaveContext::getInstance().hmac_mac_lst(mac_lst);

  int eid = EnclaveContext::getInstance().get_eid();

  uint8_t* global_mac = EnclaveContext::getInstance().get_global_mac();

  std::vector<flatbuffers::Offset<tuix::LogEntry>> curr_log_entry_vector;

  // Some flatbufferes stuff to serialize mac lst and global mac into vectors
  // auto mac_lst_serialized = log_entry_builder.CreateVector(mac_lst);
  // auto global_mac_serialized = log_entry_builder.CreateVector(global_mac);
  // Some more flatbuffers stuff to create a Flatbuffers LogEntry object from the above 4 things
  // Retrieve the offset of the root
  auto log_entry_serialized = tuix::CreateLogEntry(log_entry_builder, 
      log_entry_builder.CreateString(curr_ecall),
      job_id,
      eid,
      mac_lst_len,
      log_entry_builder.CreateVector(mac_lst, mac_lst_len * SGX_AESGCM_MAC_SIZE),
      log_entry_builder.CreateVector(global_mac, SGX_AESGCM_MAC_SIZE));

  
  log_entry_builder.Finish(log_entry_serialized);
  // TODO: push the log entry to untrusted memory
  uint8_t* serialized_log_entry_ptr = nullptr;
  ocall_malloc(log_entry_builder.GetSize(), &serialized_log_entry_ptr);

  std::unique_ptr<uint8_t, decltype(&ocall_free)> log_entry(serialized_log_entry_ptr, &ocall_free);

  memcpy(log_entry.get(), log_entry_builder.GetBufferPointer(), log_entry_builder.GetSize());

  curr_log_entry_vector.push_back(log_entry.get())
  // log_entry.get() now contains pointer to serialized log_entry in untrusted memory
  
  // TODO: Retrieve all input log entries
  // TODO: Some more flatbuffers stuff to create a LogEntryChain serialization from the above created LogEntry and teh past entries

  // TODO: Add LogEntryChain to EncryptedBlocks object
  // TODO: hmac the serialized log entry chains?
  // auto enc_block_vector_offset = enc_block_builder.CreateVector(enc_block_vector, enc_block_vector.size());
  // auto result = tuix::CreateEncryptedBlocks(enc_block_builder, enc_block_vector_offset);
  auto result = tuix::CreateEncryptedBlocksDirect(enc_block_builder, &enc_block_vector, NULL); // TODO: Comment out this line in favor of the above two
  enc_block_builder.Finish(result);
  enc_block_vector.clear();

  finished = true;

  // Once we've serialized the log entry, reset it
  EnclaveContext::getInstance().reset_log_entry();

  return result;
}

void SortedRunsWriter::clear() {
  container.clear();
  runs.clear();
}

void SortedRunsWriter::append(const tuix::Row *row) {
  container.append(row);
}

void SortedRunsWriter::append(const std::vector<const tuix::Field *> &row_fields) {
  container.append(row_fields);
}

void SortedRunsWriter::append(const tuix::Row *row1, const tuix::Row *row2) {
  container.append(row1, row2);
}

void SortedRunsWriter::finish_run() {
  runs.push_back(container.finish_blocks());
  // TODO: Reset log entry
  // EnclaveContext::getInstance().reset_log_entry();
}

uint32_t SortedRunsWriter::num_runs() {
  return runs.size();
}

UntrustedBufferRef<tuix::SortedRuns> SortedRunsWriter::output_buffer() {
  container.enc_block_builder.Finish(
    tuix::CreateSortedRunsDirect(container.enc_block_builder, &runs));

  uint8_t *buf_ptr;
  ocall_malloc(container.enc_block_builder.GetSize(), &buf_ptr);

  std::unique_ptr<uint8_t, decltype(&ocall_free)> buf(buf_ptr, &ocall_free);
  memcpy(buf.get(),
         container.enc_block_builder.GetBufferPointer(),
         container.enc_block_builder.GetSize());

  UntrustedBufferRef<tuix::SortedRuns> buffer(
    std::move(buf), container.enc_block_builder.GetSize());

  // TODO do we need to reset log entry here?
  // EnclaveContext::getInstance().reset_log_entry();
  return buffer;
}

RowWriter *SortedRunsWriter::as_row_writer() {
  if (runs.size() > 1) {
    throw std::runtime_error("Invalid attempt to convert SortedRunsWriter with more than one run "
                             "to RowWriter");
  }

  return &container;
}
