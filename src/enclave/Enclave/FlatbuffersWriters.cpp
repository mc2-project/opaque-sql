#include "FlatbuffersWriters.h"
#include "IntegrityUtils.h"
#include <iostream>

void RowWriter::clear() {
  builder.Clear();
  rows_vector.clear();
  total_num_rows = 0;
  enc_block_builder.Clear();
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

UntrustedBufferRef<tuix::EncryptedBlocks> RowWriter::output_buffer(std::string ecall) {
  if (!finished) { 
    finish_blocks(ecall);
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

void RowWriter::output_buffer(uint8_t **output_rows, size_t *output_rows_length, 
    std::string ecall) {
  // Get the UntrustedBufferRef
  auto result = output_buffer(ecall);

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

  // TODO: create a temporary buffer that stores serialized rows inside enclave, 
  // then copy these rows to enc_rows.get() to retrieve MAC
  encrypt(builder.GetBufferPointer(), builder.GetSize(), enc_rows.get());

  // Add each EncryptedBlock's MAC to the log entry so that next partition can check it
  // we only want to add the mac if it's not part of the join primary group reader
  if (EnclaveContext::getInstance().to_append_mac()) {
    uint8_t mac[SGX_AESGCM_MAC_SIZE];
    memcpy(mac, enc_rows.get() + SGX_AESGCM_IV_SIZE + builder.GetSize(), SGX_AESGCM_MAC_SIZE);
    EnclaveContext::getInstance().add_mac_to_mac_lst(mac);
  }

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

flatbuffers::Offset<tuix::EncryptedBlocks> RowWriter::finish_blocks(std::string curr_ecall) {
  if (rows_vector.size() > 0) {
    finish_block();
  }

  std::vector<flatbuffers::Offset<tuix::LogEntry>> curr_log_entry_vector;
  std::vector<flatbuffers::Offset<tuix::Crumb>> serialized_crumbs_vector;
  std::vector<int> num_crumbs_vector;
  std::vector<flatbuffers::Offset<tuix::Mac>> log_mac_vector;
  
  if (curr_ecall != std::string("")) {
    // Only write log entry chain if this is the output of an ecall, 
    // i.e. not intermediate output within an ecall
    int num_macs = static_cast<int>(EnclaveContext::getInstance().get_num_macs());
    uint8_t mac_lst[num_macs * SGX_AESGCM_MAC_SIZE];
    uint8_t mac_lst_mac[OE_HMAC_SIZE];
    EnclaveContext::getInstance().hmac_mac_lst(mac_lst, mac_lst_mac);

    int curr_ecall_id = EnclaveContext::getInstance().get_ecall_id(curr_ecall);

    // Copy mac list to untrusted memory
    uint8_t* untrusted_mac_lst = nullptr;
    ocall_malloc(num_macs * SGX_AESGCM_MAC_SIZE, &untrusted_mac_lst);
    std::unique_ptr<uint8_t, decltype(&ocall_free)> mac_lst_ptr(untrusted_mac_lst, 
        &ocall_free);
    memcpy(mac_lst_ptr.get(), mac_lst, num_macs * SGX_AESGCM_MAC_SIZE);

    // Copy global mac to untrusted memory
    uint8_t* untrusted_mac_lst_mac = nullptr;
    ocall_malloc(OE_HMAC_SIZE, &untrusted_mac_lst_mac);
    std::unique_ptr<uint8_t, decltype(&ocall_free)> mac_lst_mac_ptr(untrusted_mac_lst_mac, 
        &ocall_free);
    memcpy(mac_lst_mac_ptr.get(), mac_lst_mac, OE_HMAC_SIZE);

    // Copy input macs to untrusted memory
    std::vector<uint8_t> vector_input_macs = EnclaveContext::getInstance().get_input_macs();
    int num_input_macs = EnclaveContext::getInstance().get_num_input_macs();
    uint8_t* input_macs = vector_input_macs.data();

    uint8_t* untrusted_input_macs = nullptr;
    ocall_malloc(OE_HMAC_SIZE * num_input_macs, &untrusted_input_macs);
    std::unique_ptr<uint8_t, decltype(&ocall_free)> input_macs_ptr(untrusted_input_macs, 
        &ocall_free);
    memcpy(input_macs_ptr.get(), input_macs, OE_HMAC_SIZE * num_input_macs);

    // This is an offset into enc block builder
    auto log_entry_serialized = tuix::CreateLogEntry(enc_block_builder,
        curr_ecall_id,
        num_macs,
        enc_block_builder.CreateVector(mac_lst_ptr.get(), num_macs * SGX_AESGCM_MAC_SIZE),
        enc_block_builder.CreateVector(mac_lst_mac_ptr.get(), OE_HMAC_SIZE),
        enc_block_builder.CreateVector(input_macs_ptr.get(), num_input_macs * OE_HMAC_SIZE),
        num_input_macs);

    curr_log_entry_vector.push_back(log_entry_serialized);


    // Serialize stored crumbs
    std::vector<Crumb> crumbs = EnclaveContext::getInstance().get_crumbs();
    for (Crumb crumb : crumbs) {
        int crumb_num_input_macs = crumb.num_input_macs;
        int crumb_ecall = crumb.ecall;

        // FIXME: do these need to be memcpy'ed
        std::vector<uint8_t> crumb_input_macs = crumb.input_log_macs;
        uint8_t* crumb_all_outputs_mac = crumb.all_outputs_mac;
        uint8_t* crumb_log_mac = crumb.log_mac;

        // Copy crumb input macs to untrusted memory
        uint8_t* untrusted_crumb_input_macs = nullptr;
        ocall_malloc(crumb_num_input_macs * OE_HMAC_SIZE, &untrusted_crumb_input_macs);
        std::unique_ptr<uint8_t, decltype(&ocall_free)> crumb_input_macs_ptr(untrusted_crumb_input_macs, 
            &ocall_free);
        memcpy(crumb_input_macs_ptr.get(), crumb_input_macs.data(), crumb_num_input_macs * OE_HMAC_SIZE);

        // Copy crumb all_outputs_mac to untrusted memory
        uint8_t* untrusted_crumb_all_outputs_mac = nullptr;
        ocall_malloc(OE_HMAC_SIZE, &untrusted_crumb_all_outputs_mac);
        std::unique_ptr<uint8_t, decltype(&ocall_free)> crumb_all_outputs_mac_ptr(untrusted_crumb_all_outputs_mac, 
            &ocall_free);
        memcpy(crumb_all_outputs_mac_ptr.get(), crumb_all_outputs_mac, OE_HMAC_SIZE);

        // Copy crumb log_mac to untrusted memory
        uint8_t* untrusted_crumb_log_mac = nullptr;
        ocall_malloc(OE_HMAC_SIZE, &untrusted_crumb_log_mac);
        std::unique_ptr<uint8_t, decltype(&ocall_free)> crumb_log_mac_ptr(untrusted_crumb_log_mac, 
            &ocall_free);
        memcpy(crumb_log_mac_ptr.get(), crumb_log_mac, OE_HMAC_SIZE);

        auto serialized_crumb = tuix::CreateCrumb(enc_block_builder,
          enc_block_builder.CreateVector(crumb_input_macs_ptr.get(), crumb_num_input_macs * OE_HMAC_SIZE),
          crumb_num_input_macs,
          enc_block_builder.CreateVector(crumb_all_outputs_mac_ptr.get(), OE_HMAC_SIZE),
          crumb_ecall,
          enc_block_builder.CreateVector(crumb_log_mac_ptr.get(), OE_HMAC_SIZE));

        serialized_crumbs_vector.push_back(serialized_crumb);
    }


    int num_crumbs = (int) serialized_crumbs_vector.size();
    num_crumbs_vector.push_back(num_crumbs);

    // Calculate how many bytes we should MAC over
    // curr_log_entry contains:
    //  * mac_lst_mac of size OE_HMAC_SIZE
    //  * input_macs of size OE_HMAC_SIZE * num_input_macs
    //  * 3 ints (ecall, num_macs, num_input_macs)
    // 1 crumb contains:
    //  * 2 ints (ecall, num_input_macs)
    //  * input_macs of size OE_HMAC_SIZE * num_input_macs
    //  * all_outputs_mac of size OE_HMAC_SIZE
    //  * log_mac of size OE_HMAC_SIZE
    int log_entry_num_bytes_to_mac = 3 * sizeof(int) + OE_HMAC_SIZE + num_input_macs * OE_HMAC_SIZE;

    int total_crumb_bytes = 0;
    for (uint32_t k = 0; k < crumbs.size(); k++) {
        int num_bytes_in_crumb = 2 * sizeof(int) + 2 * OE_HMAC_SIZE + OE_HMAC_SIZE * crumbs[k].num_input_macs; 
        total_crumb_bytes += num_bytes_in_crumb;
    } 

    // Below, we add sizeof(int) to include the num_past_entries entry that is part of LogEntryChain 
    int num_bytes_to_mac = log_entry_num_bytes_to_mac + total_crumb_bytes + sizeof(int);
    // FIXME: VLA
    uint8_t to_mac[num_bytes_to_mac];
    uint8_t log_mac[OE_HMAC_SIZE];
    mac_log_entry_chain(num_bytes_to_mac, to_mac, curr_ecall_id, num_macs, num_input_macs,
            mac_lst_mac, input_macs, num_crumbs, crumbs, 0, num_crumbs, log_mac); 

    // Copy the log_mac to untrusted memory
    uint8_t* untrusted_log_mac = nullptr;
    ocall_malloc(OE_HMAC_SIZE, &untrusted_log_mac);
    std::unique_ptr<uint8_t, decltype(&ocall_free)> log_mac_ptr(untrusted_log_mac, &ocall_free);
    memcpy(log_mac_ptr.get(), log_mac, OE_HMAC_SIZE);
    auto log_mac_offset = tuix::CreateMac(enc_block_builder, 
        enc_block_builder.CreateVector(log_mac_ptr.get(), OE_HMAC_SIZE));
    log_mac_vector.push_back(log_mac_offset);

    EnclaveContext::getInstance().append_log_mac(log_mac);

    // Clear log entry state
    EnclaveContext::getInstance().reset_log_entry();
  } 
  auto log_entry_chain_serialized = tuix::CreateLogEntryChainDirect(enc_block_builder, 
      &curr_log_entry_vector, &serialized_crumbs_vector, &num_crumbs_vector);

  // Create dummy array that isn't default, so that we can modify it using Flatbuffers mutation later
  uint8_t dummy_all_outputs_mac[OE_HMAC_SIZE] = {1};
  std::vector<uint8_t> all_outputs_mac_vector (dummy_all_outputs_mac, dummy_all_outputs_mac + OE_HMAC_SIZE);

  auto result = tuix::CreateEncryptedBlocksDirect(enc_block_builder, &enc_block_vector, 
      log_entry_chain_serialized, &log_mac_vector, &all_outputs_mac_vector);
  enc_block_builder.Finish(result);
  enc_block_vector.clear();

  finished = true;

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

void SortedRunsWriter::finish_run(std::string ecall) {
  runs.push_back(container.finish_blocks(ecall));
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

  return buffer;
}

RowWriter *SortedRunsWriter::as_row_writer() {
  if (runs.size() > 1) {
    throw std::runtime_error("Invalid attempt to convert SortedRunsWriter with more than one run "
                             "to RowWriter");
  }

  return &container;
}
