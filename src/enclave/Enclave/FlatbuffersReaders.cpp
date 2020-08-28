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

RowReader::RowReader(const tuix::EncryptedBlocks *encrypted_blocks) {
  reset(encrypted_blocks);
}

void RowReader::reset(BufferRefView<tuix::EncryptedBlocks> buf) {
  buf.verify();
  reset(buf.root());
}

void RowReader::reset(const tuix::EncryptedBlocks *encrypted_blocks) {
  this->encrypted_blocks = encrypted_blocks;
  init_log(encrypted_blocks);

  block_idx = 0;
  init_block_reader();
}

void init_log(const tuix::EncryptedBlocks *encrypted_blocks) {
  // Add past entries to log first
  std::vector<LogEntry> past_log_entries;
  auto curr_entries_vec = encrypted_blocks->log()->curr_entries();
  auto past_entries_vec = encrypted_blocks->log()->past_entries();

  for (uint32_t i = 0; i < past_entries_vec->size(); i++) {
    auto entry = past_entries_vec->Get(i);
    std::string op = entry->op()->str();
    int snd_pid = entry->snd_pid();
    int rcv_pid = entry->rcv_pid();
    if (rcv_pid == -1) { // Received by PID hasn't been set yet
      rcv_pid = EnclaveContext::getInstance().get_pid();
    }
    int job_id = entry->job_id();
    EnclaveContext::getInstance().append_past_log_entry(op, snd_pid, rcv_pid, job_id);

    // Initialize log entry object
    LogEntry le;
    le.op = op;
    le.snd_pid = snd_pid;
    le.rcv_pid = rcv_pid;
    le.job_id = job_id;
    past_log_entries.push_back(le);
  }

  if (curr_entries_vec->size() > 0) {
    verify_log(encrypted_blocks, past_log_entries);
  }

  // Master list of mac lists of all input partitions
  std::vector<std::vector<std::vector<uint8_t>>> partition_mac_lsts;

  // Check that each input partition's global_mac is indeed a HMAC over the mac_lst
  // auto curr_entries_vec = encrypted_blocks->log()->curr_entries();
  for (uint32_t i = 0; i < curr_entries_vec->size(); i++) {
    auto input_log_entry = curr_entries_vec->Get(i);

    // Copy over the global mac for this input log entry
    uint8_t global_mac[SGX_AESGCM_MAC_SIZE];
    memcpy(global_mac, input_log_entry->global_mac()->data(), SGX_AESGCM_MAC_SIZE);

    // Copy over the mac_lst
    uint32_t num_macs = input_log_entry->num_macs();
    uint8_t mac_lst[num_macs * SGX_AESGCM_MAC_SIZE];
    memcpy(mac_lst, input_log_entry->mac_lst()->data(), num_macs * SGX_AESGCM_MAC_SIZE);
    
    uint8_t computed_hmac[OE_HMAC_SIZE];
    mcrypto.hmac(mac_lst, num_macs * SGX_AESGCM_MAC_SIZE, computed_hmac);

    // Check that the global mac is as computed
    if (!std::equal(std::begin(global_mac), std::end(global_mac), std::begin(computed_hmac))) {
      throw std::runtime_error("MAC over Encrypted Block MACs from one partition is invalid");
    }
    
    uint8_t* tmp_ptr = mac_lst;

    // the mac list of one input log entry (from one partition) in vector form
    std::vector<std::vector<uint8_t>> p_mac_lst;
    for (uint32_t i = 0; i < num_macs; i++) {
      std::vector<uint8_t> a_mac (tmp_ptr, tmp_ptr + SGX_AESGCM_MAC_SIZE);
      p_mac_lst.push_back(a_mac);
      tmp_ptr += SGX_AESGCM_MAC_SIZE;
    }

    // Add the macs of this partition to the master list
    partition_mac_lsts.push_back(p_mac_lst);

    // Add this input log entry to history of log entries
    EnclaveContext::getInstance().append_past_log_entry(input_log_entry->op()->str(), input_log_entry->snd_pid(), EnclaveContext::getInstance().get_pid(), input_log_entry->job_id());
  }

  if (curr_entries_vec->size() > 0) {
    // Check that the MAC of each input EncryptedBlock was expected, i.e. also sent in the LogEntry
    for (auto it = encrypted_blocks->blocks()->begin(); it != encrypted_blocks->blocks()->end(); ++it) {
      size_t ptxt_size = dec_size(it->enc_rows()->size());
      uint8_t* mac_ptr = (uint8_t*) (it->enc_rows()->data() + SGX_AESGCM_IV_SIZE + ptxt_size);
      std::vector<uint8_t> cipher_mac (mac_ptr, mac_ptr + SGX_AESGCM_MAC_SIZE); 

      // Find this element in partition_mac_lsts;
      bool mac_in_lst = false;
      for (uint32_t i = 0; i < partition_mac_lsts.size(); i++) {
        bool found = false;
        for (uint32_t j = 0; j < partition_mac_lsts[i].size(); j++) {
          if (cipher_mac == partition_mac_lsts[i][j]) {
            partition_mac_lsts[i].erase(partition_mac_lsts[i].begin() + j);
            found = true;
            // std::cout << "Found one mac!\n";
            break;
          }
        }
        if (found) {
          mac_in_lst = true;
          break;
        }
      }

      if (!mac_in_lst) {
        throw std::runtime_error("Unexpected block given as input to the enclave");
      }
    }

    // Check that partition_mac_lsts is now empty - we should've found all expected MACs
    for (std::vector<std::vector<uint8_t>> p_lst : partition_mac_lsts) {
      if (!p_lst.empty()) {
        throw std::runtime_error("Did not receive expected EncryptedBlocks");
      }
    }
  }
}

void verify_log(const tuix::EncryptedBlocks *encrypted_blocks, std::vector<LogEntry> past_log_entries) {
  std::cout << "Verifying log\n";
  std::cout << "past log etnries size: " << past_log_entries.size() << std::endl;
  uint8_t expected_hash[32];
  memcpy(expected_hash, encrypted_blocks->log_hash()->data(), 32);

  std::cout << "retrieved expected hash\n";

  auto curr_entries_vec = encrypted_blocks->log()->curr_entries();
  if (curr_entries_vec->size() > 0) {
    // TODO: figure out how concatenation will work
    auto curr_log_entry = curr_entries_vec->Get(0);
    std::cout << "got curr log entry\n";
    std::string curr_ecall = curr_log_entry->op()->str();
    std::cout << "curr eacall\n";
    int snd_pid = curr_log_entry->snd_pid();
    std::cout << "snd pid\n";
    int rcv_pid = -1;
    int job_id = curr_log_entry->job_id();
    std::cout << "job id\n";
    int num_macs = curr_log_entry->num_macs();

    std::cout << "Retrieved attributes from log entry\n";

    uint8_t global_mac[SGX_AESGCM_MAC_SIZE];
    memcpy(global_mac, curr_log_entry->global_mac()->data(), SGX_AESGCM_MAC_SIZE);

    int num_bytes_to_hash = OE_HMAC_SIZE + 3 * sizeof(int) + sizeof(size_t) + curr_ecall.length() + 1 + past_log_entries.size() * sizeof(LogEntry);
    uint8_t to_hash[num_bytes_to_hash];
    memcpy(to_hash, global_mac, OE_HMAC_SIZE);
    memcpy(to_hash + OE_HMAC_SIZE, curr_ecall.c_str(), curr_ecall.length() + 1);
    *(to_hash + OE_HMAC_SIZE + curr_ecall.length() + 1) = snd_pid;
    *(to_hash + OE_HMAC_SIZE + curr_ecall.length() + 1 + sizeof(int)) = rcv_pid;
    *(to_hash + OE_HMAC_SIZE + curr_ecall.length() + 1 + 2 * sizeof(int)) = job_id;
    *(to_hash + OE_HMAC_SIZE + curr_ecall.length() + 1 + 3 * sizeof(int)) = num_macs;
    memcpy(to_hash + OE_HMAC_SIZE + curr_ecall.length() + 1 + 3 * sizeof(int) + sizeof(size_t), past_log_entries.data(), past_log_entries.size() * sizeof(LogEntry));

    // Hash the data
    std::cout << "About to hash data\n";
    uint8_t actual_hash[32];
    mcrypto.sha256(to_hash, num_bytes_to_hash, actual_hash);

    std::cout << "Hashed data\n";
    for (int i = 0; i < 32; i++) {
      if (expected_hash[i] != actual_hash[i]) {
        throw std::runtime_error("Hash did not match");
      }
    }
  }
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

SortedRunsReader::SortedRunsReader(BufferRefView<tuix::SortedRuns> buf) {
  reset(buf);
}

void SortedRunsReader::reset(BufferRefView<tuix::SortedRuns> buf) {
  buf.verify();
  sorted_runs = buf.root();
  run_readers.clear();
  for (auto it = sorted_runs->runs()->begin(); it != sorted_runs->runs()->end(); ++it) {
    run_readers.push_back(RowReader(*it));
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
