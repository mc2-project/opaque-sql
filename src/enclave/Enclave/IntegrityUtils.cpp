#include "IntegrityUtils.h"
#include <iostream>

void init_log(const tuix::EncryptedBlocks *encrypted_blocks) {
  // Add past entries to log first
  std::vector<Crumb> crumbs;
  auto curr_entries_vec = encrypted_blocks->log()->curr_entries(); // of type LogEntry
  auto past_entries_vec = encrypted_blocks->log()->past_entries(); // of type Crumb

  // Store received crumbs
  for (uint32_t i = 0; i < past_entries_vec->size(); i++) {
    auto crumb = past_entries_vec->Get(i);
    int crumb_ecall = crumb->ecall();
    const uint8_t* crumb_log_mac = crumb->log_mac()->data(); 
    const uint8_t* crumb_all_outputs_mac = crumb->all_outputs_mac()->data();
    const uint8_t* crumb_input_macs = crumb->input_macs()->data();
    int crumb_num_input_macs = crumb->num_input_macs();

    std::vector<uint8_t> crumb_vector_input_macs(crumb_input_macs, crumb_input_macs + crumb_num_input_macs * OE_HMAC_SIZE);

    EnclaveContext::getInstance().append_crumb(crumb_ecall, crumb_log_mac, crumb_all_outputs_mac, crumb_num_input_macs, crumb_vector_input_macs);

    // Initialize crumb for LogEntryChain MAC verification
    Crumb new_crumb;
    new_crumb.ecall = crumb_ecall;
    memcpy(new_crumb.log_mac, crumb_log_mac, OE_HMAC_SIZE);
    memcpy(new_crumb.all_outputs_mac, crumb_all_outputs_mac, OE_HMAC_SIZE);
    new_crumb.num_input_macs = crumb_num_input_macs;
    new_crumb.input_log_macs = crumb_vector_input_macs;
    crumbs.push_back(new_crumb);
  }

  if (curr_entries_vec->size() > 0) {
    verify_log(encrypted_blocks, crumbs);
  }

  // Master list of mac lists of all input partitions
  std::vector<std::vector<std::vector<uint8_t>>> partition_mac_lsts;

  const uint8_t* mac_inputs = encrypted_blocks->all_outputs_mac()->data();
  int all_outputs_mac_index = 0;

  // Check that each input partition's mac_lst_mac is indeed a HMAC over the mac_lst
  for (uint32_t i = 0; i < curr_entries_vec->size(); i++) {
    auto input_log_entry = curr_entries_vec->Get(i);

    // Retrieve mac_lst and mac_lst_mac
    const uint8_t* mac_lst_mac = input_log_entry->mac_lst_mac()->data();
    int num_macs = input_log_entry->num_macs();
    const uint8_t* mac_lst = input_log_entry->mac_lst()->data();
    
    uint8_t computed_hmac[OE_HMAC_SIZE];
    mcrypto.hmac(mac_lst, num_macs * SGX_AESGCM_MAC_SIZE, computed_hmac);

    // Check that the mac lst hasn't been tampered with
    for (int j = 0; j < OE_HMAC_SIZE; j++) {
        if (mac_lst_mac[j] != computed_hmac[j]) {
            throw std::runtime_error("MAC over Encrypted Block MACs from one partition is invalid");
        }
    }
    
    uint8_t* tmp_ptr = (uint8_t*) mac_lst;

    // the mac list of one input log entry (from one partition) in vector form
    std::vector<std::vector<uint8_t>> p_mac_lst;
    for (int j = 0; j < num_macs; j++) {
      std::vector<uint8_t> a_mac (tmp_ptr, tmp_ptr + SGX_AESGCM_MAC_SIZE);
      p_mac_lst.push_back(a_mac);
      tmp_ptr += SGX_AESGCM_MAC_SIZE;
    }

    // Add the macs of this partition to the master list
    partition_mac_lsts.push_back(p_mac_lst);

    // Add this input log entry to history of log entries
    int logged_ecall = input_log_entry->ecall();
    int num_prev_input_macs = input_log_entry->num_input_macs();
    const uint8_t* prev_input_macs = input_log_entry->input_macs()->data(); 
    std::vector<uint8_t> vector_prev_input_macs(prev_input_macs, prev_input_macs + num_prev_input_macs * OE_HMAC_SIZE);

    // Create new crumb given recently received EncryptedBlocks
    // const uint8_t* mac_input = encrypted_blocks->all_outputs_mac()->Get(i)->mac()->data(); 
    const uint8_t* mac_input = mac_inputs + all_outputs_mac_index;

    // The following prints out the received all_outputs_mac
    // for (int j = 0; j < OE_HMAC_SIZE; j++) {
    //     std::cout << (int) mac_input[j] << " ";
    // }
    // std::cout << std::endl;
    EnclaveContext::getInstance().append_crumb(
        logged_ecall, encrypted_blocks->log_mac()->Get(i)->mac()->data(), 
        mac_input, num_prev_input_macs, vector_prev_input_macs);

    std::vector<uint8_t> mac_input_vector(mac_input, mac_input + OE_HMAC_SIZE);
    EnclaveContext::getInstance().append_input_mac(mac_input_vector);

    all_outputs_mac_index += OE_HMAC_SIZE;

  }

  if (curr_entries_vec->size() > 0) {
    // Check that the MAC of each input EncryptedBlock was expected, i.e. also sent in the LogEntry
    for (auto it = encrypted_blocks->blocks()->begin(); it != encrypted_blocks->blocks()->end(); 
        ++it) {
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
        throw std::runtime_error("Did not receive expected EncryptedBlock");
      }
    }
  }
}

// Check that log entry chain has not been tampered with
void verify_log(const tuix::EncryptedBlocks *encrypted_blocks, 
    std::vector<Crumb> crumbs) {
  auto num_past_entries_vec = encrypted_blocks->log()->num_past_entries();
  auto curr_entries_vec = encrypted_blocks->log()->curr_entries();

  if (curr_entries_vec->size() > 0) {
    int num_curr_entries = curr_entries_vec->size();
    int past_entries_seen = 0;

    for (int i = 0; i < num_curr_entries; i++) {
      auto curr_log_entry = curr_entries_vec->Get(i);
      int curr_ecall = curr_log_entry->ecall();
      int num_macs = curr_log_entry->num_macs();
      int num_input_macs = curr_log_entry->num_input_macs();
      int num_past_entries = num_past_entries_vec->Get(i);

      // Calculate how many bytes we need to MAC over
      int log_entry_num_bytes_to_mac = 3 * sizeof(int) + OE_HMAC_SIZE + num_input_macs * OE_HMAC_SIZE;
      int total_crumb_bytes = 0;
      for (int j = past_entries_seen; j < past_entries_seen + num_past_entries; j++) {
          // crumb.ecall, crumb.num_input_macs are ints 
          // crumb.all_outputs_mac, crumb.log_mac are of size OE_HMAC_SIZE
          // crumb.input_macs is of size num_input_macs * OE_HMAC_SIZE
          int num_bytes_in_crumb = 2 * sizeof(int) + 2 * OE_HMAC_SIZE + OE_HMAC_SIZE * crumbs[j].num_input_macs;
          total_crumb_bytes += num_bytes_in_crumb;
      }
      // Below, we add sizeof(int) to include the num_past_entries entry that is part of LogEntryChain 
      int total_bytes_to_mac = log_entry_num_bytes_to_mac + total_crumb_bytes + sizeof(int);

      // std::cout << "log entry num bytes: " << log_entry_num_bytes_to_mac << std::endl;
      // std::cout << "total crumb bytes: " << total_crumb_bytes << std::endl;

      // FIXME: variable length array
      uint8_t to_mac[total_bytes_to_mac];

      // MAC the data
      // std::cout << "Macing data" << std::endl;
      uint8_t actual_mac[OE_HMAC_SIZE];
      // std::cout << "Checking log mac************" << std::endl;
      mac_log_entry_chain(total_bytes_to_mac, to_mac, curr_ecall, num_macs, num_input_macs, 
              (uint8_t*) curr_log_entry->mac_lst_mac()->data(), (uint8_t*) curr_log_entry->input_macs()->data(),
              num_past_entries, crumbs, past_entries_seen,
              past_entries_seen + num_past_entries, actual_mac);

      uint8_t expected_mac[OE_HMAC_SIZE];
      memcpy(expected_mac, encrypted_blocks->log_mac()->Get(i)->mac()->data(), OE_HMAC_SIZE);

      if (!std::equal(std::begin(expected_mac), std::end(expected_mac), std::begin(actual_mac))) {
        throw std::runtime_error("MAC did not match");
      }
      past_entries_seen += num_past_entries;
    }
  }
}

void mac_log_entry_chain(int num_bytes_to_mac, uint8_t* to_mac, int curr_ecall, int num_macs, int num_input_macs,
    uint8_t* mac_lst_mac, uint8_t* input_macs, 
    int num_past_entries, std::vector<Crumb> crumbs, int first_crumb_index, 
    int last_crumb_index, uint8_t* ret_hmac) {

    // first_crumb_index refers to the first index in crumbs where the element was originally part of same EncryptedBlocks as 
    // the curr_log_entry

  // Copy what we want to mac to contiguous memory
  // MAC over num_past_entries || LogEntry.ecall || LogEntry.num_macs || LogEntry.num_input_macs || LogEntry.mac_lst_mac || LogEntry.input_macs
  memcpy(to_mac, &num_past_entries, sizeof(int));
  memcpy(to_mac + sizeof(int), &curr_ecall, sizeof(int));
  memcpy(to_mac + 2 * sizeof(int), &num_macs, sizeof(int));
  memcpy(to_mac + 3 * sizeof(int), &num_input_macs, sizeof(int));
  memcpy(to_mac + 4 * sizeof(int), mac_lst_mac, OE_HMAC_SIZE);
  memcpy(to_mac + 4 * sizeof(int) + OE_HMAC_SIZE, input_macs, num_input_macs * OE_HMAC_SIZE);

  // Copy over data from crumbs
  // std::cout << "Copying data from crumbs" << std::endl;
  uint8_t* tmp_ptr = to_mac + 4 * sizeof(int) + OE_HMAC_SIZE + num_input_macs * OE_HMAC_SIZE;
  for (int i = first_crumb_index; i < last_crumb_index; i++) {
    auto crumb = crumbs[i];
    int past_ecall = crumb.ecall;
    int num_input_macs = crumb.num_input_macs;
    std::vector<uint8_t> input_log_macs = crumb.input_log_macs;
    uint8_t* all_outputs_mac = crumb.all_outputs_mac;
    uint8_t* log_mac = crumb.log_mac;
    
    memcpy(tmp_ptr, &past_ecall, sizeof(int));
    memcpy(tmp_ptr + sizeof(int), &num_input_macs, sizeof(int));
    memcpy(tmp_ptr + 2 * sizeof(int), input_log_macs.data(), num_input_macs * OE_HMAC_SIZE);
    memcpy(tmp_ptr + 2 * sizeof(int) + num_input_macs * OE_HMAC_SIZE, all_outputs_mac, OE_HMAC_SIZE);
    memcpy(tmp_ptr + 2 * sizeof(int) + (num_input_macs + 1) * OE_HMAC_SIZE, log_mac, OE_HMAC_SIZE);

    tmp_ptr += 2 * sizeof(int) + (num_input_macs + 2) * OE_HMAC_SIZE;
  }
  // std::cout << "maced!" << std::endl;
  // MAC the data
  
  // The following prints out what is mac'ed over for debugging
  // std::cout << "Macing log entry chain ===============================" << std::endl;
  // for (int i = 0; i < num_bytes_to_mac; i++) {
  //     std::cout << (int) to_mac[i] << " ";
  // }
  // std::cout << std::endl;
  mcrypto.hmac(to_mac, num_bytes_to_mac, ret_hmac);

}

// Replace dummy all_outputs_mac in output EncryptedBlocks with actual all_outputs_mac
void complete_encrypted_blocks(uint8_t* encrypted_blocks) {
    // std::cout << "completeing encrypted blocks" << std::endl;
    uint8_t all_outputs_mac[OE_HMAC_SIZE];
    generate_all_outputs_mac(all_outputs_mac);

    // Allocate memory outside enclave for the all_outputs_mac
    uint8_t* host_all_outputs_mac = (uint8_t*) oe_host_malloc(OE_HMAC_SIZE * sizeof(uint8_t));
    memcpy(host_all_outputs_mac, (const uint8_t*) all_outputs_mac, OE_HMAC_SIZE);

    // flatbuffers::FlatBufferBuilder all_outputs_mac_builder;

    // Copy generated all_outputs_mac to untrusted memory
    // uint8_t* host_all_outputs_mac = nullptr;
    // ocall_malloc(OE_HMAC_SIZE, &host_all_outputs_mac);
    // std::unique_ptr<uint8_t, decltype(&ocall_free)> host_all_outputs_mac_ptr(host_all_outputs_mac, 
    //     &ocall_free);
    // memcpy(host_all_outputs_mac_ptr.get(), (const uint8_t*) all_outputs_mac, OE_HMAC_SIZE);
    // 
    // // Serialize all_outputs_mac
    // auto all_outputs_mac_offset = tuix::CreateMac(all_outputs_mac_builder, 
    //     all_outputs_mac_builder.CreateVector(host_all_outputs_mac_ptr.get(), OE_HMAC_SIZE));
    // all_outputs_mac_builder.Finish(all_outputs_mac_offset);

    // Perform in-place flatbuffers mutation to modify EncryptedBlocks with updated all_outputs_mac
    auto blocks = tuix::GetMutableEncryptedBlocks(encrypted_blocks);
    // blocks->mutable_all_outputs_mac()->Mutate(0, all_outputs_mac_offset);
    for (int i = 0; i < OE_HMAC_SIZE; i++) {
        // blocks->mutable_all_outputs_mac()->Get(0)->mutable_mac()->Mutate(i, host_all_outputs_mac[i]);
        blocks->mutable_all_outputs_mac()->Mutate(i, host_all_outputs_mac[i]);
        // blocks->mutable_all_outputs_mac()->Mutate(i, all_outputs_mac[i]);
        // blocks->mutable_all_outputs_mac()->Mutate(i, "hello");
    }
    // TODO: check that buffer was indeed modified
    
    // The following prints out the generated all_outputs_mac for this ecall
    // std::cout << "Generated output mac: -------------------" << std::endl;
    // for (int i = 0; i < OE_HMAC_SIZE; i++) {
    //     std::cout << (int) all_outputs_mac[i] << " ";
    // }
    // std::cout << std::endl;
}

void generate_all_outputs_mac(uint8_t all_outputs_mac[32]) {
    // FIXME: for some reason compiler thinks the parameter is unused
    (void) all_outputs_mac;
    std::vector<uint8_t> log_macs_vector = EnclaveContext::getInstance().get_log_macs();
    int num_log_macs = EnclaveContext::getInstance().get_num_log_macs();
    uint8_t* log_macs = log_macs_vector.data();
    mcrypto.hmac(log_macs, num_log_macs * OE_HMAC_SIZE, all_outputs_mac); 
}
