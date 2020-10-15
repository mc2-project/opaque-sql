#include "IntegrityUtils.h"

void mac_log_entry_chain(int num_bytes_to_mac, uint8_t* to_mac, uint8_t* global_mac, 
    std::string curr_ecall, int curr_pid, int rcv_pid, int job_id, int num_macs, 
    int num_past_entries, std::vector<LogEntry> past_log_entries, int first_le_index, 
    int last_le_index, uint8_t* ret_hmac) {

  // Copy what we want to mac to contiguous memory
  memcpy(to_mac, global_mac, OE_HMAC_SIZE);
  memcpy(to_mac + OE_HMAC_SIZE, curr_ecall.c_str(), curr_ecall.length());
  memcpy(to_mac + OE_HMAC_SIZE + curr_ecall.length(), &curr_pid, sizeof(int));
  memcpy(to_mac + OE_HMAC_SIZE + curr_ecall.length() + sizeof(int), &rcv_pid, sizeof(int));
  memcpy(to_mac + OE_HMAC_SIZE + curr_ecall.length() + 2 * sizeof(int), &job_id, sizeof(int));
  memcpy(to_mac + OE_HMAC_SIZE + curr_ecall.length() + 3 * sizeof(int), &num_macs, sizeof(int));
  memcpy(to_mac + OE_HMAC_SIZE + curr_ecall.length() + 4 * sizeof(int), 
      &num_past_entries, sizeof(int));

  // Copy over data from past log entries
  uint8_t* tmp_ptr = to_mac + OE_HMAC_SIZE + curr_ecall.length() + 5 * sizeof(int);
  for (int i = first_le_index; i < last_le_index; i++) {
    auto past_log_entry = past_log_entries[i];
    std::string ecall = past_log_entry.ecall;
    int pe_snd_pid = past_log_entry.snd_pid;
    int pe_rcv_pid = past_log_entry.rcv_pid;
    int pe_job_id = past_log_entry.job_id;
    
    int bytes_copied = ecall.length() + 3 * sizeof(int);

    memcpy(tmp_ptr, ecall.c_str(), ecall.length());
    memcpy(tmp_ptr + ecall.length(), &pe_snd_pid, sizeof(int));
    memcpy(tmp_ptr + ecall.length() + sizeof(int), &pe_rcv_pid, sizeof(int));
    memcpy(tmp_ptr + ecall.length() + 2 * sizeof(int), &pe_job_id, sizeof(int));

    tmp_ptr += bytes_copied;
  }
  // MAC the data
  mcrypto.hmac(to_mac, num_bytes_to_mac, ret_hmac);

}
