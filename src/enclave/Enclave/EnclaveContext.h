#include "Enclave_t.h"
#include <unordered_set>
#include <vector>
#include <string>
#include <unordered_map>
#include "../Common/common.h"
#include "../Common/mCrypto.h"

struct LogEntry;

struct LogEntry {
  std::string ecall; // ecall executed
  int snd_pid; // partition where ecall was executed
  int rcv_pid; // partition of subsequent ecall
  int job_id; // number of ecalls executed in this enclave before this ecall

  bool operator==(const LogEntry& le) const
  { 
      return (this->ecall == le.ecall && this->snd_pid == le.snd_pid && this->rcv_pid == le.rcv_pid && this->job_id == le.job_id); 
  }
}; 

class LogEntryHashFunction { 
public: 
    // Example taken from https://www.geeksforgeeks.org/how-to-create-an-unordered_set-of-user-defined-class-or-struct-in-c/ 
    size_t operator()(const LogEntry& le) const
    { 
        return (std::hash<std::string>()(le.ecall)) ^ (std::hash<int>()(le.snd_pid)) ^ (std::hash<int>()(le.rcv_pid)) ^ (std::hash<int>()(le.job_id)); 
    } 
};

static Crypto mcrypto;

class EnclaveContext {
  private:
    std::unordered_set<LogEntry, LogEntryHashFunction> ecall_log_entries;
    int operators_ctr;
    unsigned char shared_key[SGX_AESGCM_KEY_SIZE] = {0};

    // For this ecall log entry
    std::string this_ecall;
    std::vector<std::vector<uint8_t>> log_entry_mac_lst;

    std::string curr_row_writer;
    // Special vectors of nonObliviousAggregateStep1
    std::vector<std::vector<uint8_t>> first_row_log_entry_mac_lst;
    std::vector<std::vector<uint8_t>> last_group_log_entry_mac_lst;
    std::vector<std::vector<uint8_t>> last_row_log_entry_mac_lst;

    int pid;
    bool append_mac;

    // Map of job ID for partition
    std::unordered_map<int, int> pid_jobid;


    EnclaveContext() {
      pid = -1;
      append_mac = true;
    }

  public:
    // Don't forget to declare these two. You want to make sure they
    // are unacceptable otherwise you may accidentally get copies of
    // your singleton appearing.
    EnclaveContext(EnclaveContext const&) = delete;
    void operator=(EnclaveContext const&) = delete;

    static EnclaveContext& getInstance() {
      static EnclaveContext instance;
      return instance;
    }

    unsigned char* get_shared_key() {
      return shared_key;
    }

    void set_shared_key(uint8_t* shared_key_bytes, uint32_t shared_key_size) {
      memcpy_s(shared_key, sizeof(shared_key), shared_key_bytes, shared_key_size);
    }

    void set_curr_row_writer(std::string row_writer) {
      curr_row_writer = row_writer;
    }

    void reset_log_entry() {
      this_ecall = std::string("");
      log_entry_mac_lst.clear();
    }

    void reset_past_log_entries() {
      ecall_log_entries.clear();
    }

    void set_append_mac(bool to_append) {
      append_mac = to_append;
    }

    bool to_append_mac() {
      return append_mac;
    }

    void append_past_log_entry(std::string ecall, int snd_pid, int rcv_pid, int job_id) {
      LogEntry le;
      le.ecall = ecall;
      le.snd_pid = snd_pid;
      le.rcv_pid = rcv_pid;
      le.job_id = job_id;
      ecall_log_entries.insert(le);
    }

    std::unordered_set<LogEntry> get_past_log_entries() {
      return ecall_log_entries;
    }

    int get_pid() {
      return pid;
    }

    void set_pid(int id) {
      pid = id;
    }

    void finish_ecall() {
      // Increment the job id of this pid
      if (pid_jobid.find(pid) != pid_jobid.end()) {
        pid_jobid[pid]++;
      } else {
        pid_jobid[pid] = 0;
      }
      ecall_log_entries.clear();
      pid = -1;

      curr_row_writer = std::string("");

      first_row_log_entry_mac_lst.clear();
      last_group_log_entry_mac_lst.clear();
      last_row_log_entry_mac_lst.clear();
      log_entry_mac_lst.clear();
    }

    void add_mac_to_mac_lst(uint8_t* mac) {
      std::vector<uint8_t> mac_vector (mac, mac + SGX_AESGCM_MAC_SIZE);
      if (curr_row_writer == std::string("first_row")) {
        first_row_log_entry_mac_lst.push_back(mac_vector);
      } else if (curr_row_writer == std::string("last_group")) {
        last_group_log_entry_mac_lst.push_back(mac_vector);
      } else if (curr_row_writer == std::string("last_row")) {
        last_row_log_entry_mac_lst.push_back(mac_vector);
      } else {
        log_entry_mac_lst.push_back(mac_vector);
      }
    }

    void hmac_mac_lst(const uint8_t* ret_mac_lst, const uint8_t* global_mac) {
      std::vector<std::vector<uint8_t>> chosen_mac_lst;
      if (curr_row_writer == std::string("first_row")) {
        chosen_mac_lst = first_row_log_entry_mac_lst;
      } else if (curr_row_writer == std::string("last_group")) {
        chosen_mac_lst = last_group_log_entry_mac_lst;
      } else if (curr_row_writer == std::string("last_row")) {
        chosen_mac_lst = last_row_log_entry_mac_lst;
      } else {
        chosen_mac_lst = log_entry_mac_lst;
      }

      size_t mac_lst_length = chosen_mac_lst.size() * SGX_AESGCM_MAC_SIZE;

      // Copy all macs to contiguous chunk of memory
      uint8_t contiguous_mac_lst[mac_lst_length];
      uint8_t* temp_ptr = contiguous_mac_lst;
      for (unsigned int i = 0; i < chosen_mac_lst.size(); i++) {
        memcpy(temp_ptr, chosen_mac_lst[i].data(), SGX_AESGCM_MAC_SIZE);
        temp_ptr += SGX_AESGCM_MAC_SIZE;
      }

      // hmac the contiguous chunk of memory
      uint8_t hmac_result[OE_HMAC_SIZE];
      mcrypto.hmac(contiguous_mac_lst, mac_lst_length, (uint8_t*) hmac_result);
      memcpy((uint8_t*) global_mac, hmac_result, OE_HMAC_SIZE);

      memcpy((uint8_t*) ret_mac_lst, contiguous_mac_lst, mac_lst_length);
    }

    size_t get_num_macs() {
      if (curr_row_writer == std::string("first_row")) {
        return first_row_log_entry_mac_lst.size();
      } else if (curr_row_writer == std::string("last_group")) {
        return last_group_log_entry_mac_lst.size();
      } else if (curr_row_writer == std::string("last_row")) {
        return last_row_log_entry_mac_lst.size();
      } else {
        return log_entry_mac_lst.size();
      }
    }

    void set_log_entry_ecall(std::string ecall) {
      this_ecall = ecall;
    }

    std::string get_log_entry_ecall() {
      return this_ecall;
    }

    int get_job_id() {
      return pid_jobid[pid];
    }

    void increment_job_id() {
      pid_jobid[pid]++;
    }

    void reset_pid_jobid_map() {
      pid_jobid.clear();
    }
};

