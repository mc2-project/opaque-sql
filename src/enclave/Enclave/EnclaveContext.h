#include "Enclave_t.h"
#include <unordered_set>
#include <vector>
#include <iostream>
#include <unordered_map>
#include "../Common/common.h"
#include "../Common/mCrypto.h"

struct LogEntry;

struct LogEntry {
  std::string op;
  int snd_pid;
  int rcv_pid;
  int job_id;
  int pid;

  bool operator==(const LogEntry& le) const
  { 
      return (this->op == le.op && this->snd_pid == le.snd_pid && this->rcv_pid == le.rcv_pid && this->job_id == le.job_id && this->pid == le.pid); 
  }
}; 

class LogEntryHashFunction { 
public: 
    // Example taken from https://www.geeksforgeeks.org/how-to-create-an-unordered_set-of-user-defined-class-or-struct-in-c/ 
    size_t operator()(const LogEntry& le) const
    { 
        return (std::hash<std::string>()(le.op)) ^ (std::hash<int>()(le.snd_pid)) ^ (std::hash<int>()(le.rcv_pid)) ^ (std::hash<int>()(le.job_id)) ^ (std::hash<int>()(le.pid)); 
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
    uint8_t global_mac[OE_HMAC_SIZE];
    int pid;

    // Map of job ID for partition
    std::unordered_map<int, int> pid_jobid;


    EnclaveContext() {
      pid = -1;
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

    void reset_log_entry() {
      this_ecall = std::string("");
      log_entry_mac_lst.clear();
    }

    void reset_past_log_entries() {
      ecall_log_entries.clear();
    }

    void append_past_log_entry(std::string op, int snd_pid, int rcv_pid, int job_id) {
      LogEntry le;
      le.op = op;
      le.snd_pid = snd_pid;
      le.rcv_pid = rcv_pid;
      le.job_id = job_id;
      ecall_log_entries.insert(le);
    }

    std::vector<LogEntry> get_ecall_log_entries() {
      std::vector<LogEntry> ret(ecall_log_entries.begin(), ecall_log_entries.end());
      return ret;
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
    }

    void add_mac_to_mac_lst(uint8_t* mac) {
      std::vector<uint8_t> mac_vector (mac, mac + SGX_AESGCM_MAC_SIZE);
      log_entry_mac_lst.push_back(mac_vector);
    }

    void hmac_mac_lst(const uint8_t* ret_mac_lst) {
      size_t mac_lst_length = log_entry_mac_lst.size() * SGX_AESGCM_MAC_SIZE;

      // Copy all macs to contiguous chunk of memory
      uint8_t contiguous_mac_lst[mac_lst_length];
      uint8_t* temp_ptr = contiguous_mac_lst;
      for (unsigned int i = 0; i < log_entry_mac_lst.size(); i++) {
        memcpy(temp_ptr, log_entry_mac_lst[i].data(), SGX_AESGCM_MAC_SIZE);
        temp_ptr += SGX_AESGCM_MAC_SIZE;
      }

      // hmac the contiguous chunk of memory
      mcrypto.hmac(contiguous_mac_lst, mac_lst_length, global_mac);

      memcpy((uint8_t*) ret_mac_lst, contiguous_mac_lst, mac_lst_length);
    }

    size_t get_num_macs() {
      return log_entry_mac_lst.size();
    }

    uint8_t* get_global_mac() {
      return global_mac;
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

