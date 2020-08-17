#include "Enclave_t.h"
#include <vector>
#include <iostream>
#include <unordered_map>
#include "../Common/common.h"
#include "../Common/mCrypto.h"

struct LogEntry;

typedef struct LogEntry {
  std::string op;
  int eid;
  int job_id;
  int pid;
} LogEntry;

static Crypto mcrypto;

class EnclaveContext {
  private:
    std::vector<LogEntry> ecall_log_entries;
    int operators_ctr;
    unsigned char shared_key[SGX_AESGCM_KEY_SIZE] = {0};

    // For this ecall log entry
    std::string this_ecall;
    // int job_id;
    std::vector<std::vector<uint8_t>> log_entry_mac_lst;
    uint8_t global_mac[OE_HMAC_SIZE];
    int eid;
    int pid;

    // Map of job ID for partition
    std::unordered_map<int, int> pid_jobid;


    EnclaveContext() {
      // operators_ctr = 0;
      // job_id = 0;
      eid = -1;
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
      // int job_id = -1;
      // job_id++; // dummy operation for now
      log_entry_mac_lst.clear();
      // global_mac = {0 * OE_HMAC_SIZE};
      // ecall_log_entries.clear();
    }

    void reset_past_log_entries() {
      ecall_log_entries.clear();
    }

    void append_past_log_entry(std::string op, int eid, int job_id) {
      LogEntry le;
      le.op = op;
      le.eid = eid;
      le.job_id = job_id;
      // le.pid = pid;
      ecall_log_entries.push_back(le);
    }

    std::vector<LogEntry> get_ecall_log_entries() {
      return ecall_log_entries;
    }

    int get_eid() {
      // return eid;
      return pid;
    }

    void set_partition_index(int idx) {
      eid = idx;
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
      // job_id++;
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

    void sha256_hash_ecall_log_entries(const uint8_t* ret_hash) {
      mcrypto.sha256((const uint8_t*) ecall_log_entries.data(), ecall_log_entries.size() * sizeof(LogEntry), (uint8_t*) ret_hash);
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

//     std::vector<std::string> get_executed_plan() {
//       std::vector<std::string> executed_plan_log;
//       bool possible_agg = false;
//       bool possible_sort_merge_join = false;
//       // bool integrity_error = false;
// 
//       // The following represent how many steps before the current one the operator was run, zero indexed
//       int last_sample = -1;
//       int last_find_range_bounds = -1;
//       int last_partition_for_sort = -1;
//   
//       for (std::vector<int>::size_type i = 0; i != executed_operators.size(); i++) {
//         std::string op = executed_operators[i].op;
// 
//         if (op == std::string("externalSort")) {
//           // External Sort will always be the last ecall for EncryptedSortExec
//           if (last_sample == 2 && last_find_range_bounds == 1 && last_partition_for_sort == 0) {
//             // Enclave just executed the four operators necessary for an encrypted sort exec for multiple partitions
//             last_sample = -1;
//             last_find_range_bounds = -1;
//             last_partition_for_sort = -1;
//             executed_plan_log.push_back(std::string("EncryptedSortExec"));
//           } else if (last_sample != -1 || last_find_range_bounds != -1 || last_partition_for_sort != -1) {
//             // The recent sequence of operators makes no sense
//             // integrity_error = true;
//           } else {
//             executed_plan_log.push_back(std::string("EncryptedSortExec"));
//           }
//           possible_agg = false;
//           possible_sort_merge_join = false;
//         } else if (op == std::string("project")) {
//           executed_plan_log.push_back(std::string("EncryptedProjectExec"));
// 
//           // Reset operator tracking
//           possible_agg = false;
//           possible_sort_merge_join = false;
//           last_sample = -1;
//           last_find_range_bounds = -1;
//           last_partition_for_sort = -1;
//         } else if (op == std::string("filter")) {
//           executed_plan_log.push_back(std::string("EncryptedFilterExec"));
// 
//           // Reset operator tracking
//           possible_agg = false;
//           possible_sort_merge_join = false;
//           last_sample = -1;
//           last_find_range_bounds = -1;
//           last_partition_for_sort = -1;
//         } else if (op == std::string("sample")) {
//           last_sample = 0;
// 
//           // Reset operator tracking
//           possible_agg = false;
//           possible_sort_merge_join = false;
//           last_find_range_bounds = -1;
//           last_partition_for_sort = -1;
//         } else if (op == std::string("findRangeBounds")) {
//           if (last_sample == 0) {
//             last_sample = 1;
//             last_find_range_bounds = 0;
//           } else {
//             last_sample = -1;
//           }
// 
//           // Reset operator tracking
//           possible_agg = false;
//           possible_sort_merge_join = false;
//           last_partition_for_sort = -1;
//         } else if (op == std::string("partitionForSort")) {
//           if (last_sample == 1 && last_find_range_bounds == 0) {
//             last_sample = 2;
//             last_find_range_bounds = 1;
//             last_partition_for_sort = 0;
//           }
//         } else if (op == std::string("nonObliviousAggregateStep1")) {
//           possible_agg = true;
//           possible_sort_merge_join = false;
//           last_sample = -1;
//           last_find_range_bounds = -1;
//           last_partition_for_sort = -1;
//         } else if (op == std::string("nonObliviousAggregateStep2")) {
//           if (possible_agg) {
//             executed_plan_log.push_back(std::string("EncryptedAggExec"));
//             // Reset possible agg
//             possible_agg = false;
//           } 
//           possible_sort_merge_join = false;
//           last_sample = -1;
//           last_find_range_bounds = -1;
//           last_partition_for_sort = -1;
//         } else if (op == std::string("scanCollectLastPrimary")) {
//           possible_sort_merge_join = true;
//           possible_agg = false;
//           last_sample = -1;
//           last_find_range_bounds = -1;
//           last_partition_for_sort = -1;
//         } else if (op == std::string("nonObliviousSortMergeJoin")) {
//           if (possible_sort_merge_join) {
//             executed_plan_log.push_back(std::string("EncryptedSortMergeJoinExec"));
//             // Reset possible sort merge join
//             possible_sort_merge_join = false;
//           }
//           possible_agg = false;
//           last_sample = -1;
//           last_find_range_bounds = -1;
//           last_partition_for_sort = -1;
//         } else if (op == std::string("encrypt")) {
//           executed_plan_log.push_back(std::string("EncryptExec"));
//           possible_sort_merge_join = false;
//           possible_agg = false;
//           last_sample = -1;
//           last_find_range_bounds = -1;
//           last_partition_for_sort = -1;
//         } else {
//           // Unknown operator
//           // integrity_error = true;
// 
//         }
//       }
//       return executed_plan_log;
//     }
// 
//     void print_executed_operators() {
//       std::vector<std::string> executed_plan = get_executed_plan();
//       std::cout << "\n=== Condensed Executed Plan in Enclave ===" << std::endl;
//       for (std::vector<std::string>::const_iterator i = executed_plan.begin(); i != executed_plan.end(); ++i) {
//         std::cout << *i << std::endl;
//       }
//     }
};

