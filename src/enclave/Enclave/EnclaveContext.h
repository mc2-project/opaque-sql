#include "Enclave_t.h"
#include <vector>
#include <iostream>
#include "../Common/common.h"

struct LogEntry;

typedef struct LogEntry {
  std::string op;
  int job_id;
  std::vector<std::vector<uint8_t>> mac_lst;
  uint8_t global_mac[OE_HMAC_SIZE];
  std::vector<LogEntry> log_entries;
  uint8_t input_hash[OE_SHA256_HASH_SIZE];
  uint8_t output_hash[OE_SHA256_HASH_SIZE];
  // uint8_t input_src_partitions[];
} LogEntry;

class EnclaveContext {
  private:
    std::vector<LogEntry> executed_operators;
    int operators_ctr;
    unsigned char shared_key[SGX_AESGCM_KEY_SIZE] = {0};
    LogEntry curr_log_entry;

    EnclaveContext() {
      operators_ctr = 0;
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
      curr_log_entry = {};
    }

    // Log executed operation
    void log_operation(std::string operation, uint8_t* input_hash, uint8_t* output_hash) {
      LogEntry eop;
      eop.op = operation;
      memcpy(eop.input_hash, input_hash, OE_SHA256_HASH_SIZE);
      memcpy(eop.output_hash, output_hash, OE_SHA256_HASH_SIZE);
      executed_operators.push_back(eop);
      operators_ctr++;
    }
   
    // Log executed operation
    void log_operation(std::string operation) {
      LogEntry eop;
      eop.op = operation;
      executed_operators.push_back(eop);
      operators_ctr++;
    }

    void add_mac_to_mac_lst(uint8_t* mac) {
      std::vector<uint8_t> mac_vector (mac, mac + SGX_AESGCM_MAC_SIZE);
      curr_log_entry.mac_lst.push_back(mac_vector);
    }

    std::vector<std::string> get_executed_plan() {
      std::vector<std::string> executed_plan_log;
      bool possible_agg = false;
      bool possible_sort_merge_join = false;
      // bool integrity_error = false;

      // The following represent how many steps before the current one the operator was run, zero indexed
      int last_sample = -1;
      int last_find_range_bounds = -1;
      int last_partition_for_sort = -1;
  
      for (std::vector<int>::size_type i = 0; i != executed_operators.size(); i++) {
        std::string op = executed_operators[i].op;

        if (op == std::string("externalSort")) {
          // External Sort will always be the last ecall for EncryptedSortExec
          if (last_sample == 2 && last_find_range_bounds == 1 && last_partition_for_sort == 0) {
            // Enclave just executed the four operators necessary for an encrypted sort exec for multiple partitions
            last_sample = -1;
            last_find_range_bounds = -1;
            last_partition_for_sort = -1;
            executed_plan_log.push_back(std::string("EncryptedSortExec"));
          } else if (last_sample != -1 || last_find_range_bounds != -1 || last_partition_for_sort != -1) {
            // The recent sequence of operators makes no sense
            // integrity_error = true;
          } else {
            executed_plan_log.push_back(std::string("EncryptedSortExec"));
          }
          possible_agg = false;
          possible_sort_merge_join = false;
        } else if (op == std::string("project")) {
          executed_plan_log.push_back(std::string("EncryptedProjectExec"));

          // Reset operator tracking
          possible_agg = false;
          possible_sort_merge_join = false;
          last_sample = -1;
          last_find_range_bounds = -1;
          last_partition_for_sort = -1;
        } else if (op == std::string("filter")) {
          executed_plan_log.push_back(std::string("EncryptedFilterExec"));

          // Reset operator tracking
          possible_agg = false;
          possible_sort_merge_join = false;
          last_sample = -1;
          last_find_range_bounds = -1;
          last_partition_for_sort = -1;
        } else if (op == std::string("sample")) {
          last_sample = 0;

          // Reset operator tracking
          possible_agg = false;
          possible_sort_merge_join = false;
          last_find_range_bounds = -1;
          last_partition_for_sort = -1;
        } else if (op == std::string("findRangeBounds")) {
          if (last_sample == 0) {
            last_sample = 1;
            last_find_range_bounds = 0;
          } else {
            last_sample = -1;
          }

          // Reset operator tracking
          possible_agg = false;
          possible_sort_merge_join = false;
          last_partition_for_sort = -1;
        } else if (op == std::string("partitionForSort")) {
          if (last_sample == 1 && last_find_range_bounds == 0) {
            last_sample = 2;
            last_find_range_bounds = 1;
            last_partition_for_sort = 0;
          }
        } else if (op == std::string("nonObliviousAggregateStep1")) {
          possible_agg = true;
          possible_sort_merge_join = false;
          last_sample = -1;
          last_find_range_bounds = -1;
          last_partition_for_sort = -1;
        } else if (op == std::string("nonObliviousAggregateStep2")) {
          if (possible_agg) {
            executed_plan_log.push_back(std::string("EncryptedAggExec"));
            // Reset possible agg
            possible_agg = false;
          } 
          possible_sort_merge_join = false;
          last_sample = -1;
          last_find_range_bounds = -1;
          last_partition_for_sort = -1;
        } else if (op == std::string("scanCollectLastPrimary")) {
          possible_sort_merge_join = true;
          possible_agg = false;
          last_sample = -1;
          last_find_range_bounds = -1;
          last_partition_for_sort = -1;
        } else if (op == std::string("nonObliviousSortMergeJoin")) {
          if (possible_sort_merge_join) {
            executed_plan_log.push_back(std::string("EncryptedSortMergeJoinExec"));
            // Reset possible sort merge join
            possible_sort_merge_join = false;
          }
          possible_agg = false;
          last_sample = -1;
          last_find_range_bounds = -1;
          last_partition_for_sort = -1;
        } else if (op == std::string("encrypt")) {
          executed_plan_log.push_back(std::string("EncryptExec"));
          possible_sort_merge_join = false;
          possible_agg = false;
          last_sample = -1;
          last_find_range_bounds = -1;
          last_partition_for_sort = -1;
          // WHAT TO DO HERE
        } else {
          // Unknown operator
          // integrity_error = true;

        }
      }
      return executed_plan_log;
    }

    void print_executed_operators() {
      std::vector<std::string> executed_plan = get_executed_plan();
      std::cout << "\n=== Condensed Executed Plan in Enclave ===" << std::endl;
      for (std::vector<std::string>::const_iterator i = executed_plan.begin(); i != executed_plan.end(); ++i) {
        std::cout << *i << std::endl;
      }
    }
};

