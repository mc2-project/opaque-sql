#include "Enclave_t.h"
#include <unordered_set>
#include <vector>
#include <string>
#include <unordered_map>
#include <map>
#include "../Common/common.h"
#include "../Common/mCrypto.h"

struct Crumb;

struct Crumb {
  int ecall; // ecall executed
  uint8_t log_mac[OE_HMAC_SIZE]; // LogEntryChain MAC for this output
  uint8_t all_outputs_mac[OE_HMAC_SIZE]; 
  // FIXME: change this to num_input_log_macs
  int num_input_macs; // Num MACS in the below vector
  std::vector<uint8_t> input_log_macs;

  bool operator==(const Crumb& c) const
  { 
      // Check whether the ecall is the same
      if (this->ecall != c.ecall) {
          return false;
      }

      // Check whether the log_mac and the all_outputs_mac are the same
      for (int i = 0; i < OE_HMAC_SIZE; i++) {
          if (this->log_mac[i] != c.log_mac[i]) {
              return false;
          }
          if (this->all_outputs_mac[i] != c.all_outputs_mac[i]) {
              return false;
          }
      }
      
      // Check whether input_log_macs size is the same 
      if (this->input_log_macs.size() != c.input_log_macs.size()) {
          return false;
      }

      // Check whether the input_log_macs themselves are the same
      for (uint32_t i = 0; i < this->input_log_macs.size(); i++) {
          if (this->input_log_macs[i] != c.input_log_macs[i]) {
              return false;
          }
      }
      return true;
  }
}; 

class CrumbHashFunction { 
public: 
    // Example taken from https://www.geeksforgeeks.org/how-to-create-an-unordered_set-of-user-defined-class-or-struct-in-c/ 
    size_t operator()(const Crumb& c) const
    { 
        return (std::hash<int>()(c.ecall)) ^ (std::hash<uint8_t*>()((uint8_t*) c.log_mac)) ^ (std::hash<uint8_t*>()((uint8_t*) c.all_outputs_mac)) ^ (std::hash<int>()(c.num_input_macs)) ^ (std::hash<uint8_t*>()((uint8_t*) c.input_log_macs.data())); 
    } 
};

static Crypto mcrypto;

class EnclaveContext {
  private:
    std::unordered_set<Crumb, CrumbHashFunction> crumbs;
    std::vector<uint8_t> input_macs;
    int num_input_macs;

    // Contiguous array of log_macs: log_mac_1 || log_mac_2 || ...
    // Each of length OE_HMAC_SIZE
    std::vector<uint8_t> log_macs;
    int num_log_macs;

    unsigned char shared_key[SGX_AESGCM_KEY_SIZE] = {0};

    // For this ecall log entry
    std::string this_ecall;
    std::vector<std::vector<uint8_t>> log_entry_mac_lst;

    std::string curr_row_writer;

    bool append_mac;

    // Map of job ID for partition
    std::unordered_map<int, int> pid_jobid;

    EnclaveContext() {
      num_input_macs = 0;
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
      crumbs.clear();
    }

    void set_append_mac(bool to_append) {
      append_mac = to_append;
    }

    bool to_append_mac() {
      return append_mac;
    }

    // FIXME: make the arrays here const?
    void append_crumb(int ecall, const uint8_t log_mac[OE_HMAC_SIZE], const uint8_t all_outputs_mac[OE_HMAC_SIZE], int num_input_macs, std::vector<uint8_t> input_log_macs) {
      // FIXME: for some reason, compiler thinks the following two arguments are unused
      (void) log_mac;
      (void) all_outputs_mac;
      Crumb new_crumb;

      new_crumb.ecall = ecall;
      memcpy(new_crumb.log_mac, log_mac, OE_HMAC_SIZE);
      memcpy(new_crumb.all_outputs_mac, all_outputs_mac, OE_HMAC_SIZE);
      new_crumb.num_input_macs = num_input_macs;

      // Copy over input_log_macs
      for (uint32_t i = 0; i < input_log_macs.size(); i++) {
          new_crumb.input_log_macs.push_back(input_log_macs[i]);
      }
      crumbs.insert(new_crumb);
    }

    std::vector<Crumb> get_crumbs() {
      std::vector<Crumb> past_crumbs(crumbs.begin(), crumbs.end());
      return past_crumbs;
    }

    // Add all the all_output_mac's from input EncryptedBlocks to input_macs list
    void append_input_mac(std::vector<uint8_t> input_mac) {
        for (uint32_t i = 0; i < input_mac.size(); i++) {
            input_macs.push_back(input_mac[i]);
        }
        num_input_macs++;
    }

    std::vector<uint8_t> get_input_macs() {
        return input_macs;
    }

    int get_num_input_macs() {
        return num_input_macs;
    }

    void append_log_mac(uint8_t log_mac[OE_HMAC_SIZE]) {
        for (int i = 0; i < OE_HMAC_SIZE; i++) {
            log_macs.push_back(log_mac[i]);
        }
        num_log_macs++;
    }

    std::vector<uint8_t> get_log_macs() {
        return log_macs;
    }

    int get_num_log_macs() {
        return num_log_macs;
    }

    int get_ecall_id(std::string ecall) {
      std::map<std::string, int> ecall_id = {
        {"project", 1},
        {"filter", 2},
        {"sample", 3},
        {"findRangeBounds", 4},
        {"partitionForSort", 5},
        {"externalSort", 6},
        {"scanCollectLastPrimary", 7},
        {"nonObliviousSortMergeJoin", 8},
        {"nonObliviousAggregate", 9},
        {"countRowsPerPartition", 10},
        {"computeNumRowsPerPartition", 11},
        {"localLimit", 12},
        {"limitReturnRows", 13}
      };
      return ecall_id[ecall];
    }

    void finish_ecall() {
      crumbs.clear();

      curr_row_writer = std::string("");

      log_entry_mac_lst.clear();
      log_macs.clear();
      num_log_macs = 0;
      input_macs.clear();
      num_input_macs = 0;
    }

    void add_mac_to_mac_lst(uint8_t* mac) {
      std::vector<uint8_t> mac_vector (mac, mac + SGX_AESGCM_MAC_SIZE);
      log_entry_mac_lst.push_back(mac_vector);
    }

    void hmac_mac_lst(const uint8_t* ret_mac_lst, const uint8_t* mac_lst_mac) {
      std::vector<std::vector<uint8_t>> chosen_mac_lst;
      chosen_mac_lst = log_entry_mac_lst;

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
      memcpy((uint8_t*) mac_lst_mac, hmac_result, OE_HMAC_SIZE);

      memcpy((uint8_t*) ret_mac_lst, contiguous_mac_lst, mac_lst_length);
    }

    size_t get_num_macs() {
      return log_entry_mac_lst.size();
    }

    void set_log_entry_ecall(std::string ecall) {
      this_ecall = ecall;
    }

    std::string get_log_entry_ecall() {
      return this_ecall;
    }
};

