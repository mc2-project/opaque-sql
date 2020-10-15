#include "EnclaveContext.h"
#include "Flatbuffers.h"

using namespace edu::berkeley::cs::rise::opaque;

void init_log(const tuix::EncryptedBlocks *encrypted_blocks);
void verify_log(const tuix::EncryptedBlocks *encrypted_blocks, 
    std::vector<LogEntry> past_log_entries);

void mac_log_entry_chain(int num_bytes_to_mac, uint8_t* to_mac, uint8_t* global_mac, 
    std::string curr_ecall, int curr_pid, int rcv_pid, int job_id, int num_macs, 
    int num_past_entries, std::vector<LogEntry> past_log_entries, int first_le_index, 
    int last_le_index, uint8_t* ret_hmac);

int get_past_ecalls_lengths(std::vector<LogEntry> past_log_entries, int first_le_index, 
    int last_le_index);
