#include "EnclaveContext.h"
#include "Flatbuffers.h"

using namespace edu::berkeley::cs::rise::opaque;

void init_log(const tuix::EncryptedBlocks *encrypted_blocks);

void verify_log(const tuix::EncryptedBlocks *encrypted_blocks, 
    std::vector<Crumb> crumbs);

void mac_log_entry_chain(int num_bytes_to_mac, uint8_t* to_mac, int curr_ecall, int num_macs, int num_input_macs,
    uint8_t* mac_lst_mac, uint8_t* input_macs, 
    int num_past_entries, std::vector<Crumb> crumbs, int first_crumb_index, 
    int last_crumb_index, uint8_t* ret_hmac);

void complete_encrypted_blocks(uint8_t* encrypted_blocks);

void generate_all_outputs_mac(uint8_t all_outputs_mac[32]);
