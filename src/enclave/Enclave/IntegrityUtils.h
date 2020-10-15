#include "EnclaveContext.h"
#include "Flatbuffers.h"

using namespace edu::berkeley::cs::rise::opaque;

void init_log(const tuix::EncryptedBlocks *encrypted_blocks);
void verify_log(const tuix::EncryptedBlocks *encrypted_blocks, 
    std::vector<LogEntry> past_log_entries);

int get_past_ecalls_lengths(std::vector<LogEntry> past_log_entries, int first_le_index, 
    int last_le_index);
