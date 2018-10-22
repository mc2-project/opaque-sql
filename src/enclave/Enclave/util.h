#ifndef UTIL_H
#define UTIL_H

#include <cstdarg>
#include <cstdint>
#include <ctime>
#include <string>

/*
 * printf:
 *   Invokes OCALL to display the enclave buffer to the terminal.
 */
int printf(const char *fmt, ...);

/** Invoke OCALL to exit the program. */
void exit(int exit_code);
namespace std {
    using ::exit;
}

std::string string_format(const std::string &fmt, ...);

void print_bytes(uint8_t *ptr, uint32_t len);

/** Return 0 if equal, and -1 if not equal. */
int cmp(const uint8_t *value1, const uint8_t *value2, uint32_t len);

void clear(uint8_t *dest, uint32_t len);

int log_2(int value);

int pow_2(int value);

int secs_to_tm(long long t, struct tm *tm);

#endif // UTIL_H
