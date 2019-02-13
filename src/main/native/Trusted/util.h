#ifndef UTIL_H
#define UTIL_H

#include <cstdarg>
#include <cstdint>
#include <ctime>
#include <string>

std::string string_format(const std::string &fmt, ...);

/**
 * Allocate memory outside of the enclave and return the pointer in `ret`.
 *
 * This is a checked wrapper around `unsafe_ocall_malloc`. The resulting pointer is safe to write
 * to.
 */
void ocall_malloc(size_t size, uint8_t **ret);

int secs_to_tm(long long t, struct tm *tm);

#endif // UTIL_H
