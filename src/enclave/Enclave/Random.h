#include <cstddef>
#include <cstdint>

#ifndef RANDOM_H
#define RANDOM_H

int mbedtls_read_rand(
    unsigned char* buf, size_t buf_len);

#endif