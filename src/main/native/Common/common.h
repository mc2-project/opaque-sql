#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#ifndef COMMON_H
#define COMMON_H

// Declarations for C/C++ standard library functions that are not present in the trusted standard
// libraries, but are reimplemented in Trusted/util.cpp. This allows us to use these functions
// uniformly across trusted and untrusted code.
int printf(const char* format, ...);
void exit(int exit_code);
namespace std {
    using ::exit;
}

#ifdef DEBUG
#define debug(...) printf(__VA_ARGS__)
#else
#define debug(...) do {} while (0)
#endif

#endif // COMMON_H
