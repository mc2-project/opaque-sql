/*

Copyright 2018 Intel Corporation

This software and the related documents are Intel copyrighted materials,
and your use of them is governed by the express license under which they
were provided to you (License). Unless the License provides otherwise,
you may not use, modify, copy, publish, distribute, disclose or transmit
this software or the related documents without Intel's prior written
permission.

This software and the related documents are provided as is, with no
express or implied warranties, other than those that are expressly stated
in the License.

*/

#ifndef __BASE64_H
#define __BASE64_H

#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

char *base64_encode(const char *msg, size_t sz);
char *base64_decode(const char *msg, size_t *sz);

#ifdef __cplusplus
};
#endif

#endif

