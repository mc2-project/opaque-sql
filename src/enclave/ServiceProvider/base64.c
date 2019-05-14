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


#include <sys/types.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <string.h>

char *base64_encode(const char *msg, size_t sz)
{
	BIO *b64, *bmem;
	char *bstr, *dup;
	int len;

	b64= BIO_new(BIO_f_base64());
	bmem= BIO_new(BIO_s_mem());

	/* Single line output, please */
	BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

	BIO_push(b64, bmem);

	if ( BIO_write(b64, msg, (int) sz) == -1 ) return NULL;

	BIO_flush(b64);

	len= BIO_get_mem_data(bmem, &bstr);
	dup= (char *) malloc(len+1);
	memcpy(dup, bstr, len);
	dup[len]= 0;

	BIO_free(bmem);
	BIO_free(b64);

	return dup;
}


char *base64_decode(const char *msg, size_t *sz)
{
	BIO *b64, *bmem;
	char *buf;
	size_t len= strlen(msg);

	buf= (char *) malloc(len+1);
	memset(buf, 0, len+1);

	b64= BIO_new(BIO_f_base64());
	BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

	bmem= BIO_new_mem_buf(msg, (int) len);

	BIO_push(b64, bmem);

	*sz= BIO_read(b64, buf, (int) len);
	if ( *sz == -1 ) return NULL;

	BIO_free_all(bmem);

	return buf;
}

