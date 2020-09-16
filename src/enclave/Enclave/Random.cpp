#include "Random.h"
#include "common.h"
#include "util.h"

#include "mbedtls/entropy.h"
#include "mbedtls/ctr_drbg.h"

int mbedtls_read_rand(unsigned char* buf, size_t buf_len) {
    int ret = 1;
    mbedtls_ctr_drbg_context ctr_drbg;
    mbedtls_entropy_context entropy;

    mbedtls_ctr_drbg_init( &ctr_drbg );
    mbedtls_entropy_init( &entropy );
    ret = mbedtls_ctr_drbg_seed( &ctr_drbg, mbedtls_entropy_func, &entropy, (const unsigned char *) "RANDOM_GEN", 10 );
    if (ret != 0)
    {
        printf("seed failed");
        goto cleanup;
    }

    mbedtls_ctr_drbg_set_prediction_resistance( &ctr_drbg, MBEDTLS_CTR_DRBG_PR_OFF );

    ret = mbedtls_ctr_drbg_random( &ctr_drbg, buf, buf_len );
    if( ret != 0 )
    {
        printf("random failed");
        goto cleanup;
        
    }
    mbedtls_ctr_drbg_free( &ctr_drbg );
    mbedtls_entropy_free( &entropy );

cleanup:
    return ret;
}
