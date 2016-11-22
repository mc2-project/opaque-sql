# Opaque for Apache Spark

How to build Opaque:

1. `SPARKSGX_DATA_DIR=... LIBSGXENCLAVE_PATH=$PWD/libSGXEnclave.so LIBENCLAVESIGNED_PATH=$PWD/enclave.signed.so build/sbt`
2. Within SBT: `test`
    You can instead use `~test` to test continuously.

### Remote attestation

To use the remote attestation feature, you should first generate a key-pair using OpenSSL. Only the NIST p-256 curve is supported.

`openssl ecparam -name prime256v1 -genkey -noout -out private_key.pem`

The file path should be set via `export PRIVATE_KEY_PATH=` in spark-env.sh.
