# Opaque for Apache Spark

How to build Opaque:

1. `SPARKSGX_DATA_DIR=... LIBSGXENCLAVE_PATH=$PWD/libSGXEnclave.so LIBENCLAVESIGNED_PATH=$PWD/enclave.signed.so build/sbt`
2. Within SBT: `test`
    You can instead use `~test` to test continuously.
