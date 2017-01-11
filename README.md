# Opaque for Apache Spark

How to build and test Opaque:

1. Install dependencies:

    ```sh
    sudo yum -y install gcc48.x86_64 gcc48-c++.x86_64
    sudo yum -y update binutils
    ```

2. On the master, generate a keypair using OpenSSL for remote attestation. Only
   the NIST p-256 curve is supported.

    ```sh
    cd ${OPAQUE_HOME}
    openssl ecparam -name prime256v1 -genkey -noout -out private_key.pem
    ```
    
4. Set the following environment variables:

    ```sh
    export SPARKSGX_DATA_DIR=${OPAQUE_HOME}/data
    export LIBSGXENCLAVE_PATH=${OPAQUE_HOME}/libSGXEnclave.so
    export LIBENCLAVESIGNED_PATH=${OPAQUE_HOME}/enclave.signed.so
    export LIBSGX_SP_PATH=${OPAQUE_HOME}/libservice_provider.so
    export PRIVATE_KEY_PATH=${OPAQUE_HOME}/private_key.pem
    ```

3. Synthesize test data:

    ```sh
    cd ${OPAQUE_HOME}
    data/disease/synth-disease-data
    ```

5. Run the Opaque tests:

    ```sh
    cd ${OPAQUE_HOME}
    build/sbt test
    ```
