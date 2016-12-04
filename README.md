# Opaque for Apache Spark

How to build and test Opaque:

1. Install dependencies:

    ```sh
    sudo yum -y install gcc48.x86_64 gcc48-c++.x86_64
    sudo yum -y update binutils
    # For fetching benchmark data
    yum -y --enablerepo epel install s3cmd
    s3cmd --configure
    ```

2. Install the Linux SGX SDK with C++11 support:

    ```sh
    git clone https://github.com/ankurdave/linux-sgx -b c++11
    cd linux-sgx
    ./download_prebuilt.sh
    make sdk_install_pkg
    # Installer will prompt for install path, which can be user-local
    ./linux/installer/bin/sgx_linux_x64_sdk_*.bin
    ```
    
3. On the master, generate a keypair using OpenSSL for remote attestation. Only
   the NIST p-256 curve is supported.

    ```sh
    cd ${OPAQUE_HOME}
    openssl ecparam -name prime256v1 -genkey -noout -out private_key.pem
    ```
    
4. Fetch the benchmark data:

    ```sh
    cd ${OPAQUE_HOME}
    DATA_DIR=... REPO_DIR=${OPAQUE_HOME} ./data/fetch-data.sh
    ```

5. Set the following environment variables in `${SPARK_HOME}/conf/spark-env.sh`
   and in your shell:

    ```sh
    export SGX_SDK=.../sgxsdk # from step 2
    export SPARKSGX_DATA_DIR=... # from step 4
    export LIBSGXENCLAVE_PATH=${OPAQUE_HOME}/libSGXEnclave.so
    export LIBENCLAVESIGNED_PATH=${OPAQUE_HOME}/enclave.signed.so
    export LIBSGX_SP_PATH=${OPAQUE_HOME}/libservice_provider.so
    export PRIVATE_KEY_PATH=${OPAQUE_HOME}/private_key.pem
    ```

6. Run the Opaque tests:

    ```sh
    cd ${OPAQUE_HOME}
    build/sbt test
    ```
