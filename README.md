# Opaque: Secure Apache Spark SQL

[![Build Status](https://travis-ci.org/ucbrise/opaque.svg?branch=master)](https://travis-ci.org/ucbrise/opaque)

Opaque is a package for Apache Spark SQL that enables very strong security for DataFrames -- data encryption and access pattern hiding -- using Intel SGX trusted hardware. The aim is to enable analytics on sensitive data in an untrusted cloud. See our upcoming NSDI 2017 paper [1] for more details.

Opaque allows marking DataFrames as <em>encrypted</em> or <em>oblivious</em> (encrypted with access pattern protection). The contents of these DataFrames will be encrypted, and subsequent operations on them will run within SGX enclaves.

Warning: This is an alpha preview of Opaque, which means the software is still early stage! It is currently not production-ready. Opaque supports a subset of Spark SQL operations, and does not support UDFs. Unlike the Spark cluster, the master must be trusted. The current version also does not support computation integrity verification.
If you find bugs in the code, please let us know!

[1] Wenting Zheng, Ankur Dave, Jethro Beekman, Raluca Ada Popa, Joseph Gonzalez, and Ion Stoica.
[Opaque: An Oblivious and Encrypted Distributed Analytics Platform](https://people.eecs.berkeley.edu/~wzheng/opaque.pdf). NSDI 2017 (to appear), March 2017.

## Installation

After downloading the Opaque codebase, build and test it as follows:

1. Install GCC 4.8+ and the Intel SGX SDK:

    ```sh
    sudo yum -y install gcc48.x86_64 gcc48-c++.x86_64
    sudo yum -y update binutils
    wget https://download.01.org/intel-sgx/linux-1.7/sgx_linux_x64_sdk_1.7.100.36470.bin -O sgx_sdk.bin
    chmod +x sgx_sdk.bin
    # Installer will prompt for install path, which can be user-local
    ./sgx_sdk.bin
    ```

2. On the master, generate a keypair using OpenSSL for remote attestation. The public key will be automatically hardcoded into the enclave code.
   Note that only the NIST p-256 curve is supported.

    ```sh
    cd ${OPAQUE_HOME}
    openssl ecparam -name prime256v1 -genkey -noout -out private_key.pem
    ```

3. Set the following environment variables:

    ```sh
    source sgxsdk/environment # from SGX SDK install directory in step 1
    export CXX=/usr/bin/g++-4.8
    export SPARKSGX_DATA_DIR=${OPAQUE_HOME}/data
    export LIBSGXENCLAVE_PATH=${OPAQUE_HOME}/libSGXEnclave.so
    export LIBENCLAVESIGNED_PATH=${OPAQUE_HOME}/enclave.signed.so
    export LIBSGX_SP_PATH=${OPAQUE_HOME}/libservice_provider.so
    export PRIVATE_KEY_PATH=${OPAQUE_HOME}/private_key.pem
    ```

    If running with real SGX hardware, also set `export SGX_MODE=HW` and `export SGX_PRERELEASE=1`.

4. Run the Opaque tests:

    ```sh
    cd ${OPAQUE_HOME}
    build/sbt test
    ```

## Usage

Next, run Apache Spark SQL queries with Opaque as follows, assuming Spark is already installed:

1. Package Opaque into a JAR:

    ```sh
    cd ${OPAQUE_HOME}
    build/sbt package
    ```

2. Launch the Spark shell with Opaque:

    ```sh
    ${SPARK_HOME}/bin/spark-shell --jars ${OPAQUE_HOME}/target/scala-2.11/opaque_2.11-0.1.jar
    ```

3. Inside the Spark shell, import Opaque's DataFrame methods and install Opaque's query planner rules:

    ```scala
    import edu.berkeley.cs.rise.opaque.implicits._

    edu.berkeley.cs.rise.opaque.Utils.initSQLContext(spark.sqlContext)
    ```

4. Create encrypted and oblivious DataFrames:

    ```scala
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5))
    val df = spark.createDataFrame(data).toDF("word", "count")
    val dfEncrypted = df.encrypted
    val dfOblivious = df.oblivious
    ```

5. Query the DataFrames and explain the query plan to see the secure operators:


    ```scala
    dfEncrypted.filter($"count" > lit(3)).explain(true)
    // [...]
    // == Optimized Logical Plan ==
    // EncryptedFilter (count#6 > 3)
    // +- EncryptedLocalRelation [word#5, count#6]
    // [...]

    dfOblivious.filter($"count" > lit(3)).explain(true)
    // [...]
    // == Optimized Logical Plan ==
    // ObliviousFilter (count#6 > 3)
    // +- ObliviousPermute
    //    +- EncryptedLocalRelation [word#5, count#6]
    // [...]

    dfEncrypted.filter($"count" > lit(3)).show
    // +----+-----+
    // |word|count|
    // +----+-----+
    // | foo|    4|
    // | baz|    5|
    // +----+-----+
    ```

## Contact

If you want to know more about our project or have questions, please contact Wenting (wzheng@eecs.berkeley.edu) and/or Ankur (ankurdave@gmail.com).
