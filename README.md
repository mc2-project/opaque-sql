<img src="https://ucbrise.github.io/opaque/opaque.svg" width="315" alt="Opaque">

**Secure Apache Spark SQL**

[![Build Status](https://travis-ci.org/ucbrise/opaque.svg?branch=master)](https://travis-ci.org/ucbrise/opaque)

Opaque is a package for Apache Spark SQL that enables strong security for DataFrames using Intel SGX trusted hardware. The aim is to enable analytics on sensitive data in an untrusted cloud. Opaque allows encrypting the contents of a DataFrame. Subsequent operations on them will run within SGX enclaves.

This project is based on our NSDI 2017 paper [1]. The oblivious execution mode is not included in this release.

Disclaimers: This is an alpha preview of Opaque, which means the software is still in development (not production-ready!). Unlike the Spark cluster, the master must be run within a trusted environment (e.g., on the client).

Work-in-progress:

- Currently, Opaque supports a subset of Spark SQL operations and not yet UDFs. We are working on adding support for UDFs.

- The current version also does not yet support computation integrity verification, though we are actively working on it.

- The remote attestation code is not complete as it contains sample code from the Intel SDK.

- If you find bugs in the code, please file an issue.

[1] Wenting Zheng, Ankur Dave, Jethro Beekman, Raluca Ada Popa, Joseph Gonzalez, and Ion Stoica.
[Opaque: An Oblivious and Encrypted Distributed Analytics Platform](https://people.eecs.berkeley.edu/~wzheng/opaque.pdf). NSDI 2017, March 2017.

## Installation

After downloading the Opaque codebase, build and test it as follows:

1. Install dependencies and the Intel SGX SDK with C++11 support:

    ```sh
    # For Ubuntu 16.04:
    sudo apt-get install build-essential ocaml automake autoconf libtool wget python default-jdk cmake libssl-dev

    git clone https://github.com/ankurdave/linux-sgx -b c++11
    cd linux-sgx
    ./download_prebuilt.sh
    make sdk_install_pkg
    # Installer will prompt for install path, which can be user-local
    ./linux/installer/bin/sgx_linux_x64_sdk_*.bin
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
    export SPARKSGX_DATA_DIR=${OPAQUE_HOME}/data
    export PRIVATE_KEY_PATH=${OPAQUE_HOME}/private_key.pem
    ```

    If running with real SGX hardware, also set `export SGX_MODE=HW` and `export SGX_PRERELEASE=1`.

4. Run the Opaque tests:

    ```sh
    cd ${OPAQUE_HOME}
    ./data/fetch-test-data.sh
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

4. Create an encrypted DataFrame:

    ```scala
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5))
    val df = spark.createDataFrame(data).toDF("word", "count")
    val dfEncrypted = df.encrypted
    ```

5. Query the DataFrames and explain the query plan to see the secure operators:


    ```scala
    dfEncrypted.filter($"count" > lit(3)).explain(true)
    // [...]
    // == Optimized Logical Plan ==
    // EncryptedFilter (count#6 > 3)
    // +- EncryptedLocalRelation [word#5, count#6]
    // [...]

    dfEncrypted.filter($"count" > lit(3)).show
    // +----+-----+
    // |word|count|
    // +----+-----+
    // | foo|    4|
    // | baz|    5|
    // +----+-----+
    ```

6. Save and load an encrypted DataFrame:

    ```scala
    dfEncrypted.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save("dfEncrypted")
    // The file dfEncrypted/part-00000 now contains encrypted data

    import org.apache.spark.sql.types._
    val df2 = (spark.read.format("edu.berkeley.cs.rise.opaque.EncryptedSource")
      .schema(StructType(Seq(StructField("word", StringType), StructField("count", IntegerType))))
      .load("dfEncrypted"))
    df2.show
    // +----+-----+
    // |word|count|
    // +----+-----+
    // | foo|    4|
    // | bar|    1|
    // | baz|    5|
    // +----+-----+
    ```

## Contact

If you want to know more about our project or have questions, please contact Wenting (wzheng@eecs.berkeley.edu) and/or Ankur (ankurdave@gmail.com).
