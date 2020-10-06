<img src="https://mc2-project.github.io/opaque/opaque.svg" width="315" alt="Opaque">

**Secure Apache Spark SQL**

[![Build Status](https://travis-ci.org/mc2-project/opaque.svg?branch=master)](https://travis-ci.org/mc2-project/opaque)

Opaque is a package for Apache Spark SQL that enables encryption for DataFrames using the OpenEnclave framework. The aim is to enable analytics on sensitive data in an untrusted cloud. Once the contents of a DataFrame are encrypted, subsequent operations will run within hardware enclaves (such as Intel SGX).

This project is based on the following NSDI 2017 paper [1]. The oblivious execution mode is not included in this release.

This is an alpha preview of Opaque, which means the software is still in development (not production-ready!). It currently has the following limitations:

- Unlike the Spark cluster, the master must be run within a trusted environment (e.g., on the client).

- Not all Spark SQL operations are supported. UDFs must be [implemented in C++](#user-defined-functions-udfs).

- Computation integrity verification (section 4.2 of the NSDI paper) is currently work in progress.

[1] Wenting Zheng, Ankur Dave, Jethro Beekman, Raluca Ada Popa, Joseph Gonzalez, and Ion Stoica.
[Opaque: An Oblivious and Encrypted Distributed Analytics Platform](https://people.eecs.berkeley.edu/~wzheng/opaque.pdf). NSDI 2017, March 2017.

## Installation

After downloading the Opaque codebase, build and test it as follows.

1. Install dependencies and the [OpenEnclave SDK](https://github.com/openenclave/openenclave/blob/v0.9.x/docs/GettingStartedDocs/install_oe_sdk-Ubuntu_18.04.md). We currently support OE version 0.9.0 (so please install with `open-enclave=0.9.0`) and Ubuntu 18.04.

    ```sh
    # For Ubuntu 18.04:
    sudo apt install wget build-essential openjdk-8-jdk python libssl-dev
    
    # Install a newer version of CMake (>= 3.13)
    wget https://github.com/Kitware/CMake/releases/download/v3.15.6/cmake-3.15.6-Linux-x86_64.sh
    sudo bash cmake-3.15.6-Linux-x86_64.sh --skip-license --prefix=/usr/local
    ```

2. On the master, generate a keypair using OpenSSL for remote attestation.

    ```sh
    openssl genrsa -out private_key.pem -3 3072
    ```

3. Change into the Opaque root directory and edit Opaque's environment variables in `opaqueenv` if desired. Export Opaque and OpenEnclave environment variables via

    ```sh
    source opaqueenv
    source /opt/openenclave/share/openenclave/openenclaverc
    ```

    By default, Opaque runs in hardware mode (environment variable `MODE=HARDWARE`).
    If you do not have a machine with a hardware enclave but still wish to test out Opaque's functionality locally, then set `export MODE=SIMULATE`.

4. Run the Opaque tests:

    ```sh
    cd ${OPAQUE_HOME}
    build/sbt test
    ```

## Usage

Next, run Apache Spark SQL queries with Opaque as follows, assuming [Spark 3.0](https://www.apache.org/dyn/closer.lua/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz) (`wget http://apache.mirrors.pair.com/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz`) is already installed:

1. Package Opaque into a JAR:

    ```sh
    cd ${OPAQUE_HOME}
    build/sbt package
    ```

2. Launch the Spark shell with Opaque:

    ```sh
    ${SPARK_HOME}/bin/spark-shell --jars ${OPAQUE_HOME}/target/scala-2.12/opaque_2.12-0.1.jar
    ```
    
    Alternatively, to run Opaque queries locally for development rather than on a cluster:
    
    ```sh
    cd ${OPAQUE_HOME}
    JVM_OPTS="-Xmx4G" build/sbt console
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
    
## User-Defined Functions (UDFs)

To run a Spark SQL UDF within Opaque enclaves, first name it explicitly and define it in Scala, then reimplement it in C++ against Opaque's serialized row representation.

For example, suppose we wish to implement a UDF called `dot`, which computes the dot product of two double arrays (`Array[Double]`). We [define it in Scala](src/main/scala/edu/berkeley/cs/rise/opaque/expressions/DotProduct.scala) in terms of the Breeze linear algebra library's implementation. We can then use it in a DataFrame query, such as [logistic regression](src/main/scala/edu/berkeley/cs/rise/opaque/benchmark/LogisticRegression.scala).

Now we can port this UDF to Opaque as follows:

1. Define a corresponding expression using Opaque's expression serialization format by adding the following to [Expr.fbs](src/flatbuffers/Expr.fbs), which indicates that a DotProduct expression takes two inputs (the two double arrays):

    ```protobuf
    table DotProduct {
        left:Expr;
        right:Expr;
    }
    ```

    In the same file, add `DotProduct` to the list of expressions in `ExprUnion`.

2. Implement the serialization logic from the Scala `DotProduct` UDF to the Opaque expression that we just defined. In [`Utils.flatbuffersSerializeExpression`](src/main/scala/edu/berkeley/cs/rise/opaque/Utils.scala), add a case for `DotProduct` as follows:

    ```scala
    case (DotProduct(left, right), Seq(leftOffset, rightOffset)) =>
      tuix.Expr.createExpr(
        builder,
        tuix.ExprUnion.DotProduct,
        tuix.DotProduct.createDotProduct(
          builder, leftOffset, rightOffset))
    ```

3. Finally, implement the UDF in C++. In [`FlatbuffersExpressionEvaluator#eval_helper`](src/enclave/Enclave/ExpressionEvaluation.h), add a case for `tuix::ExprUnion_DotProduct`. Within that case, cast the expression to a `tuix::DotProduct`, recursively evaluate the left and right children, perform the dot product computation on them, and construct a `DoubleField` containing the result.

## Contact

If you want to know more about our project or have questions, please contact Wenting (wzheng@eecs.berkeley.edu) and/or Ankur (ankurdave@gmail.com).
