<img src="https://mc2-project.github.io/opaque-sql/opaque.svg" width="315" alt="Opaque">

## Secure Apache Spark SQL

![Tests Status](https://github.com/mc2-project/opaque/actions/workflows/main.yml/badge.svg) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md)

Welcome to the landing page of Opaque SQL! Opaque SQL is a package for Apache Spark SQL that enables processing over encrypted DataFrames using the OpenEnclave framework. 

### Quick start
To quickly get started with Opaque SQL, you can download our Docker image (also includes other open source projects in the MC<sup>2</sup> project).

```sh
docker pull mc2project/mc2
docker run -it -p 22:22 -p 50051-50055:50051-50055 -w /root mc2project/mc2
```

Change into the Opaque directory and export the Opaque and OpenEnclave environment variables.

```sh
cd opaque
source opaqueenv
source /opt/openenclave/share/openenclave/openenclaverc
export MODE=SIMULATE
```

You are now ready to run your first Opaque SQL query! First, start a Scala shell:

```sh
build/sbt console
```

Next, import Opaque's DataFrame methods:

```scala
import edu.berkeley.cs.rise.opaque.implicits._
edu.berkeley.cs.rise.opaque.Utils.initOpaqueSQL(spark)
```

To convert an existing DataFrame into an Opaque encrypted DataFrame, simply call `.encrypted`:

```scala
val data = Seq(("foo", 4), ("bar", 1), ("baz", 5))
val df = spark.createDataFrame(data).toDF("word", "count")
val dfEncrypted = df.encrypted
```

You can use the same Spark SQL API to query the encrypted DataFrame:

```scala
val result = dfEncrypted.filter($"count" > lit(3))
result.explain(true)
// [...]
// == Optimized Logical Plan ==
// EncryptedFilter (count#6 > 3)
// +- EncryptedLocalRelation [word#5, count#6]
// [...]
```

Congrats, you've run your first encrypted query using Opaque SQL! 

### Documentation
For more details on building, using, and contributing, please see our [documentation](https://mc2-project.github.io/opaque-sql/).

### Paper
The open source is based on our NSDI 2017 [paper](https://www.usenix.org/system/files/conference/nsdi17/nsdi17-zheng.pdf).
