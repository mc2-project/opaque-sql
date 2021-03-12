***************
Usage
***************

Next, run Apache Spark SQL queries with Opaque as follows, assuming [Spark 3.0.1](https://www.apache.org/dyn/closer.lua/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz) (``wget http://apache.mirrors.pair.com/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz``) is already installed:

\* Opaque needs Spark's ``'spark.executor.instances'`` property to be set. This can be done in a custom config file, the default config file found at ``/opt/spark/conf/spark-defaults.conf``, or as a ``spark-submit`` or ``spark-shell`` argument: ``--conf 'spark.executor.instances=<value>``.

1. Package Opaque into a JAR:

   .. code-block:: bash
                   
                   cd ${OPAQUE_HOME}
                   build/sbt package

2. Launch the Spark shell with Opaque:

   .. code-block:: bash
                   
                   ${SPARK_HOME}/bin/spark-shell --jars ${OPAQUE_HOME}/target/scala-2.12/opaque_2.12-0.1.jar
    
   Alternatively, to run Opaque queries locally for development rather than on a cluster:

   .. code-block:: bash

                   cd ${OPAQUE_HOME}
                   JVM_OPTS="-Xmx4G" build/sbt console

3. Inside the Spark shell, import Opaque's DataFrame methods and install Opaque's query planner rules:

   .. code-block:: scala
                    
                   import edu.berkeley.cs.rise.opaque.implicits._
                   edu.berkeley.cs.rise.opaque.Utils.initSQLContext(spark.sqlContext)

4. Create an encrypted DataFrame:

   .. code-block:: scala
      
                   val data = Seq(("foo", 4), ("bar", 1), ("baz", 5))
                   val df = spark.createDataFrame(data).toDF("word", "count")
                   val dfEncrypted = df.encrypted

5. Query the DataFrames and explain the query plan to see the secure operators:


   .. code-block:: scala
                   
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

6. Save and load an encrypted DataFrame:

   .. code-block:: scala
                   
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
