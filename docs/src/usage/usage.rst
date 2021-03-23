****************
Using Opaque SQL
****************

Setup
*****
Opaque SQL needs two Spark properties to be set:

- ``spark.executor.instances``
- ``spark.task.maxFailures``

Both of these are used for remote attestation: Opaque SQL needs to know how many executors are in the cluster, and to use Spark's fault tolerance property to attest unattested executors.

These properties can be be set in a custom configuration file, the default being located at ``${SPARK_HOME}/conf/spark-defaults.conf``, or as a ``spark-submit`` or ``spark-shell`` argument: ``--conf <key>=<value>``.

Running Opaque SQL
******************

Once setup is finished, you can run Spark SQL queries as follows.

1. Package Opaque into a JAR.

   .. code-block:: bash
                   
                   cd ${OPAQUE_HOME}
                   build/sbt package

2. Launch the Spark shell with Opaque.

   Scala:

   .. code-block:: bash
                   
                   spark-shell --jars ${OPAQUE_HOME}/target/scala-2.12/opaque_2.12-0.1.jar

   Python:

   .. code-block:: bash
                   
                   pyspark --py-files ${OPAQUE_HOME}/target/scala-2.12/opaque_2.12-0.1.jar  \
                     --jars ${OPAQUE_HOME}/target/scala-2.12/opaque_2.12-0.1.jar
    
   (the reason we need to specify `--py-files` is because the Opaque Python functions are placed in the .jar for easier packaging)
    
3. Inside the Spark shell, import Opaque's DataFrame methods and install Opaque's query planner rules.

   Scala:

   .. code-block:: scala

                     import edu.berkeley.cs.rise.opaque.implicits._
                     edu.berkeley.cs.rise.opaque.Utils.initSQLContext(spark.sqlContext)

   Python:

   .. code-block:: python

                  from opaque_wrappers import *
                  init_sql_context()
                   
    

4. You can also run queries in Scala locally.

   .. code-block:: bash

                   cd ${OPAQUE_HOME}
                   JVM_OPTS="-Xmx4G"; build/sbt console


Encrypting, saving, and loading a DataFrame
*******************************************

1. Create an unencrypted DataFrame on the driver.
   This should be done on the client, i.e., in a trusted setting.

   Scala:

   .. code-block:: scala
                   
                   val data = Seq(("foo", 4), ("bar", 1), ("baz", 5))
                   val df = spark.createDataFrame(data).toDF("word", "count")

   Python:

   .. code-block:: python
                   
                  data = [("foo", 4), ("bar", 1), ("baz", 5)]
                  df = sqlContext.createDataFrame(data).toDF("word", "count")

2. Create an encrypted DataFrame from the unencrypted version.
   This is as easy as calling ``.encrypted``.

   Scala:
   
   .. code-block:: scala
                   
                   val dfEncrypted = df.encrypted

   Python:

   .. code-block:: python
                   
                  df_encrypted = df.encrypted()

.. _save_df:

3. Save the encrypted DataFrame to local disk.
   The encrypted data can also be uploaded to cloud storage for easy access.

   Scala:

   .. code-block:: scala
                   
                   dfEncrypted.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save("dfEncrypted")
                   // The file dfEncrypted/part-00000 now contains encrypted data

   Python:

   .. code-block:: python
                   
                  df_encrypted.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save("df_encrypted")

Using the DataFrame interface
*****************************

1. Users can load the :ref:`previously persisted encrypted DataFrame<save_df>`.

   Scala:

   .. code-block:: scala
                   
                   import org.apache.spark.sql.types._
                   val dfEncrypted = (spark.read.format("edu.berkeley.cs.rise.opaque.EncryptedSource")
                   .schema(StructType(Seq(StructField("word", StringType), StructField("count", IntegerType))))
                   .load("dfEncrypted"))

   Python:

   .. code-block:: python
                   
                  df_encrypted = spark.read.format("edu.berkeley.cs.rise.opaque.EncryptedSource").load("df_encrypted")

2. Given an encrypted DataFrame ``dfEncrypted``, construct a new query.
   Users can use ``explain`` to see the generated query plan.

   Scala:

   .. code-block:: scala
                   
                   val result = dfEncrypted.filter($"count" > lit(3))
                   result.explain(true)
                   // [...]
                   // == Optimized Logical Plan ==
                   // EncryptedFilter (count#6 > 3)
                   // +- EncryptedLocalRelation [word#5, count#6]
                   // [...]

   Python:
   
   .. code-block:: python

                  result = df_encrypted.filter(df["count"] > 3)
                  result.explain(true)
                   
Call ``.collect`` or ``.show`` to retreive the results. The final result will be decrypted on the driver. 


Using the SQL interface
***********************

1. Users can also load the :ref:`previously persisted encrypted DataFrame <save_df>` using the SQL interface.

   .. code-block:: scala

                   spark.sql(s"""
                     |CREATE TEMPORARY VIEW dfEncrypted
                     |USING edu.berkeley.cs.rise.opaque.EncryptedSource
                     |OPTIONS (
                     |  path "dfEncrypted"
                     |)""".stripMargin)

2. The SQL API can be used to run the same query on the loaded data.
   
   .. code-block:: scala
                   
                   val result = spark.sql(s"""
                     |SELECT * FROM dfEncrypted
                     |WHERE count > 3""".stripMargin)
                   result.show

