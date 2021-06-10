*************************************
Query submission via the Spark Driver
*************************************

.. figure:: https://mc2-project.github.io/opaque-sql/opaque-diagram.svg
   :align: center
   :figwidth: 100 %

   Overview of Opaque SQL running in insecure mode



Starting Opaque SQL
###################

This page goes through running Opaque SQL with the Spark driver located on the client. 

.. warning::
      This mode **should not** be used in any context where the full security of hardware enclaves is required. Remote attestation is disabled, and the Spark Driver has access to the key the worker enclaves use to encrypt/decrypt data. This is still offered to play around with the project and explore its API.

      This is Opaque SQL in **insecure** mode, and is normally only used for testing functionalities.

Running the interactive shell
*****************************


1. Package Opaque into a fat JAR. The reason we need a fat JAR is because the JAR produced by ``build/sbt package`` does not include gRPC dependencies needed for remote attestion.

   .. code-block:: bash
                   
                   cd ${OPAQUE_HOME}
                   build/sbt test:assembly

2. Launch the Spark shell with Opaque.

   Scala:

   .. code-block:: bash

                   spark-shell --jars ${OPAQUE_HOME}/target/scala-2.12/opaque-assembly-0.1.jar

   Python:

   .. code-block:: bash
                   
                   pyspark --py-files ${OPAQUE_HOME}/target/scala-2.12/opaque-assembly-0.1.jar  \
                     --jars ${OPAQUE_HOME}/target/scala-2.12/opaque-assembly-0.1.jar
    
   (we need to specify `--py-files` because the Python functions are placed in the .jar for easier packaging)

3. Alternatively, you can also run queries in Scala locally using ``sbt``.

   .. code-block:: bash

                   build/sbt console
    
4. Inside the Spark shell, import Opaque SQL's ``DataFrame`` methods and its query planning rules.

   Scala:

   .. code-block:: scala

                     import edu.berkeley.cs.rise.opaque.implicits._
                     edu.berkeley.cs.rise.opaque.Utils.initOpaqueSQL(spark, testing = true)

   Python:

   .. code-block:: python

                  from opaque_sql import *
                  init_opaque_sql(testing=True)
                   
    



Encrypting, saving, and loading a DataFrame
###########################################

1. Create an unencrypted DataFrame.

   Scala:

   .. code-block:: scala
                   
                   val data = Seq(("foo", 4), ("bar", 1), ("baz", 5))
                   val df = spark.createDataFrame(data).toDF("word", "count")

   Python:

   .. code-block:: python
                   
                  data = [("foo", 4), ("bar", 1), ("baz", 5)]
                  df = sqlContext.createDataFrame(data).toDF("word", "count")

2. Create an encrypted DataFrame from the unencrypted version. Opaque SQL makes this as easy as calling ``.encrypted`` (*note*: this call is only supported in insecure mode).

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
#############################

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

2. Given an encrypted DataFrame , construct a new query. Users can use ``explain`` to see the generated query plan.

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

                  result = df_encrypted.filter(df_encrypted["count"] > 3)
                  result.explain(True)
                   
Call ``.collect`` or ``.show`` to retreive and automatically decrypt the results.


Using the SQL interface
#######################

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

