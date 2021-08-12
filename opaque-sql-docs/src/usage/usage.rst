*****
Usage
*****

This section goes over how to manipulate an encrypted DataFrame in either client or insecure mode. 

.. _save_df:

Saving a DataFrame
##################

Save the encrypted DataFrame to local disk. The encrypted data can then be uploaded to cloud storage of your choice for easy access.

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

2. Given an encrypted DataFrame, construct a new query. Users can use ``explain`` to see the generated query plan.

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
