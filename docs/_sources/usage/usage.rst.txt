****************
Using Opaque SQL
****************

Opaque setup
###############

Once you have installed Opaque SQL, you can run Spark SQL queries as follows. Opaque SQL needs Spark's ``'spark.executor.instances'`` property to be set. This can be done in a custom config file, the default config file found at ``/opt/spark/conf/spark-defaults.conf``, or as a ``spark-submit`` or ``spark-shell`` argument: ``--conf 'spark.executor.instances=<value>``.

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

.. I think this is obsolete
   3. Inside the Spark shell, import Opaque's DataFrame methods and install Opaque's query planner rules:

      .. code-block:: scala

                      import edu.berkeley.cs.rise.opaque.implicits._
                      edu.berkeley.cs.rise.opaque.Utils.initSQLContext(spark.sqlContext)

Encrypting, saving, and loading a DataFrame
###########################################

1. Create an unencrypted DataFrame on the driver.
   This should be done on the client, i.e., in a trusted setting.

   .. code-block:: scala
                   
                   val data = Seq(("foo", 4), ("bar", 1), ("baz", 5))
                   val df = spark.createDataFrame(data).toDF("word", "count")

2. Create an encrypted DataFrame from the unencrypted version.
   This is as easy as calling ``.encrypted``.
   
   .. code-block:: scala
                   
                   val dfEncrypted = df.encrypted

3. Save the encrypted DataFrame to local disk.
   The encrypted data can also be uploaded to cloud storage for easy access.

   .. code-block:: scala
                   
                   dfEncrypted.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save("dfEncrypted")
                   // The file dfEncrypted/part-00000 now contains encrypted data

4. Users can load the persisted encrypted DataFrame back into Spark.

   .. code-block:: scala
                   
                   import org.apache.spark.sql.types._
                   val df2 = (spark.read.format("edu.berkeley.cs.rise.opaque.EncryptedSource")
                   .schema(StructType(Seq(StructField("word", StringType), StructField("count", IntegerType))))
                   .load("dfEncrypted"))

Using the DataFrame interface
#############################

1. Given an encrypted DataFrame ``dfEncrypted``, construct a new query.
   Users can use ``explain`` to see the generated query plan.

   .. code-block:: scala
                   
                   val result = dfEncrypted.filter($"count" > lit(3))
                   result.explain(true)
                   // [...]
                   // == Optimized Logical Plan ==
                   // EncryptedFilter (count#6 > 3)
                   // +- EncryptedLocalRelation [word#5, count#6]
                   // [...]

2. Call ``.collect`` or ``.show`` to retreive the results.
   The final result will be decrypted on the driver. 

   .. code-block:: scala
                   
                   result.filter($"count" > lit(3)).show
                   // +----+-----+
                   // |word|count|
                   // +----+-----+
                   // | foo|    4|
                   // | baz|    5|
                   // +----+-----+


Using the DataFrame interface should be very straightforward -- simply 
                   

Using the SQL interface
#######################

