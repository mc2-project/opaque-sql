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
                   
    



Encrypting a DataFrame with the Driver
######################################

.. note::
  The Opaque SQL methods shown in this section are *only* supported in insecure mode, since the driver needs the key for encryption/decryption.

1. Create an unencrypted DataFrame.

   Scala:

   .. code-block:: scala
                   
                   val data = Seq(("foo", 4), ("bar", 1), ("baz", 5))
                   val df = spark.createDataFrame(data).toDF("word", "count")

   Python:

   .. code-block:: python
                   
                  data = [("foo", 4), ("bar", 1), ("baz", 5)]
                  df = sqlContext.createDataFrame(data).toDF("word", "count")

2. Create an encrypted DataFrame from the unencrypted version. In insecure mode, this is as easy as calling ``.encrypted``.

   Scala:
   
   .. code-block:: scala
                   
                   val dfEncrypted = df.encrypted

   Python:

   .. code-block:: python
                   
                  df_encrypted = df.encrypted()


3. Perform any operations in the :ref:`list of supported functionalities <functionalities>`.

3. Call ``.collect`` or ``.show`` to retreive and automatically decrypt the results.

   Scala:
   
   .. code-block:: scala
                   
                  dfEncrypted.show
                  // +-----+-----+
                  // |word |count|
                  // +--99-+-----+
                  // |  foo|    4|
                  // |  bar|    1|
                  // |  baz|    5|
                  // +-----+-----+

   Python:

   .. code-block:: python
                   
                  df_encrypted.show
                  # +-----+-----+
                  # |word |count|
                  # +--99-+-----+
                  # |  foo|    4|
                  # |  bar|    1|
                  # |  baz|    5|
                  # +-----+-----+
