********************************************
Query submission via the MC\ :sup:`2` Client
********************************************

.. figure:: https://mc2-project.github.io/opaque-sql/opaque_diagram.png
   :align: center
   :figwidth: 80 %

   Overview of Opaque SQL running with the MC\ :sup:`2` Client


In this section, we outline how to use `the client <https://github.com/mc2-project/mc2>`_ for submitting queries to Opaque SQL. This is the primary method of running Opaque SQL and integrates with the rest of the MC\ :sup:`2` projects, such as `Secure XGBoost <https://github.com/mc2-project/secure-xgboost>`_.

.. warning::
      Note that, by default, Opaque SQL uses a pre-generated RSA private key for enclave signing located at ``${OPAQUE_HOME}/src/test/keys/mc2_test_key.pem``. This key **should not** be used in a production environment. This can be reconfigured by changing the environment variable ``PRIVATE_KEY_PATH`` to another key file and having the MC\ :sup:`2` Client use its public peer to verify the signature.

Running in this mode enables the driver to be located on *untrusted* hardware while still providing the complete security guarantees of the MC\ :sup:`2` platform.

Starting the gRPC Listeners
###########################

Opaque SQL uses two listeners: the first for remote attestation on port 50051, and the second for query requests on port 50052. The MC\ :sup:`2` Client uses these by default to connect to the Opaque SQL driver.

Opaque SQL supports running both regular (Scala) Spark as well as Pyspark.

Scala Instructions
******************

1. To run both listeners locally using ``sbt``:

   .. code-block:: bash

                   build/sbt run

2. To launch the listeners on a standalone Spark cluster:

   .. code-block:: bash

                  build/sbt assembly # create a fat jar with all necessary dependencies

                  spark-submit --class edu.berkeley.cs.rise.opaque.rpc.Listener  \
                    <Spark configuration parameters>  \
                      --deploy-mode client ${OPAQUE_HOME}/target/scala-2.12/opaque-assembly-0.1.jar

Python Instructions
*******************

To launch the listeners on a standalone Spark cluster:

.. code-block:: bash

    build/sbt assembly # create a fat jar with all necessary dependencies

    spark-submit --class edu.berkeley.cs.rise.opaque.rpc.Listener  \
      <Spark configuration parameters>  \
        --deploy-mode client  \
        --jars ${OPAQUE_HOME}/target/scala-2.12/opaque-assembly-0.1.jar  \
        --py-files ${OPAQUE_HOME}/target/python.zip  \
        ${OPAQUE_HOME}/target/python/listener.py  \

Using the MC\ :sup:`2` Client
#############################

The MC\ :sup:`2` Client is a Python package that provides an integrated channel for communicating with other MC\ :sup:`2` compute services, including Opaque SQL. By following the `install instructions <https://mc2-project.github.io/mc2/install.html>`_ and specifying a user configuration, the client makes it possible to run Opaque SQL remotely and receive results locally.

1. For an explanation of the user configuration and to see an example, see the `configuration documentation <https://mc2-project.github.io/mc2/config/config.html>`_.

2. For concrete instructions on how to run Opaque SQL with the client, see the `usage with Opaque SQL documentation <https://mc2-project.github.io/mc2/opaquesql_usage.html>`_.

3. (Optional) The MC\ :sup:`2` Client also contains tools to aid in setting up a production environment, like an `Azure configuration <https://mc2-project.github.io/mc2/config/azure.html>`_ for starting a cluster through a `simple API <https://mc2-project.github.io/mc2/python/usage.html#azure-resource-management>`_.

Alternatively, we offer a quickstart for running everything seamlessly in a Docker container with all dependencies installed. For a comprehensive walkthrough, see the `quickstart documentation <https://mc2-project.github.io/mc2/quickstart.html>`_.

For further reading:

`Client Source Code <https://github.com/mc2-project/mc2>`_

`Client Documentation <https://mc2-project.github.io/mc2/index.html>`_
