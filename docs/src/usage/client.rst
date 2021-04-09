********************************************
Query submission via the MC\ :sup:`2` Client
********************************************

Starting Opaque SQL
###################

Here we outline how to use `the client <https://github.com/mc2-project/mc2>`_ to submit queries. In this security model, the Spark driver is considered *untrusted*, and should not have any way to decrypt results or any intermediate computation.

**Note** that this method of computation is still not entirely supported. It is possible to submit queries from MC\ :sup:`2` Client, but only with a shared key on the driver, so it must still be considered *trusted*.

Starting the gRPC Listener
**************************

1. To run the listener locally using ``sbt``:

   .. code-block:: bash

                   build/sbt run

2. To launch the listener on a standalone Spark cluster:

   .. code-block:: bash

                  build/sbt assembly # create a fat jar with all necessary dependencies

                  spark-submit --class edu.berkeley.cs.rise.opaque.rpc.Listener  \
                     <Spark configuration parameters> \
                     --deploy-mode client ${OPAQUE_HOME}/target/scala-2.12/opaque-assembly-0.1.jar

Both of these methods start a listener on port 50051 that allows you to use the MC\ :sup:`2` Client to submit queries to Opaque SQL remotely.

Using the MC\ :sup:`2` Client
#############################

MC\ :sup:`2` Client is a Python package that allows you to communicate with other MC\ :sup:`2` compute services. 


`Source Code <https://github.com/mc2-project/mc2>`_

`Documentation <https://mc2-project.github.io/mc2/index.html>`_
