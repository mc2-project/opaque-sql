************
Installation
************

Using the install script
########################

Opaque SQL has an install script (``opaque-sql/scripts/install.sh``) to take care of installing/downloading everything for Opaque SQL to work immediately. To use this, first clone/fork the repository, then run

.. code-block:: bash

   cd opaque-sql
   source ./scripts/install.sh

Note that the ``source`` is necessary to properly set environment variables Opaque SQL needs. If alternatively you don't want to use the install script, see the section on :ref:`installing dependencies manually <deps_manual>` below. For testing, skip to :ref:`running tests<running_tests>`.

.. _deps_manual:

Installing dependencies manually
################################

After downloading the Opaque codebase, install necessary dependencies as follows.

1. Install dependencies and the `OpenEnclave SDK <https://github.com/openenclave/openenclave/blob/v0.12.0/docs/GettingStartedDocs/install_oe_sdk-Ubuntu_18.04.md>`_. We currently support OE version 0.12.0 (so please install with ``open-enclave=0.12.0``) and Ubuntu 18.04.

   .. code-block:: bash
               
                   # For Ubuntu 18.04:
                   sudo apt -y install wget build-essential openjdk-8-jdk python libssl-dev libmbedtls-dev
                   pip3 install grpcio grpcio-tools # Needed for Pyspark listener

                   # Install a newer version of CMake (3.15)
                   wget https://github.com/Kitware/CMake/releases/download/v3.15.6/cmake-3.15.6-Linux-x86_64.sh
                   sudo bash cmake-3.15.6-Linux-x86_64.sh --skip-license --prefix=/usr/local
                   rm cmake-3.15.6-Linux-x86_64.sh

                   # Install Spark 3.1.1 (if not already done)
                   wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
                   tar xvf spark-3.1.1*
                   sudo mkdir /opt/spark
                   sudo mv spark-3.1.1*/* /opt/spark
                   rm -rf spark-3.1.1*
                   sudo mkdir /opt/spark/work
                   sudo chmod -R a+wx /opt/spark/work

2. Change into the Opaque root directory and edit Opaque's environment variables in ``opaqueenv`` (including Spark configurations) if desired. Export Opaque SQL and Open Enclave environment variables via

   .. code-block:: bash
                   
                   source opaqueenv
                   source /opt/openenclave/share/openenclave/openenclaverc

   By default, Opaque runs in hardware mode (environment variable ``MODE=HARDWARE``).
   If you do not have a machine with a hardware enclave but still wish to test out Opaque's functionality locally, then set ``export MODE=SIMULATE``.


.. _running_tests:

Running tests
#############

1. To run the Opaque tests:

   .. code-block:: bash
                
                   build/sbt test

2. Alternatively, to run tests *and* generate coverage reports:

   .. code-block:: bash

                  build/sbt clean coverage test
                  build/sbt coverageReport


Additional configurations for running on a Spark cluster
########################################################

Opaque SQL needs three Spark properties to be set:

- ``spark.executor.instances=n`` (n is usually the number of machines in the cluster)
- ``spark.task.maxFailures=10`` (attestation uses Sparks fault tolerance property)
- ``spark.driver.defaultJavaOptions="-Dscala.color"`` (if querying with MC\ :sup:`2` Client)

These properties can be be set in a custom configuration file, the default being located at ``${SPARK_HOME}/conf/spark-defaults.conf``, or as a ``spark-submit`` or ``spark-shell`` argument: ``--conf <key>=<value>``. For more details on running a Spark cluster, see the `Spark documentation <https://spark.apache.org/docs/latest/cluster-overview.html>`_
