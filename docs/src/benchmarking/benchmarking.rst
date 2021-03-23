************
Benchmarking
************

Running on your own cluster
###########################

Opaque supports a command-line interface for benchmarking against plaintext Spark. The following steps show you how to build and submit benchmarking jobs to a Spark cluster.

1. Create the benchmarking data:

   .. code-block:: bash
               
                build/sbt data

2. Create a fat jar that contains both source and test classes:

   .. code-block:: bash
               
                build/sbt test:assembly

3. For usage and to see a list of available flags, specify ``--help`` to the benchmarking class:

   .. code-block:: bash

                build/sbt 'test:runMain edu.berkeley.cs.rise.opaque.benchmark.Benchmark --help'
                    # Available flags:
                    # --num-partitions: specify the number of partitions the data should be split into.
                        # Default: spark.default.parallelism
                    # --size: specify the size of the dataset that should be loaded into Spark.
                        # Default: sf_001
                        # Supported values: sf_001, sf_01, sf_1 
                        # Note: sf_{scalefactor} indicates {scalefactor} * 1GB size datasets.
                    # --filesystem-url: optional arguments to specify filesystem master node URL.
                        # Default: file://
                    # --log-operators: boolean whether or not to log individual physical operators.
                        # Default: false
                        # Note: may reduce performance if set to true (forces caching of
                        # intermediate values).
                    # --operations: select the different operations that should be benchmarked.
                        # Default: all
                        # Available operations: logistic-regression, tpc-h
                        # Syntax: --operations logistic-regression,tpc-h
                    # Leave --operations flag blank to run all benchmarks

Alternatively, you can look at `Benchmark.scala <https://github.com/mc2-project/opaque/blob/master/src/test/scala/edu/berkeley/cs/rise/opaque/benchmark/Benchmark.scala>`_

4. Submit the job to Spark:

   .. code-block:: bash

                spark-submit --class edu.berkeley.cs.rise.opaque.benchmark.Benchmark \
                    <Spark configuration parameters> \
                    ${OPAQUE_HOME}/target/scala-2.12/opaque-assembly-0.1.jar \
                        <flags>

For more help on how to submit jobs to Spark, see `Submitting applications <https://spark.apache.org/docs/3.1.1/submitting-applications.html>`_. For a complete list of values possible in ``<Spark configuration parameters>``, see `Spark properties <https://spark.apache.org/docs/3.1.1/configuration.html>`_

Our TPC-H results
#################

We used a 3 node cluster with 4 cores and 16GB memory per node. 

1. Our ``spark-defaults.conf``:

   .. code-block:: bash

                spark.driver.memory                3g
                spark.executor.memory              11g
                spark.executor.instances           3

                spark.default.parallelism          36
                spark.task.maxFailures             10

2. The command we used to submit the benchmark:

   .. code-block:: bash

        spark-submit --class edu.berkeley.cs.rise.opaque.benchmark.Benchmark 
            --master spark://<master IP>:7077 \
            --deploy-mode client \
            ${OPAQUE_HOME}/target/scala-2.12/opaque-assembly-0.1.jar \
                --filesystem-url hdfs://<master IP>:9000 \
                --size sf_1 \
                --operations tpc-h \

3. Final results:

.. csv-table::  TPC-H Query Results
    :file: ../resources/tpch-results.csv 
    :header-rows: 1
    :class: gridtable
