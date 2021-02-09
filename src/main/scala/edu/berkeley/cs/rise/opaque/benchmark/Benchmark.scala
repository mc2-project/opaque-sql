/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.rise.opaque.benchmark

import edu.berkeley.cs.rise.opaque.Utils
import org.apache.spark.sql.SparkSession

/**
 * Convenient runner for benchmarks.
 *
 * To run locally, use
 * `$OPAQUE_HOME/build/sbt 'run edu.berkeley.cs.rise.opaque.benchmark.Benchmark <flags>'`.
 * Available flags:
 *   --num-partitions: specify the number of partitions the data should be split into.
 *       Default: 2 * number of executors if exists, 4 otherwise
 *   --size: specify the size of the dataset that should be loaded into Spark.
 *       Default: sf_small
 *   --operations: select the different operations that should be benchmarked.
 *       Default: all
 *       Available operations: logistic-regression, tpc-h
 *       Syntax: --operations "logistic-regression,tpc-h"
 *   --run-local: boolean whether to use HDFS or the local filesystem
 *       Default: HDFS
 * Leave --operations flag blank to run all benchmarks
 *
 * To run on a cluster, use `$SPARK_HOME/bin/spark-submit` with appropriate arguments.
 */
object Benchmark {

  val spark = SparkSession.builder()
      .appName("Benchmark")
      .getOrCreate()
  var numPartitions = spark.sparkContext.defaultParallelism
  var size = "sf_med"

  // Configure your HDFS namenode url here
  var fileUrl = "hdfs://10.0.3.4:8020"

  def dataDir: String = {
    if (System.getenv("SPARKSGX_DATA_DIR") == null) {
      throw new Exception("Set SPARKSGX_DATA_DIR")
    }
    System.getenv("SPARKSGX_DATA_DIR")
  }

  def logisticRegression() = {
    // TODO: this fails when Spark is ran on a cluster
    /*
    // Warmup
    LogisticRegression.train(spark, Encrypted, 1000, 1)
    LogisticRegression.train(spark, Encrypted, 1000, 1)

    // Run
    LogisticRegression.train(spark, Insecure, 100000, 1)
    LogisticRegression.train(spark, Encrypted, 100000, 1)
    */
  }

  def runAll() = {
    logisticRegression()
    TPCHBenchmark.run(spark.sqlContext, numPartitions, size, fileUrl)
  }

  def main(args: Array[String]): Unit = {
    Utils.initSQLContext(spark.sqlContext)

    if (args.length >= 2 && args(1) == "--help") {
      println(
"""Available flags:
    --num-partitions: specify the number of partitions the data should be split into.
          Default: 2 * number of executors if exists, 4 otherwise
    --size: specify the size of the dataset that should be loaded into Spark.
          Default: sf_small
    --operations: select the different operations that should be benchmarked.
          Default: all
          Available operations: logistic-regression, tpc-h
          Syntax: --operations "logistic-regression,tpc-h"
          Leave --operations flag blank to run all benchmarks
    --run-local: boolean whether to use HDFS or the local filesystem
          Default: HDFS"""
      )
    }

    var runAll = true
    args.slice(1, args.length).sliding(2, 2).toList.collect {
      case Array("--num-partitions", numPartitions: String) => {
        this.numPartitions = numPartitions.toInt
      }
      case Array("--size", size: String) => {
        val supportedSizes = Set("sf_small, sf_med")
        if (supportedSizes.contains(size)) {
          this.size = size
        } else {
          println("Given size is not supported: available values are " + supportedSizes.toString())
        }
      }
      case Array("--run-local", runLocal: String) => {
        runLocal match {
          case "true" => {
            fileUrl = "file://"
          }
          case _ => {}
        }
      }
      case Array("--operations", operations: String) => {
        runAll = false
        val operationsArr = operations.split(",").map(_.trim)
        for (operation <- operationsArr) {
          operation match {
            case "logistic-regression" => {
              logisticRegression()
            }
            case "tpc-h" => {
              TPCHBenchmark.run(spark.sqlContext, numPartitions, size, fileUrl)
            }
          }
        }
      }
    }
    if (runAll) {
      this.runAll();
    }
    spark.stop()
  }
}
