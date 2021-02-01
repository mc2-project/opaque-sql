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
 * Leave --operations flag blank to run all benchmarks
 *
 * To run on a cluster, use `$SPARK_HOME/bin/spark-submit` with appropriate arguments.
 */
object Benchmark {

  val spark = SparkSession.builder()
      .appName("Benchmark")
      .getOrCreate()
  var numPartitions = 2 * spark.sparkContext
      .getConf
      .getInt("spark.executor.instances", 2)
  var size = "sf_small"

  def dataDir: String = {
    if (System.getenv("SPARKSGX_DATA_DIR") == null) {
      throw new Exception("Set SPARKSGX_DATA_DIR")
    }
    System.getenv("SPARKSGX_DATA_DIR")
  }

  def logisticRegression() = {
    // Warmup
    LogisticRegression.train(spark, Encrypted, 1000, 1)
    LogisticRegression.train(spark, Encrypted, 1000, 1)

    // Run
    LogisticRegression.train(spark, Insecure, 100000, 1)
    LogisticRegression.train(spark, Encrypted, 100000, 1)
  }

  def runAll() = {
    logisticRegression()
    TPCHBenchmark.run(spark.sqlContext, numPartitions, size)
  }

  def main(args: Array[String]): Unit = {
    Utils.initSQLContext(spark.sqlContext)

    var runAll = true
    args.sliding(2, 2).toList.collect {
      case Array("--num-partitions", numPartitions: String) => {
        this.numPartitions = numPartitions.toInt
      }
      case Array("--size", size: String) => {
        this.size = size
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
              TPCHBenchmark.run(spark.sqlContext, numPartitions, size)
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
