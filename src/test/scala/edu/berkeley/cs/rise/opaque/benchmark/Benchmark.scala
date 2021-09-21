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

import java.util.Random

import edu.berkeley.cs.rise.opaque.execution.udfs.LogisticRegression

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.{SecurityLevel, Encrypted, Insecure}

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * Convenient runner for benchmarks.
 *
 * To run locally, use
 * `$OPAQUE_HOME/build/sbt 'test:runMain edu.berkeley.cs.rise.opaque.benchmark.Benchmark <flags>'`.
 * Available flags:
 *   --num-partitions: specify the number of partitions the data should be split into.
 *       Default: spark.default.parallelism
 *   --size: specify the size of the dataset that should be loaded into Spark.
 *       Default: sf_001
 *       Available options: sf_01, sf_1, sf_3, sf_5, sf_10
 *   --filesystem-url: optional arguments to specify filesystem master node URL.
 *       Default: file://
 *   --log-operators: boolean whether or not to log individual physical operators.
 *       Default: false
 *       Note: may reduce performance if set to true (forces caching of
 *       intermediate values).
 *   --operations: select the different operations that should be benchmarked.
 *       Default: all
 *       Available operations: logistic-regression, tpc-h
 *       Syntax: --operations "logistic-regression,tpc-h"
 * Leave --operations flag blank to run all benchmarks
 *
 * To run on a cluster, use `$SPARK_HOME/bin/spark-submit` with appropriate arguments.
 */
object Benchmark {

  val spark = SparkSession
    .builder()
    .appName("Benchmark")
    .getOrCreate()

  var numPartitions = spark.sparkContext.defaultParallelism
  var size = "sf_001"
  var fileUrl = "file://"

  def dataDir: String = {
    if (System.getenv("OPAQUE_DATA_DIR") == null) {
      throw new Exception("Set OPAQUE_DATA_DIR")
    }
    System.getenv("OPAQUE_DATA_DIR")
  }

  def logisticRegression() = {
    def generateData(
        spark: SparkSession,
        sl: SecurityLevel,
        N: Int,
        D: Int,
        R: Double
    ): DataFrame = {
      val rand = new Random(42)
      def generatePoint(i: Int): (Array[Double], Double) = {
        val y = if (i % 2 == 0) -1 else 1
        val x = Array.fill(D) { rand.nextGaussian + y * R }
        (x, y)
      }

      val data = Array.tabulate(N)(generatePoint)
      val schema = StructType(
        Seq(StructField("x", DataTypes.createArrayType(DoubleType)), StructField("y", DoubleType))
      )

      sl.applyTo(
        spark.createDataFrame(spark.sparkContext.makeRDD(data.map(Row.fromTuple)), schema)
      )
    }

    // Hyperparameters and dimensions
    val N = 100000
    val D = 5
    val R = 0.7
    val A = 0.1
    val ITERATIONS = 8

    // Insecure
    var trainingData =
      Utils.ensureCached(generateData(spark, Insecure, N, D, R))
    var w = Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "logistic regression - training",
      "system" -> Insecure.name,
      "N" -> N
    ) { LogisticRegression.train(spark, D, A, ITERATIONS, trainingData) }

    var evalData = generateData(spark, Insecure, N, D, R).drop("y")
    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "logistic regression - evaluation",
      "system" -> Insecure.name,
      "N" -> N
    ) {
      LogisticRegression.predict(spark, w, evalData)
    }

    // Encrypted
    trainingData = Utils.ensureCached(generateData(spark, Encrypted, N, D, R))
    w = Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "logistic regression - training",
      "system" -> Encrypted.name,
      "N" -> N
    ) { LogisticRegression.train(spark, D, A, ITERATIONS, trainingData) }

    evalData = generateData(spark, Encrypted, N, D, R).drop("y")
    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "logistic regression - evaluation",
      "system" -> Encrypted.name,
      "N" -> N
    ) {
      LogisticRegression.predict(spark, w, evalData)
    }
  }

  def runAll() = {
    println("Running all supported benchmarks.")
    logisticRegression()
    TPCHBenchmark.run(spark.sqlContext, numPartitions, size, fileUrl)
  }

  def main(args: Array[String]): Unit = {
    Utils.initOpaqueSQL(spark, testing = true)

    if (args.length >= 1 && args(0) == "--help") {
      println("""
    Available flags:
    --num-partitions: specify the number of partitions the data should be split into.
          Default: spark.default.parallelism
    --size: specify the size of the dataset that should be loaded into Spark.
          Default: sf_001
          Available options: sf_01, sf_1, sf_3, sf_5, sf_10
    --filesystem-url: optional arguments to specify filesystem master node URL.
          Default: file://
    --log-operators: boolean whether or not to log individual physical operators.
          Default: false
          Note: may reduce performance if set to true (forces caching of
            intermediate values).
    --operations: select the different operations that should be benchmarked.
          Default: all
          Available operations: logistic-regression, tpc-h
          Syntax: --operations logistic-regression,tpc-h
    Leave --operations flag blank to run all benchmarks
      """)
      return
    }

    var benchmarks = Seq[() => Any]()
    args.sliding(2, 2).toList.collect {
      case Array("--num-partitions", numPartitions: String) => {
        this.numPartitions = numPartitions.toInt
      }
      case Array("--size", size: String) => {
        if (
          size == "sf_001" || size == "sf_01" || size == "sf_1" || size == "sf_3" || size == "sf_5" || size == "sf_10"
        ) {
          this.size = size
        } else {
          println(s"Given size is not supported: $size")
        }
      }
      case Array("--filesystem-url", url: String) => {
        fileUrl = url
      }
      case Array("--log-operators", bool: String) => {
        Utils.setOperatorLoggingLevel(bool.toBoolean)
      }
      case Array("--operations", operations: String) => {
        val operationsArr = operations.split(",").map(_.trim)
        for (operation <- operationsArr) {
          operation match {
            case "logistic-regression" => {
              benchmarks = benchmarks :+ { () => logisticRegression() }
            }
            case "tpc-h" => {
              benchmarks = benchmarks :+ { () =>
                TPCHBenchmark.run(spark.sqlContext, numPartitions, size, fileUrl)
              }
            }
          }
        }
      }
    }
    if (benchmarks.isEmpty) {
      this.runAll();
    } else {
      benchmarks.foreach(f => f())
    }

    Utils.cleanup(spark)
  }
}
