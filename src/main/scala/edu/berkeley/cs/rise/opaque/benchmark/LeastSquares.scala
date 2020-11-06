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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object LeastSquares {

  def data(
    spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame =
    securityLevel.applyTo(
      spark.read.schema(
        StructType(Seq(
          StructField("x1", FloatType),
          StructField("x2", FloatType),
          StructField("y", FloatType))))
        .csv(s"${Benchmark.dataDir}/least_squares/$size")
        .repartition(numPartitions))


  def query(spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame = {
    import spark.implicits._
    val dataDF = Utils.ensureCached(data(spark, securityLevel, size, numPartitions))
    Utils.time("Load least squares data") { Utils.force(dataDF) }
    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "least squares",
      "system" -> securityLevel.name,
      "size" -> size) {

      val df = dataDF.select(
        ($"x1" * $"x1").as("c11"),
        ($"x1" * $"x2"). as("c12"),
        ($"x2" * $"x2").as("c22"),
        ($"x1" * $"y").as("b1"),
        ($"x2" * $"y"). as("b2"))
        .agg(
          sum("c11").as("c11sum"),
          sum("c12").as("c12sum"),
          sum("c22").as("c22sum"),
          sum("b1").as("b1sum"),
          sum("b2").as("b2sum"))

      // Utils.force(df)

      df
    }
  }

}
