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

package edu.berkeley.cs.rise.opaque.examples

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BigDataBenchmark {
  def rankings(spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame =
    securityLevel.applyTo(
      spark.read.schema(
        StructType(Seq(
          StructField("pageURL", StringType),
          StructField("pageRank", IntegerType),
          StructField("avgDuration", IntegerType))))
        .csv(s"${Benchmark.dataDir}/big-data-benchmark-files/rankings/$size")
        .repartition(numPartitions))

  def q1(spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame = {
    import spark.implicits._
    val rankingsDF = rankings(spark, securityLevel, size, numPartitions).cache()
    Utils.time("load rankings") { Utils.force(rankingsDF) }
    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "big data 1",
      "system" -> securityLevel.name,
      "size" -> size) {
      val df = rankingsDF.filter($"pageRank" > 1000)
      df.explain(true)
      Utils.force(df)
      df
    }
  }
}
