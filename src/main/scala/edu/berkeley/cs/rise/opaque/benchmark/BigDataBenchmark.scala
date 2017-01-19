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
import edu.berkeley.cs.rise.opaque.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BigDataBenchmark {
  def rankings(
      spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame =
    securityLevel.applyTo(
      spark.read.schema(
        StructType(Seq(
          StructField("pageURL", StringType),
          StructField("pageRank", IntegerType),
          StructField("avgDuration", IntegerType))))
        .csv(s"${Benchmark.dataDir}/bdb/rankings/$size")
        .repartition(numPartitions))

  def uservisits(
      spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame =
    securityLevel.applyTo(
      spark.read.schema(
        StructType(Seq(
          StructField("sourceIP", StringType),
          StructField("destURL", StringType),
          StructField("visitDate", DateType),
          StructField("adRevenue", FloatType),
          StructField("userAgent", StringType),
          StructField("countryCode", StringType),
          StructField("languageCode", StringType),
          StructField("searchWord", StringType),
          StructField("duration", IntegerType))))
        .csv(s"${Benchmark.dataDir}/bdb/uservisits/$size")
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
      //df.explain
      Utils.force(df)
      df
    }
  }

  def q2(spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame = {
    import spark.implicits._
    val uservisitsDF = uservisits(spark, securityLevel, size, numPartitions).cache()
    Utils.time("load uservisits") { Utils.force(uservisitsDF) }
    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "big data 2",
      "system" -> securityLevel.name,
      "size" -> size) {
      val df = uservisitsDF
        .select(substring($"sourceIP", 0, 8).as("sourceIPSubstr"), $"adRevenue")
        .groupBy($"sourceIPSubstr").sum("adRevenue")
      //df.explain
      Utils.force(df)
      df
    }
  }

  def q3(spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame = {
    import spark.implicits._
    val uservisitsDF = uservisits(spark, securityLevel, size, numPartitions).cache()
    Utils.time("load uservisits") { Utils.force(uservisitsDF) }
    val rankingsDF = rankings(spark, securityLevel, size, numPartitions).cache()
    Utils.time("load rankings") { Utils.force(rankingsDF) }
    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "big data 3",
      "system" -> securityLevel.name,
      "size" -> size) {
      val df = rankingsDF
        .join(
          uservisitsDF
            .filter($"visitDate" >= lit("1980-01-01") && $"visitDate" <= lit("1980-04-01"))
            .select($"destURL", $"sourceIP", $"adRevenue"),
          rankingsDF("pageURL") === uservisitsDF("destURL"))
        .select($"sourceIP", $"pageRank", $"adRevenue")
        .groupBy("sourceIP")
        .agg(avg("pageRank").as("avgPageRank"), sum("adRevenue").as("totalRevenue"))
        .select($"sourceIP", $"totalRevenue", $"avgPageRank")
        .orderBy($"totalRevenue".asc)
      //df.explain(true)
      Utils.force(df)
      df
    }
  }
}
