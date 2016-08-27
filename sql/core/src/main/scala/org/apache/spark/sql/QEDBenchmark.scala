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

package org.apache.spark.sql

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.QEDOpcode._
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object QEDBenchmark {
  import QED.time

  def dataDir: String = System.getenv("SPARKSGX_DATA_DIR")

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("QEDBenchmark")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val distributed = !sc.isLocal

    // Warmup
    QEDBenchmark.bd2Encrypted(sqlContext, "tiny", distributed)
    QEDBenchmark.bd2Encrypted(sqlContext, "tiny", distributed)

    // Run
    QEDBenchmark.bd1SparkSQL(sqlContext, "1million")

    QEDBenchmark.bd1Opaque(sqlContext, "1million", distributed)

    QEDBenchmark.bd1Encrypted(sqlContext, "1million", distributed)

    QEDBenchmark.bd2SparkSQL(sqlContext, "1million")

    QEDBenchmark.bd2Opaque(sqlContext, "1million", distributed)

    QEDBenchmark.bd2Encrypted(sqlContext, "1million", distributed)

    QEDBenchmark.bd3SparkSQL(sqlContext, "1million")

    QEDBenchmark.bd3Opaque(sqlContext, "1million", distributed)

    QEDBenchmark.bd3Encrypted(sqlContext, "1million", distributed)

    for (i <- 8 to 20) {
      QEDBenchmark.pagerank(sqlContext, math.pow(2, i).toInt.toString, distributed)
    }

    sc.stop()
  }

  def pagerank(sqlContext: SQLContext, size: String, distributed: Boolean = false): DataFrame = {
    import sqlContext.implicits._
    val data = sqlContext.read
      .schema(
        StructType(Seq(
          StructField("src", IntegerType, false),
          StructField("dst", IntegerType, false),
          StructField("isVertex", IntegerType, false))))
      .option("delimiter", " ")
      .csv(s"$dataDir/pagerank-files/PageRank$size.in")
    val edges = sqlContext.createEncryptedDataFrame(
      data.filter($"isVertex" === lit(0))
        .select($"src", $"dst", lit(1.0f).as("weight"))
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.pagerankEncryptEdges),
        StructType(Seq(
          StructField("src", IntegerType),
          StructField("dst", IntegerType),
          StructField("weight", FloatType))))
    val vertices = sqlContext.createEncryptedDataFrame(
      data.filter($"isVertex" === lit(1))
        .select($"src".as("id"), lit(1.0f).as("rank"))
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.pagerankEncryptVertices),
        StructType(Seq(
          StructField("id", IntegerType),
          StructField("rank", FloatType))))
    val numEdges = edges.count
    val numVertices = vertices.count
    val newV =
      time(s"pagerank $size") {
        val result =
          vertices.encJoin(edges, $"id" === $"src")
            .encSelect($"dst", ($"rank" * $"weight").as("weightedRank"))
            .groupBy("dst").encAgg(sum("weightedRank").as("totalIncomingRank"))
            .encSelect($"dst", (lit(0.15) + lit(0.85) * $"totalIncomingRank").as("rank"))
        result.count
        result
      }
    newV
  }

  def bd1SparkSQL(sqlContext: SQLContext, size: String): DataFrame = {
    import sqlContext.implicits._
    val rankingsDF = rankings(sqlContext, size).cache()
    rankingsDF.count
    val result = time("big data 1 - spark sql") {
      val df = rankingsDF.filter($"pageRank" > 1000).select($"pageURL", $"pageRank")
      val count = df.count
      println("big data 1 spark sql - num rows: " + count)
      df
    }
    result
  }

  def bd1Opaque(sqlContext: SQLContext, size: String, distributed: Boolean = false): DataFrame = {
    import sqlContext.implicits._
    val rankingsDF = sqlContext.createEncryptedDataFrame(
      rankings(sqlContext, size)
        .select($"pageURL", $"pageRank")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.bd1Encrypt2),
      StructType(Seq(
        StructField("pageURL", StringType),
        StructField("pageRank", IntegerType),
        StructField("avgDuration", IntegerType))))
    rankingsDF.count
    val result = time("big data 1") {
      val df = rankingsDF.encFilter($"pageRank" > 1000)
      val count = df.count
      println("big data 1 - num rows: " + count)
      df
    }
    result.mapPartitions(QED.bd1Decrypt2).toDF("pageURL", "pageRank")
  }

  def bd1Encrypted(
      sqlContext: SQLContext, size: String, distributed: Boolean = false): DataFrame = {
    import sqlContext.implicits._
    val rankingsDF = sqlContext.createEncryptedDataFrame(
      rankings(sqlContext, size)
        .select($"pageURL", $"pageRank")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.bd1Encrypt2),
      StructType(Seq(
        StructField("pageURL", StringType),
        StructField("pageRank", IntegerType))))
    rankingsDF.count
    val result = time("big data 1 encrypted") {
      val df = rankingsDF.nonObliviousFilter($"pageRank" > 1000)
      val count = df.count
      println("big data 1 encrypted - num rows: " + count)
      df
    }
    result.mapPartitions(QED.bd1Decrypt2).toDF("pageURL", "pageRank")
  }

  def bd2SparkSQL(sqlContext: SQLContext, size: String): Seq[(String, Float)] = {
    import sqlContext.implicits._
    val uservisitsDF = uservisits(sqlContext, size).cache()
    uservisitsDF.count
    val result = time("big data 2 - spark sql") {
      val df = uservisitsDF.select(substring($"sourceIP", 0, 8).as("sourceIPSubstr"), $"adRevenue")
        .groupBy($"sourceIPSubstr").sum("adRevenue")
      val count = df.count
      println("big data 2 spark sql - num rows: " + count)
      df
    }
    result.collect.map { case Row(a: String, b: Double) => (a, b.toFloat) }
  }

  def bd2Opaque(sqlContext: SQLContext, size: String, distributed: Boolean = false)
    : Seq[(String, Float)] = {
    import sqlContext.implicits._
    val uservisitsDF = sqlContext.createEncryptedDataFrame(
      uservisits(sqlContext, size)
        .select($"sourceIP", $"adRevenue")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.bd2Encrypt2),
      StructType(Seq(
        StructField("sourceIP", StringType),
        StructField("adRevenue", FloatType))))
    uservisitsDF.count
    val result = time("big data 2") {
      val df = uservisitsDF
        .encSelect(substring($"sourceIP", 0, 8).as("sourceIP"), $"adRevenue")
        .groupBy("sourceIP").encAgg(sum("adRevenue").as("totalAdRevenue"))
      val count = df.count
      println("big data 2 - num rows: " + count)
      df
    }
    QED.decrypt2[String, Float](result.encCollect)
  }

  def bd2Encrypted(
      sqlContext: SQLContext, size: String, distributed: Boolean = false)
    : Seq[(String, Float)] = {
    import sqlContext.implicits._
    val uservisitsDF = sqlContext.createEncryptedDataFrame(
      uservisits(sqlContext, size)
        .select($"sourceIP", $"adRevenue")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.bd2Encrypt2),
      StructType(Seq(
        StructField("sourceIP", StringType),
        StructField("adRevenue", FloatType))))
    uservisitsDF.count
    val result = time("big data 2 encrypted") {
      val df = uservisitsDF
        .encSelect(substring($"sourceIP", 0, 8).as("sourceIP"), $"adRevenue")
        .groupBy("sourceIP").nonObliviousAgg(sum("adRevenue").as("totalAdRevenue"))
      val count = df.count
      println("big data 2 encrypted - num rows: " + count)
      df
    }
    QED.decrypt2[String, Float](result.encCollect)
  }

  def bd3SparkSQL(sqlContext: SQLContext, size: String): Seq[(String, Float, Float)] = {
    import sqlContext.implicits._
    val uservisitsDF = uservisits(sqlContext, size).cache()
    uservisitsDF.count
    val rankingsDF = rankings(sqlContext, size).cache()
    rankingsDF.count
    val result = time("big data 3 - spark sql") {
      val df = uservisitsDF.filter($"visitDate" >= lit("1980-01-01"))
        .filter($"visitDate" <= lit("1980-04-01"))
        .select($"destURL", $"sourceIP", $"adRevenue")
        .join(rankingsDF.select($"pageURL", $"pageRank"), rankingsDF("pageURL") === uservisitsDF("destURL"))
        .select($"sourceIP", $"pageRank", $"adRevenue")
        .groupBy($"sourceIP")
        .agg(avg("pageRank").as("avgPageRank"), sum("adRevenue").as("totalRevenue"))
        .select($"sourceIP", $"totalRevenue", $"avgPageRank")
        .orderBy($"totalRevenue".asc)
      val count = df.count
      println("big data 3 spark sql - num rows: " + count)
      df
    }
    result.collect.map { case Row(a: String, b: Double, c: Double) => (a, b.toFloat, c.toFloat) }
  }

  def bd3Opaque(sqlContext: SQLContext, size: String, distributed: Boolean = false)
    : Seq[(String, Float, Float)] = {
    import sqlContext.implicits._
    val uservisitsDF = sqlContext.createEncryptedDataFrame(
      uservisits(sqlContext, size)
        .select($"visitDate", $"destURL", $"sourceIP", $"adRevenue")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.bd3EncryptUV),
      StructType(Seq(
        StructField("visitDate", DateType),
        StructField("destURL", StringType),
        StructField("sourceIP", StringType),
        StructField("adRevenue", FloatType))))
    uservisitsDF.count
    val rankingsDF = sqlContext.createEncryptedDataFrame(
      rankings(sqlContext, size)
        .select($"pageURL", $"pageRank")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.bd1Encrypt2),
      StructType(Seq(
        StructField("pageURL", StringType),
        StructField("pageRank", IntegerType))))
    rankingsDF.count

    val result = time("big data 3") {
      val df =
        rankingsDF
          .encJoin(
            uservisitsDF
              .encFilter($"visitDate" >= lit("1980-01-01") && $"visitDate" <= lit("1980-04-01"))
              .encSelect($"destURL", $"sourceIP", $"adRevenue"),
            rankingsDF("pageURL") === uservisitsDF("destURL"))
          .encSelect($"pageRank", $"sourceIP", $"adRevenue")
          .encSelect($"sourceIP", $"pageRank", $"adRevenue")
          .groupBy("sourceIP")
          .encAgg(avg("pageRank").as("avgPageRank"), sum("adRevenue").as("totalRevenue"))
          .encSelect($"sourceIP", $"totalRevenue", $"avgPageRank")
          .encSort($"totalRevenue")
      val count = df.count
      println("big data 3 - num rows: " + count)
      df
    }
    QED.decrypt3[String, Float, Float](result.encCollect)
  }

  def bd3Encrypted(sqlContext: SQLContext, size: String, distributed: Boolean = false)
    : Seq[(String, Float, Float)] = {
    import sqlContext.implicits._
    val uservisitsDF = sqlContext.createEncryptedDataFrame(
      uservisits(sqlContext, size)
        .select($"visitDate", $"destURL", $"sourceIP", $"adRevenue")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.bd3EncryptUV),
      StructType(Seq(
        StructField("visitDate", DateType),
        StructField("destURL", StringType),
        StructField("sourceIP", StringType),
        StructField("adRevenue", FloatType))))
    uservisitsDF.count
    val rankingsDF = sqlContext.createEncryptedDataFrame(
      rankings(sqlContext, size)
        .select($"pageURL", $"pageRank")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.bd1Encrypt2),
      StructType(Seq(
        StructField("pageURL", StringType),
        StructField("pageRank", IntegerType))))
    rankingsDF.count

    val result = time("big data 3") {
      val df =
        rankingsDF
          .nonObliviousJoin(
            uservisitsDF
              .nonObliviousFilter($"visitDate" >= lit("1980-01-01") && $"visitDate" <= lit("1980-04-01"))
              .encSelect($"destURL", $"sourceIP", $"adRevenue"),
            rankingsDF("pageURL") === uservisitsDF("destURL"))
          .encSelect($"pageRank", $"sourceIP", $"adRevenue")
          .encSelect($"sourceIP", $"pageRank", $"adRevenue")
          .groupBy("sourceIP")
          .nonObliviousAgg(avg("pageRank").as("avgPageRank"), sum("adRevenue").as("totalRevenue"))
          .encSelect($"sourceIP", $"totalRevenue", $"avgPageRank")
          .nonObliviousSort($"totalRevenue")
      val count = df.count
      println("big data 3 encrypted - num rows: " + count)
      df
    }
    QED.decrypt3[String, Float, Float](result.encCollect)
  }

  def numPartitions(sqlContext: SQLContext, distributed: Boolean): Int =
    if (distributed) sqlContext.sparkContext.defaultParallelism else 1

  def rankings(sqlContext: SQLContext, size: String): DataFrame =
    sqlContext.read.schema(
      StructType(Seq(
        StructField("pageURL", StringType),
        StructField("pageRank", IntegerType),
        StructField("avgDuration", IntegerType))))
      .csv(s"$dataDir/big-data-benchmark-files/rankings/$size")

  def uservisits(sqlContext: SQLContext, size: String): DataFrame =
    sqlContext.read.schema(
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
      .csv(s"$dataDir/big-data-benchmark-files/uservisits/$size")
}
