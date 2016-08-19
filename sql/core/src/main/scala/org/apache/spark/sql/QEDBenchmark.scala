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
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.substring
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
    val edges = data.filter($"isVertex" === lit(0))
      .select($"src", $"dst", lit(1.0f).as("weight"))
      .repartition(numPartitions(sqlContext, distributed))
      .mapPartitions(QED.pagerankEncryptEdges)
      .toDF("src", "dst", "weight")
      .cache()
    val vertices = data.filter($"isVertex" === lit(1))
      .select($"src".as("id"), lit(1.0f).as("rank"))
      .repartition(numPartitions(sqlContext, distributed))
      .mapPartitions(QED.pagerankEncryptVertices)
      .toDF("id", "rank")
      .cache()
    val numEdges = edges.count
    val numVertices = vertices.count
    val newV =
      time(s"pagerank $size") {
        val result =
          vertices.encJoin(edges, $"id", $"src", Some(OP_JOIN_PAGERANK))
            .encProject(OP_PROJECT_PAGERANK_WEIGHT_RANK, $"dst", $"rank".as("weightedRank"))
            .encAggregate(OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP1,
              $"dst", $"weightedRank".as("totalIncomingRank"))
            .encProject(OP_PROJECT_PAGERANK_APPLY_INCOMING_RANK, $"dst", $"totalIncomingRank".as("rank"))
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
    rankingsDF.unpersist()
    result
  }

  def bd1Opaque(sqlContext: SQLContext, size: String, distributed: Boolean = false): DataFrame = {
    import sqlContext.implicits._
    val rankingsDF = rankings(sqlContext, size)
      .mapPartitions(QED.bd1Encrypt3)
      .toDF("pageURL", "pageRank", "avgDuration")
      .repartition(numPartitions(sqlContext, distributed))
      .cache()
    rankingsDF.count
    val result = time("big data 1") {
      val df = rankingsDF.select($"pageURL", $"pageRank").encFilter($"pageRank", OP_BD1)
      val count = df.count
      println("big data 1 - num rows: " + count)
      df
    }
    rankingsDF.unpersist()
    result.mapPartitions(QED.bd1Decrypt2).toDF("pageURL", "pageRank")
  }

  def bd1Encrypted(
      sqlContext: SQLContext, size: String, distributed: Boolean = false): DataFrame = {
    import sqlContext.implicits._
    val rankingsDF = rankings(sqlContext, size)
      .mapPartitions(QED.bd1Encrypt3)
      .toDF("pageURL", "pageRank", "avgDuration")
      .repartition(numPartitions(sqlContext, distributed))
      .cache()
    rankingsDF.count
    val result = time("big data 1 encrypted") {
      val df = rankingsDF.select($"pageURL", $"pageRank").nonObliviousFilter($"pageRank", OP_BD1)
      val count = df.count
      println("big data 1 encrypted - num rows: " + count)
      df
    }
    rankingsDF.unpersist()
    result.mapPartitions(QED.bd1Decrypt2).toDF("pageURL", "pageRank")
  }

  def bd2SparkSQL(sqlContext: SQLContext, size: String): DataFrame = {
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
    uservisitsDF.unpersist()
    result
  }

  def bd2Opaque(sqlContext: SQLContext, size: String, distributed: Boolean = false): DataFrame = {
    import sqlContext.implicits._
    val uservisitsDF = uservisits(sqlContext, size)
      .mapPartitions(QED.bd2Encrypt9)
      .toDF("sourceIP", "destURL", "visitDate",
        "adRevenue", "userAgent", "countryCode",
        "languageCode", "searchWord", "duration")
      .repartition(numPartitions(sqlContext, distributed))
      .cache()
    uservisitsDF.count
    val result = time("big data 2") {
      val df = uservisitsDF
        .select($"sourceIP", $"adRevenue")
        .encProject(OP_BD2, $"sourceIP", $"adRevenue")
        .encAggregate(OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP1,
          $"sourceIP", $"adRevenue".as("totalAdRevenue"))
      val count = df.count
      println("big data 2 - num rows: " + count)
      df
    }
    uservisitsDF.unpersist()
    result.mapPartitions(QED.bd2Decrypt2).toDF("sourceIPSubstr", "adRevenue")
  }

  def bd2Encrypted(
      sqlContext: SQLContext, size: String, distributed: Boolean = false): DataFrame = {
    import sqlContext.implicits._
    val uservisitsDF = uservisits(sqlContext, size)
      .mapPartitions(QED.bd2Encrypt9)
      .toDF("sourceIP", "destURL", "visitDate",
        "adRevenue", "userAgent", "countryCode",
        "languageCode", "searchWord", "duration")
      .repartition(numPartitions(sqlContext, distributed))
      .cache()
    uservisitsDF.count
    val result = time("big data 2 encrypted") {
      val df = uservisitsDF
        .select($"sourceIP", $"adRevenue")
        .encProject(OP_BD2, $"sourceIP", $"adRevenue")
        .nonObliviousAggregate(OP_GROUPBY_COL1_SUM_COL2_FLOAT,
          $"sourceIP", $"adRevenue".as("totalAdRevenue"))
      val count = df.count
      println("big data 2 encrypted - num rows: " + count)
      df
    }
    uservisitsDF.unpersist()
    result.mapPartitions(QED.bd2Decrypt2).toDF("sourceIPSubstr", "adRevenue")
  }

  def bd3SparkSQL(sqlContext: SQLContext, size: String): DataFrame = {
    import sqlContext.implicits._
    import org.apache.spark.sql.functions.{lit, sum, avg}
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
    uservisitsDF.unpersist()
    rankingsDF.unpersist()
    result
  }

  def bd3Opaque(sqlContext: SQLContext, size: String, distributed: Boolean = false): DataFrame = {
    import sqlContext.implicits._
    val uservisitsDF = uservisits(sqlContext, size)
      .mapPartitions(QED.bd2Encrypt9)
      .toDF("sourceIP", "destURL", "visitDate",
        "adRevenue", "userAgent", "countryCode",
        "languageCode", "searchWord", "duration")
      .repartition(numPartitions(sqlContext, distributed))
      .cache()
    uservisitsDF.count
    val rankingsDF = rankings(sqlContext, size)
      .mapPartitions(QED.bd1Encrypt3)
      .toDF("pageURL", "pageRank", "avgDuration")
      .repartition(numPartitions(sqlContext, distributed))
      .cache()
    rankingsDF.count

    val result = time("big data 3") {
      val df =
        rankingsDF
          .select($"pageURL", $"pageRank")
          .encJoin(
            uservisitsDF
              .select($"visitDate", $"destURL", $"sourceIP", $"adRevenue")
              .encFilter($"visitDate", OP_FILTER_COL1_DATE_BETWEEN_1980_01_01_AND_1980_04_01)
              .encProject(OP_PROJECT_DROP_COL1, $"destURL", $"sourceIP", $"adRevenue"),
            rankingsDF("pageURL"), uservisitsDF("destURL"))
          .encProject(OP_PROJECT_DROP_COL1, $"pageRank", $"sourceIP", $"adRevenue")
          .encProject(OP_PROJECT_SWAP_COL1_COL2, $"sourceIP", $"pageRank", $"adRevenue")
          .encAggregate(OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP1,
            $"sourceIP", $"pageRank".as("avgPageRank"), $"adRevenue".as("totalRevenue"))
          .encProject(OP_PROJECT_SWAP_COL2_COL3, $"sourceIP", $"totalRevenue", $"avgPageRank")
          .encSort($"totalRevenue")
      val count = df.count
      println("big data 3 - num rows: " + count)
      df
    }
    uservisitsDF.unpersist()
    rankingsDF.unpersist()
    result.mapPartitions(QED.bd3Decrypt3).toDF("sourceIP", "totalRevenue", "avgPageRank")
  }

  def bd3Encrypted(
      sqlContext: SQLContext, size: String, distributed: Boolean = false): DataFrame = {
    import sqlContext.implicits._
    val uservisitsDF = uservisits(sqlContext, size)
      .mapPartitions(QED.bd2Encrypt9)
      .toDF("sourceIP", "destURL", "visitDate",
        "adRevenue", "userAgent", "countryCode",
        "languageCode", "searchWord", "duration")
      .repartition(numPartitions(sqlContext, distributed))
      .cache()
    uservisitsDF.count
    val rankingsDF = rankings(sqlContext, size)
      .mapPartitions(QED.bd1Encrypt3)
      .toDF("pageURL", "pageRank", "avgDuration")
      .repartition(numPartitions(sqlContext, distributed))
      .cache()
    rankingsDF.count

    val result = time("big data 3 encrypted") {
      val df =
        rankingsDF
          .select($"pageURL", $"pageRank")
          .nonObliviousJoin(
            uservisitsDF
              .select($"visitDate", $"destURL", $"sourceIP", $"adRevenue")
              .nonObliviousFilter($"visitDate", OP_FILTER_COL1_DATE_BETWEEN_1980_01_01_AND_1980_04_01)
              .encProject(OP_PROJECT_DROP_COL1, $"destURL", $"sourceIP", $"adRevenue"),
            rankingsDF("pageURL"), uservisitsDF("destURL"))
          .encProject(OP_PROJECT_DROP_COL1, $"pageRank", $"sourceIP", $"adRevenue")
          .encProject(OP_PROJECT_SWAP_COL1_COL2, $"sourceIP", $"pageRank", $"adRevenue")
          .nonObliviousAggregate(OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT,
            $"sourceIP", $"pageRank".as("avgPageRank"), $"adRevenue".as("totalRevenue"))
          .encProject(OP_PROJECT_SWAP_COL2_COL3, $"sourceIP", $"totalRevenue", $"avgPageRank")
          .nonObliviousSort($"totalRevenue")
      val count = df.count
      println("big data 3 encrypted - num rows: " + count)
      df
    }
    uservisitsDF.unpersist()
    rankingsDF.unpersist()
    result.mapPartitions(QED.bd3Decrypt3).toDF("sourceIP", "totalRevenue", "avgPageRank")
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

  def sortSparkSQL(sqlContext: SQLContext, n: Int) {
    import sqlContext.implicits._
    val data = Random.shuffle((0 until n).map(x => (x.toString, x)).toSeq)
    val sorted = time("spark sql sorting") {
      val df = sqlContext.sparkContext.makeRDD(data).toDF("str", "x").sort($"x")
      df.count()
      df
    }
  }

  def sortOpaque(sqlContext: SQLContext, n: Int) {
    import sqlContext.implicits._
    val data = Random.shuffle((0 until n).map(x => (x.toString, x)).toSeq)
    val sorted = time("Enc sorting: ") {
      val df = sqlContext.sparkContext.makeRDD(QED.encrypt2(data)).toDF("str", "x").encSort($"x")
      df.count()
      df
    }
  }
}
