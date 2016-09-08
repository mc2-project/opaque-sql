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
import org.apache.spark.sql.functions.min
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
  def time[A](desc: String)(f: => A): A = {
    val start = System.nanoTime
    val result = f
    println(s"$desc: ${(System.nanoTime - start) / 1000000.0} ms")
    result
  }

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

    QEDBenchmark.tpch9SparkSQL(sqlContext, "sf0.2", None)
    QEDBenchmark.tpch9Generic(sqlContext, "sf0.2", None)
    QEDBenchmark.tpch9Opaque(sqlContext, "sf0.2", None)

    QEDBenchmark.diseaseQuery(sqlContext, "500")
    QEDBenchmark.diseaseQuery(sqlContext, "500")

    for (i <- 1 to 8) {
      QEDBenchmark.diseaseQuery(sqlContext, (math.pow(2, i) * 500).toInt.toString)
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
        StructField("pageRank", IntegerType)
        //StructField("avgDuration", IntegerType)
      )
      )
    )
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

  /** TPC-H query 9 - Product Type Profit Measure Query - generic join order */
  def tpch9SparkSQL(
      sqlContext: SQLContext, size: String, quantityThreshold: Option[Int])
    : Seq[(String, Int, Float)] = {
    import sqlContext.implicits._
    val partDF = part(sqlContext, size)
    val supplierDF = supplier(sqlContext, size)
    val lineitemDF = lineitem(sqlContext, size)
    val partsuppDF = partsupp(sqlContext, size)
    val ordersDF = orders(sqlContext, size)
    val nationDF = nation(sqlContext, size)

    val result = time("TPC-H Query 9 - Spark SQL") {
      val df =
        nationDF // 6. nation
          .join(
            supplierDF // 5. supplier
              .join(
                ordersDF.select($"o_orderkey", year($"o_orderdate").as("o_year")) // 4. orders
                  .join(
                    partsuppDF.join( // 3. partsupp
                      partDF // 1. part
                        .filter($"p_name".contains("maroon"))
                        .join(
                          // 2. lineitem
                          quantityThreshold match {
                            case Some(q) => lineitemDF.filter($"l_quantity" > lit(q))
                            case None => lineitemDF
                          },
                          $"p_partkey" === $"l_partkey"),
                      $"ps_suppkey" === $"l_suppkey" && $"ps_partkey" === $"p_partkey"),
                    $"l_orderkey" === $"o_orderkey"),
                $"ps_suppkey" === $"s_suppkey"),
            $"s_nationkey" === $"n_nationkey")
          .select(
            $"n_name",
            $"o_year",
            ($"l_extendedprice" * (lit(1) - $"l_discount") - $"ps_supplycost" * $"l_quantity")
              .as("amount"))
          .groupBy("n_name", "o_year").agg(sum($"amount").as("sum_profit"))
      val count = df.count
      println("TPC-H Query 9 - Spark SQL - num rows: " + count)
      df
    }
    result.collect.map { case Row(a: String, b: Int, c: Double) => (a, b, c.toFloat) }
  }

  /** TPC-H query 9 - Product Type Profit Measure Query - generic join order */
  def tpch9Generic(
      sqlContext: SQLContext, size: String, quantityThreshold: Option[Int],
      distributed: Boolean = false)
    : Seq[(String, Int, Float)] = {
    import sqlContext.implicits._
    val (partDF, supplierDF, lineitemDF, partsuppDF, ordersDF, nationDF) =
      tpch9EncryptedDFs(sqlContext, size, distributed)
    val result = time("TPC-H Query 9 - generic join order") {
      val df =
        nationDF // 6. nation
          .encJoin(
            supplierDF // 5. supplier
              .encJoin(
                ordersDF.encSelect($"o_orderkey", year($"o_orderdate").as("o_year")) // 4. orders
                  .encJoin(
                    partsuppDF.encJoin( // 3. partsupp
                      partDF // 1. part
                        .nonObliviousFilter($"p_name".contains("maroon"))
                        .encSelect($"p_partkey")
                        .encJoin(
                          // 2. lineitem
                          quantityThreshold match {
                            case Some(q) => lineitemDF.nonObliviousFilter($"l_quantity" > lit(q))
                            case None => lineitemDF
                          },
                          $"p_partkey" === $"l_partkey"),
                      $"ps_suppkey" === $"l_suppkey" && $"ps_partkey" === $"p_partkey"),
                    $"l_orderkey" === $"o_orderkey"),
                $"ps_suppkey" === $"s_suppkey"),
            $"s_nationkey" === $"n_nationkey")
          .encSelect(
            $"n_name",
            $"o_year",
            ($"l_extendedprice" * (lit(1) - $"l_discount") - $"ps_supplycost" * $"l_quantity")
              .as("amount"))
          .groupBy("n_name", "o_year").encAgg(sum($"amount").as("sum_profit"))
      QED.decrypt3[String, Int, Float](df.encCollect)
    }
    println("TPC-H Query 9 - generic join order - num rows: " + result.size)
    result
  }

  /** TPC-H query 9 - Product Type Profit Measure Query - Opaque join order */
  def tpch9Opaque(
      sqlContext: SQLContext, size: String, quantityThreshold: Option[Int],
      distributed: Boolean = false)
    : Seq[(String, Int, Float)] = {
    import sqlContext.implicits._
    val (partDF, supplierDF, lineitemDF, partsuppDF, ordersDF, nationDF) =
      tpch9EncryptedDFs(sqlContext, size, distributed)
    val result = time("TPC-H Query 9 - opaque join order") {
      val df =
        ordersDF.encSelect($"o_orderkey", year($"o_orderdate").as("o_year")) // 6. orders
          .encJoin(
            (nationDF // 4. nation
              .nonObliviousJoin(
                supplierDF // 3. supplier
                  .nonObliviousJoin(
                    partDF // 1. part
                      .nonObliviousFilter($"p_name".contains("maroon"))
                      .encSelect($"p_partkey")
                      .nonObliviousJoin(partsuppDF, $"p_partkey" === $"ps_partkey"), // 2. partsupp
                    $"ps_suppkey" === $"s_suppkey"),
                $"s_nationkey" === $"n_nationkey"))
              .encJoin(
                // 5. lineitem
                quantityThreshold match {
                  case Some(q) => lineitemDF.nonObliviousFilter($"l_quantity" > lit(q))
                  case None => lineitemDF
                },
                $"s_suppkey" === $"l_suppkey" && $"p_partkey" === $"l_partkey"),
            $"l_orderkey" === $"o_orderkey")
          .encSelect(
            $"n_name",
            $"o_year",
            ($"l_extendedprice" * (lit(1) - $"l_discount") - $"ps_supplycost" * $"l_quantity")
              .as("amount"))
          .groupBy("n_name", "o_year").encAgg(sum($"amount").as("sum_profit"))
      QED.decrypt3[String, Int, Float](df.encCollect)
    }
    println("TPC-H Query 9 - opaque join order - num rows: " + result.size)
    result
  }

  def diseaseQuery(sqlContext: SQLContext, size: String, distributed: Boolean = false): Unit = {
    import sqlContext.implicits._
    val diseaseSchema = StructType(Seq(
      StructField("d_disease_id", StringType),
      StructField("d_name", StringType)))
    val diseaseDF = sqlContext.createEncryptedDataFrame(
      // sqlContext.createDataFrame(Seq(("d1", "disease 1"), ("d2", "disease 2")))
      sqlContext.read.schema(diseaseSchema)
        .format("csv")
        .option("delimiter", "|")
        .load(s"$dataDir/disease/icd_codes.tsv")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.diseaseQueryEncryptDisease),
      diseaseSchema)

    val patientSchema = StructType(Seq(
      StructField("p_id", IntegerType),
      StructField("p_disease_id", StringType),
      StructField("p_name", StringType)))
    val patientDF = sqlContext.createEncryptedDataFrame(
      // sqlContext.createDataFrame(Seq((1, "d1", "patient 1"), (2, "d2", "patient 2")))
      sqlContext.read.schema(patientSchema)
        .format("csv")
        .load(s"$dataDir/disease/patient-$size.csv")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.diseaseQueryEncryptPatient),
      patientSchema)

    val treatmentSchema = StructType(Seq(
      StructField("t_id", IntegerType),
      StructField("t_disease_id", StringType),
      StructField("t_name", StringType),
      StructField("t_cost", IntegerType)))
    val groupedTreatmentSchema = StructType(Seq(
      StructField("t_disease_id", StringType),
      StructField("t_min_cost", IntegerType)))
    val treatmentDF = sqlContext.createEncryptedDataFrame(
      // sqlContext.createDataFrame(Seq((3, "d1", "treatment 1", 100))).toDF("t_id", "t_disease_id", "t_name", "t_cost")
      sqlContext.read.schema(treatmentSchema)
        .format("csv")
        .load(s"$dataDir/disease/treatment.csv")
        .groupBy($"t_disease_id").agg(min("t_cost").as("t_min_cost"))
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.diseaseQueryEncryptTreatment),
      groupedTreatmentSchema)

    time(s"Disease Query $size - default join order") {
      treatmentDF.encJoin(
        diseaseDF.encJoin(
          patientDF,
          $"d_disease_id" === $"p_disease_id"),
        $"d_disease_id" === $"t_disease_id")
        .encCollect
    }

    time(s"Disease Query $size - Opaque join order") {
      diseaseDF
        .nonObliviousJoin(treatmentDF, $"d_disease_id" === $"t_disease_id")
        .encJoin(patientDF, $"d_disease_id" === $"p_disease_id")
        .encCollect
    }
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

  def part(sqlContext: SQLContext, size: String): DataFrame =
    sqlContext.read.schema(
      StructType(Seq(
        StructField("p_partkey", IntegerType),
        StructField("p_name", StringType),
        StructField("p_mfgr", StringType),
        StructField("p_brand", StringType),
        StructField("p_type", StringType),
        StructField("p_size", IntegerType),
        StructField("p_container", StringType),
        StructField("p_retailprice", FloatType),
        StructField("p_comment", StringType))))
      .format("csv")
      .option("delimiter", "|")
      .load(s"$dataDir/tpch/$size/part.tbl")

  def supplier(sqlContext: SQLContext, size: String): DataFrame =
    sqlContext.read.schema(
      StructType(Seq(
        StructField("s_suppkey", IntegerType),
        StructField("s_name", StringType),
        StructField("s_address", StringType),
        StructField("s_nationkey", IntegerType),
        StructField("s_phone", StringType),
        StructField("s_acctbal", FloatType),
        StructField("s_comment", StringType))))
      .format("csv")
      .option("delimiter", "|")
      .load(s"$dataDir/tpch/$size/supplier.tbl")

  def lineitem(sqlContext: SQLContext, size: String): DataFrame =
    sqlContext.read.schema(
      StructType(Seq(
        StructField("l_orderkey", IntegerType),
        StructField("l_partkey", IntegerType),
        StructField("l_suppkey", IntegerType),
        StructField("l_linenumber", IntegerType),
        StructField("l_quantity", IntegerType),
        StructField("l_extendedprice", FloatType),
        StructField("l_discount", FloatType),
        StructField("l_tax", FloatType),
        StructField("l_returnflag", StringType),
        StructField("l_linestatus", StringType),
        StructField("l_shipdate", DateType),
        StructField("l_commitdate", DateType),
        StructField("l_receiptdate", DateType),
        StructField("l_shipinstruct", StringType),
        StructField("l_shipmode", StringType),
        StructField("l_comment", StringType))))
      .format("csv")
      .option("delimiter", "|")
      .load(s"$dataDir/tpch/$size/lineitem.tbl")

  def partsupp(sqlContext: SQLContext, size: String): DataFrame =
    sqlContext.read.schema(
      StructType(Seq(
        StructField("ps_partkey", IntegerType),
        StructField("ps_suppkey", IntegerType),
        StructField("ps_availqty", IntegerType),
        StructField("ps_supplycost", FloatType),
        StructField("ps_comment", StringType))))
      .format("csv")
      .option("delimiter", "|")
      .load(s"$dataDir/tpch/$size/partsupp.tbl")

  def orders(sqlContext: SQLContext, size: String): DataFrame =
    sqlContext.read.schema(
      StructType(Seq(
        StructField("o_orderkey", IntegerType),
        StructField("o_custkey", IntegerType),
        StructField("o_orderstatus", StringType),
        StructField("o_totalprice", FloatType),
        StructField("o_orderdate", DateType),
        StructField("o_orderpriority", StringType),
        StructField("o_clerk", StringType),
        StructField("o_shippriority", IntegerType),
        StructField("o_comment", StringType))))
      .format("csv")
      .option("delimiter", "|")
      .load(s"$dataDir/tpch/$size/orders.tbl")

  def nation(sqlContext: SQLContext, size: String): DataFrame =
    sqlContext.read.schema(
      StructType(Seq(
        StructField("n_nationkey", IntegerType),
        StructField("n_name", StringType),
        StructField("n_regionkey", IntegerType),
        StructField("n_comment", StringType))))
      .format("csv")
      .option("delimiter", "|")
      .load(s"$dataDir/tpch/$size/nation.tbl")

  private def tpch9EncryptedDFs(sqlContext: SQLContext, size: String, distributed: Boolean)
      : (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    import sqlContext.implicits._
    val partDF = sqlContext.createEncryptedDataFrame(
      part(sqlContext, size)
        .select($"p_partkey", $"p_name")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.tpch9EncryptPart),
      StructType(Seq(
        StructField("p_partkey", IntegerType),
        StructField("p_name", StringType))))
    val supplierDF = sqlContext.createEncryptedDataFrame(
      supplier(sqlContext, size)
        .select($"s_suppkey", $"s_nationkey")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.tpch9EncryptSupplier),
      StructType(Seq(
        StructField("s_suppkey", IntegerType),
        StructField("s_nationkey", IntegerType))))
    val lineitemDF = sqlContext.createEncryptedDataFrame(
      lineitem(sqlContext, size)
        .select(
          $"l_orderkey", $"l_partkey", $"l_suppkey", $"l_quantity", $"l_extendedprice",
          $"l_discount")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.tpch9EncryptLineitem),
      StructType(Seq(
        StructField("l_orderkey", IntegerType),
        StructField("l_partkey", IntegerType),
        StructField("l_suppkey", IntegerType),
        StructField("l_quantity", IntegerType),
        StructField("l_extendedprice", FloatType),
        StructField("l_discount", FloatType))))
    val partsuppDF = sqlContext.createEncryptedDataFrame(
      partsupp(sqlContext, size)
        .select($"ps_partkey", $"ps_suppkey", $"ps_supplycost")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.tpch9EncryptPartsupp),
      StructType(Seq(
        StructField("ps_partkey", IntegerType),
        StructField("ps_suppkey", IntegerType),
        StructField("ps_supplycost", FloatType))))
    val ordersDF = sqlContext.createEncryptedDataFrame(
      orders(sqlContext, size)
        .select($"o_orderkey", $"o_orderdate")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.tpch9EncryptOrders),
      StructType(Seq(
        StructField("o_orderkey", IntegerType),
        StructField("o_orderdate", DateType))))
    val nationDF = sqlContext.createEncryptedDataFrame(
      nation(sqlContext, size)
        .select($"n_nationkey", $"n_name")
        .repartition(numPartitions(sqlContext, distributed))
        .rdd
        .mapPartitions(QED.tpch9EncryptNation),
      StructType(Seq(
        StructField("n_nationkey", IntegerType),
        StructField("n_name", StringType))))
    (partDF, supplierDF, lineitemDF, partsuppDF, ordersDF, nationDF)
  }
}
