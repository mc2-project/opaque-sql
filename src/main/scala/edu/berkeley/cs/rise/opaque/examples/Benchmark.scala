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
import org.apache.spark.sql.SparkSession

object Benchmark {
  def dataDir: String = {
    if (System.getenv("SPARKSGX_DATA_DIR") == null) {
      throw new Exception("Set SPARKSGX_DATA_DIR")
    }
    System.getenv("SPARKSGX_DATA_DIR")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("QEDBenchmark")
      .getOrCreate()
    Utils.initSQLContext(spark.sqlContext)

    val numPartitions =
      if (spark.sparkContext.isLocal) 1 else spark.sparkContext.defaultParallelism

    // Warmup
    BigDataBenchmark.q2(spark, Encrypted, "tiny", numPartitions)
    BigDataBenchmark.q2(spark, Encrypted, "tiny", numPartitions)

    // Run
    BigDataBenchmark.q1(spark, Insecure, "1million", numPartitions)
    BigDataBenchmark.q1(spark, Encrypted, "1million", numPartitions)
    BigDataBenchmark.q1(spark, Oblivious, "1million", numPartitions)

    BigDataBenchmark.q2(spark, Insecure, "1million", numPartitions)
    BigDataBenchmark.q2(spark, Encrypted, "1million", numPartitions)
    BigDataBenchmark.q2(spark, Oblivious, "1million", numPartitions)

    BigDataBenchmark.q3(spark, Insecure, "1million", numPartitions)
    BigDataBenchmark.q3(spark, Encrypted, "1million", numPartitions)
    BigDataBenchmark.q3(spark, Oblivious, "1million", numPartitions)

    if (spark.sparkContext.isLocal) {
      for (i <- 8 to 20) {
        PageRank.run(spark, Oblivious, math.pow(2, i).toInt.toString, numPartitions)
      }

      for (i <- 0 to 13) {
        JoinReordering.treatmentQuery(spark, (math.pow(2, i) * 125).toInt.toString, numPartitions)
        JoinReordering.geneQuery(spark, (math.pow(2, i) * 125).toInt.toString, numPartitions)
      }

      // for (i <- 0 to 13) {
      //   JoinCost.run(spark, (math.pow(2, i) * 125).toInt.toString, numPartitions)
      // }
    }

    spark.stop()
  }

//   def joinCost(
//       sqlContext: SQLContext, size: String, distributed: Boolean = false,
//       onlyOblivious: Boolean = false): Unit = {
//     import sqlContext.implicits._
//     val diseaseSchema = StructType(Seq(
//       StructField("d_disease_id", StringType),
//       StructField("d_gene_id", IntegerType),
//       StructField("d_name", StringType)))
//     val diseaseDF = sqlContext.createEncryptedDataFrame(
//       sqlContext.read.schema(diseaseSchema)
//         .format("csv")
//         .load(s"$dataDir/disease/disease.csv")
//         .repartition(numPartitions(sqlContext, distributed))
//         .rdd
//         .mapPartitions(Utils.diseaseQueryEncryptDisease),
//       diseaseSchema)
//     time("load disease") { diseaseDF.encCache() }

//     val patientSchema = StructType(Seq(
//       StructField("p_id", IntegerType),
//       StructField("p_disease_id", StringType),
//       StructField("p_name", StringType)))
//     val patientDF = sqlContext.createEncryptedDataFrame(
//       sqlContext.read.schema(patientSchema)
//         .format("csv")
//         .load(s"$dataDir/disease/patient-$size.csv")
//         .repartition(numPartitions(sqlContext, distributed))
//         .rdd
//         .mapPartitions(Utils.diseaseQueryEncryptPatient),
//       patientSchema)
//     time("load patient") { patientDF.encCache() }

//     timeBenchmark(
//       "distributed" -> distributed,
//       "query" -> "join cost",
//       "system" -> "opaque",
//       "size" -> size) {
//       diseaseDF.encJoin(patientDF, $"d_disease_id" === $"p_disease_id").encForce()
//     }

//     if (!onlyOblivious) {
//       timeBenchmark(
//         "distributed" -> distributed,
//         "query" -> "join cost",
//         "system" -> "encrypted",
//         "size" -> size) {
//         diseaseDF.nonObliviousJoin(patientDF, $"d_disease_id" === $"p_disease_id").encForce()
//       }
//     }
//   }

//   def numPartitions(sqlContext: SQLContext, distributed: Boolean): Int =
//     if (distributed) sqlContext.sparkContext.defaultParallelism else 1

//   def rankings(sqlContext: SQLContext, size: String): DataFrame =
//     sqlContext.read.schema(
//       StructType(Seq(
//         StructField("pageURL", StringType),
//         StructField("pageRank", IntegerType),
//         StructField("avgDuration", IntegerType))))
//       .csv(s"$dataDir/big-data-benchmark-files/rankings/$size")

//   def uservisits(sqlContext: SQLContext, size: String): DataFrame =
//     sqlContext.read.schema(
//       StructType(Seq(
//         StructField("sourceIP", StringType),
//         StructField("destURL", StringType),
//         StructField("visitDate", DateType),
//         StructField("adRevenue", FloatType),
//         StructField("userAgent", StringType),
//         StructField("countryCode", StringType),
//         StructField("languageCode", StringType),
//         StructField("searchWord", StringType),
//         StructField("duration", IntegerType))))
//       .csv(s"$dataDir/big-data-benchmark-files/uservisits/$size")

//   def part(sqlContext: SQLContext, size: String): DataFrame =
//     sqlContext.read.schema(
//       StructType(Seq(
//         StructField("p_partkey", IntegerType),
//         StructField("p_name", StringType),
//         StructField("p_mfgr", StringType),
//         StructField("p_brand", StringType),
//         StructField("p_type", StringType),
//         StructField("p_size", IntegerType),
//         StructField("p_container", StringType),
//         StructField("p_retailprice", FloatType),
//         StructField("p_comment", StringType))))
//       .format("csv")
//       .option("delimiter", "|")
//       .load(s"$dataDir/tpch/$size/part.tbl")

//   def supplier(sqlContext: SQLContext, size: String): DataFrame =
//     sqlContext.read.schema(
//       StructType(Seq(
//         StructField("s_suppkey", IntegerType),
//         StructField("s_name", StringType),
//         StructField("s_address", StringType),
//         StructField("s_nationkey", IntegerType),
//         StructField("s_phone", StringType),
//         StructField("s_acctbal", FloatType),
//         StructField("s_comment", StringType))))
//       .format("csv")
//       .option("delimiter", "|")
//       .load(s"$dataDir/tpch/$size/supplier.tbl")

//   def lineitem(sqlContext: SQLContext, size: String): DataFrame =
//     sqlContext.read.schema(
//       StructType(Seq(
//         StructField("l_orderkey", IntegerType),
//         StructField("l_partkey", IntegerType),
//         StructField("l_suppkey", IntegerType),
//         StructField("l_linenumber", IntegerType),
//         StructField("l_quantity", IntegerType),
//         StructField("l_extendedprice", FloatType),
//         StructField("l_discount", FloatType),
//         StructField("l_tax", FloatType),
//         StructField("l_returnflag", StringType),
//         StructField("l_linestatus", StringType),
//         StructField("l_shipdate", DateType),
//         StructField("l_commitdate", DateType),
//         StructField("l_receiptdate", DateType),
//         StructField("l_shipinstruct", StringType),
//         StructField("l_shipmode", StringType),
//         StructField("l_comment", StringType))))
//       .format("csv")
//       .option("delimiter", "|")
//       .load(s"$dataDir/tpch/$size/lineitem.tbl")

//   def partsupp(sqlContext: SQLContext, size: String): DataFrame =
//     sqlContext.read.schema(
//       StructType(Seq(
//         StructField("ps_partkey", IntegerType),
//         StructField("ps_suppkey", IntegerType),
//         StructField("ps_availqty", IntegerType),
//         StructField("ps_supplycost", FloatType),
//         StructField("ps_comment", StringType))))
//       .format("csv")
//       .option("delimiter", "|")
//       .load(s"$dataDir/tpch/$size/partsupp.tbl")

//   def orders(sqlContext: SQLContext, size: String): DataFrame =
//     sqlContext.read.schema(
//       StructType(Seq(
//         StructField("o_orderkey", IntegerType),
//         StructField("o_custkey", IntegerType),
//         StructField("o_orderstatus", StringType),
//         StructField("o_totalprice", FloatType),
//         StructField("o_orderdate", DateType),
//         StructField("o_orderpriority", StringType),
//         StructField("o_clerk", StringType),
//         StructField("o_shippriority", IntegerType),
//         StructField("o_comment", StringType))))
//       .format("csv")
//       .option("delimiter", "|")
//       .load(s"$dataDir/tpch/$size/orders.tbl")

//   def nation(sqlContext: SQLContext, size: String): DataFrame =
//     sqlContext.read.schema(
//       StructType(Seq(
//         StructField("n_nationkey", IntegerType),
//         StructField("n_name", StringType),
//         StructField("n_regionkey", IntegerType),
//         StructField("n_comment", StringType))))
//       .format("csv")
//       .option("delimiter", "|")
//       .load(s"$dataDir/tpch/$size/nation.tbl")

//   private def tpch9EncryptedDFs(sqlContext: SQLContext, size: String, distributed: Boolean)
//       : (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
//     import sqlContext.implicits._
//     val partDF = sqlContext.createEncryptedDataFrame(
//       part(sqlContext, size)
//         .select($"p_partkey", $"p_name")
//         .repartition(numPartitions(sqlContext, distributed))
//         .rdd
//         .mapPartitions(Utils.tpch9EncryptPart),
//       StructType(Seq(
//         StructField("p_partkey", IntegerType),
//         StructField("p_name", StringType))))
//       .encCache()
//     val supplierDF = sqlContext.createEncryptedDataFrame(
//       supplier(sqlContext, size)
//         .select($"s_suppkey", $"s_nationkey")
//         .repartition(numPartitions(sqlContext, distributed))
//         .rdd
//         .mapPartitions(Utils.tpch9EncryptSupplier),
//       StructType(Seq(
//         StructField("s_suppkey", IntegerType),
//         StructField("s_nationkey", IntegerType))))
//       .encCache()
//     val lineitemDF = sqlContext.createEncryptedDataFrame(
//       lineitem(sqlContext, size)
//         .select(
//           $"l_orderkey", $"l_partkey", $"l_suppkey", $"l_quantity", $"l_extendedprice",
//           $"l_discount")
//         .repartition(numPartitions(sqlContext, distributed))
//         .rdd
//         .mapPartitions(Utils.tpch9EncryptLineitem),
//       StructType(Seq(
//         StructField("l_orderkey", IntegerType),
//         StructField("l_partkey", IntegerType),
//         StructField("l_suppkey", IntegerType),
//         StructField("l_quantity", IntegerType),
//         StructField("l_extendedprice", FloatType),
//         StructField("l_discount", FloatType))))
//       .encCache()
//     val partsuppDF = sqlContext.createEncryptedDataFrame(
//       partsupp(sqlContext, size)
//         .select($"ps_partkey", $"ps_suppkey", $"ps_supplycost")
//         .repartition(numPartitions(sqlContext, distributed))
//         .rdd
//         .mapPartitions(Utils.tpch9EncryptPartsupp),
//       StructType(Seq(
//         StructField("ps_partkey", IntegerType),
//         StructField("ps_suppkey", IntegerType),
//         StructField("ps_supplycost", FloatType))))
//       .encCache()
//     val ordersDF = sqlContext.createEncryptedDataFrame(
//       orders(sqlContext, size)
//         .select($"o_orderkey", $"o_orderdate")
//         .repartition(numPartitions(sqlContext, distributed))
//         .rdd
//         .mapPartitions(Utils.tpch9EncryptOrders),
//       StructType(Seq(
//         StructField("o_orderkey", IntegerType),
//         StructField("o_orderdate", DateType))))
//       .encCache()
//     val nationDF = sqlContext.createEncryptedDataFrame(
//       nation(sqlContext, size)
//         .select($"n_nationkey", $"n_name")
//         .repartition(numPartitions(sqlContext, distributed))
//         .rdd
//         .mapPartitions(Utils.tpch9EncryptNation),
//       StructType(Seq(
//         StructField("n_nationkey", IntegerType),
//         StructField("n_name", StringType))))
//       .encCache()
//     (partDF, supplierDF, lineitemDF, partsuppDF, ordersDF, nationDF)
//   }
}
