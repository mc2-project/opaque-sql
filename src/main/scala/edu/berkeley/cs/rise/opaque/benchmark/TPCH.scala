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

import scala.io.Source

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext

object TPCHTables {
  def part(
      sqlContext: SQLContext, securityLevel: SecurityLevel, size: String, numPartitions: Int)
      : DataFrame =
    securityLevel.applyTo(
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
       .load(s"${Benchmark.dataDir}/tpch/$size/part.tbl")
       .repartition(numPartitions))

  def supplier(
      sqlContext: SQLContext, securityLevel: SecurityLevel, size: String, numPartitions: Int)
      : DataFrame =
    securityLevel.applyTo(
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
       .load(s"${Benchmark.dataDir}/tpch/$size/supplier.tbl")
       .repartition(numPartitions))

  def lineitem(
      sqlContext: SQLContext, securityLevel: SecurityLevel, size: String, numPartitions: Int)
      : DataFrame =
    securityLevel.applyTo(
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
      .load(s"${Benchmark.dataDir}/tpch/$size/lineitem.tbl")
       .repartition(numPartitions))

  def partsupp(
      sqlContext: SQLContext, securityLevel: SecurityLevel, size: String, numPartitions: Int)
      : DataFrame =
    securityLevel.applyTo(
      sqlContext.read.schema(
      StructType(Seq(
        StructField("ps_partkey", IntegerType),
        StructField("ps_suppkey", IntegerType),
        StructField("ps_availqty", IntegerType),
        StructField("ps_supplycost", FloatType),
        StructField("ps_comment", StringType))))
      .format("csv")
      .option("delimiter", "|")
      .load(s"${Benchmark.dataDir}/tpch/$size/partsupp.tbl")
      .repartition(numPartitions))

  def orders(
      sqlContext: SQLContext, securityLevel: SecurityLevel, size: String, numPartitions: Int)
      : DataFrame =
    securityLevel.applyTo(
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
      .load(s"${Benchmark.dataDir}/tpch/$size/orders.tbl")
      .repartition(numPartitions))

  def nation(
      sqlContext: SQLContext, securityLevel: SecurityLevel, size: String, numPartitions: Int)
      : DataFrame =
    securityLevel.applyTo(
      sqlContext.read.schema(
      StructType(Seq(
        StructField("n_nationkey", IntegerType),
        StructField("n_name", StringType),
        StructField("n_regionkey", IntegerType),
        StructField("n_comment", StringType))))
      .format("csv")
      .option("delimiter", "|")
      .load(s"${Benchmark.dataDir}/tpch/$size/nation.tbl")
      .repartition(numPartitions))
}

class TPCH(
  val sqlContext: SQLContext,
  val size: String,
  val numPartitions: Int,

  val nationDF: DataFrame
) {

  def this(
    sqlContext: SQLContext,
    size: String,
    numPartitions: Int,
  ) = {
    this(sqlContext, size, numPartitions, 

    TPCHTables.part(sqlContext, Insecure, size, numPartitions))
  }

  def clearTables(sqlContext: SQLContext) = {
    val tableNames = Seq("part", "supplier", "lineitem", "partsupp", "orders", "nation")

    for (name <- tableNames) {
      sqlContext.sql(s"""DROP TABLE IF EXISTS default.${name}""".stripMargin)
    }
  }

  def performQuery(queryNumber: Int, sqlContext: SQLContext) : DataFrame = {
    val queryLocation = sys.env.getOrElse("OPAQUE_HOME", ".") + "/src/test/resources/tpch/"
    val sqlStr = Source.fromFile(queryLocation + s"q$queryNumber.sql").getLines().mkString("\n")

    sqlContext.sparkSession.sql(sqlStr)
  }

  def tpch(
    queryNumber: Int,
    sqlContext: SQLContext,
    securityLevel: SecurityLevel,
    size: String,
    numPartitions: Int) : DataFrame = {

    val df = performQuery(queryNumber, sqlContext)
    clearTables(sqlContext)

    df
  }
}
