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

import java.io.File
import scala.io.Source

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext

import edu.berkeley.cs.rise.opaque.Utils

object TPCH {

  val tableNames = Seq("part", "supplier", "lineitem", "partsupp", "orders", "nation", "region", "customer")

  def part(
      sqlContext: SQLContext, size: String)
      : DataFrame =
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

  def supplier(
      sqlContext: SQLContext, size: String)
      : DataFrame =
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

  def lineitem(
      sqlContext: SQLContext, size: String)
      : DataFrame =
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

  def partsupp(
      sqlContext: SQLContext, size: String)
      : DataFrame =
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

  def orders(
      sqlContext: SQLContext, size: String)
      : DataFrame =
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

  def nation(
      sqlContext: SQLContext, size: String)
      : DataFrame =
      sqlContext.read.schema(
      StructType(Seq(
        StructField("n_nationkey", IntegerType),
        StructField("n_name", StringType),
        StructField("n_regionkey", IntegerType),
        StructField("n_comment", StringType))))
      .format("csv")
      .option("delimiter", "|")
      .load(s"${Benchmark.dataDir}/tpch/$size/nation.tbl")

  def region(
      sqlContext: SQLContext, size: String)
      : DataFrame =
      sqlContext.read.schema(
      StructType(Seq(
        StructField("r_regionkey", IntegerType),
        StructField("r_name", StringType),
        StructField("r_comment", StringType))))
      .format("csv")
      .option("delimiter", "|")
      .load(s"${Benchmark.dataDir}/tpch/$size/region.tbl")

  def customer(
      sqlContext: SQLContext, size: String)
      : DataFrame =
      sqlContext.read.schema(
      StructType(Seq(
        StructField("c_custkey", IntegerType),
        StructField("c_name", StringType),
        StructField("c_address", StringType),
        StructField("c_nationkey", IntegerType),
        StructField("c_phone", StringType),
        StructField("c_acctbal", FloatType),
        StructField("c_mktsegment", StringType),
        StructField("c_comment", StringType))))
      .format("csv")
      .option("delimiter", "|")
      .load(s"${Benchmark.dataDir}/tpch/$size/customer.tbl")

  def generateDFs(
      sqlContext: SQLContext, size: String)
      : Map[String, DataFrame] = {
    Map("part" -> part(sqlContext, size),
    "supplier" -> supplier(sqlContext, size),
    "lineitem" -> lineitem(sqlContext, size),
    "partsupp" -> partsupp(sqlContext, size),
    "orders" -> orders(sqlContext, size),
    "nation" -> nation(sqlContext, size),
    "region" -> region(sqlContext, size),
    "customer" -> customer(sqlContext, size)
    ),
  }
}

class TPCH(val sqlContext: SQLContext, val size: String, val fileUrl: String) {

  val tableNames = TPCH.tableNames
  val nameToDF = TPCH.generateDFs(sqlContext, size)

  private var numPartitions: Int = -1
  private var nameToPath = Map[String, File]()
  private var nameToEncryptedPath = Map[String, File]()

  def getQuery(queryNumber: Int) : String = {
    val queryLocation = sys.env.getOrElse("OPAQUE_HOME", ".") + "/src/test/resources/tpch/"
    Source.fromFile(queryLocation + s"q$queryNumber.sql").getLines().mkString("\n")
  }

  def generateFiles(numPartitions: Int) = {
    if (numPartitions != this.numPartitions) {
      this.numPartitions = numPartitions
      for ((name, df) <- nameToDF) {
        nameToPath.get(name).foreach{ path => Utils.deleteRecursively(path) }

        nameToPath += (name -> createPath(df, Insecure, numPartitions))
        nameToEncryptedPath += (name -> createPath(df, Encrypted, numPartitions))
      }
    }
  }

  private def createPath(df: DataFrame, securityLevel: SecurityLevel, numPartitions: Int): File = {
    val partitionedDF = securityLevel.applyTo(df.repartition(numPartitions))
    val path = Utils.createTempDir()
    path.delete()
    securityLevel match {
      case Insecure => {
        partitionedDF.write.format("com.databricks.spark.csv")
            .option("ignoreLeadingWhiteSpace", false)
            .option("ignoreTrailingWhiteSpace", false)
            .save(fileUrl + path.toString)
      }
      case Encrypted => {
        partitionedDF.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save(fileUrl + path.toString)
      }
    }
    path
  }

  private def loadViews(securityLevel: SecurityLevel) = {
    val (map, formatStr) = if (securityLevel == Insecure) 
        (nameToPath, "com.databricks.spark.csv") else 
        (nameToEncryptedPath, "edu.berkeley.cs.rise.opaque.EncryptedSource")
    for ((name, path) <- map) {
        val df = sqlContext.sparkSession.read
            .format(formatStr)
            .schema(nameToDF.get(name).get.schema)
            .load(fileUrl + path.toString)
        df.createOrReplaceTempView(name)
    }
  }

  def performQuery(sqlStr: String, securityLevel: SecurityLevel): DataFrame  = {
    loadViews(securityLevel)
    sqlContext.sparkSession.sql(sqlStr)
  }

  def query(queryNumber: Int, securityLevel: SecurityLevel, numPartitions: Int): DataFrame = {
    val sqlStr = getQuery(queryNumber)
    generateFiles(numPartitions)
    performQuery(sqlStr, securityLevel)
  }
}
