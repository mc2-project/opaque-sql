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

package edu.berkeley.cs.rise.opaque

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

trait AggregationSuite extends OpaqueSQLSuiteBase with SQLHelper {
  import spark.implicits._

  def numPartitions: Int

  def queries = Seq(
  )
  def failingQueries = Seq(
    """
    |SELECT
    |  AVG(value),
    |  COUNT(*),
    |  COUNT(key),
    |  COUNT(value),
    |  FIRST(key),
    |  LAST(value),
    |  MAX(key),
    |  MIN(value),
    |  SUM(key)
    |FROM emptyTable
    """.stripMargin,
    """
    |SELECT
    |  AVG(value),
    |  COUNT(*),
    |  COUNT(key),
    |  COUNT(value),
    |  FIRST(key),
    |  LAST(value),
    |  MAX(key),
    |  MIN(value),
    |  SUM(key),
    |  COUNT(DISTINCT value)
    |FROM emptyTable
    """.stripMargin,
    """
    |SELECT
    |  AVG(value),
    |  COUNT(*),
    |  COUNT(value),
    |  FIRST(value),
    |  LAST(value),
    |  MAX(value),
    |  MIN(value),
    |  SUM(value),
    |  COUNT(DISTINCT value)
    |FROM emptyTable
    |GROUP BY key
    """.stripMargin
  )
  def unsupportedQueries = Seq()

  override def loadTestData(sqlStr: String, sl: SecurityLevel) = {
    super.loadTestData(sqlStr, sl)

    val data1 = sl.applyTo(
      Seq[(Integer, Integer)](
        (1, 10),
        (null, -60),
        (1, 20),
        (1, 30),
        (2, 0),
        (null, -10),
        (2, -1),
        (2, null),
        (2, null),
        (null, 100),
        (3, null),
        (null, null),
        (3, null)
      ).toDF("key", "value")
    )
    data1.createOrReplaceTempView("agg1")

    val data2 = sl.applyTo(
      Seq[(Integer, Integer, Integer)](
        (1, 10, -10),
        (null, -60, 60),
        (1, 30, -30),
        (1, 30, 30),
        (2, 1, 1),
        (null, -10, 10),
        (2, -1, null),
        (2, 1, 1),
        (2, null, 1),
        (null, 100, -10),
        (3, null, 3),
        (null, null, null),
        (3, null, null)
      ).toDF("key", "value1", "value2")
    )
    data2.createOrReplaceTempView("agg2")

    val data3 = sl.applyTo(
      Seq[(Seq[Integer], Integer, Integer)](
        (Seq[Integer](1, 1), 10, -10),
        (Seq[Integer](null), -60, 60),
        (Seq[Integer](1, 1), 30, -30),
        (Seq[Integer](1), 30, 30),
        (Seq[Integer](2), 1, 1),
        (null, -10, 10),
        (Seq[Integer](2, 3), -1, null),
        (Seq[Integer](2, 3), 1, 1),
        (Seq[Integer](2, 3, 4), null, 1),
        (Seq[Integer](null), 100, -10),
        (Seq[Integer](3), null, 3),
        (null, null, null),
        (Seq[Integer](3), null, null)
      ).toDF("key", "value1", "value2")
    )
    data3.createOrReplaceTempView("agg3")

    val emptyDF = sl.applyTo(
      spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(StructField("key", StringType) :: StructField("value", IntegerType) :: Nil)
      )
    )
    emptyDF.createOrReplaceTempView("emptyTable")
  }
}

class MultiplePartitionAggregationSuite extends AggregationSuite {
  override def numPartitions = 3
  override val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MultiplePartitionAggregationSuite")
    .config("spark.sql.shuffle.partitions", numPartitions)
    .getOrCreate()

  runSQLQueries();
}
