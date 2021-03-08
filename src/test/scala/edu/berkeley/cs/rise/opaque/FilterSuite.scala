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

import java.util.Locale

import org.apache.spark.sql.SparkSession

trait FilterSuite extends OpaqueSQLSuiteBase with SQLHelper {
  import spark.implicits._

  def numPartitions: Int

  override def queries =
    Seq(
      "SELECT * FROM oneToTenFiltered",
      "SELECT a, b FROM oneToTenFiltered",
      "SELECT b, a FROM oneToTenFiltered",
      "SELECT a FROM oneToTenFiltered",
      "SELECT b FROM oneToTenFiltered",
      "SELECT a * 2 FROM oneToTenFiltered",
      "SELECT A AS b FROM oneToTenFiltered",
      "SELECT x.b, y.a FROM oneToTenFiltered x JOIN oneToTenFiltered y ON x.a = y.b",
      "SELECT x.a, y.b FROM oneToTenFiltered x JOIN oneToTenFiltered y ON x.a = y.b",
      "SELECT a, b FROM oneToTenFiltered WHERE a = 1",
      "SELECT a, b FROM oneToTenFiltered WHERE a IN (1,3,5)",
      "SELECT a, b FROM oneToTenFiltered WHERE A = 1",
      "SELECT a, b FROM oneToTenFiltered WHERE b = 2",
      "SELECT a, b FROM oneToTenFiltered WHERE a IS NULL",
      "SELECT a, b FROM oneToTenFiltered WHERE a IS NOT NULL",
      "SELECT a, b FROM oneToTenFiltered WHERE a < 5 AND a > 1",
      "SELECT a, b FROM oneToTenFiltered WHERE a < 3 OR a > 8",
      "SELECT a, b FROM oneToTenFiltered WHERE NOT (a < 6)",
      "SELECT a, b, c FROM oneToTenFiltered WHERE c like 'c%'",
      "SELECT a, b, c FROM oneToTenFiltered WHERE c like '%D'",
      "SELECT a, b, c FROM oneToTenFiltered WHERE c like '%eE%'",
      "SELECT * FROM oneToTenFiltered WHERE a > 1 AND a < 10",
      "SELECT * FROM oneToTenFiltered WHERE a = 20",
      "SELECT * FROM oneToTenFiltered WHERE NOT (a < 6)",
      "SELECT c FROM oneToTenFiltered WHERE c IN ('aaaaaAAAAA', 'foo')",
      """SELECT a
        |FROM oneToTenFiltered
        |WHERE a + b > 9
        |AND b < 16
        |AND c IN ('bbbbbBBBBB', 'cccccCCCCC', 'dddddDDDDD', 'foo')
      """.stripMargin
    )
  override def failingQueries = Seq()
  override def unsupportedQueries = Seq()

  override def loadTestData(sqlStr: String, sl: SecurityLevel) = {
    loadFilterData(sl)
  }

  def loadFilterData(sl: SecurityLevel) = {
    val df = sl.applyTo(
      (1 to 10)
        .map(i =>
          (
            i,
            i * 2,
            (i - 1 + 'a').toChar.toString * 5 + (i - 1 + 'a').toChar.toString
              .toUpperCase(Locale.ROOT) * 5
          )
        )
        .toSeq
        .toDF("a", "b", "c")
    )
    df.createOrReplaceTempView("oneToTenFiltered")
  }
}

class SinglePartitionFilterSuite extends FilterSuite {
  override def numPartitions = 1
  override val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("SinglePartitionFilterSuite")
    .config("spark.sql.shuffle.partitions", numPartitions)
    .getOrCreate()

  runSQLQueries()
}

class MultiplePartitionFilterSuite extends FilterSuite {
  override def numPartitions = 3
  override val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MultiplePartitionFilterSuite")
    .config("spark.sql.shuffle.partitions", numPartitions)
    .getOrCreate()

  runSQLQueries()
}
