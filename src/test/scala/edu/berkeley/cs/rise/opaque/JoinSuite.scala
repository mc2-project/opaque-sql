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

trait JoinSuite extends OpaqueSuiteBase {

  def numPartitions: Int

  def queries = Seq(
    "SELECT * FROM testData LEFT SEMI JOIN testData2 ON key = a",
    "SELECT * FROM testData LEFT SEMI JOIN testData2",
    "SELECT * FROM testData JOIN testData2",
    "SELECT * FROM testData JOIN testData2 WHERE key = 2",
    "SELECT * FROM testData LEFT JOIN testData2",
    "SELECT * FROM testData RIGHT JOIN testData2",
    "SELECT * FROM testData LEFT JOIN testData2 WHERE key = 2",
    "SELECT * FROM testData RIGHT JOIN testData2 WHERE key = 2",
    "SELECT * FROM testData JOIN testData2 WHERE key > a"
  )
  /* Unsupported Queries;
   * "SELECT * FROM testData FULL OUTER JOIN testData2"
   * "SELECT * FROM testData FULL OUTER JOIN testData2 WHERE key = 2"
   * "SELECT * FROM testData FULL OUTER JOIN testData2 WHERE key > a"
   *
   * Failing Queries:
   */

  def runTests(numPartitions: Int) = {
    for (sqlStr <- queries) {
      testAgainstSpark(sqlStr, isOrdered = false, verbose = false, printPlan = true) {
        securityLevel =>
          loadTestData(securityLevel)
          spark.sqlContext.sparkSession.sql(sqlStr)
      }
    }
  }
}

class MultiplePartitionJoinSuite extends JoinSuite {
  override def numPartitions = 3
  override val spark = SparkSession
    .builder()
    .master("local[4]")
    .appName("MultiplePartitionJoinSuite")
    .config("spark.sql.shuffle.partitions", numPartitions)
    .getOrCreate()

  runTests(numPartitions);
}
