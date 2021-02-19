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

import org.apache.spark.sql.SQLContext

object TPCHBenchmark {

  // Add query numbers here once they are supported
  val supportedQueries = Seq(1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 17, 19, 20, 22)

  def query(queryNumber: Int, tpch: TPCH, sqlContext: SQLContext, numPartitions: Int) = {
    val sqlStr = tpch.getQuery(queryNumber)
    tpch.generateFiles(numPartitions)

    Utils.timeBenchmark(
        "distributed" -> (numPartitions > 1),
        "query" -> s"TPC-H $queryNumber",
        "system" -> Insecure.name) {
      
      tpch.performQuery(sqlStr, Insecure).collect
    }

    Utils.timeBenchmark(
        "distributed" -> (numPartitions > 1),
        "query" -> s"TPC-H $queryNumber",
        "system" -> Encrypted.name) {
      
      tpch.performQuery(sqlStr, Encrypted).collect
    }
  }

  def run(sqlContext: SQLContext, numPartitions: Int, size: String, fileUrl: String) = {
    val tpch = new TPCH(sqlContext, size, fileUrl)

    for (queryNumber <- supportedQueries) {
      query(queryNumber, tpch, sqlContext, numPartitions)
    }
  }
}
