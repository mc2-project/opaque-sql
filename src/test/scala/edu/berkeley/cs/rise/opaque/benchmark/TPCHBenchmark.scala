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

import edu.berkeley.cs.rise.opaque.{Insecure, Encrypted}
import edu.berkeley.cs.rise.opaque.tpch.TPCH
import edu.berkeley.cs.rise.opaque.OpaqueFunSuite

import org.apache.spark.sql.SQLContext

object TPCHBenchmark extends OpaqueFunSuite {

  def query(queryNumber: Int, tpch: TPCH, sqlContext: SQLContext, numPartitions: Int) = {
    val sqlStr = tpch.getQuery(queryNumber)
    tpch.generateFiles(numPartitions)

    val insecure = Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> s"TPC-H $queryNumber",
      "system" -> Insecure.name
    ) {

      tpch.performQuery(sqlStr, Insecure).collect
    }

    val encrypted = Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> s"TPC-H $queryNumber",
      "system" -> Encrypted.name
    ) {

      tpch.performQuery(sqlStr, Encrypted).collect
    }

    if (TPCH.unorderedQueries.contains(queryNumber)) {
      assert(insecure.toSet === encrypted.toSet)
    } else {
      assert(insecure === encrypted)
    }
  }

  def run(sqlContext: SQLContext, numPartitions: Int, size: String, fileUrl: String) = {
    val tpch = new TPCH(sqlContext, size, fileUrl)

    for (queryNumber <- TPCH.supportedQueries) {
      query(queryNumber, tpch, sqlContext, numPartitions)
    }
  }
}
