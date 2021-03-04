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

package edu.berkeley.cs.rise.opaque.tpch

import org.apache.spark.sql.SparkSession
import edu.berkeley.cs.rise.opaque.OpaqueSuiteBase

trait TPCHSuite extends OpaqueSuiteBase { self =>

  def size = "sf_small"
  def tpch = new TPCH(spark.sqlContext, size, "file://")

  def runTests(numPartitions: Int) = {
    for (queryNum <- TPCH.supportedQueries) {
      val testStr = s"TPC-H $queryNum"
      if (TPCH.unorderedQueries.contains(queryNum)) {
        testAgainstSpark(testStr, isOrdered = false) { securityLevel =>
          tpch.query(queryNum, securityLevel, numPartitions)
        }
      } else {
        testAgainstSpark(testStr, isOrdered = true) { securityLevel =>
          tpch.query(queryNum, securityLevel, numPartitions)
        }
      }
    }
  }
}

class SinglePartitionTPCHSuite extends TPCHSuite {
  override val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("TPCHSinglePartitionSuite")
    .config("spark.sql.shuffle.partitions", numPartitions)
    .getOrCreate()
  def numPartitions: Int = 1

  runTests(numPartitions);
}

class MultipleTPCHMultipleSuite extends TPCHSuite {
  override val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("TPCHMultiplePartitionSuite")
    .config("spark.sql.shuffle.partitions", numPartitions)
    .getOrCreate()
  def numPartitions: Int = 3

  runTests(numPartitions);
}
