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

import edu.berkeley.cs.rise.opaque.benchmark._
import edu.berkeley.cs.rise.opaque.benchmark.TPCH

trait TPCHTests extends OpaqueTestsBase { self => 

  def size = "sf_small"
  def tpch = TPCH(spark.sqlContext, size)

  testAgainstSpark("TPC-H 1") { securityLevel =>
    tpch.query(1, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 2", ignore) { securityLevel =>
    tpch.query(2, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 3") { securityLevel =>
    tpch.query(3, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 4", ignore) { securityLevel =>
    tpch.query(4, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 5") { securityLevel =>
    tpch.query(5, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 6") { securityLevel =>
    tpch.query(6, securityLevel, spark.sqlContext, numPartitions).collect.toSet
  }

  testAgainstSpark("TPC-H 7") { securityLevel =>
    tpch.query(7, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 8") { securityLevel =>
    tpch.query(8, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 9") { securityLevel =>
    tpch.query(9, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 10") { securityLevel =>
    tpch.query(10, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 11", ignore) { securityLevel =>
    tpch.query(11, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 12") { securityLevel =>
    tpch.query(12, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 13", ignore) { securityLevel =>
    tpch.query(13, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 14") { securityLevel =>
    tpch.query(14, securityLevel, spark.sqlContext, numPartitions).collect.toSet
  }

  testAgainstSpark("TPC-H 15", ignore) { securityLevel =>
    tpch.query(15, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 16", ignore) { securityLevel =>
    tpch.query(16, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 17") { securityLevel =>
    tpch.query(17, securityLevel, spark.sqlContext, numPartitions).collect.toSet
  }

  testAgainstSpark("TPC-H 18", ignore) { securityLevel =>
    tpch.query(18, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 19") { securityLevel =>
    tpch.query(19, securityLevel, spark.sqlContext, numPartitions).collect.toSet
  }

  testAgainstSpark("TPC-H 20") { securityLevel =>
    tpch.query(20, securityLevel, spark.sqlContext, numPartitions).collect.toSet
  }

  testAgainstSpark("TPC-H 21", ignore) { securityLevel =>
    tpch.query(21, securityLevel, spark.sqlContext, numPartitions).collect
  }

  testAgainstSpark("TPC-H 22", ignore) { securityLevel =>
    tpch.query(22, securityLevel, spark.sqlContext, numPartitions).collect
  }
}

// class TPCHSinglePartitionSuite extends TPCHTests {
//   override def numPartitions: Int = 1
//   override val spark = SparkSession.builder()
//     .master("local[1]")
//     .appName("TPCHSinglePartitionSuite")
//     .config("spark.sql.shuffle.partitions", numPartitions)
//     .getOrCreate()
// }

// class TPCHMultiplePartitionSuite extends TPCHTests {
//   override def numPartitions: Int = 3
//   override val spark = SparkSession.builder()
//     .master("local[1]")
//     .appName("TPCHMultiplePartitionSuite")
//     .config("spark.sql.shuffle.partitions", numPartitions)
//     .getOrCreate()
// }
