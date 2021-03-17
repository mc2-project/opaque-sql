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

trait SubquerySuite extends OpaqueSQLSuiteBase with SQLHelper {
  import spark.implicits._

  def numPartitions: Int

  override def queries = Seq("select (select 1 as b) as b")
  override def failingQueries = Seq()
  override def unsupportedQueries = Seq()

  override def loadTestData(sqlStr: String, sl: SecurityLevel) = {
    super.loadTestData(sqlStr, sl)
    loadSubqueryData(sl)
  }

  def loadSubqueryData(sl: SecurityLevel) = {
    lazy val l =
      sl.applyTo(
        Seq(
          (1, 2.0),
          (1, 2.0),
          (2, 1.0),
          (2, 1.0),
          (3, 3.0),
          (null, null),
          (null, 5.0),
          (6, null)
        )
          .toDF("a", "b")
      )

    lazy val r = sl.applyTo(
      Seq((2, 3.0), (2, 3.0), (3, 2.0), (4, 1.0), (null, null), (null, 5.0), (6, null))
        .toDF("c", "d")
    )

    lazy val t = sl.applyTo(r.filter($"c".isNotNull && $"d".isNotNull))

    l.createOrReplaceTempView("l")
    r.createOrReplaceTempView("r")
    t.createOrReplaceTempView("t")
  }
}

class SinglePartitionSubquerySuite extends FilterSuite {
  override def numPartitions = 1
  override val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("SinglePartitionFilterSuite")
    .config("spark.sql.shuffle.partitions", numPartitions)
    .getOrCreate()

  runSQLQueries()
}

class MultiplePartitionSubquerySuite extends FilterSuite {
  val executorInstances = 3

  override def numPartitions = executorInstances
  override val spark = SparkSession
    .builder()
    .master(s"local-cluster[$executorInstances,1,1024]")
    .appName("MultiplePartitionFilterSuite")
    .config("spark.executor.instances", executorInstances)
    .config("spark.sql.shuffle.partitions", numPartitions)
    .config(
      "spark.jars",
      "target/scala-2.12/opaque_2.12-0.1.jar,target/scala-2.12/opaque_2.12-0.1-tests.jar"
    )
    .getOrCreate()

  runSQLQueries()
}
