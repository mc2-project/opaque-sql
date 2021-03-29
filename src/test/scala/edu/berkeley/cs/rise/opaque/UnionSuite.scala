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

trait UnionSuite extends OpaqueSQLSuiteBase with SQLHelper {

  val tableMap = Map(
    "t1" -> "CREATE OR REPLACE TEMPORARY VIEW t1_ AS VALUES (1, 'a'), (2, 'b') tbl(c1, c2);",
    "t2" -> "CREATE OR REPLACE TEMPORARY VIEW t2_ AS VALUES (1.0, 1), (2.0, 4) tbl(c1, c2);",
    "t3" -> "CREATE OR REPLACE TEMPORARY VIEW t3_ AS VALUES (decimal(1)) tbl(v);",
    "p1" -> "CREATE OR REPLACE TEMPORARY VIEW p1_ AS VALUES 1 T(col);",
    "p2" -> "CREATE OR REPLACE TEMPORARY VIEW p2_ AS VALUES 1 T(col);",
    "p3" -> "CREATE OR REPLACE TEMPORARY VIEW p3_ AS VALUES 1 T(col);"
  )

  override def beforeAll() = {
    super.beforeAll()
    tableMap.foreach { case (_, sqlStr) => spark.sql(sqlStr) }
  }

  override def afterAll() = {
    super.afterAll()
    safeDropTables("t1", "t2", "t3", "p1", "p2", "p3")
  }

  def numPartitions: Int

  def queries = Seq(
    "SELECT * FROM (SELECT * FROM t1 UNION ALL SELECT * FROM t1);",
    "SELECT a FROM (SELECT 0 a, 0 b UNION ALL SELECT SUM(1) a, CAST(0 AS BIGINT) b UNION ALL SELECT 0 a, 0 b) T;"
  )
  def failingQueries = Seq(
    "SELECT * FROM (SELECT * FROM t1 UNION ALL SELECT * FROM t2 UNION ALL SELECT * FROM t2);",
    "SELECT t.v FROM (SELECT v FROM t3 UNION ALL SELECT v + v AS v FROM t3) t;",
    "SELECT SUM(t.v) FROM (SELECT v FROM t3 UNION SELECT v + v AS v FROM t3) t;"
  )
  def unsupportedQueries = Seq(
    """
    |SELECT 1 AS x,
    |  col
    |  FROM   (SELECT col AS col
    |  FROM (SELECT p1.col AS col
    |  FROM   p1 CROSS JOIN p2
    |  UNION ALL
    |  SELECT col
    |  FROM p3) T1) T2;""".stripMargin,
    "SELECT map(1, 2), 'str' UNION ALL SELECT map(1, 2, 3, NULL), 1;",
    "SELECT array(1, 2), 'str' UNION ALL SELECT array(1, 2, 3, NULL), 1;"
  )

  override def loadTestData(sqlStr: String, sl: SecurityLevel) = {
    super.loadTestData(sqlStr, sl)
    loadUnionData(sl)
  }

  def loadUnionData(sl: SecurityLevel) = {
    for ((name, sqlStr) <- tableMap) {
      val df = sl.applyTo(spark.table(name + "_"))
      df.createOrReplaceTempView(name)
    }
  }
}

class SinglePartitionUnionSuite extends UnionSuite with SinglePartitionSparkSession {
  runSQLQueries()
}

class MultiplePartitionUnionSuite extends UnionSuite with MultiplePartitionSparkSession {
  runSQLQueries()
}
