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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession

trait JoinSuite extends OpaqueSQLSuiteBase with SQLHelper {
  import spark.implicits._

  def numPartitions: Int

  /* Tests that are passing */
  def queries = Seq(
    "SELECT * FROM testData LEFT SEMI JOIN testData2 ON key = a",
    "SELECT * FROM testData LEFT SEMI JOIN testData2",
    "SELECT * FROM testData LEFT JOIN testData2",
    "SELECT * FROM testData RIGHT JOIN testData2",
    "SELECT * FROM testData LEFT JOIN testData2 WHERE key = 2",
    "SELECT * FROM testData JOIN testData2 ON key = a",
    "SELECT * FROM testData JOIN testData2 ON key = a and key = 2",
    "SELECT * FROM testData JOIN testData2 ON key = a where key = 2",
    "SELECT * FROM testData LEFT JOIN testData2 ON key = a",
    "SELECT * FROM testData RIGHT JOIN testData2 ON key = a where key = 2",
    "SELECT * FROM testData left JOIN testData2 ON (key * a != key + a)",
    "SELECT * FROM testData right JOIN testData2 ON (key * a != key + a)",
    "SELECT * FROM testData ANTI JOIN testData2 ON key = a",
    "SELECT * FROM testData LEFT ANTI JOIN testData2",
    "SELECT * FROM testData t1 JOIN " +
      "testData2 t2 ON t1.key = t2.a JOIN testData3 t3 ON t2.a = t3.a",
    "SELECT * FROM testData t1 JOIN " +
      "testData2 t2 ON t1.key = t2.a JOIN " +
      "testData3 t3 ON t2.a = t3.a JOIN " +
      "testData t4 ON t1.key = t4.key"
  )
  /* Tests that are failing but should be passing */
  def failingQueries = Seq()
  /* Tests that contain unsupported operators */
  def unsupportedQueries = Seq(
    "SELECT * FROM testData JOIN testData2",
    "SELECT * FROM testData JOIN testData2 WHERE key = 2",
    "SELECT * FROM testData RIGHT JOIN testData2 WHERE key = 2",
    "SELECT * FROM testData JOIN testData2 WHERE key > a",
    "SELECT * FROM testData FULL OUTER JOIN testData2 WHERE key = 2",
    "SELECT * FROM testData FULL OUTER JOIN testData2",
    "SELECT * FROM testData FULL OUTER JOIN testData2 WHERE key > a",
    "SELECT * FROM testData full JOIN testData2 ON (key * a != key + a)"
  )

  test("inner join, one match per row") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer() { sl =>
        val upper = upperCaseData(sl)
        val lower = lowerCaseData(sl)
        upper.join(lower).where('n === 'N)
      }
    }
  }

  test("inner join, multiple matches") {
    checkAnswer() { sl =>
      val x = testData2(sl).where($"a" === 1).as("x")
      val y = testData2(sl).where($"a" === 1).as("y")
      x.join(y).where($"x.a" === $"y.a")
    }
  }

  test("inner join, no matches") {
    checkAnswer() { sl =>
      val x = testData2(sl).where($"a" === 1).as("x")
      val y = testData2(sl).where($"a" === 2).as("y")
      x.join(y).where($"x.a" === $"y.a")
    }
  }

  ignore("big inner join, 4 matches per row") {
    checkAnswer() { sl =>
      val bigData = testData(sl)
        .union(testData(sl))
        .union(testData(sl))
        .union(testData(sl))
      val bigDataX = bigData.as("x")
      val bigDataY = bigData.as("y")
      bigDataX.join(bigDataY).where($"x.key" === $"y.key")
    }
  }

  ignore("cartesian product join") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      checkAnswer() { sl => testData3(sl).join(testData3(sl)) }
    }
  }

  test("left outer join") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer() { sl =>
        upperCaseData(sl).join(lowerCaseData(sl), $"n" === $"N", "left")
      }
      checkAnswer() { sl =>
        upperCaseData(sl).join(lowerCaseData(sl), $"n" === $"N" && $"n" > 1, "left")
      }
      checkAnswer() { sl =>
        upperCaseData(sl).join(lowerCaseData(sl), $"n" === $"N" && $"N" > 1, "left")
      }
      checkAnswer() { sl =>
        upperCaseData(sl).join(lowerCaseData(sl), $"n" === $"N" && $"l" > $"L", "left")
      }
      checkAnswer() { sl =>
        val sqlStr = """
          |SELECT l.N, count(*)
          |FROM upperCaseData l LEFT OUTER JOIN allNulls r ON (l.N = r.a)
          |GROUP BY l.N
          """.stripMargin
        loadTestData(sqlStr, sl)
        spark.sqlContext.sparkSession.sql(sqlStr)
      }
      checkAnswer() { sl =>
        val sqlStr = """
          |SELECT r.a, count(*)
          |FROM upperCaseData l LEFT OUTER JOIN allNulls r ON (l.N = r.a)
          |GROUP BY r.a
          """.stripMargin
        loadTestData(sqlStr, sl)
        spark.sqlContext.sparkSession.sql(sqlStr)
      }
    }
  }

  test("right outer join") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer() { sl =>
        lowerCaseData(sl).join(upperCaseData(sl), $"n" === $"N", "right")
      }
      checkAnswer() { sl =>
        lowerCaseData(sl).join(upperCaseData(sl), $"n" === $"N" && $"n" > 1, "right")
      }
      checkAnswer() { sl =>
        lowerCaseData(sl).join(upperCaseData(sl), $"n" === $"N" && $"N" > 1, "right")
      }
      checkAnswer(true) { sl =>
        lowerCaseData(sl).join(upperCaseData(sl), $"n" === $"N" && $"l" > $"L", "right")
      }
      checkAnswer() { sl =>
        val sqlStr = """
            |SELECT l.a, count(*)
            |FROM allNulls l RIGHT OUTER JOIN upperCaseData r ON (l.a = r.N)
            |GROUP BY l.a
          """.stripMargin
        loadTestData(sqlStr, sl)
        spark.sqlContext.sparkSession.sql(sqlStr)
      }
      checkAnswer() { sl =>
        val sqlStr = """
            |SELECT r.N, count(*)
            |FROM allNulls l RIGHT OUTER JOIN upperCaseData r ON (l.a = r.N)
            |GROUP BY r.N
          """.stripMargin
        loadTestData(sqlStr, sl)
        spark.sqlContext.sparkSession.sql(sqlStr)
      }
    }
  }

  ignore("full outer join") {
    def createTables(sl: SecurityLevel) = {
      val left = upperCaseData(sl).where('N <= 4)
      left.createOrReplaceTempView("left")
      val right = upperCaseData(sl).where('N >= 3)
      right.createOrReplaceTempView("right")
      (left, right)
    }

    checkAnswer() { sl =>
      val (left, right) = createTables(sl)
      left.join(right, $"left.N" === $"right.N", "full")
    }
    checkAnswer() { sl =>
      val (left, right) = createTables(sl)
      left.join(right, ($"left.N" === $"right.N") && ($"left.N" =!= 3), "full")
    }
    checkAnswer() { sl =>
      val (left, right) = createTables(sl)
      left.join(right, ($"left.N" === $"right.N") && ($"right.N" =!= 3), "full")
    }
    checkAnswer() { sl =>
      val (left, right) = createTables(sl)
      val sqlStr = """
          |SELECT l.a, count(*)
          |FROM allNulls l FULL OUTER JOIN upperCaseData r ON (l.a = r.N)
          |GROUP BY l.a
        """.stripMargin
      loadTestData(sqlStr, sl)
      spark.sqlContext.sparkSession.sql(sqlStr)
    }
    checkAnswer() { sl =>
      val (left, right) = createTables(sl)
      val sqlStr = """
          |SELECT r.N, count(*)
          |FROM allNulls l FULL OUTER JOIN upperCaseData r ON (l.a = r.N)
          |GROUP BY r.N
          """.stripMargin
      loadTestData(sqlStr, sl)
      spark.sqlContext.sparkSession.sql(sqlStr)
    }
    checkAnswer() { sl =>
      val (left, right) = createTables(sl)
      val sqlStr = """
          |SELECT l.N, count(*)
          |FROM upperCaseData l FULL OUTER JOIN allNulls r ON (l.N = r.a)
          |GROUP BY l.N
          """.stripMargin
      loadTestData(sqlStr, sl)
      spark.sqlContext.sparkSession.sql(sqlStr)
    }
    checkAnswer() { sl =>
      val (left, right) = createTables(sl)
      val sqlStr = """
          |SELECT r.a, count(*)
          |FROM upperCaseData l FULL OUTER JOIN allNulls r ON (l.N = r.a)
          |GROUP BY r.a
          """.stripMargin
      loadTestData(sqlStr, sl)
      spark.sqlContext.sparkSession.sql(sqlStr)
    }

    safeDropTables("left", "right")
  }

  ignore("cross join with broadcast") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> 0.toString,
      SQLConf.CROSS_JOINS_ENABLED.key -> "true"
    ) {
      checkAnswer() { sl =>
        val sqlStr = """
          SELECT x.value, y.a, y.b FROM testData x JOIN testData2 y WHERE x.key = 2
          """.stripMargin
        loadTestData(sqlStr, sl)
        spark.sqlContext.sparkSession.sql(sqlStr)
      }
      checkAnswer() { sl =>
        val sqlStr = """
          SELECT x.value, y.a, y.b FROM testData x JOIN testData2 y WHERE x.key < y.a
          """.stripMargin
        loadTestData(sqlStr, sl)
        spark.sqlContext.sparkSession.sql(sqlStr)
      }
      checkAnswer() { sl =>
        val sqlStr = """
          SELECT x.value, y.a, y.b FROM testData x JOIN testData2 y ON x.key < y.a
          """.stripMargin
        loadTestData(sqlStr, sl)
        spark.sqlContext.sparkSession.sql(sqlStr)
      }
    }
  }

  ignore("NaN and -0.0 in join keys") {
    def createTables(sl: SecurityLevel) = {
      val v1 = sl.applyTo(Seq(Float.NaN -> Double.NaN, 0.0f -> 0.0, -0.0f -> -0.0).toDF("f", "d"))
      v1.createOrReplaceTempView("v1")
      val v2 = sl.applyTo(Seq(Float.NaN -> Double.NaN, 0.0f -> 0.0, -0.0f -> -0.0).toDF("f", "d"))
      v1.createOrReplaceTempView("v2")
      val v3 = sl.applyTo(
        Seq((Array(-0.0f, 0.0f), Tuple2(-0.0d, Double.NaN), Seq(Tuple2(-0.0d, Double.NaN))))
          .toDF("arr", "stru", "arrOfStru")
      )
      v3.createOrReplaceTempView("v3")
      val v4 = sl.applyTo(
        Seq((Array(0.0f, -0.0f), Tuple2(0.0d, 0.0 / 0.0), Seq(Tuple2(0.0d, 0.0 / 0.0))))
          .toDF("arr", "stru", "arrOfStru")
      )
      v4.createOrReplaceTempView("v4")
      (v1, v2, v3, v4)
    }

    checkAnswer() { sl =>
      createTables(sl)
      val sqlStr = """
        |SELECT v1.f, v1.d, v2.f, v2.d
        |FROM v1 JOIN v2
        |ON v1.f = v2.f AND v1.d = v2.d
        """.stripMargin
      loadTestData(sqlStr, sl)
      spark.sqlContext.sparkSession.sql(sqlStr)
    }
    checkAnswer() { sl =>
      createTables(sl)
      val sqlStr = """
        |SELECT v1.f, v1.d, v2.f, v2.d
        |FROM v1 JOIN v2
        |ON
        |  array(v1.f) = array(v2.f) AND
        |  struct(v1.d) = struct(v2.d) AND
        |  array(struct(v1.f, v1.d)) = array(struct(v2.f, v2.d)) AND
        |  struct(array(v1.f), array(v1.d)) = struct(array(v2.f), array(v2.d))
        """.stripMargin
      loadTestData(sqlStr, sl)
      spark.sqlContext.sparkSession.sql(sqlStr)
    }
    checkAnswer() { sl =>
      createTables(sl)
      val sqlStr = """
        |SELECT v3.arr, v3.stru, v3.arrOfStru, v4.arr, v4.stru, v4.arrOfStru
        |FROM v3 JOIN v4
        |ON v3.arr = v4.arr AND v3.stru = v4.stru AND v3.arrOfStru = v4.arrOfStru
        """.stripMargin
      loadTestData(sqlStr, sl)
      spark.sqlContext.sparkSession.sql(sqlStr)
    }

    safeDropTables("v1", "v2", "v3", "v4")
  }
}

class SinglePartitionJoinSuite extends JoinSuite {
  override def numPartitions = 1
  override val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("SinglePartitionJoinSuite")
    .config("spark.sql.shuffle.partitions", numPartitions)
    .getOrCreate()

  runSQLQueries();
}

class MultiplePartitionJoinSuite extends JoinSuite {
  override def numPartitions = 3
  override val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("MultiplePartitionJoinSuite")
    .config("spark.sql.shuffle.partitions", numPartitions)
    .getOrCreate()

  runSQLQueries();
}
