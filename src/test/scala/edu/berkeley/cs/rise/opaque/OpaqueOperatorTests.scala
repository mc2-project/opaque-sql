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

import java.sql.Timestamp

import scala.collection.mutable
import scala.util.Random

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.CalendarInterval
import org.scalactic.Equality
import org.scalactic.TolerantNumerics
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import edu.berkeley.cs.rise.opaque.benchmark._
import edu.berkeley.cs.rise.opaque.execution.EncryptedBlockRDDScanExec
import edu.berkeley.cs.rise.opaque.expressions.DotProduct.dot
import edu.berkeley.cs.rise.opaque.expressions.VectorMultiply.vectormultiply
import edu.berkeley.cs.rise.opaque.expressions.VectorSum

trait OpaqueOperatorTests extends FunSuite with BeforeAndAfterAll { self =>
  def spark: SparkSession
  def numPartitions: Int

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }
  import testImplicits._

  override def beforeAll(): Unit = {
    Utils.initSQLContext(spark.sqlContext)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  private def equalityToArrayEquality[A : Equality](): Equality[Array[A]] = {
    new Equality[Array[A]] {
      def areEqual(a: Array[A], b: Any): Boolean = {
        b match {
          case b: Array[_] =>
            (a.length == b.length
              && a.zip(b).forall {
                case (x, y) => implicitly[Equality[A]].areEqual(x, y)
              })
          case _ => false
        }
      }
      override def toString: String = s"TolerantArrayEquality"
    }
  }

  // Modify the behavior of === for Double and Array[Double] to use a numeric tolerance
  implicit val tolerantDoubleEquality = TolerantNumerics.tolerantDoubleEquality(1e-6)
  implicit val tolerantDoubleArrayEquality = equalityToArrayEquality[Double]

  def testAgainstSpark[A : Equality](name: String)(f: SecurityLevel => A): Unit = {
    test(name + " - encrypted") {
      // The === operator uses implicitly[Equality[A]], which compares Double and Array[Double]
      // using the numeric tolerance specified above
      assert(f(Encrypted) === f(Insecure))
    }
  }

  def testOpaqueOnly(name: String)(f: SecurityLevel => Unit): Unit = {
    test(name + " - encrypted") {
      f(Encrypted)
    }
  }

  def testSparkOnly(name: String)(f: SecurityLevel => Unit): Unit = {
    test(name + " - Spark") {
      f(Insecure)
    }
  }

  def withLoggingOff[A](f: () => A): A = {
    val sparkLoggers = Seq(
      "org.apache.spark",
      "org.apache.spark.executor.Executor",
      "org.apache.spark.scheduler.TaskSetManager")
    val logLevels = new mutable.HashMap[String, Level]
    for (l <- sparkLoggers) {
      logLevels(l) = LogManager.getLogger(l).getLevel
      LogManager.getLogger(l).setLevel(Level.OFF)
    }
    try {
      f()
    } finally {
      for (l <- sparkLoggers) {
        LogManager.getLogger(l).setLevel(logLevels(l))
      }
    }
  }

  /** Modified from https://stackoverflow.com/questions/33193958/change-nullable-property-of-column-in-spark-dataframe
    * and https://stackoverflow.com/questions/32585670/what-is-the-best-way-to-define-custom-methods-on-a-dataframe
    * Set nullable property of column.
    * @param cn is the column name to change
    * @param nullable is the flag to set, such that the column is  either nullable or not
    */
  object ExtraDFOperations {
    implicit class AlternateDF(df : DataFrame) {
      def setNullableStateOfColumn(cn: String, nullable: Boolean) : DataFrame = {
        // get schema
        val schema = df.schema
        // modify [[StructField] with name `cn`
        val newSchema = StructType(schema.map {
          case StructField( c, t, _, m) if c.equals(cn) => StructField( c, t, nullable = nullable, m)
          case y: StructField => y
        })
        // apply new schema
        df.sqlContext.createDataFrame( df.rdd, newSchema )
      }
    }
  }

  import ExtraDFOperations._

  testAgainstSpark("Interval SQL") { securityLevel =>
    val data = Seq(Tuple2(1, new java.sql.Date(new java.util.Date().getTime())))
    val df = makeDF(data, securityLevel, "index", "time")
    df.createTempView("Interval")
    try {
      spark.sql("SELECT time + INTERVAL 7 DAY FROM Interval").collect
    } finally {
      spark.catalog.dropTempView("Interval")
    }
  }

  testAgainstSpark("Interval Week SQL") { securityLevel =>
    val data = Seq(Tuple2(1, new java.sql.Date(new java.util.Date().getTime())))
    val df = makeDF(data, securityLevel, "index", "time")
    df.createTempView("Interval")
    try {
      spark.sql("SELECT time + INTERVAL 7 WEEK FROM Interval").collect
    } finally {
      spark.catalog.dropTempView("Interval")
    }
  }

  testAgainstSpark("Interval Month SQL") { securityLevel =>
    val data = Seq(Tuple2(1, new java.sql.Date(new java.util.Date().getTime())))
    val df = makeDF(data, securityLevel, "index", "time")
    df.createTempView("Interval")
    try {
      spark.sql("SELECT time + INTERVAL 6 MONTH FROM Interval").collect
    } finally {
      spark.catalog.dropTempView("Interval")
    }
  }

  testAgainstSpark("Date Add") { securityLevel =>
    val data = Seq(Tuple2(1, new java.sql.Date(new java.util.Date().getTime())))
    val df = makeDF(data, securityLevel, "index", "time")
    df.select(date_add($"time", 3)).collect
  }

  testAgainstSpark("create DataFrame from sequence") { securityLevel =>
    val data = for (i <- 0 until 5) yield ("foo", i)
    makeDF(data, securityLevel, "word", "count").collect
  }
  
  testAgainstSpark("create DataFrame with BinaryType + ByteType") { securityLevel =>
    val data: Seq[(Array[Byte], Byte)] =
      Seq((Array[Byte](0.toByte, -128.toByte, 127.toByte), 42.toByte))
    makeDF(data, securityLevel, "BinaryType", "ByteType").collect
  }
  
  testAgainstSpark("create DataFrame with CalendarIntervalType + NullType") { securityLevel =>
    val data: Seq[(CalendarInterval, Byte)] = Seq((new CalendarInterval(12, 1, 12345), 0.toByte))
    val schema = StructType(Seq(
      StructField("CalendarIntervalType", CalendarIntervalType),
      StructField("NullType", NullType)))
  
    securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema)).collect
  }
  
  testAgainstSpark("create DataFrame with ShortType + TimestampType") { securityLevel =>
    val data: Seq[(Short, Timestamp)] = Seq((13.toShort, Timestamp.valueOf("2017-12-02 03:04:00")))
    makeDF(data, securityLevel, "ShortType", "TimestampType").collect
  }
  
  testAgainstSpark("create DataFrame with ArrayType") { securityLevel =>
    val array: Array[Int] = Array(0, -128, 127, 1)
    val data = Seq(
      (array, "dog"),
      (array, "cat"),
      (array, "ant"))
    val df = makeDF(data, securityLevel, "array", "string")
    df.collect
  }
  
  testAgainstSpark("create DataFrame with MapType") { securityLevel =>
    val map: Map[String, Int] = Map("x" -> 24, "y" -> 25, "z" -> 26)
    val data = Seq(
      (map, "dog"),
      (map, "cat"),
      (map, "ant"))
    val df = makeDF(data, securityLevel, "map", "string")
    df.collect
  }
  
  testAgainstSpark("create DataFrame with nulls for all types") { securityLevel =>
    val schema = StructType(Seq(
      StructField("boolean", BooleanType),
      StructField("integer", IntegerType),
      StructField("long", LongType),
      StructField("float", FloatType),
      StructField("double", DoubleType),
      StructField("date", DateType),
      StructField("binary", BinaryType),
      StructField("byte", ByteType),
      StructField("calendar_interval", CalendarIntervalType),
      StructField("null", NullType),
      StructField("short", ShortType),
      StructField("timestamp", TimestampType),
      StructField("array_of_int", DataTypes.createArrayType(IntegerType)),
      StructField("map_int_to_int", DataTypes.createMapType(IntegerType, IntegerType)),
      StructField("string", StringType)))
  
    securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(Seq(Row.fromSeq(Seq.fill(schema.length) { null })), numPartitions),
        schema)).collect
  }
  
  testAgainstSpark("filter") { securityLevel =>
    val df = makeDF(
      (1 to 20).map(x => (true, "hello", 1.0, 2.0f, x)),
      securityLevel,
      "a", "b", "c", "d", "x")
    df.filter($"x" > lit(10)).collect
  }

  testAgainstSpark("filter with NULLs") { securityLevel =>
    val data: Seq[Tuple1[Integer]] = Random.shuffle((0 until 256).map(x => {
      if (x % 3 == 0)
        Tuple1(null.asInstanceOf[Integer])
      else
        Tuple1(x.asInstanceOf[Integer])
    }).toSeq)
    val df = makeDF(data, securityLevel, "x")
    df.filter($"x" > lit(10)).collect.toSet
  }

  testAgainstSpark("select") { securityLevel =>
    val data = for (i <- 0 until 256) yield ("%03d".format(i) * 3, i.toFloat)
    val df = makeDF(data, securityLevel, "str", "x")
    df.select($"str").collect
  }
  
  testAgainstSpark("select with expressions") { securityLevel =>
    val df = makeDF(
      (1 to 20).map(x => (true, "hello world!", 1.0, 2.0f, x)),
      securityLevel,
      "a", "b", "c", "d", "x")
    df.select(
      $"x" + $"x" * $"x" - $"x",
      substring($"b", 5, 20),
      $"x" > $"x",
      $"x" >= $"x",
      $"x" <= $"x").collect.toSet
  }
  
  testAgainstSpark("union") { securityLevel =>
    val df1 = makeDF(
      (1 to 20).map(x => (x, x.toString)).reverse,
      securityLevel,
      "a", "b")
    val df2 = makeDF(
      (1 to 20).map(x => (x, (x + 1).toString)),
      securityLevel,
      "a", "b")
    df1.union(df2).collect.toSet
  }
  
  testOpaqueOnly("cache") { securityLevel =>
    def numCached(ds: Dataset[_]): Int =
      ds.queryExecution.executedPlan.collect {
        case cached: EncryptedBlockRDDScanExec
            if cached.rdd.getStorageLevel != StorageLevel.NONE =>
          cached
      }.size
  
    val data = List((1, 3), (1, 4), (1, 5), (2, 4))
    val df = makeDF(data, securityLevel, "a", "b").cache()
  
    val agg = df.groupBy($"a").agg(sum("b"))
  
    assert(numCached(agg) === 1)
  
    val expected = data.groupBy(_._1).mapValues(_.map(_._2).sum)
    assert(agg.collect.toSet === expected.map(Row.fromTuple).toSet)
    df.unpersist()
  }
  
  testAgainstSpark("sort") { securityLevel =>
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x)).toSeq)
    val df = makeDF(data, securityLevel, "str", "x")
    df.sort($"x").collect
  }
  
  testAgainstSpark("sort zero elements") { securityLevel =>
    val data = Seq.empty[(String, Int)]
    val df = makeDF(data, securityLevel, "str", "x")
    df.sort($"x").collect
  }
  
  testAgainstSpark("sort by float") { securityLevel =>
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x.toFloat)).toSeq)
    val df = makeDF(data, securityLevel, "str", "x")
    df.sort($"x").collect
  }
  
  testAgainstSpark("sort by string") { securityLevel =>
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x.toFloat)).toSeq)
    val df = makeDF(data, securityLevel, "str", "x")
    df.sort($"str").collect
  }
  
  testAgainstSpark("sort by 2 columns") { securityLevel =>
    val data = Random.shuffle((0 until 256).map(x => (x / 16, x)).toSeq)
    val df = makeDF(data, securityLevel, "x", "y")
    df.sort($"x", $"y").collect
  }

  testAgainstSpark("sort with null values") { securityLevel =>
    val data: Seq[Tuple1[Integer]] = Random.shuffle((0 until 256).map(x => {
      if (x % 3 == 0)
        Tuple1(null.asInstanceOf[Integer])
      else
        Tuple1(x.asInstanceOf[Integer])
    }).toSeq)
    val df = makeDF(data, securityLevel, "x")
    df.sort($"x").collect
  }

  testAgainstSpark("join") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i, i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield (i, (i % 16).toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id", "pk", "x")
    val f = makeDF(f_data, securityLevel, "id", "fk", "x")
    p.join(f, $"pk" === $"fk").collect.toSet
  }
  
  testAgainstSpark("join on column 1") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield ((i % 16).toString, (i * 10).toString, i.toFloat)
    val p = makeDF(p_data, securityLevel, "pk", "x")
    val f = makeDF(f_data, securityLevel, "fk", "x", "y")
    print(p.join(f, $"pk" === $"fk").queryExecution.executedPlan)
    p.join(f, $"pk" === $"fk").collect.toSet
  }
  
  testAgainstSpark("non-foreign-key join") { securityLevel =>
    val p_data = for (i <- 1 to 128) yield (i, (i % 16).toString, i * 10)
    val f_data = for (i <- 1 to 256 - 128) yield (i, (i % 16).toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id", "join_col_2", "x")
    p.join(f, $"join_col_1" === $"join_col_2").collect.toSet
  }
  
  def abc(i: Int): String = (i % 3) match {
    case 0 => "A"
    case 1 => "B"
    case 2 => "C"
  }
  
  testAgainstSpark("aggregate average") { securityLevel =>
    val data = (0 until 256).map{ i =>
      if (i % 3 == 0 || (i + 1) % 6 == 0)
        (i, abc(i), None)
      else
        (i, abc(i), Some(i.toDouble))
    }.toSeq
    val words = makeDF(data, securityLevel, "id", "category", "price")
    words.setNullableStateOfColumn("price", true)

    val df = words.groupBy("category").agg(avg("price").as("avgPrice"))
    df.collect.sortBy { case Row(category: String, _) => category }
  }
  
  testAgainstSpark("aggregate count") { securityLevel =>
    val data = (0 until 256).map{ i =>
      if (i % 3 == 0 || (i + 1) % 6 == 0)
        (i, abc(i), None)
      else 
        (i, abc(i), Some(i))
    }.toSeq
    val words = makeDF(data, securityLevel, "id", "category", "price")
    words.setNullableStateOfColumn("price", true)
    words.groupBy("category").agg(count("category").as("itemsInCategory"))
      .collect.sortBy { case Row(category: String, _) => category }
  }
  
  testAgainstSpark("aggregate first") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "category", "price")
  
    words.groupBy("category").agg(first("category").as("firstInCategory"))
      .collect.sortBy { case Row(category: String, _) => category }
  }
  
  testAgainstSpark("aggregate last") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "category", "price")
  
    words.groupBy("category").agg(last("category").as("lastInCategory"))
      .collect.sortBy { case Row(category: String, _) => category }
  }
  
  testAgainstSpark("aggregate max") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "category", "price")
  
    words.groupBy("category").agg(max("price").as("maxPrice"))
      .collect.sortBy { case Row(category: String, _) => category }
  }
  
  testAgainstSpark("aggregate min") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "category", "price")
  
    words.groupBy("category").agg(min("price").as("minPrice"))
      .collect.sortBy { case Row(category: String, _) => category }
  }
  
  testAgainstSpark("aggregate sum") { securityLevel =>
    val data = (0 until 256).map{ i =>
      if (i % 3 == 0 || i % 4 == 0)
        (i, abc(i), None)
      else
        (i, abc(i), Some(i.toDouble))
    }.toSeq

    val words = makeDF(data, securityLevel, "id", "word", "count")
    words.setNullableStateOfColumn("count", true)

    words.groupBy("word").agg(sum("count").as("totalCount"))
      .collect.sortBy { case Row(word: String, _) => word }
  }
  
  testAgainstSpark("aggregate on multiple columns") { securityLevel =>
    val data = for (i <- 0 until 256) yield (abc(i), 1, 1.0f)
    val words = makeDF(data, securityLevel, "str", "x", "y")
  
    words.groupBy("str").agg(sum("y").as("totalY"), avg("x").as("avgX"))
      .collect.sortBy { case Row(str: String, _, _) => str }
  }

  testAgainstSpark("skewed aggregate sum") { securityLevel =>
    val data = Random.shuffle((0 until 256).map(i => {
        (i, abc(123), 1)
    }).toSeq)

    val words = makeDF(data, securityLevel, "id", "word", "count")
    words.groupBy("word").agg(sum("count").as("totalCount"))
      .collect.sortBy { case Row(word: String, _) => word }
  }

  testAgainstSpark("grouping aggregate with 0 rows") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "word", "count")
    words.filter($"id" < lit(0)).groupBy("word").agg(sum("count"))
      .collect.sortBy { case Row(word: String, _) => word }
  }

  testAgainstSpark("global aggregate") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "word", "count")
    words.agg(sum("count").as("totalCount")).collect
  }

  testAgainstSpark("global aggregate with 0 rows") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "word", "count")
    val result = words.filter($"id" < lit(0)).agg(count("*")).as("totalCount")
    result.collect
  }

  testAgainstSpark("contains") { securityLevel =>
    val data = for (i <- 0 until 256) yield(i.toString, abc(i))
    val df = makeDF(data, securityLevel, "word", "abc")
    df.filter($"word".contains(lit("1"))).collect
  }

  testAgainstSpark("between") { securityLevel =>
    val data = for (i <- 0 until 256) yield(i.toString, i)
    val df = makeDF(data, securityLevel, "word", "count")
    df.filter($"count".between(50, 150)).collect
  }

  testAgainstSpark("year") { securityLevel =>
    val data = Seq(Tuple2(1, new java.sql.Date(new java.util.Date().getTime())))
    val df = makeDF(data, securityLevel, "id", "date")
    df.select(year($"date")).collect
  }

  testAgainstSpark("case when - 1 branch with else (string)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.select(when(df("word") === "foo", "hi").otherwise("bye")).collect
  }

  testAgainstSpark("case when - 1 branch with else (int)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.select(when(df("word") === "foo", 10).otherwise(30)).collect
  }

  testAgainstSpark("case when - 1 branch without else (string)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.select(when(df("word") === "foo", "hi")).collect
  }

  testAgainstSpark("case when - 1 branch without else (int)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.select(when(df("word") === "foo", 10)).collect 
  }

  testAgainstSpark("case when - 2 branch with else (string)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.select(when(df("word") === "foo", "hi").when(df("word") === "baz", "hello").otherwise("bye")).collect
  }

  testAgainstSpark("case when - 2 branch with else (int)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.select(when(df("word") === "foo", 10).when(df("word") === "baz", 20).otherwise(30)).collect
  }

  testAgainstSpark("case when - 2 branch without else (string)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.select(when(df("word") === "foo", "hi").when(df("word") === "baz", "hello")).collect 
  }

  testAgainstSpark("case when - 2 branch without else (int)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.select(when(df("word") === "foo", 3).when(df("word") === "baz", 2)).collect
  }

  testAgainstSpark("LIKE - Contains") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.filter($"word".like("%a%")).collect
  } 

  testAgainstSpark("LIKE - StartsWith") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.filter($"word".like("ba%")).collect
  } 

  testAgainstSpark("LIKE - EndsWith") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.filter($"word".like("%ar")).collect
  }

  testAgainstSpark("LIKE - Empty Pattern") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.filter($"word".like("")).collect
  }

  testAgainstSpark("LIKE - Match All") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.filter($"word".like("%")).collect
  }

  testAgainstSpark("LIKE - Single Wildcard") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.filter($"word".like("ba_")).collect
  }

  testAgainstSpark("LIKE - SQL API") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.createTempView("df")
    try {
      spark.sql(""" SELECT word FROM df WHERE word LIKE '_a_' """).collect
    } finally {
      spark.catalog.dropTempView("df")
    }
  }

  testOpaqueOnly("save and load with explicit schema") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val df = makeDF(data, securityLevel, "id", "word", "count")
    val path = Utils.createTempDir()
    path.delete()
    df.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save(path.toString)
    try {
      val df2 = spark.read
        .format("edu.berkeley.cs.rise.opaque.EncryptedSource")
        .schema(df.schema)
        .load(path.toString)
      assert(df.collect.toSet === df2.collect.toSet)
      assert(df.groupBy("word").agg(sum("count")).collect.toSet
        === df2.groupBy("word").agg(sum("count")).collect.toSet)
    } finally {
      Utils.deleteRecursively(path)
    }
  }
  
  testOpaqueOnly("save and load without schema") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val df = makeDF(data, securityLevel, "id", "word", "count")
    val path = Utils.createTempDir()
    path.delete()
    df.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save(path.toString)
    try {
      val df2 = spark.read
        .format("edu.berkeley.cs.rise.opaque.EncryptedSource")
        .load(path.toString)
      assert(df.collect.toSet === df2.collect.toSet)
      assert(df.groupBy("word").agg(sum("count")).collect.toSet
        === df2.groupBy("word").agg(sum("count")).collect.toSet)
    } finally {
      Utils.deleteRecursively(path)
    }
  }
  
  testOpaqueOnly("load from SQL with explicit schema") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val df = makeDF(data, securityLevel, "id", "word", "count")
    val path = Utils.createTempDir()
    path.delete()
    df.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save(path.toString)
  
    try {
      spark.sql(s"""
        |CREATE TEMPORARY VIEW df2
        |(${df.schema.toDDL})
        |USING edu.berkeley.cs.rise.opaque.EncryptedSource
        |OPTIONS (
        |  path "${path}"
        |)""".stripMargin)
      val df2 = spark.sql(s"""
        |SELECT * FROM df2
        |""".stripMargin)
  
      assert(df.collect.toSet === df2.collect.toSet)
    } finally {
      spark.catalog.dropTempView("df2")
      Utils.deleteRecursively(path)
    }
  }
  
  testOpaqueOnly("load from SQL without schema") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val df = makeDF(data, securityLevel, "id", "word", "count")
    val path = Utils.createTempDir()
    path.delete()
    df.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save(path.toString)
  
    try {
      spark.sql(s"""
        |CREATE TEMPORARY VIEW df2
        |USING edu.berkeley.cs.rise.opaque.EncryptedSource
        |OPTIONS (
        |  path "${path}"
        |)""".stripMargin)
      val df2 = spark.sql(s"""
        |SELECT * FROM df2
        |""".stripMargin)
  
      assert(df.collect.toSet === df2.collect.toSet)
    } finally {
      spark.catalog.dropTempView("df2")
      Utils.deleteRecursively(path)
    }
  }
  
  testAgainstSpark("SQL API") { securityLevel =>
    val df = makeDF(
      (1 to 20).map(x => (true, "hello", 1.0, 2.0f, x)),
      securityLevel,
      "a", "b", "c", "d", "x")
    df.createTempView("df")
    try {
      spark.sql("SELECT * FROM df WHERE x > 10").collect
    } finally {
      spark.catalog.dropTempView("df")
    }
  }
  
  testOpaqueOnly("cast error") { securityLevel =>
    val data: Seq[(CalendarInterval, Byte)] = Seq((new CalendarInterval(12, 1, 12345), 0.toByte))
    val schema = StructType(Seq(
      StructField("CalendarIntervalType", CalendarIntervalType),
      StructField("NullType", NullType)))
    val df = securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema))
    // Trigger an Opaque exception by attempting an unsupported cast: CalendarIntervalType to
    // StringType
    val e = intercept[SparkException] {
      withLoggingOff {
        df.select($"CalendarIntervalType".cast(StringType)).collect
      }
    }
    assert(e.getCause.isInstanceOf[OpaqueException])
  }
  
  testAgainstSpark("exp") { securityLevel =>
    val data: Seq[(Double, Double)] = Seq(
      (2.0, 3.0))
    val schema = StructType(Seq(
      StructField("x", DoubleType),
      StructField("y", DoubleType)))
  
    val df = securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema))
    print(df.select(exp($"y")).queryExecution.executedPlan)
    df.select(exp($"y")).collect
  }
  
  testAgainstSpark("vector multiply") { securityLevel =>
    val data: Seq[(Array[Double], Double)] = Seq(
      (Array[Double](1.0, 1.0, 1.0), 3.0))
    val schema = StructType(Seq(
      StructField("v", DataTypes.createArrayType(DoubleType)),
      StructField("c", DoubleType)))
  
    val df = securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema))
  
    df.select(vectormultiply($"v", $"c")).collect
  }
  
  testAgainstSpark("dot product") { securityLevel =>
    val data: Seq[(Array[Double], Array[Double])] = Seq(
      (Array[Double](1.0, 1.0, 1.0), Array[Double](1.0, 1.0, 1.0)))
    val schema = StructType(Seq(
      StructField("v1", DataTypes.createArrayType(DoubleType)),
      StructField("v2", DataTypes.createArrayType(DoubleType))))
  
    val df = securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema))
  
    df.select(dot($"v1", $"v2")).collect
  }

  testAgainstSpark("upper") { securityLevel =>
    val data = Seq(("lower", "upper"), ("lower2", "upper2"))
    val schema = StructType(Seq(
      StructField("v1", StringType),
      StructField("v2", StringType)))

    val df = securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema))

    df.select(upper($"v1")).collect
  }

  testAgainstSpark("upper with null") { securityLevel =>
    val data = Seq(("lower", null.asInstanceOf[String]))

    val df = makeDF(data, securityLevel, "v1", "v2")

    df.select(upper($"v2")).collect
  }

  testAgainstSpark("vector sum") { securityLevel =>
    val data: Seq[(Array[Double], Double)] = Seq(
      (Array[Double](1.0, 2.0, 3.0), 4.0),
      (Array[Double](5.0, 7.0, 7.0), 8.0))
    val schema = StructType(Seq(
      StructField("v", DataTypes.createArrayType(DoubleType)),
      StructField("c", DoubleType)))
  
    val df = securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema))
  
    val vectorsum = new VectorSum
    df.groupBy().agg(vectorsum($"v")).collect
  }
  
  testAgainstSpark("create array") { securityLevel =>
    val data: Seq[(Double, Double)] = Seq(
      (1.0, 2.0),
      (3.0, 4.0))
    val schema = StructType(Seq(
      StructField("x1", DoubleType),
      StructField("x2", DoubleType)))
  
    val df = securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema))
  
    df.select(array($"x1", $"x2").as("x")).collect
  }

  testAgainstSpark("limit with fewer returned values") { securityLevel =>
    val data = Random.shuffle(for (i <- 0 until 256) yield (i, abc(i)))
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("word", StringType)))
    val df = securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema))
    df.sort($"id").limit(5).collect
  }

  testAgainstSpark("limit with more returned values") { securityLevel =>
    val data = Random.shuffle(for (i <- 0 until 256) yield (i, abc(i)))
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("word", StringType)))
    val df = securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema))
    df.sort($"id").limit(200).collect
  }

  testAgainstSpark("least squares") { securityLevel =>
    LeastSquares.query(spark, securityLevel, "tiny", numPartitions).collect
  }
  
  testAgainstSpark("logistic regression") { securityLevel =>
    LogisticRegression.train(spark, securityLevel, 1000, numPartitions)
  }
  
  testAgainstSpark("k-means") { securityLevel =>
    import scala.math.Ordering.Implicits.seqDerivedOrdering
    KMeans.train(spark, securityLevel, numPartitions, 10, 2, 3, 0.01).map(_.toSeq).sorted
  }
  
  testAgainstSpark("pagerank") { securityLevel =>
    PageRank.run(spark, securityLevel, "256", numPartitions).collect.toSet
  }
  
  testAgainstSpark("TPC-H 9") { securityLevel =>
    TPCH.tpch9(spark.sqlContext, securityLevel, "sf_small", numPartitions).collect.toSet
  }
  
  testAgainstSpark("big data 1") { securityLevel =>
    BigDataBenchmark.q1(spark, securityLevel, "tiny", numPartitions).collect
  }
  
  testAgainstSpark("big data 2") { securityLevel =>
    BigDataBenchmark.q2(spark, securityLevel, "tiny", numPartitions).collect
      .map { case Row(a: String, b: Double) => (a, b.toFloat) }
      .sortBy(_._1)
  }
  
  testAgainstSpark("big data 3") { securityLevel =>
    BigDataBenchmark.q3(spark, securityLevel, "tiny", numPartitions).collect
  }

  def makeDF[A <: Product : scala.reflect.ClassTag : scala.reflect.runtime.universe.TypeTag](
    data: Seq[A], securityLevel: SecurityLevel, columnNames: String*): DataFrame =
    securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data, numPartitions))
        .toDF(columnNames: _*))

}

class OpaqueSinglePartitionSuite extends OpaqueOperatorTests {
  override val spark = SparkSession.builder()
    .master("local[1]")
    .appName("QEDSuite")
    .config("spark.sql.shuffle.partitions", 1)
    .getOrCreate()

  override def numPartitions: Int = 1
}

class OpaqueMultiplePartitionSuite extends OpaqueOperatorTests {
  override val spark = SparkSession.builder()
    .master("local[1]")
    .appName("QEDSuite")
    .config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

  override def numPartitions: Int = 3

  import testImplicits._

  def makePartitionedDF[
      A <: Product : scala.reflect.ClassTag : scala.reflect.runtime.universe.TypeTag](
      data: Seq[A], securityLevel: SecurityLevel, numPartitions: Int, columnNames: String*)
    : DataFrame = {
    securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data, numPartitions))
        .toDF(columnNames: _*))
  }

  // FIXME: add integrity support for ecalls on dataframes with different numbers of partitions
  // testAgainstSpark("join with different numbers of partitions (#34)") { securityLevel =>
  //   val p_data = for (i <- 1 to 16) yield (i.toString, i * 10)
  //   val f_data = for (i <- 1 to 256 - 16) yield ((i % 16).toString, (i * 10).toString, i.toFloat)
  //   val p = makeDF(p_data, securityLevel, "pk", "x")
  //   val f = makePartitionedDF(f_data, securityLevel, numPartitions + 1, "fk", "x", "y")
  //   p.join(f, $"pk" === $"fk").collect.toSet
  // }
  
  testAgainstSpark("non-foreign-key join with high skew") { securityLevel =>
    // This test is intended to ensure that primary groups are never split across multiple
    // partitions, which would break our implementation of non-foreign-key join.
  
    val p_data = for (i <- 1 to 128) yield (i, 1)
    val f_data = for (i <- 1 to 128) yield (i, 1)
    val p = makeDF(p_data, securityLevel, "id", "join_col_1")
    val f = makeDF(f_data, securityLevel, "id", "join_col_2")
    p.join(f, $"join_col_1" === $"join_col_2").collect.toSet
  }

}
