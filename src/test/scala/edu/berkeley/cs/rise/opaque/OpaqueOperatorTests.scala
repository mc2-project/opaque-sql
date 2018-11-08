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

import java.io.File
import java.sql.Timestamp

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
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import edu.berkeley.cs.rise.opaque.benchmark._
import edu.berkeley.cs.rise.opaque.execution.EncryptedBlockRDDScanExec

trait OpaqueOperatorTests extends FunSuite with BeforeAndAfterAll { self =>
  def spark: SparkSession
  def numPartitions: Int

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }
  import testImplicits._

  private var path: File = null

  override def beforeAll(): Unit = {
    Utils.initSQLContext(spark.sqlContext)
    path = Utils.createTempDir()
    path.delete()
  }

  override def afterAll(): Unit = {
    spark.stop()
    Utils.deleteRecursively(path)
  }

  def testAgainstSpark(name: String)(f: SecurityLevel => Any): Unit = {
    test(name + " - encrypted") {
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
    val sparkLoggers = Seq("org.apache.spark", "org.apache.spark.executor.Executor")
    for (l <- sparkLoggers) LogManager.getLogger(l).setLevel(Level.OFF)
    try {
      f()
    } finally {
      for (l <- sparkLoggers) LogManager.getLogger(l).setLevel(Level.WARN)
    }
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
    val data: Seq[(CalendarInterval, Byte)] = Seq((new CalendarInterval(12, 12345), 0.toByte))
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

  testAgainstSpark("filter") { securityLevel =>
    val df = makeDF(
      (1 to 20).map(x => (true, "hello", 1.0, 2.0f, x)),
      securityLevel,
      "a", "b", "c", "d", "x")
    df.filter($"x" > lit(10)).collect
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
    p.join(f, $"pk" === $"fk").collect.toSet
  }

  def abc(i: Int): String = (i % 3) match {
    case 0 => "A"
    case 1 => "B"
    case 2 => "C"
  }

  testAgainstSpark("aggregate average") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), i.toDouble)
    val words = makeDF(data, securityLevel, "id", "category", "price")

    words.groupBy("category").agg(avg("price").as("avgPrice"))
      .collect.sortBy { case Row(category: String, _) => category }
  }

  testAgainstSpark("aggregate count") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "category", "price")

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
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "word", "count")

    words.groupBy("word").agg(sum("count").as("totalCount"))
      .collect.sortBy { case Row(word: String, _) => word }
  }

  testAgainstSpark("aggregate on multiple columns") { securityLevel =>
    val data = for (i <- 0 until 256) yield (abc(i), 1, 1.0f)
    val words = makeDF(data, securityLevel, "str", "x", "y")

    words.groupBy("str").agg(sum("y").as("totalY"), avg("x").as("avgX"))
      .collect.sortBy { case Row(str: String, _, _) => str }
  }

  testOpaqueOnly("global aggregate") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "word", "count")
    val result = words.agg(sum("count").as("totalCount"))
  }

  testAgainstSpark("contains") { securityLevel =>
    val data = for (i <- 0 until 256) yield(i.toString, abc(i))
    val df = makeDF(data, securityLevel, "word", "abc")
    df.filter($"word".contains(lit("1"))).collect
  }

  testAgainstSpark("year") { securityLevel =>
    val data = Seq(Tuple2(1, new java.sql.Date(new java.util.Date().getTime())))
    val df = makeDF(data, securityLevel, "id", "date")
    df.select(year($"date")).collect
  }

  testOpaqueOnly("save and load") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val df = makeDF(data, securityLevel, "id", "word", "count")
    df.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save(path.toString)
    val df2 = spark.read
      .format("edu.berkeley.cs.rise.opaque.EncryptedSource")
      .schema(df.schema)
      .load(path.toString)
    assert(df.collect.toSet === df2.collect.toSet)
    assert(df.groupBy("word").agg(sum("count")).collect.toSet
      === df2.groupBy("word").agg(sum("count")).collect.toSet)
  }

  testOpaqueOnly("cast error") { securityLevel =>
    val data: Seq[(CalendarInterval, Byte)] = Seq((new CalendarInterval(12, 12345), 0.toByte))
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

  testAgainstSpark("least squares") { securityLevel =>
    val answer = LeastSquares.query(spark, securityLevel, "tiny", numPartitions).collect
    answer
  }

  testOpaqueOnly("pagerank") { securityLevel =>
    PageRank.run(spark, securityLevel, "256", numPartitions)
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
      .map {
        case (str: String, f: Float) => (str, "%.2f".format(f))
      }
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

  testAgainstSpark("join with different numbers of partitions (#34)") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield ((i % 16).toString, (i * 10).toString, i.toFloat)
    val p = makeDF(p_data, securityLevel, "pk", "x")
    val f = makePartitionedDF(f_data, securityLevel, numPartitions + 1, "fk", "x", "y")
    p.join(f, $"pk" === $"fk").collect.toSet
  }
}
