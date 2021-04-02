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

import scala.util.Random

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

import edu.berkeley.cs.rise.opaque.benchmark._
import edu.berkeley.cs.rise.opaque.execution.EncryptedBlockRDDScanExec
import edu.berkeley.cs.rise.opaque.expressions.Decrypt.decrypt
import edu.berkeley.cs.rise.opaque.expressions.DotProduct.dot
import edu.berkeley.cs.rise.opaque.expressions.VectorMultiply.vectormultiply
import edu.berkeley.cs.rise.opaque.expressions.VectorSum

trait OpaqueOperatorTests extends OpaqueTestsBase { self =>

    protected object testImplicits extends SQLImplicits {
      protected override def _sqlContext: SQLContext = self.spark.sqlContext
    }
    import testImplicits._

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

  def integrityCollect(df: DataFrame): Seq[Row] = {
    JobVerificationEngine.resetForNextJob()
    val retval = df.collect
    val postVerificationPasses = Utils.verifyJob(df)
    if (!postVerificationPasses) {
      println("Job Verification Failure")
    }
    return retval
  }

  testAgainstSpark("Interval SQL") { securityLevel =>
    val data = Seq(Tuple2(1, new java.sql.Date(new java.util.Date().getTime())))
    val df = makeDF(data, securityLevel, "index", "time")
    df.createTempView("Interval")
    try {
      integrityCollect(spark.sql("SELECT time + INTERVAL 7 DAY FROM Interval"))
    } finally {
      spark.catalog.dropTempView("Interval")
    }
  }

  testAgainstSpark("Interval Week SQL") { securityLevel =>
    val data = Seq(Tuple2(1, new java.sql.Date(new java.util.Date().getTime())))
    val df = makeDF(data, securityLevel, "index", "time")
    df.createTempView("Interval")
    try {
      integrityCollect(spark.sql("SELECT time + INTERVAL 7 WEEK FROM Interval"))
    } finally {
      spark.catalog.dropTempView("Interval")
    }
  }

  testAgainstSpark("Interval Month SQL") { securityLevel =>
    val data = Seq(Tuple2(1, new java.sql.Date(new java.util.Date().getTime())))
    val df = makeDF(data, securityLevel, "index", "time")
    df.createTempView("Interval")
    try {
      integrityCollect(spark.sql("SELECT time + INTERVAL 6 MONTH FROM Interval"))
    } finally {
      spark.catalog.dropTempView("Interval")
    }
  }

  testAgainstSpark("Date Add") { securityLevel =>
    val data = Seq(Tuple2(1, new java.sql.Date(new java.util.Date().getTime())))
    val df = makeDF(data, securityLevel, "index", "time")
    integrityCollect(df.select(date_add($"time", 3)))
  }

  testAgainstSpark("create DataFrame from sequence") { securityLevel =>
    val data = for (i <- 0 until 5) yield ("foo", i)
    integrityCollect(makeDF(data, securityLevel, "word", "count"))
  }

  testAgainstSpark("create DataFrame with BinaryType + ByteType") { securityLevel =>
    val data: Seq[(Array[Byte], Byte)] =
      Seq((Array[Byte](0.toByte, -128.toByte, 127.toByte), 42.toByte))
    integrityCollect(makeDF(data, securityLevel, "BinaryType", "ByteType"))
  }

  testAgainstSpark("create DataFrame with CalendarIntervalType + NullType") { securityLevel =>
    val data: Seq[(CalendarInterval, Byte)] = Seq((new CalendarInterval(12, 1, 12345), 0.toByte))
    val schema = StructType(Seq(
      StructField("CalendarIntervalType", CalendarIntervalType),
      StructField("NullType", NullType)))

    integrityCollect(securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema)))
  }

  testAgainstSpark("create DataFrame with ShortType + TimestampType") { securityLevel =>
    val data: Seq[(Short, Timestamp)] = Seq((13.toShort, Timestamp.valueOf("2017-12-02 03:04:00")))
    integrityCollect(makeDF(data, securityLevel, "ShortType", "TimestampType"))
  }

  testAgainstSpark("create DataFrame with ArrayType") { securityLevel =>
    val array: Array[Int] = Array(0, -128, 127, 1)
    val data = Seq(
      (array, "dog"),
      (array, "cat"),
      (array, "ant"))
    val df = makeDF(data, securityLevel, "array", "string")
    integrityCollect(df)
  }

  testAgainstSpark("create DataFrame with MapType") { securityLevel =>
    val map: Map[String, Int] = Map("x" -> 24, "y" -> 25, "z" -> 26)
    val data = Seq(
      (map, "dog"),
      (map, "cat"),
      (map, "ant"))
    val df = makeDF(data, securityLevel, "map", "string")
    integrityCollect(df)
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

    integrityCollect(securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(Seq(Row.fromSeq(Seq.fill(schema.length) { null })), numPartitions),
        schema)))
  }

  testAgainstSpark("filter") { securityLevel =>
    val df = makeDF(
      (1 to 20).map(x => (true, "hello", 1.0, 2.0f, x)),
      securityLevel,
      "a", "b", "c", "d", "x")
    integrityCollect(df.filter($"x" > lit(10)))
  }

  testAgainstSpark("filter with NULLs") { securityLevel =>
    val data: Seq[Tuple1[Integer]] = Random.shuffle((0 until 256).map(x => {
      if (x % 3 == 0)
        Tuple1(null.asInstanceOf[Integer])
      else
        Tuple1(x.asInstanceOf[Integer])
    }).toSeq)
    val df = makeDF(data, securityLevel, "x")
    integrityCollect(df.filter($"x" > lit(10))).toSet
  }

  testAgainstSpark("select") { securityLevel =>
    val data = for (i <- 0 until 256) yield ("%03d".format(i) * 3, i.toFloat)
    val df = makeDF(data, securityLevel, "str", "x")
    integrityCollect(df.select($"str"))
  }

  testAgainstSpark("select with expressions") { securityLevel =>
    val df = makeDF(
      (1 to 20).map(x => (true, "hello world!", 1.0, 2.0f, x)),
      securityLevel,
      "a", "b", "c", "d", "x")
    integrityCollect(df.select(
      $"x" + $"x" * $"x" - $"x",
      substring($"b", 5, 20),
      $"x" > $"x",
      $"x" >= $"x",
      $"x" <= $"x")).toSet
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
    integrityCollect(df1.union(df2)).toSet
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
    assert(integrityCollect(agg).toSet === expected.map(Row.fromTuple).toSet)
    df.unpersist()
  }

  testAgainstSpark("sort") { securityLevel =>
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x)).toSeq)
    val df = makeDF(data, securityLevel, "str", "x")
    integrityCollect(df.sort($"x"))
  }

  testAgainstSpark("sort zero elements") { securityLevel =>
    val data = Seq.empty[(String, Int)]
    val df = makeDF(data, securityLevel, "str", "x")
    integrityCollect(df.sort($"x"))
  }

  testAgainstSpark("sort by float") { securityLevel =>
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x.toFloat)).toSeq)
    val df = makeDF(data, securityLevel, "str", "x")
    integrityCollect(df.sort($"x"))
  }

  testAgainstSpark("sort by string") { securityLevel =>
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x.toFloat)).toSeq)
    val df = makeDF(data, securityLevel, "str", "x")
    integrityCollect(df.sort($"str"))
  }

  testAgainstSpark("sort by 2 columns") { securityLevel =>
    val data = Random.shuffle((0 until 256).map(x => (x / 16, x)).toSeq)
    val df = makeDF(data, securityLevel, "x", "y")
    integrityCollect(df.sort($"x", $"y"))
  }

  testAgainstSpark("sort with null values") { securityLevel =>
    val data: Seq[Tuple1[Integer]] = Random.shuffle((0 until 256).map(x => {
      if (x % 3 == 0)
        Tuple1(null.asInstanceOf[Integer])
      else
        Tuple1(x.asInstanceOf[Integer])
    }).toSeq)
    val df = makeDF(data, securityLevel, "x")
    integrityCollect(df.sort($"x"))
  }

  testAgainstSpark("join") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i, i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield (i, (i % 16).toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id", "pk", "x")
    val f = makeDF(f_data, securityLevel, "id", "fk", "x")
    integrityCollect(p.join(f, $"pk" === $"fk")).toSet
  }

  testAgainstSpark("join on column 1") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield ((i % 16).toString, (i * 10).toString, i.toFloat)
    val p = makeDF(p_data, securityLevel, "pk", "x")
    val f = makeDF(f_data, securityLevel, "fk", "x", "y")
    integrityCollect(p.join(f, $"pk" === $"fk")).toSet
  }

  testAgainstSpark("non-foreign-key join") { securityLevel =>
    val p_data = for (i <- 1 to 128) yield (i, (i % 16).toString, i * 10)
    val f_data = for (i <- 1 to 256 - 128) yield (i, (i % 16).toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id", "join_col_2", "x")
    integrityCollect(p.join(f, $"join_col_1" === $"join_col_2")).toSet
  }

  testAgainstSpark("left semi join") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i, (i % 8).toString, i * 10)
    val f_data = for (i <- 1 to 32) yield (i, (i % 8).toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id1", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id2", "join_col_2", "y")
    val df = p.join(f, $"join_col_1" === $"join_col_2", "left_semi").sort($"join_col_1", $"id1")
    integrityCollect(df) 
  }
  
  testAgainstSpark("left semi join with condition") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i, (i % 8).toString, i * 10)
    val f_data = for (i <- 1 to 32) yield (i, (i % 8).toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id1", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id2", "join_col_2", "y")
    val df = p.join(f, $"join_col_1" === $"join_col_2" && $"x" > $"y", "left_semi").sort($"join_col_1", $"id1")
    integrityCollect(df)
  }

  testAgainstSpark("non-equi left semi join") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i, (i % 8).toString, i * 10)
    val f_data = for (i <- 1 to 32) yield (i, (i % 8).toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id1", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id2", "join_col_2", "x")
    val df = p.join(f, $"join_col_1" >= $"join_col_2", "left_semi").sort($"join_col_1", $"id1")
    integrityCollect(df)
  }

  testAgainstSpark("non-equi left semi join negated") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i, (i % 8).toString, i * 10)
    val f_data = for (i <- 1 to 32) yield (i, (i % 8).toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id1", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id2", "join_col_2", "x")
    val df = p.join(f, $"join_col_1" < $"join_col_2", "left_semi").sort($"join_col_1", $"id1")
    integrityCollect(df)
  }

  testAgainstSpark("left anti join") { securityLevel =>
    val p_data = for (i <- 1 to 128) yield (i, (i % 16).toString, i * 10)
    val f_data = for (i <- 1 to 256 if (i % 3) + 1 == 0 || (i % 3) + 5 == 0) yield (i, i.toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id", "join_col_2", "x")
    val df = p.join(f, $"join_col_1" === $"join_col_2", "left_anti").sort($"join_col_1", $"id")
    integrityCollect(df)
  }

  testAgainstSpark("left anti join with condition") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i, (i % 8).toString, i * 10)
    val f_data = for (i <- 1 to 32) yield (i, (i % 8).toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id1", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id2", "join_col_2", "y")
    val df = p.join(f, $"join_col_1" === $"join_col_2" && $"x" > $"y", "left_anti").sort($"join_col_1", $"id1")
    integrityCollect(df)
  }

  testAgainstSpark("non-equi left anti join 1") { securityLevel =>
    val p_data = for (i <- 1 to 128) yield (i, (i % 16).toString, i * 10)
    val f_data = for (i <- 1 to 256 if (i % 3) + 1 == 0 || (i % 3) + 5 == 0) yield (i, i.toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id", "join_col_2", "x")
    val df = p.join(f, $"join_col_1" >= $"join_col_2", "left_anti").sort($"join_col_1", $"id")
    integrityCollect(df)
  }

  testAgainstSpark("non-equi left anti join 1 negated") { securityLevel =>
    val p_data = for (i <- 1 to 128) yield (i, (i % 16).toString, i * 10)
    val f_data = for (i <- 1 to 256 if (i % 3) + 1 == 0 || (i % 3) + 5 == 0) yield (i, i.toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id", "join_col_2", "x")
    val df = p.join(f, $"join_col_1" < $"join_col_2", "left_anti").sort($"join_col_1", $"id")
    integrityCollect(df)
  }

  testAgainstSpark("left anti join 2") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i, (i % 4).toString, i * 10)
    val f_data = for (i <- 1 to 32) yield (i, i.toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id", "join_col_2", "x")
    val df = p.join(f, $"join_col_1" === $"join_col_2", "left_anti").sort($"join_col_1", $"id")
    df.collect
  }

  testAgainstSpark("non-equi left anti join 2") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i, (i % 4).toString, i * 10)
    val f_data = for (i <- 1 to 32) yield (i, i.toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id", "join_col_2", "x")
    val df = p.join(f, $"join_col_1" >= $"join_col_2", "left_anti").sort($"join_col_1", $"id")
    integrityCollect(df)
  }

  testAgainstSpark("non-equi left anti join 2 negated") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i, (i % 4).toString, i * 10)
    val f_data = for (i <- 1 to 32) yield (i, i.toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id", "join_col_1", "x")
    val f = makeDF(f_data, securityLevel, "id", "join_col_2", "x")
    val df = p.join(f, $"join_col_1" < $"join_col_2", "left_anti").sort($"join_col_1", $"id")
    integrityCollect(df)
  }

  testAgainstSpark("join on floats") { securityLevel =>
    val p_data = for (i <- 0 to 16) yield (i, i.toFloat, i * 10)
    val f_data = (0 until 256).map(x => {
      if (x % 3 == 0)
        (x, null.asInstanceOf[Float], x * 10)
      else
        (x, (x % 16).asInstanceOf[Float], x * 10)
    }).toSeq

    val p = makeDF(p_data, securityLevel, "id", "pk", "x")
    val f = makeDF(f_data, securityLevel, "id", "fk", "x")
    val df = p.join(f, $"pk" === $"fk")
    integrityCollect(df).toSet
  }

  testAgainstSpark("join on doubles") { securityLevel =>
    val p_data = for (i <- 0 to 16) yield (i, i.toDouble, i * 10)
    val f_data = (0 until 256).map(x => {
      if (x % 3 == 0)
        (x, null.asInstanceOf[Double], x * 10)
      else
        (x, (x % 16).asInstanceOf[Double], x * 10)
    }).toSeq

    val p = makeDF(p_data, securityLevel, "id", "pk", "x")
    val f = makeDF(f_data, securityLevel, "id", "fk", "x")
    val df = p.join(f, $"pk" === $"fk")
    integrityCollect(df).toSet
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
    integrityCollect(df).sortBy { case Row(category: String, _) => category }
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
    integrityCollect(words.groupBy("category").agg(count("category").as("itemsInCategory")))
      .sortBy { case Row(category: String, _) => category }
  }

  testAgainstSpark("aggregate count distinct and indistinct") { securityLevel =>
    val data = (0 until 64).map{ i =>
      if (i % 6 == 0)
        (abc(i), null.asInstanceOf[Int], i % 8)
      else
        (abc(i), i % 4, i % 8)
    }.toSeq
    val words = makeDF(data, securityLevel, "category", "id", "price")
    words.groupBy("category").agg(countDistinct("id").as("num_unique_ids"),
        count("price").as("num_prices")).collect.toSet
  }

  testAgainstSpark("aggregate count distinct") { securityLevel =>
    val data = (0 until 64).map{ i =>
      if (i % 6 == 0)
        (abc(i), null.asInstanceOf[Int])
      else
        (abc(i), i % 8)
    }.toSeq
    val words = makeDF(data, securityLevel, "category", "price")
    words.groupBy("category").agg(countDistinct("price").as("num_unique_prices"))
      .collect.sortBy { case Row(category: String, _) => category }
  }

  testAgainstSpark("aggregate first") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "category", "price")

    integrityCollect(words.groupBy("category").agg(first("category").as("firstInCategory")))
      .sortBy { case Row(category: String, _) => category }
  }

  testAgainstSpark("aggregate last") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "category", "price")

    integrityCollect(words.groupBy("category").agg(last("category").as("lastInCategory")))
      .sortBy { case Row(category: String, _) => category }
  }

  testAgainstSpark("aggregate max") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "category", "price")

    integrityCollect(words.groupBy("category").agg(max("price").as("maxPrice")))
      .sortBy { case Row(category: String, _) => category }
  }

  testAgainstSpark("aggregate min") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "category", "price")

    integrityCollect(words.groupBy("category").agg(min("price").as("minPrice")))
      .sortBy { case Row(category: String, _) => category }
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

    integrityCollect(words.groupBy("word").agg(sum("count").as("totalCount")))
      .sortBy { case Row(word: String, _) => word }
  }

  testAgainstSpark("aggregate sum distinct and indistinct") { securityLevel =>
    val data = (0 until 64).map{ i =>
      if (i % 6 == 0)
        (abc(i), null.asInstanceOf[Int], i % 8)
      else
        (abc(i), i % 4, i % 8)
    }.toSeq
    val words = makeDF(data, securityLevel, "category", "id", "price")
    words.groupBy("category").agg(sumDistinct("id").as("sum_unique_ids"),
        sum("price").as("sum_prices")).collect.toSet
  }

  testAgainstSpark("aggregate sum distinct") { securityLevel =>
    val data = (0 until 64).map{ i =>
      if (i % 6 == 0)
        (abc(i), null.asInstanceOf[Int])
      else
        (abc(i), i % 8)
    }.toSeq
    val words = makeDF(data, securityLevel, "category", "price")
    words.groupBy("category").agg(sumDistinct("price").as("sum_unique_prices"))
      .collect.sortBy { case Row(category: String, _) => category }
  }

  testAgainstSpark("aggregate on multiple columns") { securityLevel =>
    val data = for (i <- 0 until 256) yield (abc(i), 1, 1.0f)
    val words = makeDF(data, securityLevel, "str", "x", "y")

    integrityCollect(words.groupBy("str").agg(sum("y").as("totalY"), avg("x").as("avgX")))
      .sortBy { case Row(str: String, _, _) => str }
  }

  testAgainstSpark("skewed aggregate sum") { securityLevel =>
    val data = Random.shuffle((0 until 256).map(i => {
        (i, abc(123), 1)
    }).toSeq)

    val words = makeDF(data, securityLevel, "id", "word", "count")
    integrityCollect(words.groupBy("word").agg(sum("count").as("totalCount")))
      .sortBy { case Row(word: String, _) => word }
  }

  testAgainstSpark("grouping aggregate with 0 rows") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "word", "count")
    integrityCollect(words.filter($"id" < lit(0)).groupBy("word").agg(sum("count")))
      .sortBy { case Row(word: String, _) => word }
  }

  testAgainstSpark("global aggregate") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "word", "count")
    integrityCollect(words.agg(sum("count").as("totalCount")))
  }

  testAgainstSpark("global aggregate count distinct") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), i % 64)
    val words = makeDF(data, securityLevel, "id", "word", "price")
    words.agg(countDistinct("price").as("num_unique_prices")).collect
  }

  testAgainstSpark("global aggregate with 0 rows") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "word", "count")
    val result = words.filter($"id" < lit(0)).agg(count("*")).as("totalCount")
    integrityCollect(result)
  }

  testAgainstSpark("contains") { securityLevel =>
    val data = for (i <- 0 until 256) yield(i.toString, abc(i))
    val df = makeDF(data, securityLevel, "word", "abc")
    integrityCollect(df.filter($"word".contains(lit("1"))))
  }

  testAgainstSpark("concat with string") { securityLevel =>
    val data = for (i <- 0 until 256) yield ("%03d".format(i) * 3, i.toString)
    val df = makeDF(data, securityLevel, "str", "x")
    integrityCollect(df.select(concat(col("str"),lit(","),col("x"))))
  }

  testAgainstSpark("concat with other datatype") { securityLevel =>
    // float causes a formating issue where opaque outputs 1.000000 and spark produces 1.0 so the following line is commented out 
    // val data = for (i <- 0 until 3) yield ("%03d".format(i) * 3, i, 1.0f)
    // you can't serialize date so that's not supported as well 
    // opaque doesn't support byte 
    val data = for (i <- 0 until 3) yield ("%03d".format(i) * 3, i, null.asInstanceOf[Int],"")
    val df = makeDF(data, securityLevel, "str", "int","null","emptystring")
    integrityCollect(df.select(concat(col("str"),lit(","),col("int"),col("null"),col("emptystring"))))
  }
  
  testAgainstSpark("isin1") { securityLevel =>
    val ids = Seq((1, 2, 2), (2, 3, 1))
    val df = makeDF(ids, securityLevel, "x", "y", "id")
    val c = $"id" isin ($"x", $"y")
    val result = df.filter(c)
    integrityCollect(result)
  }
    
  testAgainstSpark("isin2") { securityLevel =>
    val ids2 = Seq((1, 1, 1), (2, 2, 2), (3,3,3), (4,4,4))
    val df2 = makeDF(ids2, securityLevel, "x", "y", "id")
    val c2 = $"id" isin (1 ,2, 4, 5, 6)
    val result = df2.filter(c2)
    integrityCollect(result)
  }
    
  testAgainstSpark("isin with string") { securityLevel =>
    val ids3 = Seq(("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), ("b", "b", "b"), ("c","c","c"), ("d","d","d"))
    val df3 = makeDF(ids3, securityLevel, "x", "y", "id")
    val c3 = $"id" isin ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ,"b", "c", "d", "e")
    val result = df3.filter(c3)
    integrityCollect(result)
  }
    
  testAgainstSpark("isin with null") { securityLevel =>
    val ids4 = Seq((1, 1, 1), (2, 2, 2), (3,3,null.asInstanceOf[Int]), (4,4,4))
    val df4 = makeDF(ids4, securityLevel, "x", "y", "id")
    val c4 = $"id" isin (null.asInstanceOf[Int])
    val result = df4.filter(c4)
    integrityCollect(result)
  }

  testAgainstSpark("between") { securityLevel =>
    val data = for (i <- 0 until 256) yield(i.toString, i)
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.filter($"count".between(50, 150)))
  }

  testAgainstSpark("year") { securityLevel =>
    val data = Seq(Tuple2(1, new java.sql.Date(new java.util.Date().getTime())))
    val df = makeDF(data, securityLevel, "id", "date")
    integrityCollect(df.select(year($"date")))
  }

  testAgainstSpark("case when - 1 branch with else (string)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.select(when(df("word") === "foo", "hi").otherwise("bye")))
  }

  testAgainstSpark("case when - 1 branch with else (int)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.select(when(df("word") === "foo", 10).otherwise(30)))
  }

  testAgainstSpark("case when - 1 branch without else (string)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.select(when(df("word") === "foo", "hi")))
  }

  testAgainstSpark("case when - 1 branch without else (int)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.select(when(df("word") === "foo", 10)))
  }

  testAgainstSpark("case when - 2 branch with else (string)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.select(when(df("word") === "foo", "hi").when(df("word") === "baz", "hello").otherwise("bye")))
  }

  testAgainstSpark("case when - 2 branch with else (int)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.select(when(df("word") === "foo", 10).when(df("word") === "baz", 20).otherwise(30)))
  }

  testAgainstSpark("case when - 2 branch without else (string)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.select(when(df("word") === "foo", "hi").when(df("word") === "baz", "hello")))
  }

  testAgainstSpark("case when - 2 branch without else (int)") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), ("bear", null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.select(when(df("word") === "foo", 3).when(df("word") === "baz", 2)))
  }

  testAgainstSpark("LIKE - Contains") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.filter($"word".like("%a%")))
  } 

  testAgainstSpark("LIKE - StartsWith") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.filter($"word".like("ba%")))
  } 

  testAgainstSpark("LIKE - EndsWith") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.filter($"word".like("%ar")))
  }

  testAgainstSpark("LIKE - Empty Pattern") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.filter($"word".like("")))
  }

  testAgainstSpark("LIKE - Match All") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.filter($"word".like("%")))
  }

  testAgainstSpark("LIKE - Single Wildcard") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    integrityCollect(df.filter($"word".like("ba_")))
  }

  testAgainstSpark("LIKE - SQL API") { securityLevel =>
    val data = Seq(("foo", 4), ("bar", 1), ("baz", 5), (null.asInstanceOf[String], null.asInstanceOf[Int]))
    val df = makeDF(data, securityLevel, "word", "count")
    df.createTempView("df")
    try {
      integrityCollect(spark.sql(""" SELECT word FROM df WHERE word LIKE '_a_' """))
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
      integrityCollect(spark.sql("SELECT * FROM df WHERE x > 10"))
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
    integrityCollect(df.select(exp($"y")))
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

    integrityCollect(df.select(vectormultiply($"v", $"c")))
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

    integrityCollect(df.select(dot($"v1", $"v2")))
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

    integrityCollect(df.select(upper($"v1")))
  }

  testAgainstSpark("upper with null") { securityLevel =>
    val data = Seq(("lower", null.asInstanceOf[String]))

    val df = makeDF(data, securityLevel, "v1", "v2")

    integrityCollect(df.select(upper($"v2")))
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
    integrityCollect(df.groupBy().agg(vectorsum($"v")))
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

    integrityCollect(df.select(array($"x1", $"x2").as("x")))
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
    integrityCollect(df.sort($"id").limit(5))
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
    integrityCollect(df.sort($"id").limit(200))
  }

  testAgainstSpark("least squares") { securityLevel =>
    integrityCollect(LeastSquares.query(spark, securityLevel, "tiny", numPartitions))
  }

  testAgainstSpark("logistic regression") { securityLevel =>
    LogisticRegression.train(spark, securityLevel, 1000, numPartitions)
  }

  testAgainstSpark("k-means") { securityLevel =>
    import scala.math.Ordering.Implicits.seqDerivedOrdering
    KMeans.train(spark, securityLevel, numPartitions, 10, 2, 3, 0.01).map(_.toSeq).sorted
  }

  testAgainstSpark("encrypted literal") { securityLevel =>
    val input = 10
    val enc_str = Utils.encryptScalar(input, IntegerType)

    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "word", "count")
    val df = words.filter($"id" < decrypt(lit(enc_str), IntegerType)).sort($"id")
    integrityCollect(df)
  }

  testAgainstSpark("scalar subquery") { securityLevel =>
    // Example taken from https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2728434780191932/1483312212640900/6987336228780374/latest.html
    val data = for (i <- 0 until 256) yield (i, abc(i), i)
    val words = makeDF(data, securityLevel, "id", "word", "count")
    words.createTempView("words")

    try {
      val df = spark.sql("""SELECT id, word, (SELECT MAX(count) FROM words) max_age FROM words ORDER BY id, word""")
      integrityCollect(df)
    } finally {
      spark.catalog.dropTempView("words")
    }
  }

  testAgainstSpark("pagerank") { securityLevel =>
    integrityCollect(PageRank.run(spark, securityLevel, "256", numPartitions)).toSet
  }
  testAgainstSpark("big data 1") { securityLevel =>
    integrityCollect(BigDataBenchmark.q1(spark, securityLevel, "tiny", numPartitions))
  }

  testAgainstSpark("big data 2") { securityLevel =>
    integrityCollect(BigDataBenchmark.q2(spark, securityLevel, "tiny", numPartitions))
      .map { case Row(a: String, b: Double) => (a, b.toFloat) }
      .sortBy(_._1)
  }

  testAgainstSpark("big data 3") { securityLevel =>
    integrityCollect(BigDataBenchmark.q3(spark, securityLevel, "tiny", numPartitions))
  }

  def makeDF[A <: Product : scala.reflect.ClassTag : scala.reflect.runtime.universe.TypeTag](
    data: Seq[A], securityLevel: SecurityLevel, columnNames: String*): DataFrame =
    securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data, numPartitions))
        .toDF(columnNames: _*))

}

class OpaqueOperatorSinglePartitionSuite extends OpaqueOperatorTests {
  override val spark = SparkSession.builder()
    .master("local[1]")
    .appName("OpaqueOperatorSinglePartitionSuite")
    .config("spark.sql.shuffle.partitions", 1)
    .getOrCreate()

  override def numPartitions: Int = 1
}

class OpaqueOperatorMultiplePartitionSuite extends OpaqueOperatorTests {
  override val spark = SparkSession.builder()
    .master("local[1]")
    .appName("OpaqueOperatorMultiplePartitionSuite")
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
