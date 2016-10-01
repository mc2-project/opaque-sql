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

import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.util.Random

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import edu.berkeley.cs.rise.opaque.examples._
import edu.berkeley.cs.rise.opaque.execution._
import edu.berkeley.cs.rise.opaque.execution.Opcode._
import edu.berkeley.cs.rise.opaque.implicits._

class QEDSuite extends FunSuite with BeforeAndAfterAll { self =>
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("QEDSuite")
    .getOrCreate()

  Utils.initSQLContext(spark.sqlContext)

  import spark.implicits._

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("pagerank") {
    PageRank.run(spark, Oblivious, "256", 1)
  }

  // test("big data 1") {
  //   val answer = QEDBenchmark.bd1SparkSQL(spark, "tiny").collect
  //   assert(answer === QEDBenchmark.bd1Opaque(spark, "tiny").collect)
  //   assert(answer === QEDBenchmark.bd1Encrypted(spark, "tiny").collect)
  // }

  // test("big data 2") {
  //   val answer = QEDBenchmark.bd2SparkSQL(spark, "tiny")
  //     .collect
  //     .map { case Row(a: String, b: Double) => (a, b.toFloat) }
  //     .sortBy(_._1)
  //     .map {
  //       case (str: String, f: Float) => (str, "%.2f".format(f))
  //     }

  //   val opaque =
  //     Utils.decrypt2[String, Float](QEDBenchmark.bd2Opaque(spark, "tiny").encCollect)
  //       .map {
  //         case (str: String, f: Float) => (str, "%.2f".format(f))
  //       }
  //   assert(answer === opaque)

  //   val encrypted =
  //     Utils.decrypt2[String, Float](QEDBenchmark.bd2Encrypted(spark, "tiny").encCollect)
  //       .map {
  //         case (str: String, f: Float) => (str, "%.2f".format(f))
  //       }
  //   assert(answer === encrypted)
  // }

  // test("big data 3") {
  //   val answer = QEDBenchmark.bd3SparkSQL(spark, "tiny")
  //     .collect.map { case Row(a: String, b: Double, c: Double) => (a, b.toFloat, c.toFloat) }
  //   assert(answer === Utils.decrypt3[String, Float, Float](
  //     QEDBenchmark.bd3Opaque(spark, "tiny").encCollect))
  //   assert(answer === Utils.decrypt3[String, Float, Float](
  //     QEDBenchmark.bd3Encrypted(spark, "tiny").encCollect))
  // }

  // test("TPC-H query 9") {
  //   val a = QEDBenchmark.tpch9SparkSQL(spark, "sf_small", Some(25))
  //     .collect.map { case Row(a: String, b: Int, c: Double) => (a, b, c.toFloat) }.sorted
  //   val b = Utils.decrypt3[String, Int, Float](
  //     QEDBenchmark.tpch9Generic(spark, "sf_small", Some(25)).encCollect).sorted
  //   val c = Utils.decrypt3[String, Int, Float](
  //     QEDBenchmark.tpch9Opaque(spark, "sf_small", Some(25)).encCollect).sorted
  //   assert(a.size === b.size)
  //   assert(a.map { case (a, b, c) => (a, b)} === b.map { case (a, b, c) => (a, b)})
  //   assert(a.size === c.size)
  //   assert(a.map { case (a, b, c) => (a, b)} === c.map { case (a, b, c) => (a, b)})
  // }

  // test("disease query") {
  //   QEDBenchmark.diseaseQuery(spark, "500")
  // }

  test("columnsort on join rows") {
    val p_data = for (i <- 1 to 16) yield (i.toString, i * 10)
    val f_data = for (i <- 1 to 256) yield ((i % 16).toString, (i * 10).toString, i.toFloat)
    val pTypes = Seq(StringType, IntegerType)
    val fTypes = Seq(StringType, StringType, FloatType)
    val p = spark.sparkContext.makeRDD(Utils.encryptTuples(p_data, pTypes), 5)
    val f = spark.sparkContext.makeRDD(Utils.encryptTuples(f_data, fTypes), 5)
    val j = p.zipPartitions(f) { (pIter, fIter) =>
      val (enclave, eid) = Utils.initEnclave()
      val pArr = pIter.map(Utils.fieldsToRow).toArray
      val fArr = fIter.map(Utils.fieldsToRow).toArray
      val p = Utils.createBlock(pArr, false)
      val f = Utils.createBlock(fArr, false)
      val r = enclave.JoinSortPreprocess(
        eid, 0, 5, OP_JOIN_COL1.value, p, pArr.length, f, fArr.length)
      Iterator(Block(r, pArr.length + fArr.length))
    }
    val sorted = ObliviousSort.sortBlocks(j, OP_JOIN_COL1).flatMap { block =>
      Utils.splitBlock(block.bytes, block.numRows, true)
        .map(serRow => Row.fromSeq(Utils.parseRow(serRow)))
    }
    assert(sorted.collect.length === p_data.length + f_data.length)
  }

  test("encFilter") {
    val data = for (i <- 0 until 5) yield ("foo", i)
    val words = spark.createDataFrame(data).toDF("word", "count").oblivious

    assert(words.collect === data.map(Row.fromTuple))

    val filtered = words.filter($"count" > lit(3))
    assert(filtered.collect === data.filter(_._2 > 3).map(Row.fromTuple))
  }

  test("encFilter on date") {
    import java.sql.Date
    val dates = List("1975-01-01", "1980-01-01", "1980-03-02", "1980-04-01", "1990-01-01")
    val filteredDates = List("1980-01-01", "1980-03-02", "1980-04-01")
    val javaDates = dates.map(d =>
      DateTimeUtils.toJavaDate(DateTimeUtils.stringToDate(UTF8String.fromString(d)).get))
    val data = spark.createDataFrame(javaDates.map(Tuple1(_))).toDF("date")
    val filtered = data.filter($"date" >= lit("1980-01-01") && $"date" <= lit("1980-04-01"))
    assert(filtered.collect.map(_.get(0).toString).toSet === filteredDates.toSet)

    val encFiltered = data.oblivious.filter(
      $"date" >= lit("1980-01-01") && $"date" <= lit("1980-04-01"))
    assert(encFiltered.collect.map(_.get(0).toString).toSet ===
      filteredDates.toSet)
  }

  test("nonObliviousFilter") {
    val data = for (i <- 0 until 256) yield ("foo", i)
    val words = spark.createDataFrame(data).toDF("word", "count").encrypted
    assert(words.collect === data.map(Row.fromTuple))

    val filtered = words.filter($"count" > lit(3))
    assert(filtered.collect.toSet === data.filter(_._2 > 3).toSet.map(Row.fromTuple))
  }

  test("nonObliviousAggregate") {
    def abc(i: Int): String = (i % 3) match {
      case 0 => "A"
      case 1 => "B"
      case 2 => "C"
    }
    val data = for (i <- 0 until 256) yield (abc(i), 1)
    val words = spark.createDataFrame(data).toDF("word", "count").encrypted

    val summed = words.groupBy($"word").agg(sum("count").as("totalCount"))
    val expected = data.groupBy(_._1).mapValues(_.map(_._2).sum)
    assert(summed.collect.toSet === expected.map(Row.fromTuple).toSet)
  }

  test("encAggregate") {
    def abc(i: Int): String = (i % 3) match {
      case 0 => "A"
      case 1 => "B"
      case 2 => "C"
    }
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = spark.createDataFrame(data).toDF("id", "word", "count").oblivious

    val summed = words.groupBy("word").agg(sum("count").as("totalCount"))
    val expected = data.map(p => (p._2, p._3)).groupBy(_._1).mapValues(_.map(_._2).sum)
    assert(summed.collect.toSet === expected.map(Row.fromTuple).toSet)
  }

  test("encAggregate - final run split across multiple partitions") {
    val data = for (i <- 0 until 256) yield (i, "A", 1)
    val words = spark.createDataFrame(spark.sparkContext.makeRDD(data, 2))
      .toDF("id", "word", "count").oblivious

    val summed = words.groupBy("word").agg(sum("count").as("totalCount"))
    assert(summed.collect.toSet ===
      data.map(p => (p._2, p._3)).groupBy(_._1).mapValues(_.map(_._2).sum)
      .map(Row.fromTuple).toSet)
  }

  test("encAggregate on multiple columns") {
    def abc(i: Int): String = (i % 3) match {
      case 0 => "A"
      case 1 => "B"
      case 2 => "C"
    }
    val data = for (i <- 0 until 256) yield (abc(i), 1, 1.0f)
    val words = spark.createDataFrame(data).toDF("str", "x", "y").oblivious

    val summed = words.groupBy("str").agg(avg("x").as("avgX"), sum("y").as("totalY"))
    assert(summed.collect.toSet ===
      data.groupBy(_._1).mapValues(group =>
        (group.map(_._2).sum / group.map(_._2).size, group.map(_._3).sum))
      .map { case (str, (avgX, avgY)) => (str, avgX, avgY) }.map(Row.fromTuple).toSet)
  }

  test("encSort") {
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x)).toSeq)
    val sorted = spark.createDataFrame(spark.sparkContext.makeRDD(data, 1)).toDF("str", "x")
      .oblivious.sort($"x")
    assert(sorted.collect === data.sortBy(_._2).map(Row.fromTuple))
  }

  test("nonObliviousSort") {
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x)).toSeq)
    val sorted = spark.createDataFrame(spark.sparkContext.makeRDD(data, 1)).toDF("str", "x")
      .encrypted.sort($"x")
    assert(sorted.collect === data.sortBy(_._2).map(Row.fromTuple))
  }

  test("encSort by float") {
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x.toFloat)).toSeq)
    val sorted = spark.createDataFrame(spark.sparkContext.makeRDD(data, 1)).toDF("str", "x")
      .oblivious.sort($"x")
    assert(sorted.collect === data.sortBy(_._2).map(Row.fromTuple))
  }

  test("encSort multiple partitions") {
    val data = Random.shuffle(for (i <- 0 until 256) yield (i, i.toString, 1))
    val sorted = spark.createDataFrame(spark.sparkContext.makeRDD(data, 3))
      .toDF("id", "word", "count")
      .oblivious.sort($"word")
    assert(sorted.collect === data.sortBy(_._2).map(Row.fromTuple))
  }

  test("nonObliviousSort multiple partitions") {
    val data = Random.shuffle(for (i <- 0 until 256) yield (i, i.toString, 1))
    val sorted = spark.createDataFrame(spark.sparkContext.makeRDD(data, 3))
      .toDF("id", "word", "count")
      .encrypted.sort($"word")
    assert(sorted.collect === data.sortBy(_._2).map(Row.fromTuple))
  }

  test("encJoin") {
    val p_data = for (i <- 1 to 16) yield (i, i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield (i, (i % 16).toString, i * 10)
    val p = spark.createDataFrame(p_data).toDF("id", "pk", "x").oblivious
    val f = spark.createDataFrame(f_data).toDF("id", "fk", "x").oblivious
    val joined = p.join(f, $"pk" === $"fk")
    val expectedJoin =
      for {
        (p_id, pk, p_x) <- p_data
        (f_id, fk, f_x) <- f_data
        if pk == fk
      } yield (p_id, pk, p_x, f_id, fk, f_x)
    assert(joined.collect.toSet === expectedJoin.map(Row.fromTuple).toSet)
  }

  test("encJoin on column 1") {
    val p_data = for (i <- 1 to 16) yield (i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield ((i % 16).toString, (i * 10).toString, i.toFloat)
    val p = spark.createDataFrame(p_data).toDF("pk", "x").oblivious
    val f = spark.createDataFrame(f_data).toDF("fk", "x", "y").oblivious
    val joined = p.join(f, $"pk" === $"fk")
    val expectedJoin =
      for {
        (pk, p_x) <- p_data
        (fk, f_x, f_y) <- f_data
        if pk == fk
      } yield (pk, p_x, fk, f_x, f_y)
    assert(joined.collect.toSet === expectedJoin.map(Row.fromTuple).toSet)
  }

  test("nonObliviousJoin") {
    val p_data = for (i <- 1 to 16) yield (i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield ((i % 16).toString, (i * 10).toString, i.toFloat)
    val p = spark.createDataFrame(spark.sparkContext.makeRDD(p_data, 1)).toDF("pk", "x").encrypted
    val f = spark.createDataFrame(spark.sparkContext.makeRDD(f_data, 1)).toDF("fk", "x", "y").encrypted
    val joined = p.join(f, $"pk" === $"fk")
    val expectedJoin =
      for {
        (pk, p_x) <- p_data
        (fk, f_x, f_y) <- f_data
        if pk == fk
      } yield (pk, p_x, fk, f_x, f_y)
    assert(joined.collect.toSet === expectedJoin.map(Row.fromTuple).toSet)
  }

  // test("nonObliviousJoin multiple partitions") {
  //   val p_data = for (i <- 0 until 4) yield (i.toString, i * 10)
  //   val f_data = for (i <- 0 until 16 - 4) yield ((i % 4).toString, (i * 10).toString, i.toFloat)
  //   val p = spark.createDataFrame(spark.sparkContext.makeRDD(p_data, 3)).toDF("pk", "x").encrypted
  //   val f = spark.createDataFrame(spark.sparkContext.makeRDD(f_data, 3)).toDF("fk", "x", "y").encrypted
  //   val joined = p.join(f, $"pk" === $"fk")
  //   val expectedJoin =
  //     for {
  //       (pk, p_x) <- p_data
  //       (fk, f_x, f_y) <- f_data
  //       if pk == fk
  //     } yield (pk, p_x, fk, f_x, f_y)
  //   println(p_data)
  //   println(f_data)
  //   joined.explain(true)
  //   assert(joined.collect.toSet === expectedJoin.map(Row.fromTuple).toSet)
  // }

  test("encSelect") {
    val data = for (i <- 0 until 256) yield ("%03d".format(i) * 3, i.toFloat)
    val rdd = spark.createDataFrame(data).toDF("str", "x").oblivious
    val proj = rdd.select(substring($"str", 0, 8), $"x")
    assert(proj.collect ===
      data.map { case (str, x) => (str.substring(0, 8), x) }.map(Row.fromTuple))
  }

  test("encCache") {
    def numCached(ds: Dataset[_]): Int =
      ds.queryExecution.executedPlan.collect {
        case cached: PhysicalEncryptedBlockRDD => cached
      }.size

    val data = List((1, 3), (1, 4), (1, 5), (2, 4))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(data, 1))
      .toDF("a", "b").oblivious.cache()

    val agg = df.groupBy($"a").agg(sum("b"))

    assert(numCached(agg) === 1)

    val expected = data.groupBy(_._1).mapValues(_.map(_._2).sum)
    assert(agg.collect.toSet === expected.map(Row.fromTuple).toSet)
  }

  // TODO: test sensitivity propagation on operators

  // TODO: test nonObliviousAggregate on multiple partitions

}
