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

package org.apache.spark.sql

import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.util.Random

import oblivious_sort.ObliviousSort
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.unsafe.types.UTF8String

import org.apache.spark.sql.QEDOpcode._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.Block
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

class QEDSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  import QED.time

  LogManager.getLogger(classOf[org.apache.spark.scheduler.TaskSetManager]).setLevel(Level.ERROR)
  LogManager.getLogger(classOf[org.apache.spark.storage.BlockManager]).setLevel(Level.ERROR)

  def byte_to_int(array: Array[Byte], index: Int) = {
    val int_bytes = array.slice(index, index + 4)
    val buf = ByteBuffer.wrap(int_bytes)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.getInt
  }

  def byte_to_string(array: Array[Byte], index: Int, length: Int) = {
    val string_bytes = array.slice(index, index + length)
    new String(string_bytes)
  }

  // ignore("pagerank") {
  //   QEDBenchmark.pagerank(sqlContext, "256")
  // }

  // ignore("big data 1") {
  //   val answer = QEDBenchmark.bd1SparkSQL(sqlContext, "tiny").collect
  //   assert(answer === QEDBenchmark.bd1Opaque(sqlContext, "tiny").collect)
  //   assert(answer === QEDBenchmark.bd1Encrypted(sqlContext, "tiny").collect)
  // }

  // ignore("big data 2") {
  //   val answer = QEDBenchmark.bd2SparkSQL(sqlContext, "tiny")
  //     .collect
  //     .map { case Row(a: String, b: Double) => (a, b.toFloat) }
  //     .sortBy(_._1)
  //     .map {
  //       case (str: String, f: Float) => (str, "%.2f".format(f))
  //     }

  //   val opaque =
  //     QED.decrypt2[String, Float](QEDBenchmark.bd2Opaque(sqlContext, "tiny").encCollect)
  //       .map {
  //         case (str: String, f: Float) => (str, "%.2f".format(f))
  //       }
  //   assert(answer === opaque)

  //   val encrypted =
  //     QED.decrypt2[String, Float](QEDBenchmark.bd2Encrypted(sqlContext, "tiny").encCollect)
  //       .map {
  //         case (str: String, f: Float) => (str, "%.2f".format(f))
  //       }
  //   assert(answer === encrypted)
  // }

  // ignore("big data 3") {
  //   val answer = QEDBenchmark.bd3SparkSQL(sqlContext, "tiny")
  //     .collect.map { case Row(a: String, b: Double, c: Double) => (a, b.toFloat, c.toFloat) }
  //   assert(answer === QED.decrypt3[String, Float, Float](
  //     QEDBenchmark.bd3Opaque(sqlContext, "tiny").encCollect))
  //   assert(answer === QED.decrypt3[String, Float, Float](
  //     QEDBenchmark.bd3Encrypted(sqlContext, "tiny").encCollect))
  // }

  // ignore("TPC-H query 9") {
  //   val a = QEDBenchmark.tpch9SparkSQL(sqlContext, "sf_small", Some(25))
  //     .collect.map { case Row(a: String, b: Int, c: Double) => (a, b, c.toFloat) }.sorted
  //   val b = QED.decrypt3[String, Int, Float](
  //     QEDBenchmark.tpch9Generic(sqlContext, "sf_small", Some(25)).encCollect).sorted
  //   val c = QED.decrypt3[String, Int, Float](
  //     QEDBenchmark.tpch9Opaque(sqlContext, "sf_small", Some(25)).encCollect).sorted
  //   assert(a.size === b.size)
  //   assert(a.map { case (a, b, c) => (a, b)} === b.map { case (a, b, c) => (a, b)})
  //   assert(a.size === c.size)
  //   assert(a.map { case (a, b, c) => (a, b)} === c.map { case (a, b, c) => (a, b)})
  // }

  // ignore("disease query") {
  //   QEDBenchmark.diseaseQuery(sqlContext, "500")
  // }

  test("columnsort on join rows") {
    val p_data = for (i <- 1 to 16) yield (i.toString, i * 10)
    val f_data = for (i <- 1 to 256) yield ((i % 16).toString, (i * 10).toString, i.toFloat)
    val p = sparkContext.makeRDD(
      QED.encryptTuples(p_data, Seq(StringType, IntegerType)), 5)
    val f = sparkContext.makeRDD(
      QED.encryptTuples(f_data, Seq(StringType, StringType, FloatType)), 5)
    val j = p.zipPartitions(f) { (pIter, fIter) =>
      val (enclave, eid) = QED.initEnclave()
      val pArr = pIter.toArray
      val fArr = fIter.toArray
      val p = QED.createBlock(
        pArr.map(r => InternalRow.fromSeq(r)).map(_.encSerialize), false)
      val f = QED.createBlock(
        fArr.map(r => InternalRow.fromSeq(r)).map(_.encSerialize), false)
      val r = enclave.JoinSortPreprocess(
        eid, 0, 5, OP_JOIN_COL1.value, p, pArr.length, f, fArr.length)
      Iterator(Block(r, pArr.length + fArr.length))
    }
    val sorted = ObliviousSort.sortBlocks(j, OP_JOIN_COL1).flatMap { block =>
      QED.splitBlock(block.bytes, block.numRows, true)
        .map(serRow => Row.fromSeq(QED.parseRow(serRow)))
    }
    assert(sorted.collect.length === p_data.length + f_data.length)
  }

  test("encFilter") {
    val data = for (i <- 0 until 5) yield ("foo", i)
    val words = sqlContext.createDataFrame(data).toDF("word", "count").oblivious

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
    val data = sqlContext.createDataFrame(javaDates.map(Tuple1(_))).toDF("date")
    val filtered = data.filter($"date" >= lit("1980-01-01") && $"date" <= lit("1980-04-01"))
    assert(filtered.collect.map(_.get(0).toString).toSet === filteredDates.toSet)

    val encFiltered = data.oblivious.filter(
      $"date" >= lit("1980-01-01") && $"date" <= lit("1980-04-01"))
    assert(encFiltered.collect.map(_.get(0).toString).toSet ===
      filteredDates.toSet)
  }

  test("nonObliviousFilter") {
    val data = for (i <- 0 until 256) yield ("foo", i)
    val words = sqlContext.createDataFrame(data).toDF("word", "count").encrypted
    assert(words.collect === data.map(Row.fromTuple))

    val filtered = words.filter($"count" > lit(3))
    assert(filtered.collect.toSet === data.filter(_._2 > 3).toSet.map(Row.fromTuple))
  }

  // ignore("nonObliviousAggregate") {
  //   def abc(i: Int): String = (i % 3) match {
  //     case 0 => "A"
  //     case 1 => "B"
  //     case 2 => "C"
  //   }
  //   val data = for (i <- 0 until 256) yield (abc(i), 1)
  //   val words = sqlContext.createDataFrame(data).toDF("word", "count").encrypted

  //   val summed = words.groupBy($"word").agg(sum("count").as("totalCount"))
  //   val expected = data.groupBy(_._1).mapValues(_.map(_._2).sum)
  //   assert(summed.collect.toSet === expected.map(Row.fromTuple).toSet)
  // }

  test("encAggregate") {
    def abc(i: Int): String = (i % 3) match {
      case 0 => "A"
      case 1 => "B"
      case 2 => "C"
    }
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = sqlContext.createDataFrame(data).toDF("id", "word", "count").oblivious

    val summed = words.groupBy("word").agg(sum("count").as("totalCount"))
    val expected = data.map(p => (p._2, p._3)).groupBy(_._1).mapValues(_.map(_._2).sum)
    assert(summed.collect.toSet === expected.map(Row.fromTuple).toSet)
  }

  // ignore("encAggregate - final run split across multiple partitions") {
  //   val data = for (i <- 0 until 256) yield (i, "A", 1)
  //   val words = sqlContext.createEncryptedDataFrame(
  //     sparkContext.makeRDD(QED.encryptN(data), 2),
  //     StructType(Seq(
  //       StructField("id", IntegerType),
  //       StructField("word", StringType),
  //       StructField("count", IntegerType))))

  //   val summed = words.groupBy("word").encAgg(sum("count").as("totalCount"))
  //   assert(QED.decrypt2[String, Int](summed.encCollect) ===
  //     data.map(p => (p._2, p._3)).groupBy(_._1).mapValues(_.map(_._2).sum).toSeq.sorted)
  // }

  // ignore("encAggregate on multiple columns") {
  //   def abc(i: Int): String = (i % 3) match {
  //     case 0 => "A"
  //     case 1 => "B"
  //     case 2 => "C"
  //   }
  //   val data = for (i <- 0 until 256) yield (abc(i), 1, 1.0f)
  //   val words = sqlContext.createEncryptedDataFrame(
  //     sparkContext.makeRDD(QED.encryptN(data), 1),
  //     StructType(Seq(
  //       StructField("str", StringType),
  //       StructField("x", IntegerType),
  //       StructField("y", FloatType))))

  //   val summed = words.groupBy("str").encAgg(avg("x").as("avgX"), sum("y").as("totalY"))
  //   assert(QED.decrypt3[String, Int, Float](summed.encCollect) ===
  //     data.groupBy(_._1).mapValues(group =>
  //       (group.map(_._2).sum / group.map(_._2).size, group.map(_._3).sum))
  //     .toSeq.map { case (str, (avgX, avgY)) => (str, avgX, avgY) }.sorted)
  // }

  test("encSort") {
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x)).toSeq)
    val sorted = sqlContext.createDataFrame(sparkContext.makeRDD(data, 1)).toDF("str", "x")
      .oblivious.sort($"x")
    assert(sorted.collect === data.sortBy(_._2).map(Row.fromTuple))
  }

  test("nonObliviousSort") {
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x)).toSeq)
    val sorted = sqlContext.createDataFrame(sparkContext.makeRDD(data, 1)).toDF("str", "x")
      .encrypted.sort($"x")
    assert(sorted.collect === data.sortBy(_._2).map(Row.fromTuple))
  }

  test("encSort by float") {
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x.toFloat)).toSeq)
    val sorted = sqlContext.createDataFrame(sparkContext.makeRDD(data, 1)).toDF("str", "x")
      .oblivious.sort($"x")
    assert(sorted.collect === data.sortBy(_._2).map(Row.fromTuple))
  }

  test("encSort multiple partitions") {
    val data = Random.shuffle(for (i <- 0 until 256) yield (i, i.toString, 1))
    val sorted = sqlContext.createDataFrame(sparkContext.makeRDD(data, 3))
      .toDF("id", "word", "count")
      .oblivious.sort($"word")
    assert(sorted.collect === data.sortBy(_._2).map(Row.fromTuple))
  }

  test("nonObliviousSort multiple partitions") {
    val data = Random.shuffle(for (i <- 0 until 256) yield (i, i.toString, 1))
    val sorted = sqlContext.createDataFrame(sparkContext.makeRDD(data, 3))
      .toDF("id", "word", "count")
      .encrypted.sort($"word")
    assert(sorted.collect === data.sortBy(_._2).map(Row.fromTuple))
  }

  test("encJoin") {
    val p_data = for (i <- 1 to 16) yield (i, i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield (i, (i % 16).toString, i * 10)
    val p = sqlContext.createDataFrame(p_data).toDF("id", "pk", "x").oblivious
    val f = sqlContext.createDataFrame(f_data).toDF("id", "fk", "x").oblivious
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
    val p = sqlContext.createDataFrame(p_data).toDF("pk", "x").oblivious
    val f = sqlContext.createDataFrame(f_data).toDF("fk", "x", "y").oblivious
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
    val p = sqlContext.createDataFrame(sparkContext.makeRDD(p_data, 1)).toDF("pk", "x").encrypted
    val f = sqlContext.createDataFrame(sparkContext.makeRDD(f_data, 1)).toDF("fk", "x", "y").encrypted
    val joined = p.join(f, $"pk" === $"fk")
    val expectedJoin =
      for {
        (pk, p_x) <- p_data
        (fk, f_x, f_y) <- f_data
        if pk == fk
      } yield (pk, p_x, fk, f_x, f_y)
    assert(joined.collect.toSet === expectedJoin.map(Row.fromTuple).toSet)
  }

  // ignore("nonObliviousJoin multiple partitions") {
  //   val p_data = for (i <- 0 until 4) yield (i.toString, i * 10)
  //   val f_data = for (i <- 0 until 16 - 4) yield ((i % 4).toString, (i * 10).toString, i.toFloat)
  //   val p = sqlContext.createDataFrame(sparkContext.makeRDD(p_data, 3)).toDF("pk", "x").encrypted
  //   val f = sqlContext.createDataFrame(sparkContext.makeRDD(f_data, 3)).toDF("fk", "x", "y").encrypted
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
    val rdd = sqlContext.createDataFrame(data).toDF("str", "x").oblivious
    val proj = rdd.select(substring($"str", 0, 8), $"x")
    assert(proj.collect ===
      data.map { case (str, x) => (str.substring(0, 8), x) }.map(Row.fromTuple))
  }

  test("encSelect - pagerank weight * rank") {
    val data = List((1, 2.0f, 3, 4.0f), (2, 0.5f, 1, 2.0f))
    val df = sqlContext.createDataFrame(data).toDF("id", "rank", "dst", "weight").oblivious
      .select($"dst", $"rank" * $"weight")
    val expected = for ((id, rank, dst, weight) <- data) yield (dst, rank * weight)
    assert(df.collect === expected.map(Row.fromTuple))
  }

  // ignore("encCache") {
  //   val data = List((1, 3), (1, 4), (1, 5), (2, 4))
  //   val df = sqlContext.createEncryptedDataFrame(
  //     sparkContext.makeRDD(QED.encryptN(data), 1),
  //     StructType(Seq(
  //       StructField("a", IntegerType),
  //       StructField("b", IntegerType))))
  //     .encCache()
  //   assert(QED.decrypt2[Int, Int](df.encCollect).sorted === data)
  //   val agg = df.groupBy($"a").encAgg(sum("b"))
  //   assert(QED.decrypt2[Int, Int](agg.encCollect).sorted === List((1, 12), (2, 4)))
  // }

  // TODO: test sensitivity propagation on operators

}
