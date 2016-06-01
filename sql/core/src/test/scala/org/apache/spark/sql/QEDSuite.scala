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
import org.apache.spark.sql.execution.ConvertToBlocks
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DateType
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

  test("pagerank") {
    QEDBenchmark.pagerank(sqlContext, "32768")
  }

  test("big data 1") {
    assert(QEDBenchmark.bd1SparkSQL(sqlContext, "tiny").collect ===
      QEDBenchmark.bd1Opaque(sqlContext, "tiny").collect)
  }

  test("big data 2") {
    val a = QEDBenchmark.bd2SparkSQL(sqlContext, "tiny").collect.sortBy(_.getString(0)).map {
      case Row(str: String, f: Double) => Row(str, "%.2f".format(f))
    }

    val b = QEDBenchmark.bd2Opaque(sqlContext, "tiny").collect.map {
      case Row(str: String, f: Float) => Row(str, "%.2f".format(f))
    }

    assert(a.length === b.length)

    for ((x, y) <- a.zip(b)) {
      assert(x === y)
    }
  }

  test("big data 3") {
    assert(QEDBenchmark.bd3SparkSQL(sqlContext, "tiny").collect ===
      QEDBenchmark.bd3Opaque(sqlContext, "tiny").collect)
  }

  test("columnsort padding") {
    val data = Random.shuffle((0 until 3).map(x => (x.toString, x)).toSeq)
    val encData = QED.encrypt2(data).map {
      case (str, x) => InternalRow(str, x).encSerialize
    }
    val sorted = ObliviousSort.ColumnSort(
      sparkContext, sparkContext.makeRDD(encData, 1), OP_SORT_COL2)
      .map(row => Row(QED.parseRow(row): _*)).collect
    assert(QED.decrypt2[String, Int](sorted) === data.sortBy(_._2))
  }

  test("columnsort on join rows") {
    val p_data = for (i <- 1 to 16) yield (i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield ((i % 16).toString, (i * 10).toString, i.toFloat)
    val p = sparkContext.makeRDD(QED.encrypt2(p_data), 5)
    val f = sparkContext.makeRDD(QED.encrypt3(f_data), 5)
    val j = p.zipPartitions(f) { (pIter, fIter) =>
      val (enclave, eid) = QED.initEnclave()
      val pArr = pIter.toArray
      val fArr = fIter.toArray
      val p = QED.createBlock(
        pArr.map(r => InternalRow.fromSeq(r.productIterator.toSeq)).map(_.encSerialize), false)
      val f = QED.createBlock(
        fArr.map(r => InternalRow.fromSeq(r.productIterator.toSeq)).map(_.encSerialize), false)
      val r = enclave.JoinSortPreprocess(
        eid, OP_JOIN_COL1.value, p, pArr.length, f, fArr.length)
      Iterator(Block(r, pArr.length + fArr.length))
    }
    val sorted = ObliviousSort.sortBlocks(j, OP_JOIN_COL1).flatMap { block =>
      QED.splitBlock(block.bytes, block.numRows, true)
        .map(serRow => Row.fromSeq(QED.parseRow(serRow)))
    }
    assert(sorted.collect.length === p_data.length + f_data.length)
  }

  test("encFilter") {
    val data = for (i <- 0 until 256) yield ("foo", i)
    val words = sparkContext.makeRDD(QED.encrypt2(data), 1).toDF("word", "count")
    assert(QED.decrypt2(words.collect) === data)

    val filtered = words.encFilter($"count", OP_FILTER_COL2_GT3)
    assert(QED.decrypt2[String, Int](filtered.collect).sorted === data.filter(_._2 > 3).sorted)
  }

  test("encFilter on date") {
    import java.sql.Date
    val dates = List("1975-01-01", "1980-01-01", "1980-03-02", "1980-04-01", "1990-01-01")
    val filteredDates = List("1980-01-01", "1980-03-02", "1980-04-01")
    val data = sqlContext.createDataFrame(
      sparkContext.makeRDD(dates.map(d =>
        Row(DateTimeUtils.toJavaDate(DateTimeUtils.stringToDate(UTF8String.fromString(d)).get))), 1),
      StructType(Seq(StructField("date", DateType))))
    val filtered = data.filter($"date" >= lit("1980-01-01"))
      .filter($"date" <= lit("1980-04-01"))
    assert(filtered.collect.map(_.get(0).toString).sorted === filteredDates.sorted)

    val encDates = data.mapPartitions { iter =>
      val (enclave, eid) = QED.initEnclave()
      iter.map {
        case Row(d: java.sql.Date) => QED.encrypt(enclave, eid, d)
      }
    }.toDF("date")
    val encFiltered = encDates.encFilter(
      $"date", OP_FILTER_COL1_DATE_BETWEEN_1980_01_01_AND_1980_04_01)
    assert(QED.decrypt1[java.sql.Date](encFiltered.collect).map(_.toString).sorted ===
      filteredDates.sorted)
  }

  test("encPermute") {
    val array = (0 until 256).toArray
    val permuted =
      sqlContext.createDataFrame(
        sparkContext.makeRDD(QED.encrypt1(array).map(Row(_)), 1),
        StructType(List(StructField("x", BinaryType, true))))
        .encPermute().collect
    assert(QED.decrypt1[Int](permuted) !== array)
    assert(QED.decrypt1[Int](permuted).sorted === array)
  }

  test("encAggregate") {
    def abc(i: Int): String = (i % 3) match {
      case 0 => "A"
      case 1 => "B"
      case 2 => "C"
    }
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = sparkContext.makeRDD(QED.encrypt3(data), 1).toDF("id", "word", "count")

    val summed = words.encAggregate(OP_GROUPBY_COL2_SUM_COL3_INT_STEP1,
      $"word", $"count".as("totalCount"))
    assert(QED.decrypt2[String, Int](summed.collect) ===
      data.map(p => (p._2, p._3)).groupBy(_._1).mapValues(_.map(_._2).sum).toSeq.sorted)
  }

  test("encAggregate - final run split across multiple partitions") {
    val data = for (i <- 0 until 256) yield (i, "A", 1)
    val words = sparkContext.makeRDD(QED.encrypt3(data), 2).toDF("id", "word", "count")

    val summed = words.encAggregate(OP_GROUPBY_COL2_SUM_COL3_INT_STEP1,
      $"word", $"count".as("totalCount"))
    assert(QED.decrypt2[String, Int](summed.collect) ===
      data.map(p => (p._2, p._3)).groupBy(_._1).mapValues(_.map(_._2).sum).toSeq.sorted)
  }

  test("encAggregate on multiple columns") {
    def abc(i: Int): String = (i % 3) match {
      case 0 => "A"
      case 1 => "B"
      case 2 => "C"
    }
    val data = for (i <- 0 until 256) yield (abc(i), 1, 1.0f)
    val words = sparkContext.makeRDD(QED.encrypt3(data), 1).toDF("str", "x", "y")

    val summed = words.encAggregate(OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP1,
      $"str", $"x".as("avgX"), $"y".as("totalY"))
    assert(QED.decrypt3[String, Int, Float](summed.collect) ===
      data.groupBy(_._1).mapValues(group =>
        (group.map(_._2).sum / group.map(_._2).size, group.map(_._3).sum))
      .toSeq.map { case (str, (avgX, avgY)) => (str, avgX, avgY) }.sorted)
  }

  test("encSort") {
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x)).toSeq)
    val sorted = sparkContext.makeRDD(QED.encrypt2(data), 1).toDF("str", "x").encSort($"x").collect
    assert(QED.decrypt2[String, Int](sorted) === data.sortBy(_._2))
  }

  test("encSort by float") {
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x.toFloat)).toSeq)
    val sorted = sparkContext.makeRDD(QED.encrypt2(data), 1).toDF("str", "x").encSort($"x").collect
    assert(QED.decrypt2[String, Float](sorted) === data.sortBy(_._2))
  }

  test("encSort multiple partitions") {
    val data = Random.shuffle(for (i <- 0 until 256) yield (i, i.toString, 1))
    val sorted = sparkContext.makeRDD(QED.encrypt3(data), 3).toDF("id", "word", "count").encSort($"word").collect
    assert(QED.decrypt3[Int, String, Int](sorted) === data.sortBy(_._2))
  }

  test("encJoin") {
    val p_data = for (i <- 1 to 16) yield (i, i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield (i, (i % 16).toString, i * 10)
    val p = sparkContext.makeRDD(QED.encrypt3(p_data), 1).toDF("id", "pk", "x")
    val f = sparkContext.makeRDD(QED.encrypt3(f_data), 1).toDF("id", "fk", "x")
    val joined = p.encJoin(f, $"pk", $"fk").collect
    val expectedJoin =
      for {
        (p_id, pk, p_x) <- p_data
        (f_id, fk, f_x) <- f_data
        if pk == fk
      } yield (p_id, pk, p_x, f_id, f_x)
    assert(QED.decrypt5[Int, String, Int, Int, Int](joined).toSet === expectedJoin.toSet)
  }

  test("encJoin on column 1") {
    val p_data = for (i <- 1 to 16) yield (i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield ((i % 16).toString, (i * 10).toString, i.toFloat)
    val p = sparkContext.makeRDD(QED.encrypt2(p_data), 1).toDF("pk", "x")
    val f = sparkContext.makeRDD(QED.encrypt3(f_data), 1).toDF("fk", "x", "y")
    val joined = p.encJoin(f, $"pk", $"fk").collect
    val expectedJoin =
      for {
        (pk, p_x) <- p_data
        (fk, f_x, f_y) <- f_data
        if pk == fk
      } yield (pk, p_x, f_x, f_y)
    assert(QED.decrypt4[String, Int, String, Float](joined).toSet === expectedJoin.toSet)
  }

  test("encProject") {
    def encrypt2[A, B](rows: Seq[(A, B)]): Seq[(Array[Byte], Array[Byte])] = {
      val (enclave, eid) = QED.initEnclave()
      rows.map {
        case (a, b) =>
          (QED.encrypt(enclave, eid, a, Some(QEDColumnType.IP_TYPE)),
            QED.encrypt(enclave, eid, b))
      }
    }
    val data = for (i <- 0 until 256) yield ("%03d".format(i) * 3, i.toFloat)
    val rdd = sparkContext.makeRDD(encrypt2(data), 1).toDF("str", "x")
    val proj = rdd.encProject(OP_BD2, $"str", $"x") // substring($"str", 0, 8)
    assert(QED.decrypt2(proj.collect) === data.map { case (str, x) => (str.substring(0, 8), x) })
  }

  test("encProject - pagerank weight * rank") {
    val data = List((1, 2.0f, 3, 4.0f), (2, 0.5f, 1, 2.0f))
    val df = sparkContext.makeRDD(QED.encrypt4(data), 1).toDF("id", "rank", "dst", "weight")
      .encProject(OP_PROJECT_PAGERANK_WEIGHT_RANK, $"dst", $"rank")
    val expected = for ((id, rank, dst, weight) <- data) yield (dst, rank * weight)
    assert(QED.decrypt2(df.collect) === expected)
  }

  test("JNIEncrypt") {

    def byteArrayToString(x: Array[Byte]) = {
      val loc = x.indexOf(0)
      if (-1 == loc)
        new String(x)
      else if (0 == loc)
        ""
      else
        new String(x, 0, loc, "UTF-8") // or appropriate encoding
    }

    val (enclave, eid) = QED.initEnclave()

    // Test encryption and decryption

    val plaintext = "Hello world!1234"
    val plaintext_bytes = plaintext.getBytes
    val ciphertext = enclave.Encrypt(eid, plaintext_bytes)

    val decrypted = enclave.Decrypt(eid, ciphertext)

    // println("decrypted's length is " + decrypted.length)

    assert(plaintext_bytes.length == decrypted.length)

    for (idx <- 0 to plaintext_bytes.length - 1) {
      assert(plaintext_bytes(idx) == decrypted(idx))
    }
  }
}
