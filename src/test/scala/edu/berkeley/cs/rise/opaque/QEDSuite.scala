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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import edu.berkeley.cs.rise.opaque.benchmark._
import edu.berkeley.cs.rise.opaque.execution._
import edu.berkeley.cs.rise.opaque.implicits._

class QEDSuite extends FunSuite with BeforeAndAfterAll {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("QEDSuite")
    .getOrCreate()

  Utils.initSQLContext(spark.sqlContext)

  import spark.implicits._

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("flatbuffers filter") {
    val df = spark.createDataFrame(
      (1 to 20).map(x => (true, "hello", 1.0, 2.0f, x)))
      .toDF("a", "b", "c", "d", "x").encrypted
      .filter($"x" > lit(10))
    df.explain(true)
    df.show
  }

  test("flatbuffers project") {
    val df = spark.createDataFrame(
      (1 to 20).map(x => (true, "hello world!", 1.0, 2.0f, x)))
      .toDF("a", "b", "c", "d", "x").encrypted
      .select(
        $"x" + $"x" * $"x" - $"x",
        substring($"b", 5, 20),
        $"x" > $"x",
        $"x" >= $"x",
        $"x" <= $"x")
    df.explain(true)
    df.show
  }

  test("flatbuffers sort") {
    val df = spark.createDataFrame(
      (1 to 20).map(x => (x, x.toString)).reverse)
      .toDF("a", "b").encrypted
      .sort($"a")
    df.explain(true)
    df.show(50)
  }

  test("flatbuffers union") {
    val df1 = spark.createDataFrame(
      (1 to 20).map(x => (x, x.toString)).reverse)
      .toDF("a", "b").encrypted
    val df2 = spark.createDataFrame(
      (1 to 20).map(x => (x, (x + 1).toString)))
      .toDF("a", "b").encrypted
    val df = df1.union(df2)
    df.explain(true)
    df.show(50)
  }

  test("flatbuffers join") {
    val df1 = spark.createDataFrame(
      (1 to 20).map(x => (x, x.toString)).reverse)
      .toDF("a", "b").encrypted
    val df2 = spark.createDataFrame(
      (1 to 20).map(x => (x, (x + 1).toString)))
      .toDF("c", "d").encrypted
    val df = df1.join(df2, $"a" === $"c")
    df.explain(true)
    df.show(50)
  }

  test("flatbuffers join - different schemas") {
    val df1 = spark.createDataFrame(
      (1 to 20).map(x => (x + 1, (x + 1).toString, x + 1)).reverse)
      .toDF("a", "b", "c").encrypted
    val df2 = spark.createDataFrame(
      (1 to 20).map(x => (x, x * 2)))
      .toDF("d", "e").encrypted
    val df = df1.join(df2, $"a" === $"d")
    df.explain(true)
    df.show(50)
  }

  test("nonObliviousSort multiple partitions") {
    val data = Random.shuffle(for (i <- 0 until 256) yield (i, i.toString, 1))
    val sorted = spark.createDataFrame(spark.sparkContext.makeRDD(data, 3))
      .toDF("id", "word", "count")
      .encrypted.sort($"word")
    assert(sorted.collect === data.sortBy(_._2).map(Row.fromTuple))
  }

  // ignore("nonObliviousJoin multiple partitions") {
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

  // TODO: test sensitivity propagation on operators

  // TODO: test nonObliviousAggregate on multiple partitions

  // test remote attestation
  ignore("Remote attestation") {
    val data = for (i <- 0 until 8) yield (i)
    RA.initRA(spark.sparkContext.parallelize(data, 2))
  }
}
