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
import org.apache.spark.sql.DataFrame
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

import edu.berkeley.cs.rise.opaque.benchmark._
import edu.berkeley.cs.rise.opaque.execution.Opcode._
import edu.berkeley.cs.rise.opaque.execution._
import edu.berkeley.cs.rise.opaque.implicits._

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

  def testAgainstSpark(name: String)(f: SecurityLevel => Seq[Any]): Unit = {
    test(name + " - encrypted") {
      assert(f(Encrypted) === f(Insecure))
    }
    test(name + " - oblivious") {
      assert(f(Oblivious) === f(Insecure))
    }
  }

  def testOpaqueOnly(name: String)(f: SecurityLevel => Unit): Unit = {
    test(name + " - encrypted") {
      f(Encrypted)
    }
    test(name + " - oblivious") {
      f(Oblivious)
    }
  }

  def testOpaqueObliviousOnly(name: String)(f: SecurityLevel => Unit): Unit = {
    test(name + " - oblivious") {
      f(Oblivious)
    }
  }

  def testSparkOnly(name: String)(f: SecurityLevel => Unit): Unit = {
    test(name + " - Spark") {
      f(Insecure)
    }
  }

  testOpaqueOnly("pagerank") { securityLevel =>
    PageRank.run(spark, securityLevel, "256", numPartitions)
  }

  testOpaqueOnly("join reordering") { securityLevel =>
    JoinReordering.treatmentQuery(spark, "125", numPartitions)
  }

  testOpaqueOnly("join cost") { securityLevel =>
    JoinCost.run(spark, Oblivious, "125", numPartitions)
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

  testAgainstSpark("create DataFrame from sequence") { securityLevel =>
    val data = for (i <- 0 until 5) yield ("foo", i)
    makeDF(data, securityLevel, "word", "count").collect
  }

  testAgainstSpark("filter") { securityLevel =>
    val data = for (i <- 0 until 5) yield ("foo", i)
    val words = makeDF(data, securityLevel, "word", "count")

    words.select($"word", $"count" + 1).collect
  }

  def abc(i: Int): String = (i % 3) match {
    case 0 => "A"
    case 1 => "B"
    case 2 => "C"
  }

  testAgainstSpark("aggregate") { securityLevel =>
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

  testAgainstSpark("sort") { securityLevel =>
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x)).toSeq)
    val df = makeDF(data, securityLevel, "str", "x")
    df.sort($"x").collect
  }

  testAgainstSpark("sort by float") { securityLevel =>
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x.toFloat)).toSeq)
    val df = makeDF(data, securityLevel, "str", "x")
    df.sort($"x").collect
  }

  testAgainstSpark("join") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i, i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield (i, (i % 16).toString, i * 10)
    val p = makeDF(p_data, securityLevel, "id", "pk", "x")
    val f = makeDF(f_data, securityLevel, "id", "fk", "x")
    p.join(f, $"pk" === $"fk").collect.toSet.toSeq
  }

  testAgainstSpark("join on column 1") { securityLevel =>
    val p_data = for (i <- 1 to 16) yield (i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield ((i % 16).toString, (i * 10).toString, i.toFloat)
    val p = makeDF(p_data, securityLevel, "pk", "x")
    val f = makeDF(f_data, securityLevel, "fk", "x", "y")
    p.join(f, $"pk" === $"fk").collect.toSet.toSeq
  }

  testAgainstSpark("select") { securityLevel =>
    val data = for (i <- 0 until 256) yield ("%03d".format(i) * 3, i.toFloat)
    val df = makeDF(data, securityLevel, "str", "x")
    df.select($"str").collect
  }

  testOpaqueOnly("cache") { securityLevel =>
    def numCached(ds: Dataset[_]): Int =
      ds.queryExecution.executedPlan.collect {
        case cached: EncryptedBlockRDDScanExec => cached
      }.size

    val data = List((1, 3), (1, 4), (1, 5), (2, 4))
    val df = makeDF(data, securityLevel, "a", "b").cache()

    val agg = df.groupBy($"a").agg(sum("b"))

    assert(numCached(agg) === 1)

    val expected = data.groupBy(_._1).mapValues(_.map(_._2).sum)
    assert(agg.collect.toSet === expected.map(Row.fromTuple).toSet)
  }

  testOpaqueOnly("global aggregate") { securityLevel =>
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = makeDF(data, securityLevel, "id", "word", "count")
    val result = words.agg(sum("count").as("totalCount"))
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
    .getOrCreate()

  override def numPartitions = 1
}

// class OpaqueMultiplePartitionSuite extends OpaqueOperatorTests {
//   override val spark = SparkSession.builder()
//     .master("local[3]")
//     .appName("QEDSuite")
//     .getOrCreate()

//   override def numPartitions = 3
// }
