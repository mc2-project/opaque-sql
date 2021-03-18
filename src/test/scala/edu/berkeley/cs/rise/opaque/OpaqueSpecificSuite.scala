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

class OpaqueSpecificSuite extends OpaqueSuiteBase with SinglePartitionSparkSession {
  import spark.implicits._

  def abc(i: Int): String = (i % 3) match {
    case 0 => "A"
    case 1 => "B"
    case 2 => "C"
  }

  def makeEncryptedDF[
      A <: Product: scala.reflect.ClassTag: scala.reflect.runtime.universe.TypeTag
  ](data: Seq[A], columnNames: String*): DataFrame =
    Encrypted.applyTo(
      spark
        .createDataFrame(spark.sparkContext.makeRDD(data, numPartitions))
        .toDF(columnNames: _*)
    )

  test("java encryption/decryption") {
    val data = Array[Byte](0, 1, 2)
    val (enclave, eid) = Utils.initEnclave()
    assert(data === Utils.decrypt(Utils.encrypt(data)))
    assert(data === Utils.decrypt(enclave.Encrypt(eid, data)))
  }

  test("cache") {
    def numCached(ds: Dataset[_]): Int =
      ds.queryExecution.executedPlan.collect {
        case cached: EncryptedBlockRDDScanExec
            if cached.rdd.getStorageLevel != StorageLevel.NONE =>
          cached
      }.size

    val data = List((1, 3), (1, 4), (1, 5), (2, 4))
    val df = makeEncryptedDF(data, "a", "b").cache()

    val agg = df.groupBy($"a").agg(sum("b"))

    assert(numCached(agg) === 1)

    val expected = data.groupBy(_._1).mapValues(_.map(_._2).sum)
    assert(agg.collect.toSet === expected.map(Row.fromTuple).toSet)
    df.unpersist()
  }

  test("save and load EncryptedSource with explicit schema") {
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val df = makeEncryptedDF(data, "id", "word", "count")
    val path = Utils.createTempDir()
    path.delete()
    df.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save(path.toString)
    try {
      val df2 = spark.read
        .format("edu.berkeley.cs.rise.opaque.EncryptedSource")
        .schema(df.schema)
        .load(path.toString)
      assert(df.collect.toSet === df2.collect.toSet)
      assert(
        df.groupBy("word").agg(sum("count")).collect.toSet
          === df2.groupBy("word").agg(sum("count")).collect.toSet
      )
    } finally {
      Utils.deleteRecursively(path)
    }
  }

  test("save and load EncryptedSource without schema") {
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val df = makeEncryptedDF(data, "id", "word", "count")
    val path = Utils.createTempDir()
    path.delete()
    df.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save(path.toString)
    try {
      val df2 = spark.read
        .format("edu.berkeley.cs.rise.opaque.EncryptedSource")
        .load(path.toString)
      assert(df.collect.toSet === df2.collect.toSet)
      assert(
        df.groupBy("word").agg(sum("count")).collect.toSet
          === df2.groupBy("word").agg(sum("count")).collect.toSet
      )
    } finally {
      Utils.deleteRecursively(path)
    }
  }

  test("load from SQL with explicit schema") {
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val df = makeEncryptedDF(data, "id", "word", "count")
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

  test("load from SQL without schema") {
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val df = makeEncryptedDF(data, "id", "word", "count")
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

  test("cast error") {
    val data: Seq[(CalendarInterval, Byte)] = Seq((new CalendarInterval(12, 1, 12345), 0.toByte))
    val schema = StructType(
      Seq(
        StructField("CalendarIntervalType", CalendarIntervalType),
        StructField("NullType", NullType)
      )
    )
    val df = Encrypted.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema
      )
    )
    // Trigger an Opaque exception by attempting an unsupported cast: CalendarIntervalType to StringType
    val e = intercept[SparkException] {
      withLoggingOff {
        df.select($"CalendarIntervalType".cast(StringType)).collect
      }
    }
    assert(e.getCause.isInstanceOf[OpaqueException])
  }
}
