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

import java.nio.charset.StandardCharsets

import edu.berkeley.cs.rise.opaque.implicits._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext, SQLImplicits}
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * A collection of sample data used in SQL tests.
 * From https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/test/SQLTestData.scala
 */
protected trait SQLTestData {

  protected def spark: SparkSession

  // Helper object to import SQL implicits without a concrete SQLContext
  private object internalImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  import internalImplicits._
  import SQLTestData._

  // Note: all test data should be lazy because the SQLContext is not set up yet.

  def emptyTestData(securityLevel: SecurityLevel): DataFrame = {
    val df =
      securityLevel.applyTo(
        spark.sparkContext.parallelize(Seq.empty[Int].map(i => TestData(i, i.toString))).toDF()
      )
    df.createOrReplaceTempView("emptyTestData")
    df
  }

  def testData(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext.parallelize((1 to 100).map(i => TestData(i, i.toString))).toDF()
    )
    df.createOrReplaceTempView("testData")
    df
  }

  def testData2(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          TestData2(1, 1) ::
            TestData2(1, 2) ::
            TestData2(2, 1) ::
            TestData2(2, 2) ::
            TestData2(3, 1) ::
            TestData2(3, 2) :: Nil,
          2
        )
        .toDF()
    )
    df.createOrReplaceTempView("testData2")
    df
  }

  def testData3(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          TestData3(1, None) ::
            TestData3(2, Some(2)) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("testData3")
    df
  }

  def negativeData(securityLevel: SecurityLevel): DataFrame = {
    val df =
      securityLevel.applyTo(
        spark.sparkContext.parallelize((1 to 100).map(i => TestData(-i, (-i).toString))).toDF()
      )
    df.createOrReplaceTempView("negativeData")
    df
  }

  def largeAndSmallInts(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          LargeAndSmallInts(2147483644, 1) ::
            LargeAndSmallInts(1, 2) ::
            LargeAndSmallInts(2147483645, 1) ::
            LargeAndSmallInts(2, 2) ::
            LargeAndSmallInts(2147483646, 1) ::
            LargeAndSmallInts(3, 2) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("largeAndSmallInts")
    df
  }

  def decimalData(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          DecimalData(1, 1) ::
            DecimalData(1, 2) ::
            DecimalData(2, 1) ::
            DecimalData(2, 2) ::
            DecimalData(3, 1) ::
            DecimalData(3, 2) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("decimalData")
    df
  }

  def binaryData(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          BinaryData("12".getBytes(StandardCharsets.UTF_8), 1) ::
            BinaryData("22".getBytes(StandardCharsets.UTF_8), 5) ::
            BinaryData("122".getBytes(StandardCharsets.UTF_8), 3) ::
            BinaryData("121".getBytes(StandardCharsets.UTF_8), 2) ::
            BinaryData("123".getBytes(StandardCharsets.UTF_8), 4) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("binaryData")
    df
  }

  def upperCaseData(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          UpperCaseData(1, "A") ::
            UpperCaseData(2, "B") ::
            UpperCaseData(3, "C") ::
            UpperCaseData(4, "D") ::
            UpperCaseData(5, "E") ::
            UpperCaseData(6, "F") :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("upperCaseData")
    df
  }

  def lowerCaseData(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          LowerCaseData(1, "a") ::
            LowerCaseData(2, "b") ::
            LowerCaseData(3, "c") ::
            LowerCaseData(4, "d") :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("lowerCaseData")
    df
  }

  def lowerCaseDataWithDuplicates(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          LowerCaseData(1, "a") ::
            LowerCaseData(2, "b") ::
            LowerCaseData(2, "b") ::
            LowerCaseData(3, "c") ::
            LowerCaseData(3, "c") ::
            LowerCaseData(3, "c") ::
            LowerCaseData(4, "d") :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("lowerCaseData")
    df
  }

  def arrayData(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          ArrayData(Seq(1, 2, 3), Seq(Seq(1, 2, 3))) ::
            ArrayData(Seq(2, 3, 4), Seq(Seq(2, 3, 4))) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("arrayData")
    df
  }

  def mapData(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          MapData(Map(1 -> "a1", 2 -> "b1", 3 -> "c1", 4 -> "d1", 5 -> "e1")) ::
            MapData(Map(1 -> "a2", 2 -> "b2", 3 -> "c2", 4 -> "d2")) ::
            MapData(Map(1 -> "a3", 2 -> "b3", 3 -> "c3")) ::
            MapData(Map(1 -> "a4", 2 -> "b4")) ::
            MapData(Map(1 -> "a5")) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("mapData")
    df
  }

  def calendarIntervalData(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext.parallelize(IntervalData(new CalendarInterval(1, 1, 1)) :: Nil).toDF()
    )
    df.createOrReplaceTempView("calendarIntervalData")
    df
  }

  def repeatedData(securityLevel: SecurityLevel): DataFrame = {
    val df =
      securityLevel.applyTo(
        spark.sparkContext.parallelize(List.fill(2)(StringData("test"))).toDF()
      )
    df.createOrReplaceTempView("repeatedData")
    df
  }

  def nullableRepeatedData(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          List.fill(2)(StringData(null)) ++
            List.fill(2)(StringData("test"))
        )
        .toDF()
    )
    df.createOrReplaceTempView("nullableRepeatedData")
    df
  }

  def nullInts(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          NullInts(1) ::
            NullInts(2) ::
            NullInts(3) ::
            NullInts(null) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("nullInts")
    df
  }

  def allNulls(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          NullInts(null) ::
            NullInts(null) ::
            NullInts(null) ::
            NullInts(null) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("allNulls")
    df
  }

  def nullStrings(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          NullStrings(1, "abc") ::
            NullStrings(2, "ABC") ::
            NullStrings(3, null) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("nullStrings")
    df
  }

  def tableName(securityLevel: SecurityLevel): DataFrame = {
    val df =
      securityLevel.applyTo(spark.sparkContext.parallelize(TableName("test") :: Nil).toDF())
    df.createOrReplaceTempView("tableName")
    df
  }

  def unparsedStrings(securityLevel: SecurityLevel): DataFrame = {
    securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          "1, A1, true, null" ::
            "2, B2, false, null" ::
            "3, C3, true, null" ::
            "4, D4, true, 2147483644" :: Nil
        )
        .toDF()
    )
  }

  // An RDD with 4 elements and 8 partitions
  def withEmptyParts(securityLevel: SecurityLevel): DataFrame = {
    val df =
      securityLevel.applyTo(spark.sparkContext.parallelize((1 to 4).map(IntField), 8).toDF())
    df.createOrReplaceTempView("withEmptyParts")
    df
  }

  def person(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          Person(0, "mike", 30) ::
            Person(1, "jim", 20) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("person")
    df
  }

  def salary(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          Salary(0, 2000.0) ::
            Salary(1, 1000.0) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("salary")
    df
  }

  def complexData(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          ComplexData(Map("1" -> 1), TestData(1, "1"), Seq(1, 1, 1), true) ::
            ComplexData(Map("2" -> 2), TestData(2, "2"), Seq(2, 2, 2), false) ::
            Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("complexData")
    df
  }

  def courseSales(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          CourseSales("dotNET", 2012, 10000) ::
            CourseSales("Java", 2012, 20000) ::
            CourseSales("dotNET", 2012, 5000) ::
            CourseSales("dotNET", 2013, 48000) ::
            CourseSales("Java", 2013, 30000) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("courseSales")
    df
  }

  def trainingSales(securityLevel: SecurityLevel): DataFrame = {
    val df = securityLevel.applyTo(
      spark.sparkContext
        .parallelize(
          TrainingSales("Experts", CourseSales("dotNET", 2012, 10000)) ::
            TrainingSales("Experts", CourseSales("JAVA", 2012, 20000)) ::
            TrainingSales("Dummies", CourseSales("dotNet", 2012, 5000)) ::
            TrainingSales("Experts", CourseSales("dotNET", 2013, 48000)) ::
            TrainingSales("Dummies", CourseSales("Java", 2013, 30000)) :: Nil
        )
        .toDF()
    )
    df.createOrReplaceTempView("trainingSales")
    df
  }

  /**
   * Initialize all test data such that all temp tables are properly registered.
   */
  def loadTestData(securityLevel: SecurityLevel): Unit = {
    assert(spark != null, "attempted to initialize test data before SparkSession.")
    emptyTestData(securityLevel)
    testData(securityLevel)
    testData2(securityLevel)
    testData3(securityLevel)
    negativeData(securityLevel)
    largeAndSmallInts(securityLevel)
    decimalData(securityLevel)
    binaryData(securityLevel)
    upperCaseData(securityLevel)
    lowerCaseData(securityLevel)
    arrayData(securityLevel)
    mapData(securityLevel)
    repeatedData(securityLevel)
    nullableRepeatedData(securityLevel)
    nullInts(securityLevel)
    allNulls(securityLevel)
    nullStrings(securityLevel)
    tableName(securityLevel)
    unparsedStrings(securityLevel)
    withEmptyParts(securityLevel)
    person(securityLevel)
    salary(securityLevel)
    complexData(securityLevel)
    courseSales(securityLevel)
  }

  def showData(tableName: String) = {
    spark.table(tableName).show()
  }
}

/**
 * Case classes used in test data.
 */
protected object SQLTestData {
  case class TestData(key: Int, value: String)
  case class TestData2(a: Int, b: Int)
  case class TestData3(a: Int, b: Option[Int])
  case class LargeAndSmallInts(a: Int, b: Int)
  case class DecimalData(a: BigDecimal, b: BigDecimal)
  case class BinaryData(a: Array[Byte], b: Int)
  case class UpperCaseData(N: Int, L: String)
  case class LowerCaseData(n: Int, l: String)
  case class ArrayData(data: Seq[Int], nestedData: Seq[Seq[Int]])
  case class MapData(data: scala.collection.Map[Int, String])
  case class StringData(s: String)
  case class IntField(i: Int)
  case class NullInts(a: Integer)
  case class NullStrings(n: Int, s: String)
  case class TableName(tableName: String)
  case class Person(id: Int, name: String, age: Int)
  case class Salary(personId: Int, salary: Double)
  case class ComplexData(m: Map[String, Int], s: TestData, a: Seq[Int], b: Boolean)
  case class CourseSales(course: String, year: Int, earnings: Double)
  case class TrainingSales(training: String, sales: CourseSales)
  case class IntervalData(data: CalendarInterval)
}
