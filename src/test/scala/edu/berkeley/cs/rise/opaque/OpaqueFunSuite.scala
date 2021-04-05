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

import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import org.apache.log4j.Level
import org.apache.log4j.LogManager

import org.scalatest.FunSuite
import org.scalactic.Equality
import org.scalactic.TolerantNumerics
import scala.collection.mutable

trait OpaqueFunSuite extends FunSuite {

  private def equalityToArrayEquality[A: Equality](): Equality[Array[A]] = {
    new Equality[Array[A]] {
      def areEqual(a: Array[A], b: Any): Boolean = {
        b match {
          case b: Array[_] =>
            (a.length == b.length
              && a.zip(b).forall { case (x, y) =>
                implicitly[Equality[A]].areEqual(x, y)
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
  implicit val tolerantFloatEquality = TolerantNumerics.tolerantFloatEquality(1e-6.toFloat)
  implicit val tolerantFloatArrayEquality = equalityToArrayEquality[Float]

  def checkAnswer[A: Equality](
      ignore: Boolean = false,
      isOrdered: Boolean = false,
      verbose: Boolean = false,
      printPlan: Boolean = false,
      shouldLogOperators: Boolean = false
  )(f: SecurityLevel => A): Unit = {
    if (ignore) {
      return
    }
    Utils.setOperatorLoggingLevel(shouldLogOperators)
    val (insecure, encrypted) = (f(Insecure), f(Encrypted))
    (insecure, encrypted) match {
      case (insecure: DataFrame, encrypted: DataFrame) =>
        val (insecureSeq, encryptedSeq) = (insecure.collect, encrypted.collect)
        val equal =
          if (isOrdered) insecureSeq === encryptedSeq
          else insecureSeq.toSet === encryptedSeq.toSet
        if (!equal) {
          if (printPlan) {
            println("**************** Spark Plan ****************")
            insecure.explain()
            println("**************** Opaque Plan ****************")
            encrypted.explain()
          }
          println(genError(insecureSeq, encryptedSeq, isOrdered, verbose))
        }
        assert(equal)
      case (insecure: Array[Array[Double]], encrypted: Array[Array[Double]]) =>
        for ((x, y) <- insecure.zip(encrypted)) {
          assert(x === y)
        }
      case _ =>
        assert(insecure === encrypted)
    }
  }

  def testOpaqueOnly(name: String)(f: SecurityLevel => Unit): Unit = {
    test(name + " - encrypted only") {
      f(Encrypted)
    }
  }

  def testSparkOnly(name: String)(f: SecurityLevel => Unit): Unit = {
    test(name + " - Spark only") {
      f(Insecure)
    }
  }

  def withLoggingOff[A](f: () => A): A = {
    val sparkLoggers = Seq(
      "org.apache.spark",
      "org.apache.spark.executor.Executor",
      "org.apache.spark.scheduler.TaskSetManager"
    )
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

  private def genError(
      sparkAnswer: Seq[Row],
      opaqueAnswer: Seq[Row],
      isOrdered: Boolean,
      verbose: Boolean
  ): String = {
    val getRowType: Option[Row] => String = row =>
      row
        .map(row =>
          if (row.schema == null) {
            "struct<>"
          } else {
            s"${row.schema.catalogString}"
          }
        )
        .getOrElse("struct<>")

    val sparkPrepared = prepareAnswer(sparkAnswer, isOrdered)
    val opaquePrepared = prepareAnswer(opaqueAnswer, isOrdered)
    val sparkDiff = sparkPrepared.diff(opaquePrepared)
    val opaqueDiff = opaquePrepared.diff(sparkPrepared)

    s"""
       |== Results ==
       |${sideBySide(
      s"== Spark ${if (verbose) "Result" else "Diff"} - ${if (verbose) sparkPrepared.size
       else sparkDiff.size} ==" +:
        getRowType(sparkAnswer.headOption) +:
        (if (verbose) sparkPrepared else sparkDiff).map(_.toString()),
      s"== Opaque ${if (verbose) "Result" else "Diff"} - ${if (verbose) opaquePrepared.size
       else opaqueDiff.size} ==" +:
        getRowType(opaqueAnswer.headOption) +:
        (if (verbose) opaquePrepared else opaqueDiff).map(_.toString())
    ).mkString("\n")}
    """.stripMargin
  }

  def prepareAnswer(answer: Seq[Row], isOrdered: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (!isOrdered) converted.sortBy(_.toString()) else converted
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case bd: java.math.BigDecimal => BigDecimal(bd)
      // Equality of WrappedArray differs for AnyVal and AnyRef in Scala 2.12.2+
      case seq: Seq[_] =>
        seq.map {
          case b: java.lang.Byte => b.byteValue
          case s: java.lang.Short => s.shortValue
          case i: java.lang.Integer => i.intValue
          case l: java.lang.Long => l.longValue
          case f: java.lang.Float => f.floatValue
          case d: java.lang.Double => d.doubleValue
          case x => x
        }
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }
}
