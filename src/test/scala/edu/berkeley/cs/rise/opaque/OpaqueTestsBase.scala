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


import scala.collection.mutable

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.scalactic.TolerantNumerics
import org.scalactic.Equality
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Tag

import edu.berkeley.cs.rise.opaque.benchmark._

trait OpaqueTestsBase extends FunSuite with BeforeAndAfterAll { self =>

  def spark: SparkSession
  def numPartitions: Int

  override def beforeAll(): Unit = {
    Utils.initSQLContext(spark.sqlContext)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  // Modify the behavior of === for Double and Array[Double] to use a numeric tolerance
  implicit val tolerantDoubleEquality = TolerantNumerics.tolerantDoubleEquality(1e-6)

  def equalityToArrayEquality[A : Equality](): Equality[Array[A]] = {
    new Equality[Array[A]] {
      def areEqual(a: Array[A], b: Any): Boolean = {
        b match {
          case b: Array[_] =>
            (a.length == b.length
              && a.zip(b).forall {
                case (x, y) => implicitly[Equality[A]].areEqual(x, y)
              })
          case _ => false
        }
      }
      override def toString: String = s"TolerantArrayEquality"
    }
  }

  def testAgainstSpark[A : Equality](name: String, testFunc: (String, Tag*) => ((=> Any) => Unit) = test)
      (f: SecurityLevel => A): Unit = {
    testFunc(name + " - encrypted") {
      // The === operator uses implicitly[Equality[A]], which compares Double and Array[Double]
      // using the numeric tolerance specified above
      assert(f(Insecure) === f(Encrypted))
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
    val sparkLoggers = Seq(
      "org.apache.spark",
      "org.apache.spark.executor.Executor",
      "org.apache.spark.scheduler.TaskSetManager")
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
}
