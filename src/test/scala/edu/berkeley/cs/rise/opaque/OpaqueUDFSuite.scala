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

import java.util.Random

import org.apache.spark.sql.functions._

import edu.berkeley.cs.rise.opaque.execution.udfs.LogisticRegression

import edu.berkeley.cs.rise.opaque.benchmark._
import edu.berkeley.cs.rise.opaque.expressions.DotProduct.dot
import edu.berkeley.cs.rise.opaque.expressions.VectorMultiply.vectormultiply
import edu.berkeley.cs.rise.opaque.expressions.VectorSum

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/* Contains non-query related Spark tests */
trait OpaqueUDFSuite extends OpaqueSuiteBase {
  import spark.implicits._

  test("exp") {
    checkAnswer() { sl =>
      val data: Seq[(Double, Double)] = Seq((2.0, 3.0))
      val df = makeDF(data, sl, "x", "y")
      df.select(exp($"y"))
    }
  }

  test("vector multiply") {
    checkAnswer() { sl =>
      val data: Seq[(Array[Double], Double)] = Seq((Array[Double](1.0, 1.0, 1.0), 3.0))
      val df = makeDF(data, sl, "v", "c")
      df.select(vectormultiply($"v", $"c"))
    }
  }

  test("dot product") {
    checkAnswer() { sl =>
      val data: Seq[(Array[Double], Array[Double])] =
        Seq((Array[Double](1.0, 1.0, 1.0), Array[Double](1.0, 1.0, 1.0)))
      val df = makeDF(data, sl, "v1", "v2")
      df.select(dot($"v1", $"v2"))
    }
  }

  test("vector sum") {
    checkAnswer() { sl =>
      val data: Seq[(Array[Double], Double)] =
        Seq((Array[Double](1.0, 2.0, 3.0), 4.0), (Array[Double](5.0, 7.0, 7.0), 8.0))
      val df = makeDF(data, sl, "v", "c")
      val vectorsum = new VectorSum
      df.groupBy().agg(vectorsum($"v"))
    }
  }

  test("least squares") {
    checkAnswer() { sl =>
      LeastSquares.query(spark, sl, "tiny", numPartitions)
    }
  }

  test("logistic regression") {
    checkAnswer() { sl =>
      def generateData(
          spark: SparkSession,
          sl: SecurityLevel,
          N: Int,
          D: Int,
          R: Double
      ): DataFrame = {
        val rand = new Random(42)
        def generatePoint(i: Int): (Array[Double], Double) = {
          val y = if (i % 2 == 0) 0 else 1
          val x = Array.fill(D) { rand.nextGaussian + y * R }
          (x, y)
        }

        val data = Array.tabulate(N)(generatePoint)
        val schema = StructType(
          Seq(
            StructField("x", DataTypes.createArrayType(DoubleType)),
            StructField("y", DoubleType)
          )
        )

        sl.applyTo(
          spark.createDataFrame(spark.sparkContext.makeRDD(data.map(Row.fromTuple)), schema)
        )
      }
      checkAnswer() { sl =>
        val N = 1000
        val D = 5
        val R = 0.7
        val A = 0.1
        val ITERATIONS = 1

        val trainingData =
          Utils.ensureCached(generateData(spark, sl, N, D, R))
        val w = LogisticRegression.train(spark, D, A, ITERATIONS, trainingData)

        val evalData = generateData(spark, sl, N, D, R).drop("y")
        LogisticRegression.predict(spark, w, evalData)
      }
    }
  }

  test("prepare") {
    checkAnswer(ignore = true) { sl =>
      val df =
        makeDF({ for (i <- 1 to 100) yield (i, -i, 0.25, i % 2) }, sl, "a", "b", "c", "d")
      LogisticRegression.prepare(Seq("a", "b", "c"), Some("d"), df)
    }

    checkAnswer() { sl =>
      val df =
        makeDF({ for (i <- 1 to 100) yield (i, -i, 0.25, i % 2) }, sl, "a", "b", "c", "d")
      LogisticRegression.prepare(Seq("a", "b"), None, df)
    }
  }

  test("k-means") {
    checkAnswer() { sl =>
      KMeans.train(spark, sl, numPartitions, 10, 2, 3, 0.01).sortBy(_(0))
    }
  }
}

class SinglePartitionOpaqueUDFSuite extends OpaqueUDFSuite with SinglePartitionSparkSession {}

class MultiplePartitionOpaqueUDFSuite extends OpaqueUDFSuite with MultiplePartitionSparkSession {}
