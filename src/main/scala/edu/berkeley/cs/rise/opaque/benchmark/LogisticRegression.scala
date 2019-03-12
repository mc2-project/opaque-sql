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

package edu.berkeley.cs.rise.opaque.benchmark

import java.util.Random

import breeze.linalg.DenseVector
import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.expressions.DotProduct.dot
import edu.berkeley.cs.rise.opaque.expressions.VectorMultiply.vectormultiply
import edu.berkeley.cs.rise.opaque.expressions.VectorSum
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object LogisticRegression {

  def data(
      spark: SparkSession,
      securityLevel: SecurityLevel,
      numPartitions: Int,
      rand: Random,
      N: Int,
      D: Int,
      R: Double)
    : DataFrame = {
    def generatePoint(i: Int): (Array[Double], Double) = {
      val y = if (i % 2 == 0) -1 else 1
      val x = Array.fill(D) {rand.nextGaussian + y * R}
      (x, y)
    }

    val data = Array.tabulate(N)(generatePoint)
    val schema = StructType(Seq(
      StructField("x", DataTypes.createArrayType(DoubleType)),
      StructField("y", DoubleType)))

    securityLevel.applyTo(
      spark.createDataFrame(
        spark.sparkContext.makeRDD(data.map(Row.fromTuple), numPartitions),
        schema))
  }

  def train(spark: SparkSession, securityLevel: SecurityLevel, N: Int, numPartitions: Int)
    : Array[Double] = {
    import spark.implicits._
    val rand = new Random(42)
    val D = 10
    val ITERATIONS = 5

    val vectorsum = new VectorSum

    val points = Utils.ensureCached(data(spark, securityLevel, numPartitions, rand, N, D, 0.7))
    Utils.time("Generate logistic regression data") { Utils.force(points) }
    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "logistic regression",
      "system" -> securityLevel.name,
      "N" -> N) {

      val w = DenseVector.fill(D) {2 * rand.nextDouble - 1}

      for (i <- 1 to ITERATIONS) {
        val gradient = points
          .select(
            vectormultiply(
              $"x",
              (lit(1.0) / (lit(1.0) + exp(-$"y" * dot(lit(w.toArray), $"x"))) - lit(1.0)) * $"y")
              .as("v"))
          .groupBy().agg(vectorsum($"v"))
          .first().getSeq[Double](0).toArray
        w -= new DenseVector(gradient)
      }

      w.toArray
    }
  }
}
