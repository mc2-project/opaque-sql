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
import breeze.linalg.squaredDistance
import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.expressions.ClosestPoint.closestPoint
import edu.berkeley.cs.rise.opaque.expressions.VectorMultiply.vectormultiply
import edu.berkeley.cs.rise.opaque.expressions.VectorSum
import edu.berkeley.cs.rise.opaque.SecurityLevel
import edu.berkeley.cs.rise.opaque.SPHelper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KMeans {

  def data(
      spark: SparkSession,
      securityLevel: SecurityLevel,
      numPartitions: Int,
      rand: Random,
      N: Int,
      D: Int
  ): DataFrame = {
    def generatePoint(): Array[Double] = {
      Array.fill(D) { rand.nextGaussian }
    }

    val data = Array.fill(N)(Row(generatePoint()))
    val schema = StructType(Seq(StructField("p", DataTypes.createArrayType(DoubleType))))

    securityLevel.applyTo(
      spark.createDataFrame(spark.sparkContext.makeRDD(data, numPartitions), schema)
    )
  }

  def train(
      spark: SparkSession,
      securityLevel: SecurityLevel,
      numPartitions: Int,
      N: Int,
      D: Int,
      K: Int,
      convergeDist: Double
  ): Array[Array[Double]] = {
    import spark.implicits._
    val rand = new Random(42)
    val vectorsum = new VectorSum

    val points = Utils.ensureCached(data(spark, securityLevel, numPartitions, rand, N, D))
    Utils.time("Generate k-means data") { Utils.force(points) }

    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "k-means",
      "system" -> securityLevel.name,
      "N" -> N
    ) {
      if (securityLevel.name == "spark sql") {

        // Sample k random points.
        // TODO: Assumes points are already permuted randomly.
        var centroids = points.take(K).map(_.getSeq[Double](0).toArray)

        var tempDist = 1.0

        while (tempDist > convergeDist) {
          val newCentroids = points
            .select(
              closestPoint($"p", lit(centroids)).as("oldCentroid"),
              $"p".as("centroidPartialSum"),
              lit(1).as("centroidPartialCount")
            )
            .groupBy($"oldCentroid")
            .agg(
              vectorsum($"centroidPartialSum").as("centroidSum"),
              sum($"centroidPartialCount").as("centroidCount")
            )
            .select(
              $"oldCentroid",
              vectormultiply($"centroidSum", (lit(1.0) / $"centroidCount")).as("newCentroid")
            )
            .collect

          tempDist = 0.0
          for (row <- newCentroids) {
            tempDist += squaredDistance(
              new DenseVector(row.getSeq[Double](0).toArray),
              new DenseVector(row.getSeq[Double](1).toArray)
            )
          }

          centroids = newCentroids.map(_.getSeq[Double](1).toArray)
        }

        centroids
      } else {

        // First operation block. Instead of using take, use collect for simplicity
        // points.take(K)
        points.collect

        var centroids = SPHelper.obtainRows(points).map(x => SPHelper.convertGenericArrayData(x, 0))
                                                   .map(x => x(0).asInstanceOf[Array[Double]])
                                                   .toArray.slice(0, 3)

        var tempDist = 1.0

        while (tempDist > convergeDist) {

          // Second operation block
          val df_2 = points
            .select(
              closestPoint($"p", lit(centroids)).as("oldCentroid"),
              $"p".as("centroidPartialSum"),
              lit(1).as("centroidPartialCount")
            ).collect

          // Of form Seq[Row[GenericArrayData, GenericArrayData, Int]]
          val rows_2 = SPHelper.obtainRows(points)
              .map(x => SPHelper.convertGenericArrayDataKMeans(x, 0))

          // Third operation block
          val schema = StructType(
              Seq(StructField("oldCentroid", DataTypes.createArrayType(DoubleType)),
                  StructField("centroidPartialSum", DataTypes.createArrayType(DoubleType)),
                  StructField("centroidPartialCount", DataTypes.IntegerType))
          )

          val df_3 = securityLevel.applyTo(
                               spark.createDataFrame(
                               spark.sparkContext.makeRDD(rows_2, numPartitions),
                               schema))

          df_3.groupBy($"oldCentroid")
            .agg(
              vectorsum($"centroidPartialSum").as("centroidSum"),
              sum($"centroidPartialCount").as("centroidCount")
            ).collect

          // Of form Seq[Row[GenericArrayData, GenericArrayData, Int]]
          val rows_3 = SPHelper.obtainRows(df_3)
              .map(x => SPHelper.convertGenericArrayDataKMeans(x, 0))
          
          // Fourth operation block
          val schema_2 = StructType(
              Seq(StructField("oldCentroid", DataTypes.createArrayType(DoubleType)),
                  StructField("centroidSum", DataTypes.createArrayType(DoubleType)),
                  StructField("centroidCount", DataTypes.LongType))
          )

          val df_4 = securityLevel.applyTo(
                               spark.createDataFrame(
                               spark.sparkContext.makeRDD(rows_3, numPartitions),
                               schema_2))

          df_4.select(
              $"oldCentroid",
              vectormultiply($"centroidSum", (lit(1.0) / $"centroidCount")).as("newCentroid"),
              lit(1).as("filler")
            )
            .collect

          val rows_4 = SPHelper.obtainRows(df_3)
              .map(x => SPHelper.convertGenericArrayDataKMeans(x, 0))

          // Final operation block

          tempDist = 0.0
            for (row <- rows_4) {
              tempDist += squaredDistance(
                new DenseVector(row(0).asInstanceOf[Array[Double]]),
                new DenseVector(row(1).asInstanceOf[Array[Double]])
              )
          }

          centroids = rows_4.map(x => x(1).asInstanceOf[Array[Double]]).toArray
        }


        centroids
      }
    }
  }
}
