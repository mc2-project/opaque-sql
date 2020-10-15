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

import edu.berkeley.cs.rise.opaque.Utils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object PageRank {
  def run(spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame = {
    import spark.implicits._
    val inputSchema = StructType(Seq(
      StructField("src", IntegerType, false),
      StructField("dst", IntegerType, false),
      StructField("isVertex", IntegerType, false)))
    val data = spark.read
      .schema(inputSchema)
      .option("delimiter", " ")
      .csv(s"${Benchmark.dataDir}/pagerank/PageRank$size.in")
    val edges =
      Utils.ensureCached(
        securityLevel.applyTo(
          data
            .filter($"isVertex" === lit(0))
            .select($"src", $"dst", lit(1.0f).as("weight"))
            .repartition(numPartitions)))
    // Utils.time("load edges") { Utils.force(edges) }
    val vertices =
      Utils.ensureCached(
        securityLevel.applyTo(
          data
            .filter($"isVertex" === lit(1))
            .select($"src".as("id"), lit(1.0f).as("rank"))
            .repartition(numPartitions)))
    // Utils.time("load vertices") { Utils.force(vertices) }
    val newV =
      Utils.timeBenchmark(
        "distributed" -> (numPartitions > 1),
        "query" -> "pagerank",
        "system" -> securityLevel.name,
        "size" -> size) {
        val result =
          vertices.join(edges, $"id" === $"src")
            .select($"dst", ($"rank" * $"weight").as("weightedRank"))
            .groupBy("dst").agg(sum("weightedRank").as("totalIncomingRank"))
            .select($"dst", (lit(0.15) + lit(0.85) * $"totalIncomingRank").as("rank"))
        // Utils.force(result)
        result
      }
    newV
  }
}
