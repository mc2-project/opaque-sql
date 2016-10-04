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

package edu.berkeley.cs.rise.opaque.examples

import edu.berkeley.cs.rise.opaque.Utils
import org.apache.spark.sql.SparkSession

object Benchmark {
  def dataDir: String = {
    if (System.getenv("SPARKSGX_DATA_DIR") == null) {
      throw new Exception("Set SPARKSGX_DATA_DIR")
    }
    System.getenv("SPARKSGX_DATA_DIR")
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("QEDBenchmark")
      .getOrCreate()
    Utils.initSQLContext(spark.sqlContext)

    val numPartitions =
      if (spark.sparkContext.isLocal) 1 else spark.sparkContext.defaultParallelism

    // Warmup
    BigDataBenchmark.q2(spark, Encrypted, "tiny", numPartitions)
    BigDataBenchmark.q2(spark, Encrypted, "tiny", numPartitions)

    // Run
    BigDataBenchmark.q1(spark, Insecure, "1million", numPartitions)
    BigDataBenchmark.q1(spark, Encrypted, "1million", numPartitions)
    BigDataBenchmark.q1(spark, Oblivious, "1million", numPartitions)

    BigDataBenchmark.q2(spark, Insecure, "1million", numPartitions)
    BigDataBenchmark.q2(spark, Encrypted, "1million", numPartitions)
    BigDataBenchmark.q2(spark, Oblivious, "1million", numPartitions)

    BigDataBenchmark.q3(spark, Insecure, "1million", numPartitions)
    BigDataBenchmark.q3(spark, Encrypted, "1million", numPartitions)
    BigDataBenchmark.q3(spark, Oblivious, "1million", numPartitions)

    if (spark.sparkContext.isLocal) {
      for (i <- 8 to 20) {
        PageRank.run(spark, Oblivious, math.pow(2, i).toInt.toString, numPartitions)
      }

      for (i <- 0 to 13) {
        JoinReordering.treatmentQuery(spark, (math.pow(2, i) * 125).toInt.toString, numPartitions)
        JoinReordering.geneQuery(spark, (math.pow(2, i) * 125).toInt.toString, numPartitions)
      }

      // for (i <- 0 to 13) {
      //   JoinCost.run(spark, (math.pow(2, i) * 125).toInt.toString, numPartitions)
      // }
    }

    spark.stop()
  }

//   def joinCost(
//       sqlContext: SQLContext, size: String, distributed: Boolean = false,
//       onlyOblivious: Boolean = false): Unit = {
//     import sqlContext.implicits._
//     val diseaseSchema = StructType(Seq(
//       StructField("d_disease_id", StringType),
//       StructField("d_gene_id", IntegerType),
//       StructField("d_name", StringType)))
//     val diseaseDF = sqlContext.createEncryptedDataFrame(
//       sqlContext.read.schema(diseaseSchema)
//         .format("csv")
//         .load(s"$dataDir/disease/disease.csv")
//         .repartition(numPartitions(sqlContext, distributed))
//         .rdd
//         .mapPartitions(Utils.diseaseQueryEncryptDisease),
//       diseaseSchema)
//     time("load disease") { diseaseDF.encCache() }

//     val patientSchema = StructType(Seq(
//       StructField("p_id", IntegerType),
//       StructField("p_disease_id", StringType),
//       StructField("p_name", StringType)))
//     val patientDF = sqlContext.createEncryptedDataFrame(
//       sqlContext.read.schema(patientSchema)
//         .format("csv")
//         .load(s"$dataDir/disease/patient-$size.csv")
//         .repartition(numPartitions(sqlContext, distributed))
//         .rdd
//         .mapPartitions(Utils.diseaseQueryEncryptPatient),
//       patientSchema)
//     time("load patient") { patientDF.encCache() }

//     timeBenchmark(
//       "distributed" -> distributed,
//       "query" -> "join cost",
//       "system" -> "opaque",
//       "size" -> size) {
//       diseaseDF.encJoin(patientDF, $"d_disease_id" === $"p_disease_id").encForce()
//     }

//     if (!onlyOblivious) {
//       timeBenchmark(
//         "distributed" -> distributed,
//         "query" -> "join cost",
//         "system" -> "encrypted",
//         "size" -> size) {
//         diseaseDF.nonObliviousJoin(patientDF, $"d_disease_id" === $"p_disease_id").encForce()
//       }
//     }
//   }
}
