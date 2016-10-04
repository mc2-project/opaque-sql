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
import edu.berkeley.cs.rise.opaque.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object JoinReordering {
  def disease(
      spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame =
    securityLevel.applyTo(
      spark.read.schema(
        StructType(Seq(
          StructField("d_disease_id", StringType),
          StructField("d_gene_id", IntegerType),
          StructField("d_name", StringType))))
        .csv(s"${Benchmark.dataDir}/disease/disease.csv")
        .repartition(numPartitions))

  def patient(
      spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame =
    securityLevel.applyTo(
      spark.read.schema(
        StructType(Seq(
          StructField("p_id", IntegerType),
          StructField("p_disease_id", StringType),
          StructField("p_name", StringType))))
        .csv(s"${Benchmark.dataDir}/disease/patient-$size.csv")
        .repartition(numPartitions))

  def treatment(
      spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame =
    securityLevel.applyTo(
      spark.read.schema(
        StructType(Seq(
          StructField("t_id", IntegerType),
          StructField("t_disease_id", StringType),
          StructField("t_name", StringType),
          StructField("t_cost", IntegerType))))
        .csv(s"${Benchmark.dataDir}/disease/treatment.csv")
        .repartition(numPartitions))

  def gene(
      spark: SparkSession, securityLevel: SecurityLevel, size: String, numPartitions: Int)
    : DataFrame =
    securityLevel.applyTo(
      spark.read.schema(
        StructType(Seq(
          StructField("g_id", IntegerType),
          StructField("g_name", StringType))))
        .csv(s"${Benchmark.dataDir}/disease/gene.csv")
        .repartition(numPartitions))

  def treatmentQuery(
      spark: SparkSession, size: String, numPartitions: Int)
    : Unit = {
    import spark.implicits._
    val diseaseDF = disease(spark, Encrypted, size, numPartitions).cache()
    Utils.time("load disease") { Utils.force(diseaseDF) }
    val patientDF = patient(spark, Oblivious, size, numPartitions).cache()
    Utils.time("load patient") { Utils.force(patientDF) }
    val treatmentDF = treatment(spark, Encrypted, size, numPartitions).cache()
    Utils.time("load treatment") { Utils.force(treatmentDF) }

    val groupedTreatmentDF =
      treatmentDF.groupBy($"t_disease_id").agg(min("t_cost").as("t_min_cost")).cache()
    Utils.time("compute groupedTreatment") { Utils.force(groupedTreatmentDF) }

    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "disease",
      "system" -> Oblivious.name,
      "size" -> size,
      "join order" -> "generic") {
      val df = groupedTreatmentDF.join(
        diseaseDF.join(
          patientDF,
          $"d_disease_id" === $"p_disease_id"),
        $"d_disease_id" === $"t_disease_id")
      Utils.force(df)
      df
    }

    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "disease",
      "system" -> Oblivious.name,
      "size" -> size,
      "join order" -> "opaque") {
      val df = diseaseDF
        .join(groupedTreatmentDF, $"d_disease_id" === $"t_disease_id") // should be non-oblivious
        .join(patientDF, $"d_disease_id" === $"p_disease_id")
      Utils.force(df)
      df
    }
  }

  def geneQuery(
      spark: SparkSession, size: String, numPartitions: Int)
    : Unit = {
    import spark.implicits._
    val diseaseDF = disease(spark, Encrypted, size, numPartitions).cache()
    Utils.time("load disease") { Utils.force(diseaseDF) }
    val patientDF = patient(spark, Oblivious, size, numPartitions).cache()
    Utils.time("load patient") { Utils.force(patientDF) }
    val geneDF = gene(spark, Encrypted, size, numPartitions).cache()
    Utils.time("load gene") { Utils.force(geneDF) }

    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "gene",
      "system" -> Oblivious.name,
      "size" -> size,
      "join order" -> "generic") {
      val df = geneDF.join(
        diseaseDF.join(
          patientDF,
          $"d_disease_id" === $"p_disease_id"),
        $"g_id" === $"d_gene_id")
      Utils.force(df)
      df
    }

    Utils.timeBenchmark(
      "distributed" -> (numPartitions > 1),
      "query" -> "gene",
      "system" -> Oblivious.name,
      "size" -> size,
      "join order" -> "opaque") {
      val df = geneDF
        .join(diseaseDF, $"g_id" === $"d_gene_id") // should be non-oblivious
        .join(patientDF, $"d_disease_id" === $"p_disease_id")
      Utils.force(df)
      df
    }
  }
}
