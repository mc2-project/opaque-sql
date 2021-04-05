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

import org.apache.spark.sql.SparkSession

trait SinglePartitionSparkSession {
  def numPartitions = 1
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("SinglePartitionSuiteSession")
    .config("spark.sql.shuffle.partitions", numPartitions)
    .config("spark.opaque.testing.enableSharedKey", true)
    .getOrCreate()
}

trait MultiplePartitionSparkSession {
  val executorInstances = 3

  def numPartitions = executorInstances
  val spark = SparkSession
    .builder()
    .master(s"local-cluster[$executorInstances,1,1024]")
    .appName("MultiplePartitionSubquerySuite")
    .config("spark.executor.instances", executorInstances)
    .config("spark.sql.shuffle.partitions", numPartitions)
    .config(
      "spark.jars",
      "target/scala-2.12/opaque_2.12-0.1.jar,target/scala-2.12/opaque_2.12-0.1-tests.jar"
    )
    .config("spark.opaque.testing.enableSharedKey", true)
    .getOrCreate()
}
