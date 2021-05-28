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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import org.scalatest.BeforeAndAfterAll

trait OpaqueSuiteBase extends OpaqueFunSuite with BeforeAndAfterAll with SQLTestData {

  def numPartitions: Int
  override val spark: SparkSession

  override def beforeAll(): Unit = {
    Utils.initOpaqueSQL(spark, testing = true)
  }

  override def afterAll(): Unit = {
    Utils.cleanup(spark)
  }

  def makeDF[A <: Product: scala.reflect.ClassTag: scala.reflect.runtime.universe.TypeTag](
      data: Seq[A],
      sl: SecurityLevel,
      columnNames: String*
  ): DataFrame = {
    sl.applyTo(
      spark
        .createDataFrame(spark.sparkContext.makeRDD(data, numPartitions))
        .toDF(columnNames: _*)
    )
  }
}
