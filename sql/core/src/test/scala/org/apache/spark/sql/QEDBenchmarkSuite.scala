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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.test.SharedSQLContext

class QEDBenchmarkSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  val (enclave, eid) = QED.initEnclave()

  test("big data 1 - spark sql") {
    QEDBenchmark.bd1SparkSQL(sqlContext, "1node")
  }

  test("big data 1") {
    QEDBenchmark.bd1Opaque(sqlContext, "1node")
  }

  test("big data 2 - spark sql") {
    QEDBenchmark.bd2SparkSQL(sqlContext, "1node")
  }

  test("big data 2") {
    QEDBenchmark.bd2Opaque(sqlContext, "1node")
  }

}
