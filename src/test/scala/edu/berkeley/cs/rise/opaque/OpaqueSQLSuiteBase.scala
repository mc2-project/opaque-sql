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

trait OpaqueSQLSuiteBase extends OpaqueSuiteBase {

  /* Queries that are passing */
  def queries: Seq[String]
  /* Tests that are failing but should be passing */
  def failingQueries: Seq[String]
  /* Tests that contain unsupported operators */
  def unsupportedQueries: Seq[String]

  def runSQLQueries() = {
    for (sqlStr <- queries) {
      test(sqlStr) {
        checkAnswer() { sl =>
          loadTestData(sqlStr, sl)
          spark.sqlContext.sparkSession.sql(sqlStr)
        }
      }
    }
  }
}
