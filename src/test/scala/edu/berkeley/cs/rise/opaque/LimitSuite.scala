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

trait LimitSuite extends OpaqueSQLSuiteBase with SQLHelper {

  def numPartitions: Int

  def queries = Seq(
    " SELECT * FROM testdata LIMIT 2;",
    "SELECT * FROM arraydata LIMIT 2;",
    "SELECT * FROM mapdata LIMIT 2;",
    "SELECT * FROM testdata LIMIT 2 + 1;",
    "SELECT * FROM testdata LIMIT CAST(1 AS int);",
    "SELECT * FROM testdata LIMIT CAST(1 AS INT);",
    "SELECT * FROM testdata WHERE key < 3 LIMIT ALL;"
  )
  def failingQueries = Seq("SELECT * FROM (SELECT * FROM range(10) LIMIT 5) WHERE id > 3;")
  def unsupportedQueries = Seq()

}

class SinglePartitionLimitSuite extends LimitSuite with SinglePartitionSparkSession {
  runSQLQueries()
}

class MultiplePartitionLimitSuite extends LimitSuite with MultiplePartitionSparkSession {
  runSQLQueries()
}
