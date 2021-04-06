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

import org.apache.spark.sql.functions._

trait SortSuite extends OpaqueSuiteBase with SQLHelper {
  import spark.implicits._

  def numPartitions: Int

  test("sort") {
    checkAnswer() { sl =>
      val pairs = sl.applyTo(Seq((1, 0), (2, 0), (0, 0), (3, 0)).toDF)
      pairs.sort()
    }
  }

  test("large array") {
    val unencrypted = generateRandomPairs()
    checkAnswer() { sl =>
      val pairs = sl.applyTo(unencrypted)
      pairs.sort()
    }
  }

  test("sort descending") {
    val unencrypted = generateRandomPairs()
    checkAnswer(shouldLogOperators = true) { sl =>
      val pairs = sl.applyTo(unencrypted)
      pairs.sort(desc(pairs.columns(0)))
    }
  }

  test("empty DataFrame") {
    val unencrypted = Seq[(Int, Int)]().toDF
    checkAnswer() { sl =>
      val pairs = sl.applyTo(unencrypted)
      pairs.sort()
    }
  }

  test("basic sorting") {
    def loadInput(sl: SecurityLevel) = {
      sl.applyTo(Seq(("Hello", 4, 2.0), ("Hello", 1, 1.0), ("World", 8, 3.0)).toDF("a", "b", "c"))
    }

    checkAnswer() { sl =>
      val input = loadInput(sl)
      input.sort('a, 'b)
    }

    checkAnswer() { sl =>
      val input = loadInput(sl)
      input.sort('b, 'a)
    }
  }

  test("sorting all nulls") {
    checkAnswer() { sl =>
      val input = sl.applyTo((1 to 100).map(v => Tuple1(v)).toDF.selectExpr("NULL as a"))
      input.sort()
    }
  }

  test("sort followed by limit") {
    checkAnswer() { sl =>
      val input = sl.applyTo((1 to 100).map(v => Tuple1(v)).toDF("a"))
      input.sort()
    }
  }

  test("sorting does not crash for large inputs") {
    val stringLength = 1024 * 1024 * 2
    checkAnswer() { sl =>
      val input = Seq(Tuple1("a" * stringLength), Tuple1("b" * stringLength)).toDF("a")
      input.sort()
    }
  }

  def generateRandomPairs(numPairs: Int = 1000) = {
    val rand = new scala.util.Random()
    Seq.fill(numPairs) { (rand.nextInt(), rand.nextInt()) }.toDF
  }
}

class SinglePartitionSortSuite extends SortSuite with SinglePartitionSparkSession {}

class MultiplePartitionSortSuite extends SortSuite with MultiplePartitionSparkSession {}
