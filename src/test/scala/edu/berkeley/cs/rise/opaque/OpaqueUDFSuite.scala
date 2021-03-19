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

import edu.berkeley.cs.rise.opaque.benchmark._
import edu.berkeley.cs.rise.opaque.expressions.DotProduct.dot
import edu.berkeley.cs.rise.opaque.expressions.VectorMultiply.vectormultiply
import edu.berkeley.cs.rise.opaque.expressions.VectorSum

/* Contains non-query related Spark tests */
trait OpaqueUDFSuite extends OpaqueSuiteBase {
  import spark.implicits._

  test("exp") {
    checkAnswer() { sl =>
      val data: Seq[(Double, Double)] = Seq((2.0, 3.0))
      val df = makeDF(data, sl, "x", "y")
      df.select(exp($"y"))
    }
  }

  test("vector multiply") {
    checkAnswer() { sl =>
      val data: Seq[(Array[Double], Double)] = Seq((Array[Double](1.0, 1.0, 1.0), 3.0))
      val df = makeDF(data, sl, "v", "c")
      df.select(vectormultiply($"v", $"c"))
    }
  }

  test("dot product") {
    checkAnswer() { sl =>
      val data: Seq[(Array[Double], Array[Double])] =
        Seq((Array[Double](1.0, 1.0, 1.0), Array[Double](1.0, 1.0, 1.0)))
      val df = makeDF(data, sl, "v1", "v2")
      df.select(dot($"v1", $"v2"))
    }
  }

  test("vector sum") {
    checkAnswer() { sl =>
      val data: Seq[(Array[Double], Double)] =
        Seq((Array[Double](1.0, 2.0, 3.0), 4.0), (Array[Double](5.0, 7.0, 7.0), 8.0))
      val df = makeDF(data, sl, "v", "c")
      val vectorsum = new VectorSum
      df.groupBy().agg(vectorsum($"v"))
    }
  }

  test("least squares") {
    checkAnswer() { sl =>
      LeastSquares.query(spark, sl, "tiny", numPartitions)
    }
  }

  test("logistic regression") {
    checkAnswer() { sl =>
      LogisticRegression.train(spark, sl, 1000, numPartitions)
    }
  }
  test("k-means") {
    checkAnswer() { sl =>
      KMeans.train(spark, sl, numPartitions, 10, 2, 3, 0.01).sortBy(_(0))
    }
  }
}

class SinglePartitionOpaqueUDFSuite extends OpaqueUDFSuite with SinglePartitionSparkSession {}

class MultiplePartitionOpaqueUDFSuite extends OpaqueUDFSuite with MultiplePartitionSparkSession {}
