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

import java.sql.Timestamp

import scala.util.Random

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.CalendarInterval

import edu.berkeley.cs.rise.opaque.benchmark._
import edu.berkeley.cs.rise.opaque.execution.EncryptedBlockRDDScanExec
import edu.berkeley.cs.rise.opaque.expressions.Decrypt.decrypt
import edu.berkeley.cs.rise.opaque.expressions.DotProduct.dot
import edu.berkeley.cs.rise.opaque.expressions.VectorMultiply.vectormultiply
import edu.berkeley.cs.rise.opaque.expressions.VectorSum

/* Creates DataFrames with various types */
trait CreateSuite extends OpaqueSuiteBase with SQLHelper {
  import spark.implicits._

  test("Interval SQL") {
    def createData(sl: SecurityLevel) = {
      val data = Seq(Tuple2(1, new java.sql.Date(new java.util.Date().getTime())))
      val df = sl.applyTo(data.toDF("index", "time"))
      df.createOrReplaceTempView("Interval")
      df
    }

    checkAnswer() { sl =>
      createData(sl)
      spark.sql("SELECT time + INTERVAL 7 DAY FROM Interval")
    }
    checkAnswer() { sl =>
      createData(sl)
      spark.sql("SELECT time + INTERVAL 7 WEEK FROM Interval")
    }
    checkAnswer() { sl =>
      createData(sl)
      spark.sql("SELECT time + INTERVAL 6 MONTH FROM Interval")
    }
    checkAnswer() { sl =>
      val df = createData(sl)
      df.select(date_add($"time", 3))
    }

    safeDropTables("Interval")
  }

  test("create DataFrame with BinaryType + ByteType") {
    checkAnswer() { securityLevel =>
      val data: Seq[(Array[Byte], Byte)] =
        Seq((Array[Byte](0.toByte, -128.toByte, 127.toByte), 42.toByte))
      makeDF(data, securityLevel, "BinaryType", "ByteType")
    }
  }
}

class SinglePartitionCreateSuite extends CreateSuite with SinglePartitionSparkSession {}

class MultiplePartitionCreateSuite extends CreateSuite with MultiplePartitionSparkSession {}
