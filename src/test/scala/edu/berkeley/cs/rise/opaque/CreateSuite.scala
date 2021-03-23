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

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

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
    checkAnswer() { sl =>
      val data: Seq[(Array[Byte], Byte)] =
        Seq((Array[Byte](0.toByte, -128.toByte, 127.toByte), 42.toByte))
      makeDF(data, sl, "BinaryType", "ByteType")
    }
  }

  test("create DataFrame with CalendarIntervalType + NullType") {
    checkAnswer() { sl =>
      val data: Seq[(CalendarInterval, Byte)] =
        Seq((new CalendarInterval(12, 1, 12345), 0.toByte))
      makeDF(data, sl, "CalendarIntervalType", "NullType")
    }
  }

  test("create DataFrame with ShortType + TimestampType") {
    checkAnswer() { sl =>
      val data: Seq[(Short, Timestamp)] =
        Seq((13.toShort, Timestamp.valueOf("2017-12-02 03:04:00")))
      makeDF(data, sl, "ShortType", "TimestampType")
    }
  }

  test("create DataFrame with ArrayType") {
    checkAnswer() { sl =>
      val array: Array[Int] = Array(0, -128, 127, 1)
      val data = Seq((array, "dog"), (array, "cat"), (array, "ant"))
      makeDF(data, sl, "array", "string")
    }
  }

  test("create DataFrame with MapType") {
    checkAnswer() { sl =>
      val map: Map[String, Int] = Map("x" -> 24, "y" -> 25, "z" -> 26)
      val data = Seq((map, "dog"), (map, "cat"), (map, "ant"))
      makeDF(data, sl, "map", "string")
    }
  }

  test("create DataFrame with nulls for all types") {
    checkAnswer() { sl =>
      val schema = StructType(
        Seq(
          StructField("boolean", BooleanType),
          StructField("integer", IntegerType),
          StructField("long", LongType),
          StructField("float", FloatType),
          StructField("double", DoubleType),
          StructField("date", DateType),
          StructField("binary", BinaryType),
          StructField("byte", ByteType),
          StructField("calendar_interval", CalendarIntervalType),
          StructField("null", NullType),
          StructField("short", ShortType),
          StructField("timestamp", TimestampType),
          StructField("array_of_int", DataTypes.createArrayType(IntegerType)),
          StructField("map_int_to_int", DataTypes.createMapType(IntegerType, IntegerType)),
          StructField("string", StringType)
        )
      )

      sl.applyTo(
        spark.createDataFrame(
          spark.sparkContext
            .makeRDD(Seq(Row.fromSeq(Seq.fill(schema.length) { null })), numPartitions),
          schema
        )
      )
    }
  }
}

class SinglePartitionCreateSuite extends CreateSuite with SinglePartitionSparkSession {}

class MultiplePartitionCreateSuite extends CreateSuite with MultiplePartitionSparkSession {}
