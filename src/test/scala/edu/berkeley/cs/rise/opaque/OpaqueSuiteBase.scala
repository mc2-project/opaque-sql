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
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.unsafe.types.UTF8String

import java.sql.Date
import java.util.concurrent.TimeUnit

import org.scalatest.BeforeAndAfterAll
import org.scalactic.Equality

trait OpaqueSuiteBase extends OpaqueFunSuite with BeforeAndAfterAll with SQLTestData {

  def numPartitions: Int
  override val spark: SparkSession

  override def beforeAll(): Unit = {
    Utils.initOpaqueSQL(spark, testing = true)
  }

  override def afterAll(): Unit = {
    Utils.cleanup(spark)
  }

  // TODO: Override checkAnswer for new functionality. Uncomment after merging with 
  // key-gen, re-encryption, sp, and python pull requests
//  override def checkAnswer[A: Equality](
//       ignore: Boolean = false,
//       isOrdered: Boolean = false,
//       verbose: Boolean = false,
//       printPlan: Boolean = false,
//       shouldLogOperators: Boolean = false
//   )(f: SecurityLevel => A): Unit = {
//     if (ignore) {
//       return
//     }
//
//     Utils.setOperatorLoggingLevel(shouldLogOperators)
//     val (insecure, encrypted) = (f(Insecure), f(Encrypted))
//     (insecure, encrypted) match {
//       case (insecure: DataFrame, encrypted: DataFrame) =>
//         val insecureSeq = insecure.collect
//         val schema = insecure.schema
//
//         encrypted.collect
//         val encryptedRows = SPHelper.obtainRows(encrypted) 
//         val encryptedRows2 = encryptedRows.map(x => prepareRowWithSchema(x, schema))
//         val encryptedDF = spark.createDataFrame(
//                                spark.sparkContext.makeRDD(encryptedRows2, numPartitions),
//                                schema)
//
//         val encryptedSeq = encryptedDF.collect
//
//         // Unable to test any compound filtering functionality as results returned from any
//         // operation are not returned immediately but stored in file on disk.
//         // See benchmark/KMeans.scala and benchmark/LogisticRegression.scala for example
//         val equal = insecureSeq.toSet == encryptedSeq.toSet
// //          if (isOrdered) insecureSeq === encryptedSeq
// //          else insecureSeq.toSet === encryptedSeq.toSet
//         if (!equal) {
//           if (printPlan) {
//             println("**************** Spark Plan ****************")
//             insecure.explain()
//             println("**************** Opaque Plan ****************")
//             encrypted.explain()
//           }
//           println(genError(insecureSeq, encryptedSeq, isOrdered, verbose))
//         }
//         assert(equal)
//       case (insecure: Array[Array[Double]], encrypted: Array[Array[Double]]) =>
//         for ((x, y) <- insecure.zip(encrypted)) {
//
//           assert(x === y)
//         }
//       case _ =>
//         assert(insecure === encrypted)
//     }
//   }

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

  // Using a schema, convert the row into the relevant datatypes
  // Only certain types are implemented currently. Add as needed
  private def prepareRowWithSchema(row: Row, schema: StructType): Row = {
     val fields = schema.fields

     if (row == null) {
       println("null row")
       return null
     }

     assert(row.length == fields.size)

     Row.fromSeq(for (i <- 0 until row.length) yield {
           val rowValue = row(i)
           val fieldDataType = fields(i).dataType
           val converted = fieldDataType match {
             case ArrayType(_,_) => rowValue
             case BinaryType => rowValue 
             case BooleanType => rowValue 
             case CalendarIntervalType => rowValue

             // TPCH dates are calculated in days for some reason
             case DateType => new Date(TimeUnit.DAYS.toMillis(rowValue.asInstanceOf[Integer].toLong))
             case MapType(_,_,_) => rowValue 
             case NullType => rowValue
 //            case IntegerType => rowValue.asInstanceOf[Integer]
 //            case DoubleType => rowValue.asInstanceOf[Double]
             case StringType => rowValue.asInstanceOf[UTF8String].toString()
             case StructType(_) => rowValue
             case TimestampType => rowValue
             case _ => rowValue
           }
           converted
         })
   }
}
