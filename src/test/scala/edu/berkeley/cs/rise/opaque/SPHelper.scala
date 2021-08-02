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

import org.apache.spark.sql.catalyst.util.GenericArrayData

import java.util.Base64

import java.nio.ByteBuffer

import edu.berkeley.cs.rise.opaque.execution.SP

object SPHelper {

  val sp = new SP()

  // Create a SP for decryption
  // TODO: Change hard-coded path for user_cert
  val userCert = scala.io.Source.fromFile("/home/opaque/opaque/user1.crt").mkString

  // Change empty key to key used in RA.initRA to allow for decryption
  final val GCM_KEY_LENGTH = 32
  val sharedKey: Array[Byte] = Array.fill[Byte](GCM_KEY_LENGTH)(0)
  sp.Init(sharedKey, userCert)

  def convertGenericArrayData(rowData: Row, index: Int): Row = {

    val data = rowData(index).asInstanceOf[GenericArrayData]

    val dataArray = new Array[Double](data.numElements)
    for (i <- 0 until dataArray.size) {
      dataArray(i) = data.getDouble(i)
    }
    return Row(dataArray)
  }

  def convertGenericArrayDataKMeans(rowData: Row, index: Int): Row = {

    val data = rowData(index).asInstanceOf[GenericArrayData]

    val dataArray = new Array[Double](data.numElements)
    for (i <- 0 until dataArray.size) {
      dataArray(i) = data.getDouble(i)
    }

    val dataTwo = rowData(1).asInstanceOf[GenericArrayData]

    val dataArrayTwo = new Array[Double](dataTwo.numElements)
    for (i <- 0 until dataArrayTwo.size) {
      dataArrayTwo(i) = dataTwo.getDouble(i)
    }

    return Row(dataArray, dataArrayTwo, rowData(2))
  }

  def obtainRows(df: DataFrame) : Seq[Row] = {

    // Hardcoded user1 for driver
    val ciphers = Utils.postVerifyAndReturn(df, "user1")

    val internalRow = (for (cipher <- ciphers) yield {

      val plain = sp.Decrypt(Base64.getEncoder().encodeToString(cipher))
      val rows = tuix.Rows.getRootAsRows(ByteBuffer.wrap(plain))

      for (j <- 0 until rows.rowsLength) yield {
        val row = rows.rows(j)
        assert(!row.isDummy)
        Row.fromSeq(for (k <- 0 until row.fieldValuesLength) yield {
          val field: Any =
            if (!row.fieldValues(k).isNull()) {
              Utils.flatbuffersExtractFieldValue(row.fieldValues(k))
            } else {
              null
            }
          field
        })
      }
    }).flatten

    return internalRow
  }
}
