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

import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.collection.mutable

import sun.misc.{BASE64Encoder, BASE64Decoder}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType

object QED {
  val sgx = {
    System.load(System.getenv("LIBSGXENCLAVE_PATH"))
    new SGXEnclave()
  }
  val eid = new ThreadLocal[Long]
  eid.set(sgx.StartEnclave())

  val encoder = new BASE64Encoder()
  val decoder = new BASE64Decoder()

  val predicates = mutable.Map[Int, (InternalRow) => Boolean]()
  var nextPredicateId = 0

  // TODO
  def enclaveRegisterPredicate(pred: Expression, inputSchema: Seq[Attribute]): Int = {
    if (sgx == null) {
      val curPredicateId = nextPredicateId
      nextPredicateId += 1
      predicates(curPredicateId) = GeneratePredicate.generate(pred, inputSchema)
      curPredicateId
    } else {
      // TODO: register arbitrary predicates with the enclave
      0
    }
  }

  // TODO
  def enclaveEvalPredicate(
      predOpcode: Int, row: InternalRow, schema: Seq[DataType]): Boolean = {
    // Serialize row with # columns (4 bytes), column 1 length (4 bytes), column 1 contents, etc.
    val buf = ByteBuffer.allocate(100)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putInt(schema.length)
    for ((t, i) <- schema.zip(0 until schema.length)) t match {
      case IntegerType =>
        buf.putInt(t.defaultSize)
        buf.putInt(row.getInt(i))
      case StringType =>
        val x = row.getUTF8String(i)
        buf.putInt(x.numBytes())
        buf.put(x.getBytes())
      case _ =>
        throw new Exception("Can't yet handle " + t)
    }
    buf.flip()
    val rowBytes = new Array[Byte](buf.limit())
    buf.get(rowBytes)

    sgx.Filter(eid.get, predOpcode, rowBytes)
  }

  def encodeData(value: Array[Byte]): String = {
    val encoded = encoder.encode(value)
    encoded
  }

  def decodeData(value: String): Array[Byte] = {
    val decoded = decoder.decodeBuffer(value)
    decoded
  }

  def genAndWriteData() = {
    // write this hard-coded set of columns to text file, in csv format
  }
}
