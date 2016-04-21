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

import org.apache.spark.sql.types.BinaryType

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
  def initEnclave(): (SGXEnclave, Long) = {
    System.load(System.getenv("LIBSGXENCLAVE_PATH"))
    val enclave = new SGXEnclave()
    val eid = enclave.StartEnclave()
    (enclave, eid)
  }

  val encoder = new BASE64Encoder()
  val decoder = new BASE64Decoder()

  def enclaveRegisterPredicate(
      enclave: SGXEnclave, eid: Long, pred: Expression, inputSchema: Seq[Attribute]): Int = {
    // TODO: register arbitrary predicates with the enclave
    0
  }

  def enclaveEvalPredicate(
      enclave: SGXEnclave, eid: Long,
      predOpcode: Int, row: InternalRow, schema: Seq[DataType]): Boolean = {
    // Serialize row with # columns (4 bytes), column 1 length (4 bytes), column 1 contents, etc.
    val buf = ByteBuffer.allocate(100)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putInt(schema.length)
    for ((t, i) <- schema.zip(0 until schema.length)) t match {
      case BinaryType =>
        // This is really the only valid type since it represents an encrypted field. All other
        // cleartext types are only for debugging.
        val bytes = row.getBinary(i)
        buf.putInt(bytes.length)
        buf.put(bytes)
      case _ =>
        throw new Exception("Type %s is not encrypted".format(t))
    }
    buf.flip()
    val rowBytes = new Array[Byte](buf.limit())
    buf.get(rowBytes)

    val ret = enclave.Filter(eid, predOpcode, rowBytes)
    ret
  }

  def encrypt[T](enclave: SGXEnclave, eid: Long, field: T): Array[Byte] = {
    val buf = ByteBuffer.allocate(100)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    field match {
      case x: Int =>
        buf.put(1: Byte)
        buf.putInt(4)
        buf.putInt(x)
      case s: String =>
        buf.put(2: Byte)
        val utf8 = s.getBytes("UTF-8")
        buf.putInt(utf8.length)
        buf.put(utf8)
    }
    buf.flip()
    val bytes = new Array[Byte](buf.limit)
    buf.get(bytes)
    enclave.Encrypt(eid, bytes)
  }

  def decrypt[T](enclave: SGXEnclave, eid: Long, bytes: Array[Byte]): T = {
    val buf = ByteBuffer.wrap(enclave.Decrypt(eid, bytes))
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val tpe = buf.get()
    val size = buf.getInt()
    val result = tpe match {
      case 1 =>
        assert(size == 4)
        buf.getInt()
      case 2 =>
        val sBytes = new Array[Byte](size)
        buf.get(sBytes)
        assert(!buf.hasRemaining)
        new String(sBytes, "UTF-8")
    }
    result.asInstanceOf[T]
  }

  def randomId(enclave: SGXEnclave, eid: Long): Array[Byte] = {
    // TODO: this whole function should be in the enclave, otherwise it's insecure. An attacker can
    // read and modify the random bytes before they get encrypted
    val buf = ByteBuffer.allocate(100)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val randomBytes = enclave.RandomID(eid)
    // Label the random bytes as a string so the enclave will recognize their type
    buf.put(2: Byte)
    buf.putInt(randomBytes.length)
    buf.put(randomBytes)
    buf.flip()
    val bytes = new Array[Byte](buf.limit)
    buf.get(bytes)
    enclave.Encrypt(eid, bytes)
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
