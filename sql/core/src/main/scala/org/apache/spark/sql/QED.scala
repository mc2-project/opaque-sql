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
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType

object QED {
  def initEnclave(): (SGXEnclave, Long) = {
    if (eid == 0L) {
      System.load(System.getenv("LIBSGXENCLAVE_PATH"))
      val enclave = new SGXEnclave()
      eid = enclave.StartEnclave()
      println("Starting an enclave")
      (enclave, eid)
    } else {
      val enclave = new SGXEnclave()
      (enclave, eid)
    }
  }

  var eid = 0L

  val encoder = new BASE64Encoder()
  val decoder = new BASE64Decoder()

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

  def concatByteArrays(arrays: Array[Array[Byte]]): Array[Byte] = {
    val totalBytes = arrays.map(_.length).sum
    val buf = ByteBuffer.allocate(totalBytes)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    for (a <- arrays) {
      buf.put(a)
    }
    buf.flip()
    val all = new Array[Byte](buf.limit())
    buf.get(all)
    all
  }

  def readRows(concatRows: Array[Byte]): Iterator[Array[Byte]] = {
    val buf = ByteBuffer.wrap(concatRows)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    new Iterator[Array[Byte]] {
      override def hasNext = buf.hasRemaining
      override def next() = readRow(buf)
    }
  }

  def parseRows(concatRows: Array[Byte]): Iterator[Array[Array[Byte]]] = {
    val buf = ByteBuffer.wrap(concatRows)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    new Iterator[Array[Array[Byte]]] {
      override def hasNext = buf.hasRemaining
      override def next() = parseRow(buf)
    }
  }

  def readRow(buf: ByteBuffer): Array[Byte] = {
    val buf2 = buf.duplicate()
    buf2.order(ByteOrder.LITTLE_ENDIAN)
    val numFields = buf2.getInt()
    for (i <- 0 until numFields) {
      val fieldLength = buf2.getInt()
      val field = new Array[Byte](fieldLength)
      buf2.get(field)
    }
    val rowBytes = new Array[Byte](buf2.position - buf.position)
    buf.get(rowBytes)
    rowBytes
  }

  def parseRow(bytes: Array[Byte]): Array[Array[Byte]] = {
    val buf = ByteBuffer.wrap(bytes)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    parseRow(buf)
  }

  def parseRow(buf: ByteBuffer): Array[Array[Byte]] = {
    val numFields = buf.getInt()
    val fields = new Array[Array[Byte]](numFields)
    for (i <- 0 until numFields) {
      val fieldLength = buf.getInt()
      val field = new Array[Byte](fieldLength)
      buf.get(field)
      fields(i) = field
    }
    fields
  }

  def splitBytes(bytes: Array[Byte], numSplits: Int): Array[Array[Byte]] = {
    val splitSize = bytes.length / numSplits
    assert(numSplits * splitSize == bytes.length)
    bytes.grouped(splitSize).toArray
  }

  def genAndWriteData() = {
    // write this hard-coded set of columns to text file, in csv format
  }
}
