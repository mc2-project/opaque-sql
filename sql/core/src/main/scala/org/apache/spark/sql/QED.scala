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
  def time[A](desc: String)(f: => A): A = {
    val start = System.nanoTime
    val result = f
    println(s"$desc: ${(System.nanoTime - start) / 1000000.0} ms")
    result
  }

  def initEnclave(): (SGXEnclave, Long) = {
    this.synchronized {
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
  }

  var eid = 0L

  val encoder = new BASE64Encoder()
  val decoder = new BASE64Decoder()

  def encrypt[T](enclave: SGXEnclave, eid: Long, field: T, tpe: Option[QEDColumnType] = None)
    : Array[Byte] = {
    val buf = ByteBuffer.allocate(2048) // TODO: adaptive size
    buf.order(ByteOrder.LITTLE_ENDIAN)
    import org.apache.spark.sql.QEDColumnType._
    ((field, tpe): @unchecked) match {
      case (x: Int, None) =>
        buf.put(INT.value)
        buf.putInt(4)
        buf.putInt(x)
      case (s: String, None) =>
        buf.put(STRING.value)
        val utf8 = s.getBytes("UTF-8")
        buf.putInt(utf8.length)
        buf.put(utf8)
      case (f: Float, None) =>
        buf.put(FLOAT.value)
        buf.putInt(4)
        buf.putFloat(f)
      case (d: java.sql.Date, None) =>
        buf.put(DATE.value)
        buf.putInt(8)
        buf.putLong(d.getTime / 1000)
      case (s: String, Some(URL_TYPE)) =>
        buf.put(URL_TYPE.value)
        val utf8 = s.getBytes("UTF-8")
        buf.putInt(utf8.length)
        buf.put(utf8)
      case (s: String, Some(C_CODE)) =>
        buf.put(C_CODE.value)
        val utf8 = s.getBytes("UTF-8")
        buf.putInt(utf8.length)
        buf.put(utf8)
      case (s: String, Some(L_CODE)) =>
        buf.put(L_CODE.value)
        val utf8 = s.getBytes("UTF-8")
        buf.putInt(utf8.length)
        buf.put(utf8)
      case (s: String, Some(IP_TYPE)) =>
        buf.put(IP_TYPE.value)
        val utf8 = s.getBytes("UTF-8")
        buf.putInt(utf8.length)
        buf.put(utf8)
      case (s: String, Some(USER_AGENT_TYPE)) =>
        buf.put(USER_AGENT_TYPE.value)
        val utf8 = s.getBytes("UTF-8")
        buf.putInt(utf8.length)
        buf.put(utf8)
      case (s: String, Some(SEARCH_WORD_TYPE)) =>
        buf.put(SEARCH_WORD_TYPE.value)
        val utf8 = s.getBytes("UTF-8")
        buf.putInt(utf8.length)
        buf.put(utf8)
    }
    buf.flip()
    val bytes = new Array[Byte](buf.limit)
    buf.get(bytes)
    val encrypted_value = enclave.EncryptAttribute(eid, bytes)
    encrypted_value
  }

  def decrypt[T](enclave: SGXEnclave, eid: Long, bytes: Array[Byte]): T = {
    val buf = ByteBuffer.wrap(enclave.Decrypt(eid, bytes))
    buf.order(ByteOrder.LITTLE_ENDIAN)
    val tpe = buf.get()
    val size = buf.getInt()
    import org.apache.spark.sql.QEDColumnType._
    val result = tpe match {
      case t if t == INT.value =>
        assert(size == 4)
        buf.getInt()
      case t if t == STRING.value || t == URL_TYPE.value || t == C_CODE.value ||
          t == L_CODE.value || t == IP_TYPE.value || t == USER_AGENT_TYPE.value ||
          t == SEARCH_WORD_TYPE.value =>
        val sBytes = new Array[Byte](size)
        buf.get(sBytes)
        new String(sBytes, "UTF-8")
      case t if t == FLOAT.value =>
        assert(size == 4)
        buf.getFloat()
      case t if t == DATE.value =>
        assert(size == 8)
        new java.sql.Date(buf.getLong() * 1000)
    }
    result.asInstanceOf[T]
  }

  def encrypt1[A](rows: Seq[A]): Seq[Array[Byte]] = {
    val (enclave, eid) = initEnclave()
    rows.map {
      a => QED.encrypt(enclave, eid, a)
    }
  }
  def encrypt2[A, B](rows: Seq[(A, B)]): Seq[(Array[Byte], Array[Byte])] = {
    val (enclave, eid) = initEnclave()
    rows.map {
      case (a, b) => (QED.encrypt(enclave, eid, a), QED.encrypt(enclave, eid, b))
    }
  }
  def encrypt3[A, B, C](rows: Seq[(A, B, C)]): Seq[(Array[Byte], Array[Byte], Array[Byte])] = {
    val (enclave, eid) = initEnclave()
    rows.map {
      case (a, b, c) =>
        (QED.encrypt(enclave, eid, a),
          QED.encrypt(enclave, eid, b),
          QED.encrypt(enclave, eid, c))
    }
  }
  def encrypt4[A, B, C, D](rows: Seq[(A, B, C, D)]): Seq[(Array[Byte], Array[Byte], Array[Byte], Array[Byte])] = {
    val (enclave, eid) = initEnclave()
    rows.map {
      case (a, b, c, d) =>
        (QED.encrypt(enclave, eid, a),
          QED.encrypt(enclave, eid, b),
          QED.encrypt(enclave, eid, c),
          QED.encrypt(enclave, eid, d))
    }
  }

  def decrypt1[A](rows: Seq[Row]): Seq[A] = {
    val (enclave, eid) = initEnclave()
    rows.map {
      case Row(aEnc: Array[Byte]) =>
        QED.decrypt[A](enclave, eid, aEnc)
    }
  }
  def decrypt2[A, B](rows: Seq[Row]): Seq[(A, B)] = {
    val (enclave, eid) = initEnclave()
    rows.map {
      case Row(aEnc: Array[Byte], bEnc: Array[Byte]) =>
        (QED.decrypt[A](enclave, eid, aEnc), QED.decrypt[B](enclave, eid, bEnc))
    }
  }
  def decrypt3[A, B, C](rows: Seq[Row]): Seq[(A, B, C)] = {
    val (enclave, eid) = initEnclave()
    rows.map {
      case Row(aEnc: Array[Byte], bEnc: Array[Byte], cEnc: Array[Byte]) =>
        (QED.decrypt[A](enclave, eid, aEnc),
          QED.decrypt[B](enclave, eid, bEnc),
          QED.decrypt[C](enclave, eid, cEnc))
    }
  }
  def decrypt4[A, B, C, D](rows: Seq[Row]): Seq[(A, B, C, D)] = {
    val (enclave, eid) = initEnclave()
    rows.map {
      case Row(aEnc: Array[Byte], bEnc: Array[Byte], cEnc: Array[Byte], dEnc: Array[Byte]) =>
        (QED.decrypt[A](enclave, eid, aEnc),
          QED.decrypt[B](enclave, eid, bEnc),
          QED.decrypt[C](enclave, eid, cEnc),
          QED.decrypt[D](enclave, eid, dEnc))
    }
  }
  def decrypt5[A, B, C, D, E](rows: Seq[Row]): Seq[(A, B, C, D, E)] = {
    val (enclave, eid) = initEnclave()
    rows.map {
      case Row(aEnc: Array[Byte], bEnc: Array[Byte], cEnc: Array[Byte], dEnc: Array[Byte], eEnc: Array[Byte]) =>
        (QED.decrypt[A](enclave, eid, aEnc),
          QED.decrypt[B](enclave, eid, bEnc),
          QED.decrypt[C](enclave, eid, cEnc),
          QED.decrypt[D](enclave, eid, dEnc),
          QED.decrypt[E](enclave, eid, eEnc))
    }
  }

  def randomId(enclave: SGXEnclave, eid: Long): Array[Byte] = {
    enclave.RandomID(eid)
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

  private def tableId(b: Byte): Array[Byte] = {
    val buf = ByteBuffer.allocate(8)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    for (i <- 1 to 8) {
      buf.put(b)
    }
    buf.flip()
    val result = new Array[Byte](8)
    buf.get(result)
    result
  }

  def primaryTableId(): Array[Byte] = tableId('a')
  def foreignTableId(): Array[Byte] = tableId('b')

  def genAndWriteData() = {
    // write this hard-coded set of columns to text file, in csv format
  }

  def attributeIndexOf(a: Attribute, list: Seq[Attribute]): Int = {
    var i = 0
    while (i < list.size) {
      if (list(i) semanticEquals a) {
        return i
      }
      i += 1
    }
    return -1
  }

  def bd1Encrypt3(iter: Iterator[Row]): Iterator[(Array[Byte], Array[Byte], Array[Byte])] = {
    val (enclave, eid) = QED.initEnclave()
    iter.map {
      case Row(u: String, r: Int, d: Int) =>
        (QED.encrypt(enclave, eid, u, Some(QEDColumnType.URL_TYPE)),
          QED.encrypt(enclave, eid, r),
          QED.encrypt(enclave, eid, d))
    }
  }

  def bd1Decrypt2(iter: Iterator[Row]): Iterator[(String, Int)] = {
    val (enclave, eid) = QED.initEnclave()
    iter.map {
      case Row(u: Array[Byte], r: Array[Byte]) =>
        (QED.decrypt[String](enclave, eid, u), QED.decrypt[Int](enclave, eid, r))
    }
  }

  def bd2Encrypt9(iter: Iterator[Row])
      : Iterator[(
        Array[Byte], Array[Byte], Array[Byte],
        Array[Byte], Array[Byte], Array[Byte],
        Array[Byte], Array[Byte], Array[Byte])] = {
    val (enclave, eid) = QED.initEnclave()
    iter.map {
      case Row(
        si: String,
        du: String,
        vd: java.sql.Date,
        ar: Float,
        ua: String,
        cc: String,
        lc: String,
        sw: String,
        d: Int) =>
        (QED.encrypt(enclave, eid, si, Some(QEDColumnType.IP_TYPE)),
          QED.encrypt(enclave, eid, du, Some(QEDColumnType.URL_TYPE)),
          QED.encrypt(enclave, eid, vd),
          QED.encrypt(enclave, eid, ar),
          QED.encrypt(enclave, eid, ua, Some(QEDColumnType.USER_AGENT_TYPE)),
          QED.encrypt(enclave, eid, cc, Some(QEDColumnType.C_CODE)),
          QED.encrypt(enclave, eid, lc, Some(QEDColumnType.L_CODE)),
          QED.encrypt(enclave, eid, sw, Some(QEDColumnType.SEARCH_WORD_TYPE)),
          QED.encrypt(enclave, eid, d))
    }
  }

  def bd2Decrypt2(iter: Iterator[Row]): Iterator[(String, Float)] = {
    val (enclave, eid) = QED.initEnclave()
    iter.map {
      case Row(sis: Array[Byte], ar: Array[Byte]) =>
        (QED.decrypt[String](enclave, eid, sis), QED.decrypt[Float](enclave, eid, ar))
    }
  }

  def bd3Decrypt3(iter: Iterator[Row]): Iterator[(String, Float, Int)] = {
    val (enclave, eid) = QED.initEnclave()
    iter.map {
      case Row(si: Array[Byte], tr: Array[Byte], apr: Array[Byte]) =>
        (QED.decrypt[String](enclave, eid, si), QED.decrypt[Float](enclave, eid, tr),
          QED.decrypt[Int](enclave, eid, apr))
    }
  }

  def pagerankEncryptEdges(iter: Iterator[Row])
    : Iterator[(Array[Byte], Array[Byte], Array[Byte])] = {

    val (enclave, eid) = QED.initEnclave()
    iter.map {
      case Row(src: Int, dst: Int, weight: Float) =>
        (QED.encrypt(enclave, eid, src), QED.encrypt(enclave, eid, dst),
          QED.encrypt(enclave, eid, weight))
    }
  }

  def pagerankEncryptVertices(iter: Iterator[Row]): Iterator[(Array[Byte], Array[Byte])] = {
    val (enclave, eid) = QED.initEnclave()
    iter.map {
      case Row(id: Int, rank: Float) =>
        (QED.encrypt(enclave, eid, id), QED.encrypt(enclave, eid, rank))
    }
  }

}
