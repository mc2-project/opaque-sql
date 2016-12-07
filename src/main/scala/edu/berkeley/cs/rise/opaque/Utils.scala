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

import java.nio.ByteBuffer
import java.nio.ByteOrder

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLDate
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import edu.berkeley.cs.rise.opaque.execution.Block
import edu.berkeley.cs.rise.opaque.execution.ColumnType
import edu.berkeley.cs.rise.opaque.execution.OpaqueOperatorExec
import edu.berkeley.cs.rise.opaque.execution.SGXEnclave
import edu.berkeley.cs.rise.opaque.logical.ConvertToOpaqueOperators
import edu.berkeley.cs.rise.opaque.logical.EncryptLocalRelation

import com.google.flatbuffers.FlatBufferBuilder

object Utils {
  private val perf: Boolean = System.getenv("SGX_PERF") == "1"

  def time[A](desc: String)(f: => A): A = {
    val start = System.nanoTime
    val result = f
    if (perf) {
      println(s"$desc: ${(System.nanoTime - start) / 1000000.0} ms")
    }
    result
  }

  def logPerf(message: String): Unit = {
    if (perf) {
      println(message)
    }
  }

  private def jsonSerialize(x: Any): String = (x: @unchecked) match {
    case x: Int => x.toString
    case x: Double => x.toString
    case x: Boolean => x.toString
    case x: String => s""""$x""""
    case x: Option[_] => x match {
      case Some(x) => jsonSerialize(x)
      case None => "null"
    }
    case x: Map[_, _] => x.map {
      case (k, v) => s"${jsonSerialize(k)}: ${jsonSerialize(v)}"
    }.mkString("{", ", ", "}")
  }

  def timeBenchmark[A](benchmarkAttrs: (String, Any)*)(f: => A): A = {
    val start = System.nanoTime
    val result = f
    val timeMs = (System.nanoTime - start) / 1000000.0
    val attrs = benchmarkAttrs.toMap + (
      "time" -> timeMs,
      "sgx" -> (if (System.getenv("SGX_MODE") == "HW") "hw" else "sim"))
    println(jsonSerialize(attrs))
    result
  }

  def initEnclave(): (SGXEnclave, Long) = {
    this.synchronized {
      val enclave = new SGXEnclave()
      if (eid == 0L) {
        if (System.getenv("LIBSGXENCLAVE_PATH") == null) {
          throw new Exception("Set LIBSGXENCLAVE_PATH")
        }
        System.load(System.getenv("LIBSGXENCLAVE_PATH"))
        val enclave = new SGXEnclave()
        eid = enclave.StartEnclave()
        println("Starting an enclave")
        (enclave, eid)
      } else {
        val enclave = new SGXEnclave()
      }
      (enclave, eid)
    }
  }

  var eid = 0L
  var attested : Boolean = false
  var attesting_getepid : Boolean = false
  var attesting_getmsg1 : Boolean = false
  var attesting_getmsg3 : Boolean = false
  var attesting_final_ra : Boolean = false

  def initSQLContext(sqlContext: SQLContext): Unit = {
    sqlContext.experimental.extraOptimizations =
      (Seq(EncryptLocalRelation, ConvertToOpaqueOperators) ++
        sqlContext.experimental.extraOptimizations)
    sqlContext.experimental.extraStrategies =
      (Seq(OpaqueOperators) ++
        sqlContext.experimental.extraStrategies)
  }

  def encrypt[T](enclave: SGXEnclave, eid: Long, field: T, tpe: DataType)
    : Array[Byte] = {
    val buf = ByteBuffer.allocate(2048) // TODO: adaptive size
    buf.order(ByteOrder.LITTLE_ENDIAN)
    import ColumnType._
    ((field, tpe): @unchecked) match {
      case (x: Int, IntegerType) =>
        buf.put(INT.value)
        buf.putInt(4)
        buf.putInt(x)
      case (s: UTF8String, StringType) =>
        buf.put(STRING.value)
        val utf8 = s.getBytes()
        buf.putInt(utf8.length)
        buf.put(utf8)
      case (s: String, StringType) =>
        buf.put(STRING.value)
        val utf8 = s.getBytes("UTF-8")
        buf.putInt(utf8.length)
        buf.put(utf8)
      case (f: Float, FloatType) =>
        buf.put(FLOAT.value)
        buf.putInt(4)
        buf.putFloat(f)
      case (f: Double, DoubleType) =>
        buf.put(DOUBLE.value)
        buf.putInt(8)
        buf.putDouble(f)
      case (d: SQLDate, DateType) =>
        buf.put(DATE.value)
        buf.putInt(8)
        buf.putLong(DateTimeUtils.daysToMillis(d) / 1000)
      case (f: Long, LongType) =>
        buf.put(LONG.value)
        buf.putInt(8)
        buf.putLong(f)
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
    import ColumnType._
    val result = tpe match {
      case t if t == INT.value =>
        assert(size == 4)
        buf.getInt()
      case t if t == STRING.value || t == URL_TYPE.value || t == C_CODE.value ||
          t == L_CODE.value || t == IP_TYPE.value || t == USER_AGENT_TYPE.value ||
          t == SEARCH_WORD_TYPE.value || t == TPCH_NATION_NAME_TYPE.value =>
        val sBytes = new Array[Byte](size)
        buf.get(sBytes)
        UTF8String.fromBytes(sBytes)
      case t if t == FLOAT.value =>
        assert(size == 4)
        buf.getFloat()
      case t if t == DOUBLE.value =>
        assert(size == 8)
        buf.getDouble()
      case t if t == DATE.value =>
        assert(size == 8)
        DateTimeUtils.millisToDays(buf.getLong() * 1000)
      case t if t == LONG.value =>
        assert(size == 8)
        buf.getLong()
    }
    result.asInstanceOf[T]
  }

  def fieldsToRow(fields: Array[Array[Byte]]): Array[Byte] = {
    val numFields = fields.length

    val rowSize = 4 + 4 * numFields + fields.map(_.length).sum
    val buf = ByteBuffer.allocate(rowSize)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.putInt(numFields)
    for (field <- fields) {
      buf.putInt(field.length)
      buf.put(field)
    }
    buf.flip()
    val serialized = new Array[Byte](buf.limit())
    buf.get(serialized)
    serialized
  }

  def encryptTuples(rows: Seq[Product], types: Seq[DataType]): Seq[Array[Array[Byte]]] = {
    val (enclave, eid) = initEnclave()
    rows.map(row => row.productIterator.zip(types.iterator).map {
      case (field, tpe) => Utils.encrypt(enclave, eid, field, tpe)
    }.toArray)
  }

  def encryptInternalRows(rows: Seq[InternalRow], types: Seq[DataType]): Seq[Array[Array[Byte]]] = {
    val (enclave, eid) = initEnclave()
    rows.map(row => row.toSeq(types).zip(types).map {
      case (field, tpe) => Utils.encrypt(enclave, eid, field, tpe)
    }.toArray)
  }

  def decryptN(rows: Seq[Array[Array[Byte]]]): Seq[Seq[Any]] = {
    val (enclave, eid) = initEnclave()
    rows.toSeq.map { fields =>
      fields.toSeq.map { field =>
        Utils.decrypt[Any](enclave, eid, field)
      }
    }
  }

  def createBlock(rows: Array[Array[Byte]], rowsAreJoinRows: Boolean): Array[Byte] = {
    val (enclave, eid) = Utils.initEnclave()
    enclave.CreateBlock(eid, Utils.concatByteArrays(rows), rows.length, rowsAreJoinRows)
  }

  def splitBlock(
      block: Array[Byte], numRows: Int, rowsAreJoinRows: Boolean): Iterator[Array[Byte]] = {
    val (enclave, eid) = Utils.initEnclave()
    Utils.readRows(enclave.SplitBlock(eid, block, numRows, rowsAreJoinRows))
  }

  def concatByteArrays(arrays: Array[Array[Byte]]): Array[Byte] = {
    arrays match {
      case Array() => Array.empty
      case Array(bytes) => bytes
      case _ =>
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
  }

  def readRows(concatRows: Array[Byte]): Iterator[Array[Byte]] = {
    val buf = ByteBuffer.wrap(concatRows)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    new Iterator[Array[Byte]] {
      override def hasNext = buf.hasRemaining
      override def next() = readRow(buf)
    }
  }

  def readVerifiedRows(concatRows: Array[Byte]): Iterator[Array[Byte]] = {
    val buf = ByteBuffer.wrap(concatRows)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    new Iterator[Array[Byte]] {
      override def hasNext = buf.hasRemaining
      override def next() = readVerifiedRow(buf)
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

  def readVerifiedRow(buf: ByteBuffer): Array[Byte] = {
    val buf2 = buf.duplicate()
    buf2.order(ByteOrder.LITTLE_ENDIAN)
    val taskID = buf2.getInt()
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

  def ensureCached[T](
      rdd: RDD[T], storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[T] = {
    if (rdd.getStorageLevel == StorageLevel.NONE) {
      rdd.persist(storageLevel)
    } else {
      rdd
    }
  }

  def force(ds: Dataset[_]): Unit = {
    val rdd: RDD[_] = ds.queryExecution.executedPlan match {
      case p: OpaqueOperatorExec => p.executeBlocked()
      case p => p.execute()
    }
    rdd.foreach(x => {})
  }




  def encryptInternalRowsFlatbuffers(rows: Seq[InternalRow], types: Seq[DataType]): Block = {
    val builder = new FlatBufferBuilder(1024)

    val fieldTypes = types.map {
      case IntegerType => tuix.ColType.IntegerType
    }.toArray
    val fieldTypesOffset = tuix.Row.createFieldTypesVector(builder, fieldTypes)

    val rowOffsets = rows.map { row =>
      val fieldNulls = (0 to types.length).map(i => row.isNullAt(i)).toArray
      val fieldNullsOffset = tuix.Row.createFieldNullsVector(builder, fieldNulls)

      val fieldValueOffsets = row.toSeq(types).zip(types).map {
        case (x: Int, IntegerType) =>
          val valueOffset = tuix.IntegerField.createIntegerField(builder, x)
          tuix.Field.startField(builder)
          tuix.Field.addValueType(builder, tuix.FieldUnion.IntegerField)
          tuix.Field.addValue(builder, valueOffset)
          val fieldValueOffset = tuix.Field.endField(builder)
          fieldValueOffset
      }.toArray
      val fieldValuesOffset = tuix.Row.createFieldValuesVector(builder, fieldValueOffsets)

      tuix.Row.startRow(builder)
      tuix.Row.addFieldTypes(builder, fieldTypesOffset)
      tuix.Row.addFieldNulls(builder, fieldNullsOffset)
      tuix.Row.addFieldValues(builder, fieldValuesOffset)
      tuix.Row.addIsDummy(builder, false)
      val rowOffset = tuix.Row.endRow(builder)
      rowOffset
    }.toArray
    val rowsOffset = tuix.Rows.createRowsVector(builder, rowOffsets)

    val rootOffset = tuix.Rows.createRows(builder, rowsOffset)
    builder.finish(rootOffset)
    val plaintextBytes = builder.sizedByteArray()
    Block(plaintextBytes, rows.size)

    //val (enclave, eid) = initEnclave()
  }

  def decryptBlockFlatbuffers(block: Block): Seq[InternalRow] = {
    val buf = ByteBuffer.wrap(block.bytes)
    val rows = tuix.Rows.getRootAsRows(buf)
    assert(rows.rowsLength == block.numRows)
    for (i <- 0 until rows.rowsLength) yield {
      val row = rows.rows(i)
      assert(!row.isDummy)
      InternalRow.fromSeq(
        for (j <- 0 until row.fieldValuesLength) yield {
          val field: Any =
            if (!row.fieldNulls(j)) {
              val fieldUnionType = row.fieldValues(j).valueType
              fieldUnionType match {
                case tuix.FieldUnion.IntegerField =>
                  row.fieldValues(j).value(new tuix.IntegerField)
                    .asInstanceOf[tuix.IntegerField].value
              }
            } else {
              null
            }
          field
        })
    }
  }
}
