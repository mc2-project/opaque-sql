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

package edu.berkeley.cs.rise.opaque.execution

import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.collection.mutable.ArrayBuffer

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.RA
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.execution.SparkPlan

case class SortOpcodeExec(order: Seq[SortOrder], child: SparkPlan) {
  import Opcode._

  private object Col extends ColumnNumberMatcher {
    override def input: Seq[Attribute] = child.output
  }

  def getOpcode(): Opcode = {
    val opcode = order match {
      case Seq() =>
        OP_NOSORT
      case Seq(SortOrder(Col(1, _), Ascending)) =>
        OP_SORT_COL1
      case Seq(SortOrder(Col(2, _), Ascending)) =>
        OP_SORT_COL2
      case Seq(SortOrder(Col(1, _), Ascending), SortOrder(Col(2, _), Ascending)) =>
        OP_SORT_COL1_COL2
    }
    opcode
  }
}

case class ObliviousSortExec(order: Seq[SortOrder], child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  private object Col extends ColumnNumberMatcher {
    override def input: Seq[Attribute] = child.output
  }

  override def output: Seq[Attribute] = child.output

  override def executeBlocked() = {
    import Opcode._
    val opcode = SortOpcodeExec(order, child).getOpcode()
    if (opcode != OP_NOSORT) {
      ObliviousSortExec.sortBlocks(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), opcode)
    } else {
      child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    }
  }
}

import Opcode._
case class ObliviousSortOpcodeExec(opcodes: Seq[Opcode], child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked() = {
    val opcode = opcodes match {
      case Seq(x:Opcode) =>
        x

      case Seq(OP_SORT_COL3_IS_DUMMY_COL1, OP_SORT_COL3) =>
        OP_SORT_COL3_IS_DUMMY_COL1_SORT_COL3

      case Seq(OP_SORT_COL2_IS_DUMMY_COL1, OP_SORT_COL3) =>
        OP_SORT_COL2_IS_DUMMY_COL1_SORT_COL3

      case _ => throw new Exception(
          s"OblivoiusSortOpcodeExec: unknown Opcode seq")
    }

    if (opcode != OP_NOSORT) {
      ObliviousSortExec.sortBlocks(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), opcode)
    } else {
      child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    }
  }
}

object ObliviousSortExec extends java.io.Serializable {

  val Multiplier = 1 // TODO: fix bug when this is 1

  import Utils.{time, logPerf}

  def sortBlocks(data: RDD[Block], opcode: Opcode): RDD[Block] = {
    time("oblivious sort") {
      Utils.ensureCached(data)
      RA.initRA(data)
      val sorted =
        if (data.partitions.length <= 1) {
          data.map { block =>
            val (enclave, eid) = Utils.initEnclave()
            val sortedRows = enclave.ObliviousSort(eid, opcode.value, block.bytes, 0, block.numRows)
            Block(sortedRows, block.numRows)
          }
        } else {
          val result = NewColumnSort(data.context, data, opcode)
          assert(result.partitions.length == data.partitions.length)
          result
        }
      Utils.ensureCached(sorted)
      sorted.count()
      sorted
    }
  }

  def CountRows(key: Int, data: Iterator[Block]): Iterator[(Int, Int)] = {
    var numRows = 0
    for (v <- data) {
      numRows += v.numRows
    }
    Iterator((key, numRows))
  }

  // def EnclaveCountRows(data: Array[Byte]): Int = {
  //   val (enclave, eid) = Utils.initEnclave()
  //   enclave.CountNumRows(eid, data)
  // }

  def ParseData(input: (Int, Array[Byte]), r: Int, s: Int): Iterator[(Int, (Int, Array[Byte]))] = {
    // split a buffer into s separate buffer, one for each column
    val prev_column = input._1
    val data = input._2

    val columns = new Array[(Int, (Int, Array[Byte]))](s)

    val buf = ByteBuffer.wrap(data)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    for (c <- 1 to s) {
      // read the column ID
      val columnID = buf.getInt()
      val columnSize = buf.getInt()

      //println(s"ParseData: $columnID, $columnSize")

      val column = new Array[Byte](columnSize)
      buf.get(column)
      columns(c-1) = (c, (prev_column, column))
    }

    columns.iterator
  }

  def ParseDataPostProcess(
      key: (Int, Iterable[(Int, Array[Byte])]), round: Int, r: Int, s: Int)
    : Iterator[(Int, Array[Byte])] = {

    val column = key._1
    val arrays = key._2.toArray
    val joinArray =
      if (round == 4) {
        // we need to reorder the byte arrays according to which previous column they belong
        val arraysSorted = arrays.sortBy(_._1).map(_._2)
        if (column == s) {
          val arraysRotated = arraysSorted.slice(1, arraysSorted.length) :+ arraysSorted(0)
          Utils.concatByteArrays(arraysRotated)
        } else {
          Utils.concatByteArrays(arraysSorted)
        }
      } else {
        Utils.concatByteArrays(arrays.map(_._2))
      }

    val ret_array = new Array[(Int, Array[Byte])](1)
    ret_array(0) = (key._1, joinArray)

    ret_array.iterator
  }

  def ColumnSortPreProcess(
    index: Int, data: Iterator[Block], index_offsets: Seq[Int],
    op_code: Opcode, r: Int, s: Int) : Iterator[(Int, Array[Byte])] = {

    val (enclave, eid) = Utils.initEnclave()
    val joinArray = Utils.concatByteArrays(data.map(_.bytes).toArray)

    val ret = enclave.EnclaveColumnSort(eid,
      index, s,
      op_code.value, 0, joinArray, r, s, 0, index, 0, index_offsets(index))

    val ret_array = new Array[(Int, Array[Byte])](1)
    ret_array(0) = (0, ret)

    ret_array.iterator
  }

  def ColumnSortPad(data: (Int, Array[Byte]), r: Int, s: Int, op_code: Opcode): (Int, Array[Byte]) = {
    val (enclave, eid) = Utils.initEnclave()

    val ret = enclave.EnclaveColumnSort(eid,
      data._1, r,
      op_code.value, 1, data._2, r, s, 0, 0, 0, 0)

    (data._1, ret)
  }

  def ColumnSortPartition(
    input: (Int, Array[Byte]),
    index: Int, numpart: Int,
    op_code: Opcode,
    round: Int, r: Int, s: Int) : (Int, Array[Byte]) = {

    val cur_column = input._1
    val data = input._2

    val (enclave, eid) = Utils.initEnclave()
    val ret = enclave.EnclaveColumnSort(eid,
      index, numpart,
      op_code.value, round+1, data, r, s, cur_column, 0, 0, 0)

    (cur_column, ret)
  }

  // The job is to cut off the last x number of rows
  def FinalFilter(column: Int, data: Array[Byte], r: Int, offset: Int, op_code: Opcode) : (Array[Byte], Int) = {
    val (enclave, eid) = Utils.initEnclave()

    var num_output_rows = new MutableInteger
    val ret = enclave.ColumnSortFilter(eid, op_code.value, data, column, offset, r, num_output_rows)

    (ret, num_output_rows.value)
  }

  def NewColumnSort(sc: SparkContext, data: RDD[Block], opcode: Opcode, r_input: Int = 0, s_input: Int = 0)
      : RDD[Block] = {
    // parse the bytes and split into blocks, one for each destination column

    val NumMachines = data.partitions.length
    val NumCores = 1

    // let len be N
    // divide N into r * s, where s is the number of machines, and r is the size of the
    // constraints: s | r; r >= 2 * (s-1)^2

    Utils.ensureCached(data)

    val numRows = data
      .mapPartitionsWithIndex((index, x) => CountRows(index, x)).collect.sortBy(_._1)
    var len = 0

    val offsets = ArrayBuffer.empty[Int]

    offsets += 0
    var cur = 0
    for (idx <- 0 until numRows.length) {
      cur += numRows(idx)._2
      offsets += cur
      len += numRows(idx)._2
    }

    if (len == 0) {
      return data
    }

    var s = s_input
    var r = r_input

    if (r_input == 0 && s_input == 0) {
      s = NumMachines * NumCores * Multiplier
      r = (math.ceil(len * 1.0 / s)).toInt
    }

    // r should be even and a multiple of s
    if (r < 2 * math.pow(s, 2).toInt) {
      r = 2 * math.pow(s, 2).toInt
      logPerf(s"Padding r from $r to ${2 * math.pow(s, 2).toInt}. s=$s, len=$len, r=$r")
    }

    if (s % 2 == 0 && r % s != 0) {
      logPerf(s"Padding r from $r to ${(r / s + 1) * s * 2}. s=$s, len=$len, r=$r")
      r = (r / s + 1) * s
    } else if (r % (2 * s) != 0) {
      logPerf(s"Padding r from $r to ${(r / s + 1) * s * 2}. s=$s, len=$len, r=$r")
      r = (r / (2 * s) + 1) * (s * 2)
    }

    logPerf(s"len=$len, s=$s, r=$r, NumMachines: $NumMachines, NumCores: $NumCores, Multiplier: $Multiplier")

    RA.initRA(data)

    val parsed_data = data.mapPartitionsWithIndex(
      (index, x) =>
      ColumnSortPreProcess(index, x, offsets, opcode, r, s)
    )
      .flatMap(x => ParseData(x, r, s))
      .groupByKey(s)
      .flatMap(x => ParseDataPostProcess(x, 0, r, s))

    val padded_data = parsed_data.map(x => ColumnSortPad(x, r, s, opcode))

    val data_1 = padded_data.mapPartitionsWithIndex {
      (index, l) => l.map(x => ColumnSortPartition(x, index, s, opcode, 1, r, s))
    }.flatMap(x => ParseData(x, r, s))
      .groupByKey(s)
      .flatMap(x => ParseDataPostProcess(x, 1, r, s))

    val data_2 = data_1.mapPartitionsWithIndex {
      (index, l) => l.map(x => ColumnSortPartition(x, index, s, opcode, 2, r, s))
    }.flatMap(x => ParseData(x, r, s))
    .groupByKey(s)
      .flatMap(x => ParseDataPostProcess(x, 2, r, s))

    val data_3 = data_2.mapPartitionsWithIndex {
      (index, l) => l.map(x => ColumnSortPartition(x, index, s, opcode, 3, r, s))
    }.flatMap(x => ParseData(x, r, s))
      .groupByKey(s)
      .flatMap(x => ParseDataPostProcess(x, 3, r, s))

    val data_4 = data_3.mapPartitionsWithIndex {
      (index, l) => l.map(x => ColumnSortPartition(x, index, s, opcode, 4, r, s))
    }.flatMap(x => ParseData(x, r, s))
      .groupByKey(s)
      .flatMap(x => ParseDataPostProcess(x, 4, r, s))
      .sortByKey()

    val result = data_4.map{x => FinalFilter(x._1, x._2, r, len, opcode)}
      .filter(x => x._1.nonEmpty)
      .mapPartitions { iter =>
        val array = iter.toArray
        Iterator(Block(Utils.concatByteArrays(array.map(_._1)), array.map(_._2).sum))
      }

    result
  }

}
