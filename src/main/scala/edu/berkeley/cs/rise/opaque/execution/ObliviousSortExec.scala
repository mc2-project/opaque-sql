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

object ObliviousSortExec extends java.io.Serializable {

  val Multiplier = 1 // TODO: fix bug when this is 1

  import Utils.{time, logPerf}

  def CountRows(key: Int, data: Iterator[Block]): Iterator[(Int, Int)] = {
    var numRows = 0
    for (v <- data) {
      numRows += v.numRows
    }
    Iterator((key, numRows))
  }

  def ColumnSortPad(data: Array[Byte], r: Int, s: Int): Array[Byte] = {
    val (enclave, eid) = Utils.initEnclave()

    val ret = enclave.EnclaveColumnSort(eid,
      0, 0, 0, data, r, s, 0)

    ret
  }

  def ColumnSortOp(
    data: Array[Byte],
    partition_index: Int, numpart: Int,
    sort_order: Int,
    sort_order_length: Int,
    round: Int, r: Int, s: Int) : (Int, Array[Byte]) = {

    val (enclave, eid) = Utils.initEnclave()
    val ret = enclave.EnclaveColumnSort(eid,
      sort_order, sort_order_length, round, data, r, s, partition_index)

    ret
  }

  def ColumnSortFilter(data: Array[Byte], r: Int, s: Int): Array[Byte] = {
    val (enclave, eid) = Utils.initEnclave()

    val ret = enclave.EnclaveColumnSort(eid,
      0, 0, 5, data, r, s, 0)

    ret
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

    // Pad with dummy rows
    val padded_data = data.map(x => ColumnSortPad(x, r, s))

    // Oblivious sort, transpose
    val transposed_data = padded_data.mapPartitionsWithIndex {
      (index, l) => l.map(x => ColumnSortOp(x, index, s, opcode, 1, r, s))
    }.mapPartitions(blockIter => extractShuffleOutputs(blockIter))
      .groupByKey()
      .mapPartitions(pairIter => concatByteArrays(pairIter.map(_._2)))

    // Oblivious sort, untranspose
    val untransposed_data = transposed_data.mapPartitionsWithIndex {
      (index, l) => l.map(x => ColumnSortOp(x, index, s, opcode, 2, r, s))
    }.mapPartitions(blockIter => extractShuffleOutputs(blockIter))
      .groupByKey()
      .mapPartitions(pairIter => concatByteArrays(pairIter.map(_._2)))

    // Oblivious sort, shift down
    val shifted_down_data = untransposed_data.mapPartitionsWithIndex {
      (index, l) => l.map(x => ColumnSortOp(x, index, s, opcode, 3, r, s))
    }.mapPartitions(blockIter => extractShuffleOutputs(blockIter))
      .groupByKey()
      .mapPartitions(pairIter => concatByteArrays(pairIter.map(_._2)))

    // Oblivious sort, shift up
    val shifted_up_data = shifted_down_data.mapPartitionsWithIndex {
      (index, l) => l.map(x => ColumnSort(x, index, s, opcode, 4, r, s))
    }.mapPartitions(blockIter => extractShuffleOutputs(blockIter))
      .groupByKey()
      .mapPartitions(pairIter => concatByteArrays(pairIter.map(_._2)))

    // Filter out dummy rows
    val filtered_data = shifted_up_data.map(x => ColumnSortFilter(x, r, s))

    filtered_data
  }
}