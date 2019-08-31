package edu.berkeley.cs.rise.opaque.execution

import edu.berkeley.cs.rise.opaque.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.execution.SparkPlan

case class ObliviousSortExec(order: Seq[SortOrder], child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def isOblivious: Boolean = true

  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): RDD[Block] = {
    val orderSer = Utils.serializeSortOrder(order, child.output)
    ObliviousSortExec.columnSort(
      child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), orderSer)
  }
}

object ObliviousSortExec extends java.io.Serializable {
  private def countRows(key: Int, data: Iterator[Block]): Iterator[(Int, Long)] = {
    var numRows: Long = 0L
    for (v <- data) { numRows += + v.numRows }
    Iterator((key, numRows))
  }

  private def columnSortPad(data: Block, sort_order: Array[Byte], r: Int, s: Int): Block = {
    val (enclave, eid) = Utils.initEnclave()
    val ret = enclave.EnclaveColumnSort(eid, sort_order, 0, data.bytes, r, s, 0)
    Block(ret)
  }

  private def columnSortOp(
    data: Block, partition_index: Int, sort_order: Array[Byte], round: Int, r: Int, s: Int)
      : Array[Byte] = {
    val (enclave, eid) = Utils.initEnclave()
    enclave.EnclaveColumnSort(eid, sort_order, round, data.bytes, r, s, partition_index)
  }

  private def externalSort(data: Block, sort_order: Array[Byte]): Array[Byte] = {
    val (enclave, eid) = Utils.initEnclave()
    enclave.ExternalSort(eid, sort_order, data.bytes)
  }

  private def columnSortFilter(data: Array[Byte], sort_order: Array[Byte], r: Int, s: Int)
      : Block = {
    val (enclave, eid) = Utils.initEnclave()
    val ret = enclave.EnclaveColumnSort(eid, sort_order, 5, data, r, s, 0)
    Block(ret)
  }

  def columnSort(data: RDD[Block], sort_order: Array[Byte], r_input: Int = 0, s_input: Int = 0)
      : RDD[Block] = {
    // Parse the bytes and split into blocks, one for each destination column
    val NumMachines = data.partitions.length
    val NumCores = 1

    // Let len be N
    // Divide N into r * s, where s is the number of machines, and r is the size of the
    // Constraints: s | r; r >= 2 * (s-1)^2

    Utils.ensureCached(data)

    val numRows = data
      .mapPartitionsWithIndex((index, x) => countRows(index, x)).collect.sortBy(_._1)
    var len = 0.toLong

    for (idx <- 0 until numRows.length) {
      len += numRows(idx)._2
    }

    if (len == 0) {
      return data
    }

    var s = s_input
    var r = r_input

    if (r_input == 0 && s_input == 0) {
      s = NumMachines * NumCores
      r = (math.ceil(len * 1.0 / s)).toInt
    }

    // r should be even and a multiple of s
    if (r < 2 * math.pow(s, 2).toInt) {
      r = 2 * math.pow(s, 2).toInt
    }

    if (s % 2 == 0 && r % s != 0) {
      r = (r / s + 1) * s
    } else if (r % (2 * s) != 0) {
      r = (r / (2 * s) + 1) * (s * 2)
    }

    // Pad with dummy rows
    val padded_data = data.map(x => columnSortPad(x, sort_order, r, s))

    // Oblivious sort, transpose
    val transposed_data = padded_data.mapPartitionsWithIndex {
      (index, l) => l.map(x => columnSortOp(x, index, sort_order, 1, r, s))
    }.mapPartitions(blockIter => blockIter.flatMap(block =>
      Utils.extractShuffleOutputs(Block(block))))
      .groupByKey()
      .mapPartitions(pairIter => Iterator(Utils.concatEncryptedBlocks(
        pairIter.flatMap(_._2).toSeq)))

    // Oblivious sort, untranspose
    val untransposed_data = transposed_data.mapPartitionsWithIndex {
      (index, l) => l.map(x => ColumnSortOp(x, index, sort_order, 2, r, s))
    }.mapPartitions(blockIter => blockIter.flatMap(block =>
      Utils.extractShuffleOutputs(Block(block))))
      .groupByKey()
      .mapPartitions(pairIter => Iterator(Utils.concatEncryptedBlocks(
        pairIter.flatMap(_._2).toSeq)))

    // Oblivious sort, shift down
    val shifted_down_data = untransposed_data.mapPartitionsWithIndex {
      (index, l) => l.map(x => ColumnSortOp(x, index, sort_order, 3, r, s))
    }.mapPartitions(blockIter => blockIter.flatMap(block =>
      Utils.extractShuffleOutputs(Block(block))))
      .groupByKey()
      .mapPartitions(pairIter => Iterator(Utils.concatEncryptedBlocks(
        pairIter.flatMap(_._2).toSeq)))

    // Oblivious sort, shift up
    val shifted_up_data = shifted_down_data.mapPartitionsWithIndex {
      (index, l) => l.map(x => ColumnSortOp(x, index, sort_order, 4, r, s))
    }.mapPartitions(blockIter => blockIter.flatMap(block =>
      Utils.extractShuffleOutputs(Block(block))))
      .groupByKey()
      .mapPartitions(pairIter => Iterator(Utils.concatEncryptedBlocks(
        pairIter.flatMap(_._2).toSeq)))

    // Final oblivious sort
    val sorted_data = shifted_up_data.mapPartitionsWithIndex {
      (index, l) => l.map(x => externalSort(x, sort_order))}

    // Filter out dummy rows
    val filtered_data = sorted_data.map(x => columnSortFilter(x, sort_order, r, s))

    filtered_data
  }
}
