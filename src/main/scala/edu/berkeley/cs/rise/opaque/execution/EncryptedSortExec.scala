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

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.RA
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.execution.SparkPlan

case class EncryptedSortExec(order: Seq[SortOrder], child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  private object Col extends ColumnNumberMatcher {
    override def input: Seq[Attribute] = child.output
  }

  override def output: Seq[Attribute] = child.output

  override def executeBlocked() = {
    import Opcode._
    val opcode = order match {
      case Seq(SortOrder(Col(1, _), Ascending)) =>
        OP_SORT_COL1
      case Seq(SortOrder(Col(2, _), Ascending)) =>
        OP_SORT_COL2
    }
    EncryptedSortExec.sort(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), opcode)
  }
}

object EncryptedSortExec {
  import Utils.time

  def sort(childRDD: RDD[Block], opcode: Opcode): RDD[Block] = {
    Utils.ensureCached(childRDD)
    RA.initRA(childRDD)

    time("non-oblivious sort") {
      val numPartitions = childRDD.partitions.length
      val result =
        if (numPartitions <= 1) {
          childRDD.map { block =>
            val (enclave, eid) = Utils.initEnclave()
            val sortedRows = enclave.ExternalSort(eid, 0, 0, opcode.value, block.bytes, block.numRows)
            Block(sortedRows, block.numRows)
          }
        } else {
          // Collect a sample of the input rows
          val sampled = time("non-oblivious sort - Sample") {
            childRDD.map { block =>
              val (enclave, eid) = Utils.initEnclave()
              val numOutputRows = new MutableInteger
              val sampledBlock = enclave.Sample(
                eid, 0, 0, opcode.value, block.bytes, block.numRows, numOutputRows)
              Block(sampledBlock, numOutputRows.value)
            }.collect
          }
          // Find range boundaries locally
          val (enclave, eid) = Utils.initEnclave()
          val boundaries = time("non-oblivious sort - FindRangeBounds") {
            enclave.FindRangeBounds(
              eid, opcode.value, numPartitions, Utils.concatByteArrays(sampled.map(_.bytes)),
              sampled.map(_.numRows).sum)
          }
          // Broadcast the range boundaries and use them to partition the input
          childRDD.flatMap { block =>
            val (enclave, eid) = Utils.initEnclave()
            val offsets = new Array[Int](numPartitions + 1)
            val rowsPerPartition = new Array[Int](numPartitions)
            val partitions = enclave.PartitionForSort(
              eid, 0, 0, opcode.value, numPartitions, block.bytes, block.numRows, boundaries, offsets,
              rowsPerPartition)
            offsets.sliding(2).zip(rowsPerPartition.iterator).zipWithIndex.map {
              case ((Array(start, end), numRows), i) =>
                (i, Block(partitions.slice(start, end), numRows))
            }
          }
          // Shuffle the input to achieve range partitioning and sort locally
            .groupByKey(numPartitions).map {
            case (i, blocks) =>
              val (enclave, eid) = Utils.initEnclave()
              val input = Utils.concatByteArrays(blocks.map(_.bytes).toArray)
              val numRows = blocks.map(_.numRows).sum
              val sortedRows = enclave.ExternalSort(eid, 0, 0, opcode.value, input, numRows)
              Block(sortedRows, numRows)
          }
        }
      Utils.ensureCached(result)
      result.count()
      result
    }
  }
}
