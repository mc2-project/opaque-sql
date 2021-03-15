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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.execution.SparkPlan

case class EncryptedSortExec(order: Seq[SortOrder], isGlobal: Boolean, child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): RDD[Block] = {
    val orderSer = Utils.serializeSortOrder(order, child.output)
    val childRDD = child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    val partitionedRDD = isGlobal match {
      case true => EncryptedSortExec.sampleAndPartition(childRDD, orderSer)
      case false => childRDD
    }
    EncryptedSortExec.localSort(partitionedRDD, orderSer)
  }
}

case class EncryptedRangePartitionExec(order: Seq[SortOrder], child: SparkPlan)
    extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): RDD[Block] = {
    val orderSer = Utils.serializeSortOrder(order, child.output)
    EncryptedSortExec.sampleAndPartition(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), orderSer)
  }
}

object EncryptedSortExec {
  import Utils.time

  def sampleAndPartition(childRDD: RDD[Block], orderSer: Array[Byte]): RDD[Block] = {
    Utils.ensureCached(childRDD)
    time("force child of sampleAndPartition") { childRDD.count }

    val numPartitions = childRDD.partitions.length
    if (numPartitions <= 1) {
      childRDD
    } else {
      // Collect a sample of the input rows
      val sampled = time("non-oblivious sort - Sample") {
        Utils.concatEncryptedBlocks(childRDD.map { block =>
          val (enclave, eid) = Utils.initEnclave()
          val sampledBlock = enclave.Sample(eid, block.bytes)
          Block(sampledBlock)
        }.collect)
      }
      // Find range boundaries parceled out to a single worker
      val boundaries = time("non-oblivious sort - FindRangeBounds") {
        childRDD.context.parallelize(Array(sampled.bytes), 1).map { sampledBytes =>
          val (enclave, eid) = Utils.initEnclave()
          enclave.FindRangeBounds(eid, orderSer, numPartitions, sampledBytes)
        }.collect.head
      }
      // Broadcast the range boundaries and use them to partition the input
      // Shuffle the input to achieve range partitioning and sort locally
      val result = childRDD.flatMap { block =>
        val (enclave, eid) = Utils.initEnclave()
        val partitions = enclave.PartitionForSort(
          eid, orderSer, numPartitions, block.bytes, boundaries)
        partitions.zipWithIndex.map {
          case (partition, i) => (i, Block(partition))
        }
      }.groupByKey(numPartitions).map {
        case (i, blocks) =>
          Utils.concatEncryptedBlocks(blocks.toSeq)
      }
      result
    }
  }

  def localSort(childRDD: RDD[Block], orderSer: Array[Byte]): RDD[Block] = {
    Utils.ensureCached(childRDD)
    val result = childRDD.map { block =>
      val (enclave, eid) = Utils.initEnclave()
      val sortedRows = enclave.ExternalSort(eid, orderSer, block.bytes)
      Block(sortedRows)
    }
    result
  }
}
