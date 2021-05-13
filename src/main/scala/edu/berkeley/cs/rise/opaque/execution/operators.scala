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

import scala.collection.mutable.ArrayBuffer

import java.io.{File, IOException, FileNotFoundException}
import java.nio.file.Files
import java.nio.file.Paths

import edu.berkeley.cs.rise.opaque.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.execution.SparkPlan

import edu.berkeley.cs.rise.opaque.tuix
import java.nio.ByteBuffer

trait LeafExecNode extends SparkPlan {
  override final def children: Seq[SparkPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

trait UnaryExecNode extends SparkPlan {
  def child: SparkPlan

  override final def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

trait BinaryExecNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override final def children: Seq[SparkPlan] = Seq(left, right)
}

case class EncryptedLocalTableScanExec(output: Seq[Attribute], plaintextData: Seq[InternalRow])
    extends LeafExecNode
    with OpaqueOperatorExec {

  override def name = "EncryptedLocalTableScanExec"

  private val unsafeRows: Array[InternalRow] = {
    val proj = UnsafeProjection.create(output, output)
    val result: Array[InternalRow] = plaintextData.map(r => proj(r).copy()).toArray
    result
  }

  override def executeBlocked(): RDD[Block] = {
    // Locally partition plaintextData using the same logic as ParallelCollectionRDD.slice
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
    val slicedPlaintextData: Seq[Seq[InternalRow]] =
      positions(unsafeRows.length, sqlContext.sparkContext.defaultParallelism).map {
        case (start, end) => unsafeRows.slice(start, end).toSeq
      }.toSeq

    // Encrypt each local partition
    val encryptedPartitions: Seq[Block] =
      slicedPlaintextData.map(slice =>
        Utils.encryptInternalRowsFlatbuffers(slice, output.map(_.dataType), useEnclave = false)
      )

    // Make an RDD from the encrypted partitions
    sqlContext.sparkContext.parallelize(encryptedPartitions)
  }
}

case class EncryptExec(child: SparkPlan) extends UnaryExecNode with OpaqueOperatorExec {

  override def name = "EncryptExec"

  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): RDD[Block] = {
    child.execute().mapPartitions { rowIter =>
      Iterator(
        Utils.encryptInternalRowsFlatbuffers(
          rowIter.toSeq,
          output.map(_.dataType),
          useEnclave = true
        )
      )
    }
  }
}

case class EncryptedAddDummyRowExec(output: Seq[Attribute], child: SparkPlan)
    extends UnaryExecNode
    with OpaqueOperatorExec {

  override def name = "EncryptedAddDummyRowExec"

  // Add a dummy row full of nulls to each partition of child
  override def executeBlocked(): RDD[Block] = {
    val childRDD = child.asInstanceOf[OpaqueOperatorExec].executeBlocked()

    childRDD.mapPartitions { rowIter =>
      val nullRowsBlock = Utils.encryptInternalRowsFlatbuffers(
        Seq(InternalRow.fromSeq(Seq.fill(output.length)(null))),
        output.map(_.dataType),
        useEnclave = true,
        isDummyRows = true
      )
      Iterator(Utils.concatEncryptedBlocks(nullRowsBlock +: rowIter.toSeq))
    }
  }
}

case class EncryptedBlockRDDScanExec(output: Seq[Attribute], rdd: RDD[Block])
    extends LeafExecNode
    with OpaqueOperatorExec {

  override def name = "EncryptedBlockRDDScanExec"

  override def executeBlocked(): RDD[Block] = rdd
}

case class Block(bytes: Array[Byte]) extends Serializable

trait OpaqueOperatorExec extends SparkPlan {
  def name: String

  def executeBlocked(): RDD[Block]

  /* Used for performance debugging individual operators. */
  def timeOperator[A](childRDD: RDD[A])(f: RDD[A] => RDD[Block]): RDD[Block] = {
    import Utils.time
    Utils.ensureCached(childRDD)
    childRDD.count
    time(name) {
      val result = f(childRDD)
      Utils.ensureCached(result)
      result.count
      result.unpersist(blocking = true)
      result
    }
  }

  def applyLoggingLevel[A](childRDD: RDD[A])(f: RDD[A] => RDD[Block]): RDD[Block] = {
    if (Utils.getOperatorLoggingLevel()) timeOperator(childRDD)(f) else f(childRDD)
  }

  /**
   * An Opaque operator cannot return plaintext rows, so this method should normally not be invoked.
   * Instead use executeBlocked, which returns the data as encrypted blocks.
   *
   * However, when encrypted data is cached, Spark SQL's InMemoryRelation attempts to call this
   * method and persist the resulting RDD. [[ConvertToOpaqueOperators]] later eliminates the dummy
   * relation from the logical plan, but this only happens after InMemoryRelation has called this
   * method. We therefore have to silently return an empty RDD here.
   */

  override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.emptyRDD
    // throw new UnsupportedOperationException("use executeBlocked")
  }

  def collectEncrypted(): Array[Block] = {
    executeBlocked().collect
  }

  /** 
   * Write encrypted results to file and return an empty rdd
   * 
   * 1. Creates an empty directory for the dataframe (or if the directory already exists, removes all contents
   * inside the directory)
   * 2. For each block in the result:
   * 2a. Create a file
   * 2b. Write the byte contents of the block to the file
   */
  override def executeCollect(): Array[InternalRow] = {

    val blocks = collectEncrypted()
    
    try {

      // 1
      val dir = new File("tmp");
      if (!dir.exists) {
        dir.mkdir()
      } else {
        for(file <- dir.listFiles()) { 
          file.delete() 
        }
      }

      // 2
      var i = 0
      for (block <- blocks) {
        // 2a & b
        val f = "tmp/tmp_" + i.toString
        Files.write(Paths.get(f), block.bytes)  
       
        // Increment i for next block
        i += 1
      }

    } catch {
      case x: FileNotFoundException =>
      {
        throw new FileNotFoundException("Exception: File missing")              
      }
      
      case x: IOException   => 
      {        
        throw new IOException("Input/output Exception")        
      }
    }

    sqlContext.sparkContext.emptyRDD.collect()
  }

  /* 
   * executeTake can only take in the closest block increment
   * This is because encrypted blocks must be saved to a file in block increments
   * Any additional parsing must be done externally.
   */
  override def executeTake(n: Int): Array[InternalRow] = {


    // Prepare directory for storing encrypted blocks
    try {
      val dir = new File("tmp");
      if (!dir.exists) { 
        dir.mkdir()
      } else {
        for(file <- dir.listFiles()) {
          file.delete()
        }
      }
    } catch {
      case x: FileNotFoundException =>
      {
        throw new FileNotFoundException("Exception: File missing")
      }

      case x: IOException   =>
      {
        throw new IOException("Input/output Exception")
      }
    }

    // Original code for parsing through blocks
    if (n == 0) {
      return new Array[InternalRow](0)
    }

    val childRDD = executeBlocked()

    var buf = 0
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    var i = 0
    while (buf < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        if (buf == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * n * partsScanned / buf).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry) // guard against negative num of partitions

      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val sc = sqlContext.sparkContext
      val res =
        sc.runJob(childRDD, (it: Iterator[Block]) => if (it.hasNext) Some(it.next()) else None, p)

      res.foreach {
        case Some(block) => {

          val blockBuffer = ByteBuffer.wrap(block.bytes)
          val encryptedBlocks = tuix.EncryptedBlocks.getRootAsEncryptedBlocks(blockBuffer)
          buf = buf + encryptedBlocks.blocksLength

          val f = "tmp/tmp_" + i.toString
          Files.write(Paths.get(f), block.bytes)

          // Increment i for next block
          i += 1

        }
        case None =>
      }

      partsScanned += p.size
    }

    sqlContext.sparkContext.emptyRDD.collect()
  }
}

case class EncryptedProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
    extends UnaryExecNode
    with OpaqueOperatorExec {

  override def name = "EncryptedProjectExec"

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def executeBlocked(): RDD[Block] = {
    val projectListSer = Utils.serializeProjectList(projectList, child.output)
    val childRDD = child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    applyLoggingLevel(childRDD) { childRDD =>
      childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.Project(eid, projectListSer, block.bytes))
      }
    }
  }
}

case class EncryptedFilterExec(condition: Expression, child: SparkPlan)
    extends UnaryExecNode
    with OpaqueOperatorExec {

  override def name = "EncryptedFilterExec"

  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): RDD[Block] = {

    val conditionSer = Utils.serializeFilterExpression(condition, child.output)
    val childRDD = child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    applyLoggingLevel(childRDD) { childRDD =>
      childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.Filter(eid, conditionSer, block.bytes))
      }
    }
  }
}

case class EncryptedAggregateExec(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    child: SparkPlan
) extends UnaryExecNode
    with OpaqueOperatorExec {

  override def name = "EncryptedAggregateExec"

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateExpressions) -- AttributeSet(groupingExpressions)

  override def output: Seq[Attribute] = groupingExpressions.map(_.toAttribute) ++
    aggregateExpressions.flatMap(expr => {
      expr.mode match {
        case Partial | PartialMerge =>
          expr.aggregateFunction.inputAggBufferAttributes
        case _ =>
          Seq(expr.resultAttribute)
      }
    })

  override def executeBlocked(): RDD[Block] = {

    val aggExprSer = Utils.serializeAggOp(groupingExpressions, aggregateExpressions, child.output)
    val isPartial = aggregateExpressions
      .map(expr => expr.mode)
      .exists(mode => mode == Partial || mode == PartialMerge)

    val childRDD = child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    applyLoggingLevel(childRDD) { childRDD =>
      childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.NonObliviousAggregate(eid, aggExprSer, block.bytes, isPartial))
      }
    }
  }
}

case class EncryptedSortMergeJoinExec(
    joinType: JoinType,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    leftSchema: Seq[Attribute],
    rightSchema: Seq[Attribute],
    condition: Option[Expression],
    child: SparkPlan
) extends UnaryExecNode
    with OpaqueOperatorExec {

  override def name = "EncryptedSortMergeJoinExec"

  override def output: Seq[Attribute] = {
    joinType match {
      case Inner =>
        (leftSchema ++ rightSchema).map(_.toAttribute)
      case LeftOuter =>
        leftSchema ++ rightSchema.map(_.withNullability(true))
      case LeftSemi | LeftAnti =>
        leftSchema.map(_.toAttribute)
      case RightOuter =>
        leftSchema.map(_.withNullability(true)) ++ rightSchema
      case FullOuter =>
        leftSchema.map(_.withNullability(true)) ++ rightSchema.map(_.withNullability(true))
      case _ =>
        throw new IllegalArgumentException(
          s"SortMergeJoin should not take $joinType as the JoinType"
        )
    }
  }

  override def executeBlocked(): RDD[Block] = {
    val joinExprSer = Utils.serializeJoinExpression(
      joinType,
      Some(leftKeys),
      Some(rightKeys),
      leftSchema,
      rightSchema,
      condition
    )

    val childRDD = child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    applyLoggingLevel(childRDD) { childRDD =>
      childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.NonObliviousSortMergeJoin(eid, joinExprSer, block.bytes))
      }
    }
  }
}

case class EncryptedBroadcastNestedLoopJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression]
) extends BinaryExecNode
    with OpaqueOperatorExec {

  override def name = "EncryptedBroadcastNestedLoopJoinExec"

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case LeftExistence(_) =>
        left.output
      case _ =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $joinType as the JoinType"
        )
    }
  }

  override def executeBlocked(): RDD[Block] = {
    val joinExprSer =
      Utils.serializeJoinExpression(joinType, None, None, left.output, right.output, condition)

    // We pick which side to broadcast/stream according to buildSide.
    // BuildRight means the right relation <=> the broadcast relation.
    // NOTE: outer_rows and inner_rows in C++ correspond to stream and broadcast side respectively.
    var (stream, broadcast) = buildSide match {
      case BuildRight =>
        (left, right)
      case BuildLeft =>
        (right, left)
    }

    broadcast = joinType match {
      // If outer join, then we need to add a dummy row to ensure that foreign schema is available to C++ code
      // in case of an empty foreign table.
      case LeftOuter | RightOuter | FullOuter =>
        EncryptedAddDummyRowExec(broadcast.output, broadcast)
      case _ =>
        broadcast
    }
    val streamRDD = stream.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    val broadcastRDD = broadcast.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    val broadcastBlock = Utils.concatEncryptedBlocks(broadcastRDD.collect)

    applyLoggingLevel(streamRDD) { streamRDD =>
      streamRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(
          enclave.BroadcastNestedLoopJoin(eid, joinExprSer, block.bytes, broadcastBlock.bytes)
        )
      }
    }
  }
}

case class EncryptedUnionExec(left: SparkPlan, right: SparkPlan)
    extends BinaryExecNode
    with OpaqueOperatorExec {

  override def name = "EncryptedUnionExec"

  override def output: Seq[Attribute] =
    left.output

  override def executeBlocked(): RDD[Block] = {
    var leftRDD = left.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    var rightRDD = right.asInstanceOf[OpaqueOperatorExec].executeBlocked()

    val num_left_partitions = leftRDD.getNumPartitions
    val num_right_partitions = rightRDD.getNumPartitions
    if (num_left_partitions != num_right_partitions) {
      if (num_left_partitions > num_right_partitions) {
        leftRDD = leftRDD.coalesce(num_right_partitions)
      } else {
        rightRDD = rightRDD.coalesce(num_left_partitions)
      }
    }
    applyLoggingLevel(leftRDD) { leftRDD =>
      leftRDD.zipPartitions(rightRDD) { (leftBlockIter, rightBlockIter) =>
        Iterator(Utils.concatEncryptedBlocks(leftBlockIter.toSeq ++ rightBlockIter.toSeq))
      }
    }
  }
}

case class EncryptedLocalLimitExec(limit: Int, child: SparkPlan)
    extends UnaryExecNode
    with OpaqueOperatorExec {

  override def name = "EncryptedLocalLimitExec"

  override def output: Seq[Attribute] =
    child.output

  override def executeBlocked(): RDD[Block] = {
    val childRDD = child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    applyLoggingLevel(childRDD) { childRDD =>
      childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.LocalLimit(eid, limit, block.bytes))
      }
    }
  }
}

case class EncryptedGlobalLimitExec(limit: Int, child: SparkPlan)
    extends UnaryExecNode
    with OpaqueOperatorExec {

  override def name = "EncryptedGlobalLimitExec"

  override def output: Seq[Attribute] =
    child.output

  override def executeBlocked(): RDD[Block] = {
    val childRDD = child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    val numRowsPerPartition = Utils.concatEncryptedBlocks(childRDD.map { block =>
      val (enclave, eid) = Utils.initEnclave()
      Block(enclave.CountRowsPerPartition(eid, block.bytes))
    }.collect)

    val limitPerPartition = childRDD.context
      .parallelize(Array(numRowsPerPartition.bytes), 1)
      .map { numRowsList =>
        val (enclave, eid) = Utils.initEnclave()
        enclave.ComputeNumRowsPerPartition(eid, limit, numRowsList)
      }
      .collect
      .head

    applyLoggingLevel(childRDD) { childRDD =>
      childRDD.zipWithIndex.map {
        case (block, i) => {
          val (enclave, eid) = Utils.initEnclave()
          Block(enclave.LimitReturnRows(eid, i, limitPerPartition, block.bytes))
        }
      }
    }
  }
}
