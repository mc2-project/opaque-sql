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

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.JobVerificationEngine
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan

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

case class EncryptedLocalTableScanExec(
    output: Seq[Attribute],
    plaintextData: Seq[InternalRow])
  extends LeafExecNode with OpaqueOperatorExec {

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
        Utils.encryptInternalRowsFlatbuffers(slice, output.map(_.dataType), useEnclave = false))

    // Make an RDD from the encrypted partitions
    sqlContext.sparkContext.parallelize(encryptedPartitions)
  }
}

case class EncryptExec(child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): RDD[Block] = {
    child.execute().mapPartitions { rowIter =>
      Iterator(Utils.encryptInternalRowsFlatbuffers(
        rowIter.toSeq, output.map(_.dataType), useEnclave = true))
    }
  }
}

case class EncryptedBlockRDDScanExec(
    output: Seq[Attribute],
    rdd: RDD[Block])
  extends LeafExecNode with OpaqueOperatorExec {

  override def executeBlocked(): RDD[Block] = rdd
}

case class Block(bytes: Array[Byte]) extends Serializable

trait OpaqueOperatorExec extends SparkPlan {
  def executeBlocked(): RDD[Block]

  def timeOperator[A](childRDD: RDD[A], desc: String)(f: RDD[A] => RDD[Block]): RDD[Block] = {
    import Utils.time
    Utils.ensureCached(childRDD)
    time(s"Force child of $desc") { childRDD.count }
    time(desc) {
      val result = f(childRDD)
      Utils.ensureCached(result)
      result.count
      result
    }
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

  override def executeCollect(): Array[InternalRow] = {

    val collectedRDD = executeBlocked().collect()
    collectedRDD.map { block =>
        Utils.addBlockForVerification(block)
    }

    val postVerificationPasses = Utils.verifyJob()
    JobVerificationEngine.resetForNextJob()
    if (postVerificationPasses) {
      collectedRDD.flatMap { block =>
        Utils.decryptBlockFlatbuffers(block)
      }
    } else {
      throw new Exception("Post Verification Failed")
    }
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    if (n == 0) {
      return new Array[InternalRow](0)
    }

    val childRDD = executeBlocked()

    val buf = new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * n * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val sc = sqlContext.sparkContext
      val res = sc.runJob(childRDD,
        (it: Iterator[Block]) => if (it.hasNext) Some(it.next()) else None, p)

      // TODO: take currently doesn't do post verification
      
      res.foreach {
        case Some(block) =>
          buf ++= Utils.decryptBlockFlatbuffers(block)
        case None =>
      }

      partsScanned += p.size
    }

    if (buf.size > n) {
      buf.take(n).toArray
    } else {
      buf.toArray
    }
  }
}

case class EncryptedProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def executeBlocked(): RDD[Block] = {
    val projectListSer = Utils.serializeProjectList(projectList, child.output)
    timeOperator(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), "EncryptedProjectExec") {
      childRDD => 
        JobVerificationEngine.addExpectedOperator("EncryptedProjectExec")
        childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.Project(eid, projectListSer, block.bytes))
      }
    }
  }
}


case class EncryptedFilterExec(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): RDD[Block] = {
    val conditionSer = Utils.serializeFilterExpression(condition, child.output)
    timeOperator(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), "EncryptedFilterExec") {
      childRDD => 
        JobVerificationEngine.addExpectedOperator("EncryptedFilterExec")
        childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.Filter(eid, conditionSer, block.bytes))
      }
    }
  }
}

case class EncryptedAggregateExec(
  groupingExpressions: Seq[NamedExpression],
  aggExpressions: Seq[AggregateExpression],
  mode: AggregateMode,
  child: SparkPlan)
    extends UnaryExecNode with OpaqueOperatorExec {

  override def producedAttributes: AttributeSet =
    AttributeSet(aggExpressions) -- AttributeSet(groupingExpressions)

  override def output: Seq[Attribute] = mode match {
    case Partial => groupingExpressions.map(_.toAttribute) ++ aggExpressions.map(_.copy(mode = Partial)).flatMap(_.aggregateFunction.inputAggBufferAttributes)
    case Final => groupingExpressions.map(_.toAttribute) ++ aggExpressions.map(_.resultAttribute)
    case Complete => groupingExpressions.map(_.toAttribute) ++ aggExpressions.map(_.resultAttribute)
  }

  override def executeBlocked(): RDD[Block] = {

    val (groupingExprs, aggExprs) = mode match {
      case Partial => {
        val partialAggExpressions = aggExpressions.map(_.copy(mode = Partial))
        (groupingExpressions, partialAggExpressions)
      }
      case Final => {
        val finalGroupingExpressions = groupingExpressions.map(_.toAttribute)
        val finalAggExpressions = aggExpressions.map(_.copy(mode = Final))
        (finalGroupingExpressions, finalAggExpressions)
      }
      case Complete => {
        (groupingExpressions, aggExpressions.map(_.copy(mode = Complete)))
      }
    }

     val aggExprSer = Utils.serializeAggOp(groupingExprs, aggExprs, child.output)

    timeOperator(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), "EncryptedPartialAggregateExec") {
      childRDD => 
        JobVerificationEngine.addExpectedOperator("EncryptedAggregateExec")
        childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.NonObliviousAggregate(eid, aggExprSer, block.bytes, (mode == Partial)))
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
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def executeBlocked(): RDD[Block] = {
    val joinExprSer = Utils.serializeJoinExpression(
      joinType, leftKeys, rightKeys, leftSchema, rightSchema)

    timeOperator(
      child.asInstanceOf[OpaqueOperatorExec].executeBlocked(),
      "EncryptedSortMergeJoinExec") { childRDD =>

      JobVerificationEngine.addExpectedOperator("EncryptedSortMergeJoinExec")
      val lastPrimaryRows = childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.ScanCollectLastPrimary(eid, joinExprSer, block.bytes))
      }.collect

      var shifted = Array[Block]()
      // if (childRDD.getNumPartitions == 1) {
      //   val lastLastPrimaryRow = lastPrimaryRows.last
      //   shifted = Utils.emptyBlock(lastLastPrimaryRow) +: lastPrimaryRows.dropRight(1)
      // } else {
      shifted = Utils.emptyBlock +: lastPrimaryRows.dropRight(1)
      // }
      assert(shifted.size == childRDD.partitions.length)
      val processedJoinRowsRDD =
        sparkContext.parallelize(shifted, childRDD.partitions.length)

      childRDD.zipPartitions(processedJoinRowsRDD) { (blockIter, joinRowIter) =>
        (blockIter.toSeq, joinRowIter.toSeq) match {
          case (Seq(block), Seq(joinRow)) =>
            val (enclave, eid) = Utils.initEnclave()
            Iterator(Block(enclave.NonObliviousSortMergeJoin(
              eid, joinExprSer, block.bytes, joinRow.bytes)))
        }
      }
    }
  }
}

case class EncryptedUnionExec(
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryExecNode with OpaqueOperatorExec {
  import Utils.time

  override def output: Seq[Attribute] =
    left.output

  override def executeBlocked(): RDD[Block] = {
    var leftRDD = left.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    var rightRDD = right.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    Utils.ensureCached(leftRDD)
    time("Force left child of EncryptedUnionExec") { leftRDD.count }
    Utils.ensureCached(rightRDD)
    time("Force right child of EncryptedUnionExec") { rightRDD.count }

    val num_left_partitions = leftRDD.getNumPartitions
    val num_right_partitions = rightRDD.getNumPartitions
    if (num_left_partitions != num_right_partitions) {
      if (num_left_partitions > num_right_partitions) {
        leftRDD = leftRDD.coalesce(num_right_partitions)
      } else {
        rightRDD = rightRDD.coalesce(num_left_partitions)
      }
    }
    val unioned = leftRDD.zipPartitions(rightRDD) {
      (leftBlockIter, rightBlockIter) =>
        Iterator(Utils.concatEncryptedBlocks(leftBlockIter.toSeq ++ rightBlockIter.toSeq))
    }
    Utils.ensureCached(unioned)
    time("EncryptedUnionExec") { unioned.count }
    unioned
  }
}

case class EncryptedLocalLimitExec(
    limit: Int,
    child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {
  import Utils.time

  override def output: Seq[Attribute] =
    child.output

  override def executeBlocked(): RDD[Block] = {
    timeOperator(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), "EncryptedLocalLimitExec") { childRDD =>

      JobVerificationEngine.addExpectedOperator("EncryptedLocalLimitExec")
      childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.LocalLimit(eid, limit, block.bytes))
      }
    }
  }
}

case class EncryptedGlobalLimitExec(
    limit: Int,
    child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {
  import Utils.time

  override def output: Seq[Attribute] =
    child.output

  override def executeBlocked(): RDD[Block] = {
    timeOperator(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), "EncryptedGlobalLimitExec") { childRDD =>

      JobVerificationEngine.addExpectedOperator("EncryptedGlobalLimitExec")
      val numRowsPerPartition = Utils.concatEncryptedBlocks(childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        Block(enclave.CountRowsPerPartition(eid, block.bytes))
      }.collect)

      val limitPerPartition = childRDD.context.parallelize(Array(numRowsPerPartition.bytes), 1).map { numRowsList =>
        val (enclave, eid) = Utils.initEnclave()
        enclave.ComputeNumRowsPerPartition(eid, limit, numRowsList)
      }.collect.head

      childRDD.zipWithIndex.map {
        case (block, i) => {
          val (enclave, eid) = Utils.initEnclave()
          Block(enclave.LimitReturnRows(eid, i, limitPerPartition, block.bytes))
        }
      }
    }
  }
}
