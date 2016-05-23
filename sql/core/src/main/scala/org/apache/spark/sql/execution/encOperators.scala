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

package org.apache.spark.sql.execution

import scala.math.Ordering
import scala.reflect.classTag

import oblivious_sort.ObliviousSort
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.MutableInteger
import org.apache.spark.sql.QED
import org.apache.spark.sql.QEDOpcode
import org.apache.spark.sql.QEDOpcode._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection

import org.apache.spark.sql.execution.metric.SQLMetrics

case class Block(bytes: Array[Byte], numRows: Int) extends Serializable

trait OutputsBlocks extends SparkPlan {
  override def doExecute() = throw new UnsupportedOperationException("use executeBlocked")
  def executeBlocked(): RDD[Block]
}

case class ConvertToBlocks(child: SparkPlan)
  extends UnaryNode with OutputsBlocks {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked() = {
    child.execute().mapPartitions { rowIter =>
      val serRows = rowIter.map(_.encSerialize).toArray
      Iterator(Block(QED.concatByteArrays(serRows), serRows.length))
    }
  }
}

case class ConvertFromBlocks(child: OutputsBlocks)
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override def doExecute() = {
    child.executeBlocked().flatMap { block =>
      val converter = UnsafeProjection.create(schema)
      QED.parseRows(block.bytes).map(serRow => converter(InternalRow.fromSeq(serRow)))
    }
  }
}

case class EncProject(projectList: Seq[NamedExpression], opcode: QEDOpcode, child: OutputsBlocks)
  extends UnaryNode with OutputsBlocks {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def executeBlocked() = child.executeBlocked().map { block =>
    val (enclave, eid) = QED.initEnclave()
    val serResult = enclave.Project(eid, opcode.value, block.bytes, block.numRows)
    Block(serResult, block.numRows)
  }
}

case class EncFilter(condition: Expression, opcode: QEDOpcode, child: OutputsBlocks)
  extends UnaryNode with OutputsBlocks {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked() = {
    opcode match {
      case OP_BD1 =>
        // BD1: pageRank > 1000, where pageRank is the second attribute
        assert(condition.references == AttributeSet(child.output(1)))
      case _ => {}
    }
    child.executeBlocked().map { block =>
      val (enclave, eid) = QED.initEnclave()
      val numOutputRows = new MutableInteger
      val filtered = enclave.Filter(eid, opcode.value, block.bytes, block.numRows, numOutputRows)
      Block(filtered, numOutputRows.value)
    }
  }
}

case class Permute(child: OutputsBlocks) extends UnaryNode with OutputsBlocks {
  override def output: Seq[Attribute] = child.output

  override def executeBlocked() = {
    val rowsWithRandomIds = child.executeBlocked().map { block =>
      val (enclave, eid) = QED.initEnclave()
      val serResult = enclave.Project(
        eid, OP_PROJECT_ADD_RANDOM_ID.value, block.bytes, block.numRows)
      Block(serResult, block.numRows)
    }
    ObliviousSort.sortBlocks(rowsWithRandomIds, OP_SORT_COL1).map { block =>
      val (enclave, eid) = QED.initEnclave()
      val serResult = enclave.Project(eid, OP_PROJECT_DROP_COL1.value, block.bytes, block.numRows)
      Block(serResult, block.numRows)
    }
  }
}

case class EncSort(sortExpr: Expression, child: OutputsBlocks)
  extends UnaryNode with OutputsBlocks {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked() = {
    val sortAttrPos = QED.attributeIndexOf(sortExpr.references.toSeq(0), child.output)
    val opcode = sortAttrPos match {
      case 0 => OP_SORT_COL1
      case 1 => OP_SORT_COL2
    }
    ObliviousSort.sortBlocks(child.executeBlocked(), opcode)
  }
}

case class EncAggregate(
    opcode: QEDOpcode,
    groupingExpression: NamedExpression,
    aggExpressions: Seq[NamedExpression],
    aggOutputs: Seq[Attribute],
    output: Seq[Attribute],
    child: OutputsBlocks)
  extends UnaryNode with OutputsBlocks {

  import QED.time

  override def executeBlocked(): RDD[Block] = {
    val groupingExprPos = QED.attributeIndexOf(groupingExpression.references.toSeq(0), child.output)
    val aggExprsPos = aggExpressions.map(expr => QED.attributeIndexOf(expr.references.toSeq(0), child.output)).toList
    val (aggStep1Opcode, aggStep2Opcode, aggDummySortOpcode, aggDummyFilterOpcode) =
      (opcode, child.output.size, groupingExprPos, aggExprsPos) match {
        case (OP_GROUPBY_COL1_SUM_COL2_INT_STEP1, 2, 0, List(1)) =>
          (OP_GROUPBY_COL1_SUM_COL2_INT_STEP1,
            OP_GROUPBY_COL1_SUM_COL2_INT_STEP2,
            OP_SORT_COL2_IS_DUMMY_COL1,
            OP_FILTER_NOT_DUMMY)
        case (OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP1, 2, 0, List(1)) =>
          (OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP1,
            OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP2,
            OP_SORT_COL2_IS_DUMMY_COL1,
            OP_FILTER_NOT_DUMMY)
        case (OP_GROUPBY_COL2_SUM_COL3_INT_STEP1, 3, 1, List(2)) =>
          (OP_GROUPBY_COL2_SUM_COL3_INT_STEP1,
            OP_GROUPBY_COL2_SUM_COL3_INT_STEP2,
            OP_SORT_COL2_IS_DUMMY_COL1,
            OP_FILTER_NOT_DUMMY)
        case (OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP1, 3, 0, List(1, 2)) =>
          (OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP1,
            OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP2,
            OP_SORT_COL2_IS_DUMMY_COL1,
            OP_FILTER_NOT_DUMMY)
      }

    val childRDD = child.executeBlocked().cache()
    time("aggregate - force child") { childRDD.count }
    // Process boundaries
    val boundaries = childRDD.map { block =>
      val (enclave, eid) = QED.initEnclave()
      val boundary = enclave.AggregateStep1(
        eid, aggStep1Opcode.value, block.bytes, block.numRows)
      // enclave.StopEnclave(eid)
      boundary
    }

    val boundariesCollected = time("aggregate - step 1") { boundaries.collect }
    if (boundariesCollected.forall(_.isEmpty)) {
      return sqlContext.sparkContext.emptyRDD[Block]
    }
    val (enclave, eid) = QED.initEnclave()
    val processedBoundariesConcat = time("aggregate - ProcessBoundary") {
      enclave.ProcessBoundary(
        eid, aggStep1Opcode.value,
        QED.concatByteArrays(boundariesCollected), boundariesCollected.length)
    }

    // Send processed boundaries to partitions and generate a mix of partial and final aggregates
    val processedBoundaries = QED.splitBytes(processedBoundariesConcat, boundariesCollected.length)
    val processedBoundariesRDD = sparkContext.parallelize(processedBoundaries, childRDD.partitions.length)
    val partialAggregates = childRDD.zipPartitions(processedBoundariesRDD) {
      (blockIter, boundaryIter) =>
        val blockArray = blockIter.toArray
        assert(blockArray.length == 1)
        val block = blockArray.head
        val boundaryArray = boundaryIter.toArray
        assert(boundaryArray.length == 1)
        val boundaryRecord = boundaryArray.head
        val (enclave, eid) = QED.initEnclave()
        assert(block.numRows > 0)
        val partialAgg = enclave.AggregateStep2(
          eid, aggStep2Opcode.value, block.bytes, block.numRows, boundaryRecord)
        assert(partialAgg.nonEmpty,
          s"enclave.AggregateStep2($eid, $aggStep2Opcode, ${block.bytes.length}, ${block.numRows}, ${boundaryRecord.length}) returned empty result")
        Iterator(Block(partialAgg, block.numRows))
    }

    time("aggregate - step 2") { partialAggregates.cache.count }

    // Sort the partial and final aggregates using a comparator that causes final aggregates to come first
    val sortedAggregates = time("aggregate - sort dummies") {
      val result = ObliviousSort.sortBlocks(partialAggregates, aggDummySortOpcode)
      result.cache.count
      result
    }

    // Filter out the non-final aggregates
    val finalAggregates = time("aggregate - filter out dummies") {
      val result = sortedAggregates.map { block =>
        val (enclave, eid) = QED.initEnclave()
        val numOutputRows = new MutableInteger
        val filtered = enclave.Filter(
          eid, aggDummyFilterOpcode.value, block.bytes, block.numRows, numOutputRows)
        Block(filtered, numOutputRows.value)
      }
      result.cache.count
      result
    }
    finalAggregates
  }
}

case class EncSortMergeJoin(
    left: OutputsBlocks,
    right: OutputsBlocks,
    leftCol: Expression,
    rightCol: Expression,
    opcode: Option[QEDOpcode])
  extends BinaryNode with OutputsBlocks {

  import QED.time

  override def output: Seq[Attribute] =
    left.output ++ right.output.filter(a => !rightCol.references.contains(a))

  override def executeBlocked() = {
    val leftColPos = QED.attributeIndexOf(leftCol.references.toSeq(0), left.output)
    val rightColPos = QED.attributeIndexOf(rightCol.references.toSeq(0), right.output)
    val (joinOpcode, dummySortOpcode, dummyFilterOpcode) =
      ((left.output.size, right.output.size, leftColPos, rightColPos, opcode): @unchecked) match {
        case (2, 3, 0, 0, None) =>
          (OP_JOIN_COL1, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)
        case (2, 3, 0, 0, Some(OP_JOIN_PAGERANK)) =>
          (OP_JOIN_PAGERANK, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)
        case (3, 3, 1, 1, None) =>
          (OP_JOIN_COL2, OP_SORT_COL4_IS_DUMMY_COL2, OP_FILTER_NOT_DUMMY)
      }

    val leftRDD = left.executeBlocked()
    val rightRDD = right.executeBlocked()
    time("Force left child of EncSortMergeJoin") { leftRDD.cache.count }
    time("Force right child of EncSortMergeJoin") { rightRDD.cache.count }

    val processed = leftRDD.zipPartitions(rightRDD) { (leftBlockIter, rightBlockIter) =>
      val (enclave, eid) = QED.initEnclave()

      val leftBlockArray = leftBlockIter.toArray
      assert(leftBlockArray.length == 1)
      val leftBlock = leftBlockArray.head
      val leftProcessed = enclave.JoinSortPreprocess(
        eid, joinOpcode.value, QED.primaryTableId,
        leftBlock.bytes, leftBlock.numRows)

      val rightBlockArray = rightBlockIter.toArray
      assert(rightBlockArray.length == 1)
      val rightBlock = rightBlockArray.head
      val rightProcessed = enclave.JoinSortPreprocess(
        eid, joinOpcode.value, QED.foreignTableId,
        rightBlock.bytes, rightBlock.numRows)

      Iterator(Block(QED.concatByteArrays(Array(leftProcessed, rightProcessed)),
        leftBlock.numRows + rightBlock.numRows))
    }
    time("join - preprocess") { processed.cache.count }

    val sorted = time("join - sort") {
      val result = ObliviousSort.sortBlocks(processed, joinOpcode).cache()
      result.count
      result
    }

    val lastPrimaryRows = sorted.map { block =>
      val (enclave, eid) = QED.initEnclave()
      enclave.ScanCollectLastPrimary(eid, joinOpcode.value, block.bytes, block.numRows)
    }

    val lastPrimaryRowsCollected = time("join - collect last primary") { lastPrimaryRows.collect }
    val (enclave, eid) = QED.initEnclave()
    val processedJoinRows = time("join - process boundary") {
      enclave.ProcessJoinBoundary(
        eid, joinOpcode.value, QED.concatByteArrays(lastPrimaryRowsCollected),
        lastPrimaryRowsCollected.length)
    }

    val processedJoinRowsRDD =
      sparkContext.parallelize(QED.splitBytes(processedJoinRows, lastPrimaryRowsCollected.length),
        sorted.partitions.length)

    val joined = sorted.zipPartitions(processedJoinRowsRDD) { (blockIter, joinRowIter) =>
      val block = blockIter.next()
      assert(!blockIter.hasNext)
      val joinRow = joinRowIter.next()
      assert(!joinRowIter.hasNext)
      val (enclave, eid) = QED.initEnclave()
      val joined = enclave.SortMergeJoin(
        eid, joinOpcode.value, block.bytes, block.numRows, joinRow)
      Iterator(Block(joined, block.numRows))
    }
    time("join - sort merge join") { joined.cache.count }

    // TODO: permute first, otherwise this is insecure
    val nonDummy = joined.map { block =>
      val (enclave, eid) = QED.initEnclave()
      val numOutputRows = new MutableInteger
      val filtered = enclave.Filter(
        eid, dummyFilterOpcode.value, block.bytes, block.numRows, numOutputRows)
      Block(filtered, numOutputRows.value)
    }
    time("join - filter dummies") { nonDummy.cache.count }
    nonDummy
  }
}
