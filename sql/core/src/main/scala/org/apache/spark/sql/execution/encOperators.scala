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

case class EncProject(projectList: Seq[NamedExpression], opcode: QEDOpcode, child: SparkPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def doExecute() = child.execute().mapPartitions { iter =>
    val (enclave, eid) = QED.initEnclave()
    val rows = iter.map(_.encSerialize).toArray
    val serResult = enclave.Project(eid, opcode.value, QED.concatByteArrays(rows), rows.length)
    val converter = UnsafeProjection.create(schema)
    val r = QED.parseRows(serResult).toArray
    r.iterator.map(fields => converter(InternalRow.fromSeq(fields)))
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

case class EncFilter(condition: Expression, opcode: QEDOpcode, child: SparkPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override def doExecute() = {
    opcode match {
      case OP_BD1 =>
        // BD1: pageRank > 1000, where pageRank is the second attribute
        assert(condition.references == AttributeSet(child.output(1)))
      case _ => {}
    }
    child.execute().mapPartitions { iter =>
      val (enclave, eid) = QED.initEnclave()
      iter.filter(rowSer => enclave.Filter(eid, opcode.value, rowSer.encSerialize))
    }
  }
}

case class Permute(child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def doExecute() = {
    val childRDD = child.execute().mapPartitions { rowIter =>
      val (enclave, eid) = QED.initEnclave()
      rowIter.map(row =>
        InternalRow.fromSeq(QED.randomId(enclave, eid) +: row.toSeq(schema)).encSerialize)
    }
    ObliviousSort.ColumnSort(childRDD.context, childRDD, OP_SORT_COL1).mapPartitions { serRowIter =>
      val converter = UnsafeProjection.create(schema)
      serRowIter.map(serRow => converter(
        InternalRow.fromSeq(QED.parseRow(serRow).tail)))
    }
  }
}

case class EncSort(sortExpr: Expression, child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def doExecute() = {
    val childRDD = child.execute().map(_.encSerialize)
    val sortAttrPos = QED.attributeIndexOf(sortExpr.references.toSeq(0), child.output)
    val opcode = sortAttrPos match {
      case 0 => OP_SORT_COL1
      case 1 => OP_SORT_COL2
    }
    ObliviousSort.ColumnSort(childRDD.context, childRDD, opcode).mapPartitions { serRowIter =>
      val converter = UnsafeProjection.create(schema)
      serRowIter.map(serRow => converter(InternalRow.fromSeq(QED.parseRow(serRow))))
    }
  }
}

case class EncAggregate(
    opcode: QEDOpcode,
    groupingExpression: NamedExpression,
    aggExpressions: Seq[NamedExpression],
    aggOutputs: Seq[Attribute],
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryNode {

  import QED.time

  override def doExecute(): RDD[InternalRow] = {
    val groupingExprPos = QED.attributeIndexOf(groupingExpression.references.toSeq(0), child.output)
    val aggExprsPos = aggExpressions.map(expr => QED.attributeIndexOf(expr.references.toSeq(0), child.output)).toList
    val (aggStep1Opcode, aggStep2Opcode, aggDummySortOpcode, aggDummyFilterOpcode) =
      (opcode, child.output.size, groupingExprPos, aggExprsPos) match {
        case (OP_GROUPBY_COL1_SUM_COL2_INT_STEP1, 2, 0, List(1)) =>
          (OP_GROUPBY_COL1_SUM_COL2_INT_STEP1,
            OP_GROUPBY_COL1_SUM_COL2_INT_STEP2,
            OP_SORT_COL2_IS_DUMMY_COL1,
            OP_FILTER_COL2_NOT_DUMMY)
        case (OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP1, 2, 0, List(1)) =>
          (OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP1,
            OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP2,
            OP_SORT_COL2_IS_DUMMY_COL1,
            OP_FILTER_COL2_NOT_DUMMY)
        case (OP_GROUPBY_COL2_SUM_COL3_INT_STEP1, 3, 1, List(2)) =>
          (OP_GROUPBY_COL2_SUM_COL3_INT_STEP1,
            OP_GROUPBY_COL2_SUM_COL3_INT_STEP2,
            OP_SORT_COL2_IS_DUMMY_COL1,
            OP_FILTER_COL2_NOT_DUMMY)
        case (OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP1, 3, 0, List(1, 2)) =>
          (OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP1,
            OP_GROUPBY_COL1_AVG_COL2_INT_SUM_COL3_FLOAT_STEP2,
            OP_SORT_COL2_IS_DUMMY_COL1,
            OP_FILTER_COL2_NOT_DUMMY)
      }

    val childRDD = child.execute().mapPartitions { rowIter =>
      rowIter.map(_.encSerialize)
    }.cache()
    time("Force child of EncAggregate") { childRDD.count }
    // Process boundaries
    val boundaries = childRDD.mapPartitions { rowIter =>
      val rows = rowIter.toArray
      val concatRows = QED.concatByteArrays(rows)
      val (enclave, eid) = QED.initEnclave()
      val aggSize = 4 + 12 + 16 + 4 + 4 + 2048 + 128
      val boundary = time("aggregate - step 1 - JNI call") {
        enclave.AggregateStep1(
          eid, aggStep1Opcode.value, concatRows, rows.length)
      }
      // enclave.StopEnclave(eid)
      Iterator(boundary)
    }

    val boundariesCollected = time("aggregate - step 1") { boundaries.collect }
    if (boundariesCollected.forall(_.isEmpty)) {
      return sqlContext.sparkContext.emptyRDD[InternalRow]
    }
    val (enclave, eid) = QED.initEnclave()
    val processedBoundariesConcat = time("aggregate - ProcessBoundary") {
      enclave.ProcessBoundary(
        eid, aggStep1Opcode.value,
        QED.concatByteArrays(boundariesCollected), boundariesCollected.length)
    }
    // enclave.StopEnclave(eid)

    // Send processed boundaries to partitions and generate a mix of partial and final aggregates
    val processedBoundaries = QED.splitBytes(processedBoundariesConcat, boundariesCollected.length)
    val processedBoundariesRDD = sparkContext.parallelize(processedBoundaries, childRDD.partitions.length)
    val partialAggregates = childRDD.zipPartitions(processedBoundariesRDD) {
      (rowIter, boundaryIter) =>
        val rows = rowIter.toArray
        val concatRows = QED.concatByteArrays(rows)
        val aggSize = 4 + 12 + 16 + 4 + 4 + 2048 + 128
        val boundaryArray = boundaryIter.toArray
        assert(boundaryArray.length == 1)
        val boundaryRecord = boundaryArray.head
        assert(boundaryRecord.length >= aggSize)
        val (enclave, eid) = QED.initEnclave()
        assert(rows.length > 0)
        val partialAgg = enclave.AggregateStep2(
          eid, aggStep2Opcode.value, concatRows, rows.length, boundaryRecord)
        assert(partialAgg.nonEmpty,
          s"enclave.AggregateStep2($eid, $aggStep2Opcode, ${concatRows.length}, ${rows.length}, ${boundaryRecord.length}) returned empty result given input starting with ${concatRows.slice(0, 16).toList}")
        // enclave.StopEnclave(eid)
        QED.readRows(partialAgg)
    }.cache()

    time("aggregate - step 2") { partialAggregates.count }

    // Sort the partial and final aggregates using a comparator that causes final aggregates to come first
    val sortedAggregates = time("aggregate - sort dummies") {
      val result = ObliviousSort.ColumnSort(
        partialAggregates.context, partialAggregates, aggDummySortOpcode)
      result.cache.count
      result
    }

    // Filter out the non-final aggregates
    val finalAggregates = time("aggregate - filter out dummies") {
      val result = sortedAggregates.mapPartitions { serRows =>
        val (enclave, eid) = QED.initEnclave()
        serRows.filter(serRow => enclave.Filter(eid, aggDummyFilterOpcode.value, serRow))
      }
      result.cache.count
      result
    }

    finalAggregates.flatMap { serRows =>
      val converter = UnsafeProjection.create(schema)
      QED.parseRows(serRows).map(fields => converter(InternalRow.fromSeq(fields)))
    }
  }
}

case class EncSortMergeJoin(
    left: SparkPlan,
    right: SparkPlan,
    leftCol: Expression,
    rightCol: Expression,
    opcode: Option[QEDOpcode])
  extends BinaryNode {

  import QED.time

  override def output: Seq[Attribute] =
    left.output ++ right.output.filter(a => !rightCol.references.contains(a))

  override def doExecute() = {
    val leftColPos = QED.attributeIndexOf(leftCol.references.toSeq(0), left.output)
    val rightColPos = QED.attributeIndexOf(rightCol.references.toSeq(0), right.output)
    val (joinOpcode, dummySortOpcode, dummyFilterOpcode) =
      ((left.output.size, right.output.size, leftColPos, rightColPos, opcode): @unchecked) match {
        case (2, 3, 0, 0, None) =>
          (OP_JOIN_COL1, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_COL3_NOT_DUMMY)
        case (2, 3, 0, 0, Some(OP_JOIN_PAGERANK)) =>
          (OP_JOIN_PAGERANK, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_COL3_NOT_DUMMY)
        case (3, 3, 1, 1, None) =>
          (OP_JOIN_COL2, OP_SORT_COL4_IS_DUMMY_COL2, OP_FILTER_COL4_NOT_DUMMY)
      }

    val leftRDD = left.execute().mapPartitions { rowIter =>
      rowIter.map(_.encSerialize)
    }.cache()
    val rightRDD = right.execute().mapPartitions { rowIter =>
      rowIter.map(_.encSerialize)
    }.cache()
    time("Force left child of EncSortMergeJoin") { leftRDD.count }
    time("Force right child of EncSortMergeJoin") { rightRDD.count }

    val processed = leftRDD.zipPartitions(rightRDD) { (leftRowIter, rightRowIter) =>
      val (enclave, eid) = QED.initEnclave()

      val leftRows = leftRowIter.toArray
      val leftRowsConcat = QED.concatByteArrays(leftRows)
      println("leftRowsConcat size: " + leftRowsConcat.length)
      val leftProcessed = enclave.JoinSortPreprocess(
        eid, joinOpcode.value, QED.primaryTableId,
        leftRowsConcat, leftRows.length)

      val rightRows = rightRowIter.toArray
      val rightRowsConcat = QED.concatByteArrays(rightRows)
      println("rightRowsConcat size: " + rightRowsConcat.length)
      val rightProcessed = enclave.JoinSortPreprocess(
        eid, joinOpcode.value, QED.foreignTableId,
        rightRowsConcat, rightRows.length)

      (QED.splitBytes(leftProcessed, leftRows.length).iterator ++
        QED.splitBytes(rightProcessed, rightRows.length).iterator)
    }
    time("join - preprocess") { processed.cache.count }

    val sorted = time("join - sort") {
      val result = ObliviousSort.ColumnSort(sparkContext, processed, joinOpcode)
      result.cache.count
      result
    }

    val lastPrimaryRows = sorted.mapPartitions { rowIter =>
      val rows = rowIter.toArray
      val (enclave, eid) = QED.initEnclave()
      val lastPrimary = enclave.ScanCollectLastPrimary(
        eid, joinOpcode.value, QED.concatByteArrays(rows), rows.length)
      Iterator(lastPrimary)
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

    val joined = sorted.zipPartitions(processedJoinRowsRDD) { (rowIter, joinRowIter) =>
      val rows = rowIter.toArray
      val joinRow = joinRowIter.next()
      assert(!joinRowIter.hasNext)
      val (enclave, eid) = QED.initEnclave()
      val joined = enclave.SortMergeJoin(
        eid, joinOpcode.value, QED.concatByteArrays(rows), rows.length, joinRow)
      QED.readRows(joined)
    }
    time("join - sort merge join") { joined.cache.count }

    // TODO: permute first, otherwise this is insecure
    val nonDummy = joined.mapPartitions { rowIter =>
      val (enclave, eid) = QED.initEnclave()
      rowIter.filter(row => enclave.Filter(eid, dummyFilterOpcode.value, row))
    }
    time("join - filter dummies") { nonDummy.cache.count }

    nonDummy.mapPartitions { rowIter =>
      val converter = UnsafeProjection.create(schema)
      rowIter.map { row => converter(InternalRow.fromSeq(QED.parseRow(row))) }
    }
  }
}
