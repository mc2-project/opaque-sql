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
import org.apache.spark.sql.QED
import org.apache.spark.sql.QEDOpcode._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection

import org.apache.spark.sql.execution.metric.SQLMetrics

case class EncFilter(condition: Expression, child: SparkPlan)
  extends UnaryNode with PredicateHelper {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, _) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) if child.output.contains(a) => true
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references)

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override def doExecute() = child.execute().mapPartitions { iter =>
    val (enclave, eid) = QED.initEnclave()
    iter.filter(rowSer => enclave.Filter(eid, OP_FILTER_COL2_GT3.value, rowSer.encSerialize))
  }

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

case class Permute(child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def doExecute() = {
    val childRDD = child.execute().mapPartitions { rowIter =>
      val (enclave, eid) = QED.initEnclave()
      rowIter.map(row =>
        InternalRow.fromSeq(QED.randomId(enclave, eid) +: row.toSeq(schema)).encSerialize)
    }
    ObliviousSort.ColumnSort(childRDD.context, childRDD, opcode = OP_SORT_COL1.value).mapPartitions { serRowIter =>
      val converter = UnsafeProjection.create(schema)
      serRowIter.map(serRow => converter(
        InternalRow.fromSeq(QED.parseRow(serRow).tail)))
    }
  }
}

case class EncSort(sortExpr: Expression, child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  private def attributeIndexOf(a: Attribute, list: Seq[Attribute]): Int = {
    var i = 0
    while (i < list.size) {
      if (list(i) semanticEquals a) {
        return i
      }
      i += 1
    }
    return -1
  }

  override def doExecute() = {
    val childRDD = child.execute().map(_.encSerialize)
    val sortAttrPos = attributeIndexOf(sortExpr.references.toSeq(0), child.output)
    val opcode = sortAttrPos match {
      case 0 => OP_SORT_COL1.value
      case 1 => OP_SORT_COL2.value
    }
    ObliviousSort.ColumnSort(childRDD.context, childRDD, opcode).mapPartitions { serRowIter =>
      val converter = UnsafeProjection.create(schema)
      serRowIter.map(serRow => converter(InternalRow.fromSeq(QED.parseRow(serRow))))
    }
  }
}

case class EncAggregateWithSum(
    groupingExpression: NamedExpression,
    sumExpression: NamedExpression,
    aggOutputs: Seq[Attribute],
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryNode {

  override def doExecute() = {
    val childRDD = child.execute().cache()

    // Process boundaries
    val boundaries = childRDD.mapPartitions { rowIter =>
      val rows = rowIter.map(_.copy).toArray
      val concatRows = QED.concatByteArrays(rows.map(_.encSerialize))
      val (enclave, eid) = QED.initEnclave()
      val aggSize = 4 + 12 + 16 + 4 + 4 + 2048 + 128
      val boundary = enclave.Aggregate(
        eid, OP_GROUPBY_COL2_SUM_COL3_STEP1.value, concatRows, rows.length, new Array[Byte](aggSize))
      enclave.StopEnclave(eid)
      Iterator(boundary)
    }

    val boundariesCollected = boundaries.collect
    val (enclave, eid) = QED.initEnclave()
    val processedBoundariesConcat = enclave.ProcessBoundary(
      eid, OP_GROUPBY_COL2_SUM_COL3_STEP1.value,
      QED.concatByteArrays(boundariesCollected), boundariesCollected.length)
    enclave.StopEnclave(eid)

    // Send processed boundaries to partitions and generate a mix of partial and final aggregates
    val processedBoundaries = QED.splitBytes(processedBoundariesConcat, boundariesCollected.length)
    val processedBoundariesRDD = sparkContext.parallelize(processedBoundaries)
    val partialAggregates = childRDD.zipPartitions(processedBoundariesRDD) {
      (rowIter, boundaryIter) =>
        val rows = rowIter.map(_.copy).toArray
        val concatRows = QED.concatByteArrays(rows.map(_.encSerialize))
        val aggSize = 4 + 12 + 16 + 4 + 4 + 2048 + 128
        val boundaryArray = boundaryIter.toArray
        assert(boundaryArray.length == 1)
        val boundaryRecord = boundaryArray.head
        assert(boundaryRecord.length >= aggSize)
        val (enclave, eid) = QED.initEnclave()
        val partialAgg = enclave.Aggregate(
          eid, OP_GROUPBY_COL2_SUM_COL3_STEP2.value, concatRows, rows.length, boundaryRecord)
        assert(partialAgg.nonEmpty)
        enclave.StopEnclave(eid)
        QED.readRows(partialAgg)
    }

    // Sort the partial and final aggregates using a comparator that causes final aggregates to come first
    val sortedAggregates = ObliviousSort.ColumnSort(
      partialAggregates.context, partialAggregates, OP_SORT_COL4_IS_DUMMY_COL2.value)

    // Filter out the non-final aggregates
    val finalAggregates = sortedAggregates.mapPartitions { serRows =>
      val (enclave, eid) = QED.initEnclave()
      serRows.filter(serRow => enclave.Filter(eid, OP_FILTER_COL4_NOT_DUMMY.value, serRow))
    }

    finalAggregates.flatMap { serRows =>
      val converter = UnsafeProjection.create(output, child.output ++ aggOutputs)
      QED.parseRows(serRows).map(fields => converter(InternalRow.fromSeq(fields)))
    }
  }
}
