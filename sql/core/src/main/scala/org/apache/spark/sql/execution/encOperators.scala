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
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

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
    val predicateId = QED.enclaveRegisterPredicate(enclave, eid, condition, child.output)
    val schemaTypes = child.output.map(_.dataType)
    iter.filter(QED.enclaveEvalPredicate(enclave, eid, predicateId, _, schemaTypes))
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
        InternalRow.fromSeq(row.toSeq(schema) :+ QED.randomId(enclave, eid)).encSerialize)
    }
    // TODO: pass opcode to signal sorting on random id
    ObliviousSort.ColumnSort(childRDD.context, childRDD, opcode = 50).mapPartitions { serRowIter =>
      val converter = UnsafeProjection.create(schema)
      val schemaWithRandomId = schema.add(StructField("randomId", BinaryType, true))
      serRowIter.map(serRow => converter(
        InternalRow.fromSeq(QED.parseRow(serRow).toSeq(schemaWithRandomId).init)))
    }
  }
}

case class EncSort(sortExpr: Expression, child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def doExecute() = {
    val childRDD = child.execute().map(_.encSerialize)
    // TODO: pass opcode to signal sorting on sortExpr
    ObliviousSort.ColumnSort(childRDD.context, childRDD, opcode = 50).mapPartitions { serRowIter =>
      val converter = UnsafeProjection.create(schema)
      serRowIter.map(serRow => converter(QED.parseRow(serRow)))
    }
  }
}

case class EncAggregateWithSum(
    groupingExpression: NamedExpression,
    sumExpression: NamedExpression,
    aggOutputs: Seq[Attribute],
    child: SparkPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output ++ aggOutputs

  override def doExecute() = {
    // Process boundaries
    val boundaries = child.execute().mapPartitions { rowIter =>
      val rows = rowIter.map(_.copy).toArray
      val concatRows = QED.concatByteArrays(rows.map(_.encSerialize))
      val (enclave, eid) = QED.initEnclave()
      val aggSize = 4 + 12 + 16 + 4 + 4 + 2048 + 128
      val boundary = enclave.Aggregate(eid, 1, concatRows, rows.length, new Array[Byte](aggSize))
      enclave.StopEnclave(eid)
      Iterator(boundary)
    }

    val boundariesCollected = boundaries.collect
    val (enclave, eid) = QED.initEnclave()
    val processedBoundariesConcat = enclave.ProcessBoundary(
      eid, 1, QED.concatByteArrays(boundariesCollected), boundariesCollected.length)
    enclave.StopEnclave(eid)

    // Send processed boundaries to partitions and generate a mix of partial and final aggregates
    val processedBoundaries = QED.splitBytes(processedBoundariesConcat, boundariesCollected.length)
    val processedBoundariesRDD = sparkContext.parallelize(processedBoundaries)
    val partialAggregates = child.execute().zipPartitions(processedBoundariesRDD) {
      (rowIter, boundaryIter) =>
        val rows = rowIter.map(_.copy).toArray
        val concatRows = QED.concatByteArrays(rows.map(_.encSerialize))
        val aggSize = 4 + 12 + 16 + 4 + 4 + 2048 + 128
        val boundaryArray = boundaryIter.toArray
        assert(boundaryArray.length == 1)
        val boundaryRecord = boundaryArray.head
        assert(boundaryRecord.length >= aggSize)
        val (enclave, eid) = QED.initEnclave()
        val partialAgg = enclave.Aggregate(eid, 101, concatRows, rows.length, boundaryRecord)
        assert(partialAgg.nonEmpty)
        enclave.StopEnclave(eid)
        QED.readRows(partialAgg)
    }

    // Sort the partial and final aggregates using a comparator that causes final aggregates to come first
    val sortedAggregates = ObliviousSort.ColumnSort(partialAggregates.context, partialAggregates, opcode = 51)

    sortedAggregates.flatMap { serRows =>
      val converter = UnsafeProjection.create(schema)
      QED.parseRows(serRows).map(converter)
    }
  }
}
