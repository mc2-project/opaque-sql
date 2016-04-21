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
    ObliviousSort.ColumnSort(childRDD.context, childRDD).mapPartitions { serRowIter =>
      val converter = UnsafeProjection.create(schema)
      val schemaWithRandomId = schema.add(StructField("randomId", BinaryType, true))
      serRowIter.map(serRow => converter(
        InternalRow.fromSeq(InternalRow.fromSerialized(serRow).toSeq(schemaWithRandomId).init)))
    }
  }
}

case class EncSort(sortExpr: Expression, child: SparkPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override def doExecute() = {
    val childRDD = child.execute().map(_.encSerialize)
    // TODO: pass opcode to signal sorting on sortExpr
    ObliviousSort.ColumnSort(childRDD.context, childRDD).mapPartitions { serRowIter =>
      val converter = UnsafeProjection.create(schema)
      serRowIter.map(serRow => converter(InternalRow.fromSerialized(serRow)))
    }
  }
}

case class EncAggregateWithSum(
    groupingExpression: NamedExpression,
    sumExpression: NamedExpression,
    child: SparkPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] =
    Seq(groupingExpression.toAttribute, sumExpression.toAttribute)

  override def doExecute() = {
    ???
  }
}
