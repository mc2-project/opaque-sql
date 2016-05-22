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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.QEDOpcode
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.PredicateHelper

/**
 * An operator that computes on encrypted data.
 */
trait EncOperator extends LogicalPlan {
  /**
   * Every encrypted operator relies on its input having a specific set of columns, so we override
   * references to include all inputs to prevent Catalyst from dropping any input columns.
   */
  override def references: AttributeSet = inputSet
}

trait OutputsBlocks extends EncOperator

case class ConvertToBlocks(child: LogicalPlan) extends UnaryNode with OutputsBlocks {
  override def output: Seq[Attribute] = child.output
}

case class ConvertFromBlocks(child: OutputsBlocks) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def references: AttributeSet = AttributeSet(child.output)
}

case class EncProject(projectList: Seq[NamedExpression], opcode: QEDOpcode, child: OutputsBlocks)
  extends UnaryNode with OutputsBlocks {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def maxRows: Option[Long] = child.maxRows

  override def references: AttributeSet = AttributeSet(child.output)
}

case class EncFilter(condition: Expression, opcode: QEDOpcode, child: OutputsBlocks)
  extends UnaryNode with OutputsBlocks {

  override def output: Seq[Attribute] = child.output

  override def references: AttributeSet = AttributeSet(child.output)
}

case class Permute(child: LogicalPlan) extends UnaryNode with OutputsBlocks {
  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = child.maxRows
}

case class EncSort(sortExpr: Expression, child: OutputsBlocks)
  extends UnaryNode with OutputsBlocks {
  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = child.maxRows
}

case class EncAggregate(
    opcode: QEDOpcode,
    groupingExpression: NamedExpression,
    aggExpressions: Seq[NamedExpression],
    aggOutputs: Seq[Attribute],
    child: OutputsBlocks)
  extends UnaryNode with OutputsBlocks {

  override def producedAttributes: AttributeSet = AttributeSet(aggOutputs)
  override def output: Seq[Attribute] = groupingExpression.toAttribute +: aggOutputs
  override def maxRows: Option[Long] = child.maxRows
}

case class EncJoin(
    left: OutputsBlocks,
    right: OutputsBlocks,
    leftCol: Expression,
    rightCol: Expression,
    opcode: Option[QEDOpcode])
  extends BinaryNode with OutputsBlocks {

  override def output: Seq[Attribute] =
    left.output ++ right.output.filter(a => !rightCol.references.contains(a))
}
