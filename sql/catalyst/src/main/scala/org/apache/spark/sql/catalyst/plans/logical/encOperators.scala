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

case class EncProject(projectList: Seq[NamedExpression], child: LogicalPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def maxRows: Option[Long] = child.maxRows
}

case class EncFilter(condition: QEDOpcode, child: LogicalPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output
}

case class Permute(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = child.maxRows
}

case class EncSort(sortExpr: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = child.maxRows
}

case class EncAggregate(
    groupingExpression: NamedExpression,
    aggExpressions: Seq[NamedExpression],
    aggOutputs: Seq[Attribute],
    child: LogicalPlan)
  extends UnaryNode {

  override def producedAttributes: AttributeSet = AttributeSet(aggOutputs)
  override def output: Seq[Attribute] = groupingExpression.toAttribute +: aggOutputs
  override def maxRows: Option[Long] = child.maxRows
}

case class EncJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    leftCol: Expression,
    rightCol: Expression)
  extends BinaryNode {

  override def output: Seq[Attribute] =
    left.output ++ right.output.filter(a => !rightCol.references.contains(a))
}
