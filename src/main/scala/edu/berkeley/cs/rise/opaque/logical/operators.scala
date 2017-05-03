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

package edu.berkeley.cs.rise.opaque.logical

import edu.berkeley.cs.rise.opaque.execution.Block
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.BinaryNode
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode

/**
 * An operator that computes on encrypted data.
 */
trait OpaqueOperator extends LogicalPlan {
  /**
   * Every encrypted operator relies on its input having a specific set of columns, so we override
   * references to include all inputs to prevent Catalyst from dropping any input columns.
   */
  override def references: AttributeSet = inputSet

  def isOblivious: Boolean = children.exists(_.find {
    case p: OpaqueOperator => p.isOblivious
    case _ => false
  }.nonEmpty)
}

case class Encrypt(
    override val isOblivious: Boolean,
    child: LogicalPlan)
  extends UnaryNode with OpaqueOperator {

  override def output: Seq[Attribute] = child.output
}

case class EncryptedLocalRelation(
    output: Seq[Attribute],
    plaintextData: Seq[InternalRow],
    override val isOblivious: Boolean)
  extends LeafNode with MultiInstanceRelation with OpaqueOperator {

  // A local relation must have resolved output.
  require(output.forall(_.resolved), "Unresolved attributes found when constructing LocalRelation.")

  /**
   * Returns an identical copy of this relation with new exprIds for all attributes.  Different
   * attributes are required when a relation is going to be included multiple times in the same
   * query.
   */
  override final def newInstance(): this.type = {
    EncryptedLocalRelation(output.map(_.newInstance()), plaintextData, isOblivious).asInstanceOf[this.type]
  }

  override protected def stringArgs = Iterator(output)

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case EncryptedLocalRelation(otherOutput, otherPlaintextData, otherIsOblivious) =>
      (otherOutput.map(_.dataType) == output.map(_.dataType) && otherPlaintextData == plaintextData
        && otherIsOblivious == isOblivious)
    case _ => false
  }
}

case class EncryptedBlockRDD(
    output: Seq[Attribute],
    rdd: RDD[Block],
    override val isOblivious: Boolean)
  extends OpaqueOperator with MultiInstanceRelation {

  override def children: Seq[LogicalPlan] = Nil

  override def newInstance(): EncryptedBlockRDD.this.type =
    EncryptedBlockRDD(output.map(_.newInstance()), rdd, isOblivious).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case EncryptedBlockRDD(_, otherRDD, otherIsOblivious) =>
      rdd.id == otherRDD.id && isOblivious == otherIsOblivious
    case _ => false
  }

  override def producedAttributes: AttributeSet = outputSet
}

case class ObliviousProject(projectList: Seq[NamedExpression], child: OpaqueOperator)
  extends UnaryNode with OpaqueOperator {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
}

case class EncryptedProject(projectList: Seq[NamedExpression], child: OpaqueOperator)
  extends UnaryNode with OpaqueOperator {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
}

case class ObliviousFilter(condition: Expression, child: OpaqueOperator)
  extends UnaryNode with OpaqueOperator {

  override def output: Seq[Attribute] = child.output
}

case class EncryptedFilter(condition: Expression, child: OpaqueOperator)
  extends UnaryNode with OpaqueOperator {

  override def output: Seq[Attribute] = child.output
}

case class ObliviousPermute(child: LogicalPlan) extends UnaryNode with OpaqueOperator {
  override def output: Seq[Attribute] = child.output
}

case class ObliviousSort(order: Seq[SortOrder], child: OpaqueOperator)
  extends UnaryNode with OpaqueOperator {
  override def output: Seq[Attribute] = child.output
}

case class EncryptedSort(order: Seq[SortOrder], child: OpaqueOperator)
  extends UnaryNode with OpaqueOperator {
  override def output: Seq[Attribute] = child.output
}

case class ObliviousAggregate(
    groupingExpressions: Seq[Expression],
    aggExpressions: Seq[NamedExpression],
    child: OpaqueOperator)
  extends UnaryNode with OpaqueOperator {

  override def producedAttributes: AttributeSet =
    AttributeSet(aggExpressions) -- AttributeSet(groupingExpressions)
  override def output: Seq[Attribute] = aggExpressions.map(_.toAttribute)
}

case class EncryptedAggregate(
    groupingExpressions: Seq[Expression],
    aggExpressions: Seq[NamedExpression],
    child: OpaqueOperator)
  extends UnaryNode with OpaqueOperator {

  override def producedAttributes: AttributeSet =
    AttributeSet(aggExpressions) -- AttributeSet(groupingExpressions)
  override def output: Seq[Attribute] = aggExpressions.map(_.toAttribute)
}

case class ObliviousJoin(
    left: OpaqueOperator,
    right: OpaqueOperator,
    joinType: JoinType,
    condition: Option[Expression])
  extends BinaryNode with OpaqueOperator {

  override def output: Seq[Attribute] = left.output ++ right.output
}

case class EncryptedJoin(
    left: OpaqueOperator,
    right: OpaqueOperator,
    joinType: JoinType,
    condition: Option[Expression])
  extends BinaryNode with OpaqueOperator {

  override def output: Seq[Attribute] = left.output ++ right.output
}

case class ObliviousUnion(
    left: OpaqueOperator,
    right: OpaqueOperator)
  extends BinaryNode with OpaqueOperator {

  override def output: Seq[Attribute] = left.output
}
