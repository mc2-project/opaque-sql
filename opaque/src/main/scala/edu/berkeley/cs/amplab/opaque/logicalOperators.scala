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

package edu.berkeley.cs.amplab.opaque

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

case class Encrypt(child: LogicalPlan) extends UnaryNode with EncOperator {
  override def output: Seq[Attribute] = child.output
}

case class MarkOblivious(child: EncOperator) extends UnaryNode with EncOperator {
  override def output: Seq[Attribute] = child.output
}

case class EncryptedLocalRelation(output: Seq[Attribute], plaintextData: Seq[InternalRow] = Nil)
  extends LeafNode with MultiInstanceRelation with EncOperator {

  // A local relation must have resolved output.
  require(output.forall(_.resolved), "Unresolved attributes found when constructing LocalRelation.")

  /**
   * Returns an identical copy of this relation with new exprIds for all attributes.  Different
   * attributes are required when a relation is going to be included multiple times in the same
   * query.
   */
  override final def newInstance(): this.type = {
    EncryptedLocalRelation(output.map(_.newInstance()), plaintextData).asInstanceOf[this.type]
  }

  override protected def stringArgs = Iterator(output)

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case EncryptedLocalRelation(otherOutput, otherPlaintextData) =>
      otherOutput.map(_.dataType) == output.map(_.dataType) && otherPlaintextData == plaintextData
    case _ => false
  }
}

case class LogicalEncryptedBlockRDD(
    output: Seq[Attribute],
    rdd: RDD[Block])
  extends EncOperator with MultiInstanceRelation {

  override def children: Seq[LogicalPlan] = Nil

  override def newInstance(): LogicalEncryptedBlockRDD.this.type =
    LogicalEncryptedBlockRDD(output.map(_.newInstance()), rdd).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case LogicalEncryptedBlockRDD(_, otherRDD) => rdd.id == otherRDD.id
    case _ => false
  }

  override def producedAttributes: AttributeSet = outputSet
}

case class EncProject(projectList: Seq[NamedExpression], child: EncOperator)
  extends UnaryNode with EncOperator {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
}

case class EncFilter(condition: Expression, child: EncOperator)
  extends UnaryNode with EncOperator {

  override def output: Seq[Attribute] = child.output
}

case class Permute(child: LogicalPlan) extends UnaryNode with EncOperator {
  override def output: Seq[Attribute] = child.output
}

case class EncSort(order: Seq[SortOrder], child: EncOperator)
  extends UnaryNode with EncOperator {
  override def output: Seq[Attribute] = child.output
}

case class NonObliviousSort(order: Seq[SortOrder], child: EncOperator)
  extends UnaryNode with EncOperator {
  override def output: Seq[Attribute] = child.output
}

case class EncAggregate(
    groupingExpressions: Seq[Expression],
    aggExpressions: Seq[NamedExpression],
    child: EncOperator)
  extends UnaryNode with EncOperator {

  override def producedAttributes: AttributeSet =
    AttributeSet(aggExpressions) -- AttributeSet(groupingExpressions)
  override def output: Seq[Attribute] = aggExpressions.map(_.toAttribute)
}

case class NonObliviousAggregate(
    groupingExpressions: Seq[Expression],
    aggExpressions: Seq[NamedExpression],
    child: EncOperator)
  extends UnaryNode with EncOperator {

  override def producedAttributes: AttributeSet =
    AttributeSet(aggExpressions) -- AttributeSet(groupingExpressions)
  override def output: Seq[Attribute] = aggExpressions.map(_.toAttribute)
}

case class EncJoin(
    left: EncOperator,
    right: EncOperator,
    joinExpr: Expression)
  extends BinaryNode with EncOperator {

  override def output: Seq[Attribute] = left.output ++ right.output
}

case class NonObliviousJoin(
    left: EncOperator,
    right: EncOperator,
    joinExpr: Expression)
  extends BinaryNode with EncOperator {

  override def output: Seq[Attribute] = left.output ++ right.output
}
