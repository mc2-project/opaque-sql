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

package edu.berkeley.cs.rise.opaque

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.planning.PhysicalAggregation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan

import edu.berkeley.cs.rise.opaque.execution._
import edu.berkeley.cs.rise.opaque.logical._

object OpaqueOperators extends Strategy {

  def isEncrypted(plan: LogicalPlan): Boolean = {
    plan.find {
      case _: OpaqueOperator => true
      case _ => false
    }.nonEmpty
  }

  def isEncrypted(plan: SparkPlan): Boolean = {
    plan.find {
      case _: OpaqueOperatorExec => true
      case _ => false
    }.nonEmpty
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Project(projectList, child) if isEncrypted(child) =>
      EncryptedProjectExec(projectList, planLater(child)) :: Nil

    // We don't support null values yet, so there's no point in checking whether the output of an
    // encrypted operator is null
    case p @ Filter(And(IsNotNull(_), IsNotNull(_)), child) if isEncrypted(child) =>
      planLater(child) :: Nil
    case p @ Filter(IsNotNull(_), child) if isEncrypted(child) =>
      planLater(child) :: Nil

    case Filter(condition, child) if isEncrypted(child) =>
      EncryptedFilterExec(condition, planLater(child)) :: Nil

    case Sort(sortExprs, global, child) if isEncrypted(child) =>
      EncryptedSortExec(sortExprs, global, planLater(child)) :: Nil

    case p @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right, _) if isEncrypted(p) =>
      val (leftProjSchema, leftKeysProj, tag) = tagForJoin(leftKeys, left.output, true)
      val (rightProjSchema, rightKeysProj, _) = tagForJoin(rightKeys, right.output, false)
      val leftProj = EncryptedProjectExec(leftProjSchema, planLater(left))
      val rightProj = EncryptedProjectExec(rightProjSchema, planLater(right))
      val unioned = EncryptedUnionExec(leftProj, rightProj)
      val sorted = EncryptedSortExec(sortForJoin(leftKeysProj, tag, unioned.output), true, unioned)
      val joined = EncryptedSortMergeJoinExec(
        joinType,
        leftKeysProj,
        rightKeysProj,
        leftProjSchema.map(_.toAttribute),
        rightProjSchema.map(_.toAttribute),
        (leftProjSchema ++ rightProjSchema).map(_.toAttribute),
        sorted)
      val tagsDropped = EncryptedProjectExec(dropTags(left.output, right.output), joined)
      val filtered = condition match {
        case Some(condition) => EncryptedFilterExec(condition, tagsDropped)
        case None => tagsDropped
      }
      filtered :: Nil

    case a @ PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
        if (isEncrypted(child) && aggExpressions.forall(expr => expr.isInstanceOf[AggregateExpression])) =>

      val aggregateExpressions = aggExpressions.map(expr => expr.asInstanceOf[AggregateExpression])

      if (groupingExpressions.size == 0) {
        // Global aggregation
        val partialAggregate = EncryptedAggregateExec(groupingExpressions, aggregateExpressions, Partial, planLater(child))
        val partialOutput = partialAggregate.output
        val (projSchema, tag) = tagForGlobalAggregate(partialOutput)

        EncryptedProjectExec(resultExpressions, 
          EncryptedAggregateExec(groupingExpressions, aggregateExpressions, Final, 
            EncryptedProjectExec(partialOutput, 
              EncryptedSortExec(Seq(SortOrder(tag, Ascending)), true, 
                EncryptedProjectExec(projSchema, partialAggregate))))) :: Nil
      } else {
        // Grouping aggregation
        EncryptedProjectExec(resultExpressions,
          EncryptedAggregateExec(groupingExpressions, aggregateExpressions, Final,
            EncryptedSortExec(groupingExpressions.map(_.toAttribute).map(e => SortOrder(e, Ascending)), true,
              EncryptedAggregateExec(groupingExpressions, aggregateExpressions, Partial,
                EncryptedSortExec(groupingExpressions.map(e => SortOrder(e, Ascending)), false, planLater(child)))))) :: Nil
      }

    case p @ Union(Seq(left, right)) if isEncrypted(p) =>
      EncryptedUnionExec(planLater(left), planLater(right)) :: Nil

    case ReturnAnswer(rootPlan) => rootPlan match {
      case Limit(IntegerLiteral(limit), Sort(sortExprs, true, child)) if isEncrypted(child) =>
      EncryptedGlobalLimitExec(limit,
        EncryptedLocalLimitExec(limit,
          EncryptedSortExec(sortExprs, true, planLater(child)))) :: Nil

      case Limit(IntegerLiteral(limit), Project(projectList, child)) if isEncrypted(child) =>
        EncryptedGlobalLimitExec(limit,
          EncryptedLocalLimitExec(limit,
            EncryptedProjectExec(projectList, planLater(child)))) :: Nil

      case _ => Nil
    }

    case Limit(IntegerLiteral(limit), Sort(sortExprs, true, child)) if isEncrypted(child) =>
      EncryptedGlobalLimitExec(limit,
        EncryptedLocalLimitExec(limit,
          EncryptedSortExec(sortExprs, true, planLater(child)))) :: Nil

    case Limit(IntegerLiteral(limit), Project(projectList, child)) if isEncrypted(child) =>
      EncryptedGlobalLimitExec(limit,
        EncryptedLocalLimitExec(limit,
          EncryptedProjectExec(projectList, planLater(child)))) :: Nil

    case LocalLimit(IntegerLiteral(limit), child) if isEncrypted(child) =>
      EncryptedLocalLimitExec(limit, planLater(child)) :: Nil

    case GlobalLimit(IntegerLiteral(limit), child) if isEncrypted(child) =>
      EncryptedGlobalLimitExec(limit, planLater(child)) :: Nil

    case Encrypt(child) =>
      EncryptExec(planLater(child)) :: Nil

    case EncryptedLocalRelation(output, plaintextData) =>
      EncryptedLocalTableScanExec(output, plaintextData) :: Nil

    case EncryptedBlockRDD(output, rdd) =>
      EncryptedBlockRDDScanExec(output, rdd) :: Nil

    case _ => Nil
  }

  private def tagForJoin(
      keys: Seq[Expression], input: Seq[Attribute], isLeft: Boolean)
    : (Seq[NamedExpression], Seq[NamedExpression], NamedExpression) = {
    val keysProj = keys.zipWithIndex.map { case (k, i) => Alias(k, "_" + i)() }
    val tag = Alias(Literal(if (isLeft) 0 else 1), "_tag")()
    (Seq(tag) ++ keysProj ++ input, keysProj.map(_.toAttribute), tag.toAttribute)
  }

  private def sortForJoin(
      leftKeys: Seq[Expression], tag: Expression, input: Seq[Attribute]): Seq[SortOrder] =
    leftKeys.map(k => SortOrder(k, Ascending)) :+ SortOrder(tag, Ascending)

  private def dropTags(
      leftOutput: Seq[Attribute], rightOutput: Seq[Attribute]): Seq[NamedExpression] =
    leftOutput ++ rightOutput

  private def tagForGlobalAggregate(input: Seq[Attribute])
      : (Seq[NamedExpression], NamedExpression) = {
    val tag = Alias(Literal(0), "_tag")()
    (Seq(tag) ++ input, tag.toAttribute)
  }
}
