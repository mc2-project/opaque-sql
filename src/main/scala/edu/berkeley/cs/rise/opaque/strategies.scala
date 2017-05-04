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
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.Exchange

import edu.berkeley.cs.rise.opaque.execution._
import edu.berkeley.cs.rise.opaque.logical.ConvertToOpaqueOperators
import edu.berkeley.cs.rise.opaque.logical._

object OpaqueOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ObliviousProject(projectList, child) =>
      ObliviousProjectExec(projectList, planLater(child)) :: Nil
    case EncryptedProject(projectList, child) =>
      ObliviousProjectExec(projectList, planLater(child)) :: Nil

    case ObliviousFilter(condition, child) =>
      ObliviousFilterExec(condition, planLater(child)) :: Nil
    case EncryptedFilter(condition, child) =>
      ObliviousFilterExec(condition, planLater(child)) :: Nil

    case ObliviousPermute(child) =>
      ObliviousPermuteExec(planLater(child)) :: Nil

    case ObliviousSort(order, child) =>
      ObliviousSortExec(order, planLater(child)) :: Nil
    case EncryptedSort(order, child) =>
      EncryptedSortExec(order, planLater(child)) :: Nil

    case ObliviousJoin(left, right, joinType, condition) =>
      Join(left, right, joinType, condition) match {
        case ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _) =>
          ???
          // ObliviousSortMergeJoinExec(
          //   joinType, leftKeys, rightKeys, condition,
          //   ObliviousSortExec(
          //     sortByTag,
          //     ObliviousUnionExec(
          //       ObliviousProjectExec(tagLeft(left.output), planLater(left)),
          //       ObliviousProjectExec(tagRight(right.output), planLater(right))))) :: Nil
        case _ => Nil
      }
    case EncryptedJoin(left, right, joinType, condition) =>
      Join(left, right, joinType, condition) match {
        case ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _) =>
          val unioned = ObliviousUnionExec(
            ObliviousProjectExec(tagLeft(left.output), planLater(left)),
            ObliviousProjectExec(tagRight(right.output), planLater(right)))
          ObliviousProjectExec(
            dropTags(left.output, right.output),
            EncryptedSortMergeJoinExec(
              joinType,
              leftKeys,
              rightKeys,
              tagLeft(left.output).map(_.toAttribute),
              tagRight(right.output).map(_.toAttribute),
              tagLeft(left.output).map(_.toAttribute) ++ tagRight(right.output).map(_.toAttribute),
              EncryptedSortExec(
                sortForJoin(leftKeys, rightKeys, unioned.output),
                unioned))) :: Nil
        case _ => Nil
      }

    case a @ ObliviousAggregate(groupingExpressions, aggExpressions, child) =>
      ObliviousAggregateExec(groupingExpressions, aggExpressions, planLater(child)) :: Nil
    case a @ EncryptedAggregate(groupingExpressions, aggExpressions, child) =>
      EncryptedAggregateExec(groupingExpressions, aggExpressions, planLater(child)) :: Nil

    case ObliviousUnion(left, right) =>
      ObliviousUnionExec(planLater(left), planLater(right)) :: Nil

    case Encrypt(isOblivious, child) =>
      EncryptExec(isOblivious, planLater(child)) :: Nil

    case EncryptedLocalRelation(output, plaintextData, isOblivious) =>
      EncryptedLocalTableScanExec(output, plaintextData, isOblivious) :: Nil

    case EncryptedBlockRDD(output, rdd, isOblivious) =>
      EncryptedBlockRDDScanExec(output, rdd, isOblivious) :: Nil

    case _ => Nil
  }

  private def tagLeft(input: Seq[Attribute]): Seq[NamedExpression] =
    Alias(Literal(0), "tag")() +: input

  private def tagRight(input: Seq[Attribute]): Seq[NamedExpression] =
    Alias(Literal(1), "tag")() +: input

  // projection
  private def sortForJoin(
      leftKeys: Seq[Expression], rightKeys: Seq[Expression],
      input: Seq[Attribute]): Seq[SortOrder] = {
    val tag = input(0)
    leftKeys.zip(rightKeys).map {
      case (leftKey, rightKey) =>
        SortOrder(
          If(
            EqualTo(tag, Literal(0)),
            leftKey,
            rightKey),
          Ascending)
    } :+ SortOrder(tag, Ascending)
  }

  private def dropTags(
      leftOutput: Seq[Attribute], rightOutput: Seq[Attribute]): Seq[NamedExpression] =
    leftOutput ++ rightOutput
}
