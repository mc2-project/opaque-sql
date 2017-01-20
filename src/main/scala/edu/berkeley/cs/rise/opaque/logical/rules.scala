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

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.execution.OpaqueOperatorExec
import edu.berkeley.cs.rise.opaque.logical.ExecMode._
import org.apache.spark.sql.InMemoryRelationMatcher
import org.apache.spark.sql.UndoCollapseProject
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.InMemoryRelation

object EncryptLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Encrypt(mode: ExecMode, LocalRelation(output, data)) =>
      EncryptedLocalRelation(output, data, mode)
  }
}

object ConvertToOpaqueOperators extends Rule[LogicalPlan] {
  def mode(plan: LogicalPlan): ExecMode = {
    ExecMode.getMode(plan.map {
      x => x match {
        case o: OpaqueOperator => o.mode.value
        case _ => INSECURE.value
      }
    }.foldLeft(0)((m: Int, n: Int) => math.max(m, n)))
  }

  def mode(plan: SparkPlan): ExecMode = {
    ExecMode.getMode(plan.map {
      x => x match {
        case o: OpaqueOperatorExec => o.mode.value
        case _ => INSECURE.value
      }
    }.foldLeft(0)((m: Int, n: Int) => math.max(m, n)))
  }

  def isOblivious(plan: LogicalPlan): Boolean = {
    isEncrypted(plan) && plan.find {
      case p: OpaqueOperator => p.isOblivious
      case _ => false
    }.nonEmpty
  }

  def isOblivious(plan: SparkPlan): Boolean = {
    isEncrypted(plan) && plan.find {
      case p: OpaqueOperatorExec => p.isOblivious
      case _ => false
    }.nonEmpty
  }

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

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p @ Project(projectList, child) if isOblivious(child) =>
      ObliviousProject(projectList, child.asInstanceOf[OpaqueOperator])
    case p @ Project(projectList, child) if isEncrypted(child) =>
      EncryptedProject(projectList, child.asInstanceOf[OpaqueOperator])

    // We don't support null values yet, so there's no point in checking whether the output of an
    // encrypted operator is null
    case p @ Filter(And(IsNotNull(_), IsNotNull(_)), child) if isEncrypted(child)=>
      child
    case p @ Filter(IsNotNull(_), child) if isEncrypted(child) =>
      child

    case p @ Filter(condition, child) if isOblivious(child) =>
      ObliviousFilter(condition, ObliviousPermute(child.asInstanceOf[OpaqueOperator]))
    case p @ Filter(condition, child) if isEncrypted(child)=>
      EncryptedFilter(condition, child.asInstanceOf[OpaqueOperator])

    case p @ Sort(order, true, child) if isOblivious(child) =>
      ObliviousSort(order, child.asInstanceOf[OpaqueOperator])
    case p @ Sort(order, true, child) if isEncrypted(child) =>
      EncryptedSort(order, child.asInstanceOf[OpaqueOperator])

    case p @ Join(left, right, Inner, Some(joinExpr)) if isOblivious(p) =>
      ObliviousJoin(left.asInstanceOf[OpaqueOperator], right.asInstanceOf[OpaqueOperator], joinExpr)
    case p @ Join(left, right, Inner, Some(joinExpr)) if isEncrypted(p) =>
      EncryptedJoin(left.asInstanceOf[OpaqueOperator], right.asInstanceOf[OpaqueOperator], joinExpr)

    case p @ Aggregate(groupingExprs, aggExprs, child) if isOblivious(p) =>
      UndoCollapseProject.separateProjectAndAgg(p) match {
        case Some((projectExprs, aggExprs)) =>
          ObliviousProject(
            projectExprs,
            ObliviousAggregate(
              groupingExprs, aggExprs,
              ObliviousSort(
                groupingExprs.map(e => SortOrder(e, Ascending)),
                child.asInstanceOf[OpaqueOperator])))
        case None =>
          ObliviousAggregate(
            groupingExprs, aggExprs,
            ObliviousSort(
              groupingExprs.map(e => SortOrder(e, Ascending)),
              child.asInstanceOf[OpaqueOperator]))
      }
    case p @ Aggregate(groupingExprs, aggExprs, child) if isEncrypted(p) =>
      UndoCollapseProject.separateProjectAndAgg(p) match {
        case Some((projectExprs, aggExprs)) =>
          EncryptedProject(
            projectExprs,
            EncryptedAggregate(
              groupingExprs, aggExprs,
              EncryptedSort(
                groupingExprs.map(e => SortOrder(e, Ascending)),
                child.asInstanceOf[OpaqueOperator])))
        case None =>
          EncryptedAggregate(
            groupingExprs, aggExprs,
            EncryptedSort(
              groupingExprs.map(e => SortOrder(e, Ascending)),
              child.asInstanceOf[OpaqueOperator]))
      }

    case InMemoryRelationMatcher(output, storageLevel, child) if isEncrypted(child) =>
      EncryptedBlockRDD(
        output,
        Utils.ensureCached(
          child.asInstanceOf[OpaqueOperatorExec].executeBlocked(),
          storageLevel),
        mode(child))
  }
}
