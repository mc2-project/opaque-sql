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

package edu.berkeley.cs.amplab.opaque.logical

import edu.berkeley.cs.amplab.opaque.execution
import org.apache.spark.sql.InMemoryRelationMatcher
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.InMemoryRelation

object EncryptLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Encrypt(LocalRelation(output, data)) =>
      EncryptedLocalRelation(output, data)
  }
}

object ConvertToEncryptedOperators extends Rule[LogicalPlan] {
  def isOblivious(plan: LogicalPlan): Boolean = {
    isEncrypted(plan) && plan.find {
      case MarkOblivious(_) => true
      case _ => false
    }.nonEmpty
  }

  def isEncrypted(plan: LogicalPlan): Boolean = {
    plan.find {
      case _: EncOperator => true
      case _ => false
    }.nonEmpty
  }

  def isEncrypted(plan: SparkPlan): Boolean = {
    plan.find {
      case _: execution.EncOperator => true
      case _ => false
    }.nonEmpty
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p @ Project(projectList, child) if isEncrypted(child) || isOblivious(child) =>
      EncProject(projectList, child.asInstanceOf[EncOperator])
    case p @ Filter(condition, child) if isOblivious(child) =>
      EncFilter(condition, Permute(child.asInstanceOf[EncOperator]))
    case p @ Filter(condition, child) if isEncrypted(child) =>
      EncFilter(condition, child.asInstanceOf[EncOperator])
    case p @ Sort(order, true, child) if isOblivious(child) =>
      EncSort(order, child.asInstanceOf[EncOperator])
    case p @ Sort(order, true, child) if isEncrypted(child) =>
      NonObliviousSort(order, child.asInstanceOf[EncOperator])
    case p @ Join(left, right, Inner, Some(joinExpr)) if isOblivious(p) =>
      EncJoin(left.asInstanceOf[EncOperator], right.asInstanceOf[EncOperator], joinExpr)
    case p @ Join(left, right, Inner, Some(joinExpr)) if isEncrypted(p) =>
      NonObliviousJoin(left.asInstanceOf[EncOperator], right.asInstanceOf[EncOperator], joinExpr)
    case p @ Aggregate(groupingExprs, aggExprs, child) if isOblivious(p) =>
      EncAggregate(
        groupingExprs, aggExprs,
        EncSort(
          groupingExprs.map(e => SortOrder(e, Ascending)),
          child.asInstanceOf[EncOperator]))
    case p @ Aggregate(groupingExprs, aggExprs, child) if isEncrypted(p) =>
      NonObliviousAggregate(
        groupingExprs, aggExprs,
        NonObliviousSort(
          groupingExprs.map(e => SortOrder(e, Ascending)),
          child.asInstanceOf[EncOperator]))
    case InMemoryRelationMatcher(output, storageLevel, child) if isEncrypted(child) =>
      // TODO: handle oblivious plans
      LogicalEncryptedBlockRDD(
        output,
        child.asInstanceOf[execution.EncOperator].executeBlocked().persist(storageLevel))
  }
}
