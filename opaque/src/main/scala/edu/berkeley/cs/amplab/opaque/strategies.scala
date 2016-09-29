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

object EncOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case logical.EncProject(projectList, child) =>
      execution.EncProject(
        projectList, planLater(child).asInstanceOf[EncOperator]) :: Nil
    case logical.EncFilter(condition, child) =>
      execution.EncFilter(condition, planLater(child).asInstanceOf[EncOperator]) :: Nil
    case logical.Permute(child) =>
      execution.Permute(planLater(child).asInstanceOf[EncOperator]) :: Nil
    case logical.EncSort(order, child) =>
      execution.EncSort(order, planLater(child).asInstanceOf[EncOperator]) :: Nil
    case logical.NonObliviousSort(order, child) =>
      execution.NonObliviousSort(order, planLater(child).asInstanceOf[EncOperator]) :: Nil
    case logical.EncJoin(left, right, joinExpr) =>
      logical.Join(left, right, Inner, Some(joinExpr)) match {
        case ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _) =>
          execution.EncSortMergeJoin(
            planLater(left).asInstanceOf[EncOperator],
            planLater(right).asInstanceOf[EncOperator],
            leftKeys, rightKeys, condition) :: Nil
        case _ => Nil
      }
    case logical.NonObliviousJoin(left, right, joinExpr) =>
      logical.Join(left, right, Inner, Some(joinExpr)) match {
        case ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _) =>
          execution.NonObliviousSortMergeJoin(
            planLater(left).asInstanceOf[EncOperator],
            planLater(right).asInstanceOf[EncOperator],
            leftKeys, rightKeys, condition) :: Nil
        case _ => Nil
      }
    case a @ logical.EncAggregate(
      groupingExpressions, aggExpressions, child) =>
      execution.EncAggregate(
        groupingExpressions, aggExpressions, planLater(child).asInstanceOf[EncOperator]) :: Nil
    case a @ logical.NonObliviousAggregate(
      groupingExpressions, aggExpressions, child) =>
      execution.NonObliviousAggregate(
        groupingExpressions, aggExpressions, planLater(child).asInstanceOf[EncOperator]) :: Nil
    case logical.Encrypt(child) =>
      execution.Encrypt(planLater(child)) :: Nil
    case logical.MarkOblivious(child) => planLater(child) :: Nil
    case logical.EncryptedLocalRelation(output, plaintextData) =>
      execution.EncryptedLocalTableScan(output, plaintextData) :: Nil
    case LogicalEncryptedBlockRDD(output, rdd) => PhysicalEncryptedBlockRDD(output, rdd) :: Nil
    case _ => Nil
  }
}
