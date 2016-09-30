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

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object EncOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case logical.EncProject(projectList, child) =>
      execution.EncProject(
        projectList, planLater(child)) :: Nil
    case logical.EncFilter(condition, child) =>
      execution.EncFilter(condition, planLater(child)) :: Nil
    case logical.Permute(child) =>
      execution.Permute(planLater(child)) :: Nil
    case logical.EncSort(order, child) =>
      execution.EncSort(order, planLater(child)) :: Nil
    case logical.NonObliviousSort(order, child) =>
      execution.NonObliviousSort(order, planLater(child)) :: Nil
    case logical.EncJoin(left, right, joinExpr) =>
      Join(left, right, Inner, Some(joinExpr)) match {
        case ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _) =>
          execution.EncSortMergeJoin(
            planLater(left),
            planLater(right),
            leftKeys, rightKeys, condition) :: Nil
        case _ => Nil
      }
    case logical.NonObliviousJoin(left, right, joinExpr) =>
      Join(left, right, Inner, Some(joinExpr)) match {
        case ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _) =>
          execution.NonObliviousSortMergeJoin(
            planLater(left),
            planLater(right),
            leftKeys, rightKeys, condition) :: Nil
        case _ => Nil
      }
    case a @ logical.EncAggregate(
      groupingExpressions, aggExpressions, child) =>
      execution.EncAggregate(
        groupingExpressions, aggExpressions, planLater(child)) :: Nil
    case a @ logical.NonObliviousAggregate(
      groupingExpressions, aggExpressions, child) =>
      execution.NonObliviousAggregate(
        groupingExpressions, aggExpressions, planLater(child)) :: Nil
    case logical.Encrypt(child) =>
      execution.Encrypt(planLater(child)) :: Nil
    case logical.MarkOblivious(child) => planLater(child) :: Nil
    case logical.EncryptedLocalRelation(output, plaintextData) =>
      execution.EncryptedLocalTableScan(output, plaintextData) :: Nil
    case logical.LogicalEncryptedBlockRDD(output, rdd) =>
      execution.PhysicalEncryptedBlockRDD(output, rdd) :: Nil
    case _ => Nil
  }
}
