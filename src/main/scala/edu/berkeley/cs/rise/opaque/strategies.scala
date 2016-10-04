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
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

import edu.berkeley.cs.rise.opaque.execution._
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

    case ObliviousJoin(left, right, joinExpr) =>
      Join(left, right, Inner, Some(joinExpr)) match {
        case ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _) =>
          ObliviousSortMergeJoinExec(
            planLater(left),
            planLater(right),
            leftKeys, rightKeys, condition) :: Nil
        case _ => Nil
      }
    case EncryptedJoin(left, right, joinExpr) =>
      Join(left, right, Inner, Some(joinExpr)) match {
        case ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _) =>
          EncryptedSortMergeJoinExec(
            planLater(left),
            planLater(right),
            leftKeys, rightKeys, condition) :: Nil
        case _ => Nil
      }

    case a @ ObliviousAggregate(groupingExpressions, aggExpressions, child) =>
      ObliviousAggregateExec(groupingExpressions, aggExpressions, planLater(child)) :: Nil
    case a @ EncryptedAggregate(groupingExpressions, aggExpressions, child) =>
      EncryptedAggregateExec(groupingExpressions, aggExpressions, planLater(child)) :: Nil

    case Encrypt(isOblivious, child) =>
      EncryptExec(isOblivious, planLater(child)) :: Nil

    case EncryptedLocalRelation(output, plaintextData, isOblivious) =>
      EncryptedLocalTableScanExec(output, plaintextData, isOblivious) :: Nil

    case EncryptedBlockRDD(output, rdd, isOblivious) =>
      EncryptedBlockRDDScanExec(output, rdd, isOblivious) :: Nil

    case _ => Nil
  }
}
