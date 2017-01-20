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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import edu.berkeley.cs.rise.opaque.execution._
import edu.berkeley.cs.rise.opaque.logical._

object OpaqueOperators extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    // sort(project(join())) -- only when projection can be pushed up
    case ObliviousSort(order, ObliviousProject(projectList, ObliviousJoin(left, right, joinExpr))) =>
      println("=====Matched!!=====")

      Join(left, right, Inner, Some(joinExpr)) match {
        case ExtractEquiJoinKeys(_, leftKeys, rightKeys, condition, _, _) =>
          val (joinOpcode, dummySortOpcode, dummyFilterOpcode) = JoinOpcodeExec(
            planLater(left), planLater(right), leftKeys, rightKeys, condition).getOpcodes()

          val joinPlan = ObliviousFilterExec(
            Literal(-1, IntegerType),
            ObliviousSortOpcodeExec(
              List(dummySortOpcode),
              ObliviousSortMergeJoinExec(
                ObliviousSortOpcodeExec(
                  List(joinOpcode),
                  ObliviousUnionExec(
                    planLater(left),
                    planLater(right),
                    joinOpcode
                  )
                ), joinOpcode, dummySortOpcode, dummyFilterOpcode
              )
            )
          )


          val projectOpcode = ProjectOpcodeExec(projectList, joinPlan).getOpcode()
          val projectPlan = ObliviousProjectExec(projectList, joinPlan)

          val sortOpcode = SortOpcodeExec(order, projectPlan).getOpcode()
          val sortPlan = ObliviousSortExec(order, projectPlan)

          import Opcode._
          val newSortOpcode = (projectOpcode, sortOpcode) match {
            case (OP_PROJECT_COL4_COL2_COL5, OP_SORT_COL1) =>
              OP_SORT_COL3

            case _ =>
              sortOpcode
          }

          if (sortOpcode == newSortOpcode) {
            return sortPlan :: Nil
          }

          // else, construct an optimized plan
          ObliviousProjectExec(projectList,
            ObliviousFilterExec(
              Literal(-1, IntegerType),
              ObliviousSortOpcodeExec(
                List(dummySortOpcode, newSortOpcode),
                ObliviousSortMergeJoinExec(
                  ObliviousSortOpcodeExec(
                    List(joinOpcode),
                    ObliviousUnionExec(
                      planLater(left),
                      planLater(right),
                      joinOpcode
                    )
                  ), joinOpcode, dummySortOpcode, dummyFilterOpcode
                )
              )
            )
          ) :: Nil

        case _ => Nil
      }


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

          val (joinOpcode, dummySortOpcode, dummyFilterOpcode) = JoinOpcodeExec(
            planLater(left), planLater(right), leftKeys, rightKeys, condition).getOpcodes()

          ObliviousFilterExec(
            Literal(-1, IntegerType),
            ObliviousSortOpcodeExec(
              List(dummySortOpcode),
              ObliviousSortMergeJoinExec(
                ObliviousSortOpcodeExec(
                  List(joinOpcode),
                  ObliviousUnionExec(
                    planLater(left),
                    planLater(right),
                    joinOpcode
                  )
                ), joinOpcode, dummySortOpcode, dummyFilterOpcode
              )
            )
          ) :: Nil

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

      val (aggStep1Opcode, aggStep2Opcode, aggDummySortOpcode, aggDummyFilterOpcode) = ObliviousAggregateOpcodeExec(
        groupingExpressions, aggExpressions, planLater(child)).getOpcodes()

      ObliviousFilterExec(
        Literal(-1, IntegerType),
        ObliviousSortOpcodeExec(
          List(aggDummySortOpcode),
          ObliviousAggregateExec(
            groupingExpressions,
            aggExpressions,
            planLater(child)
          )
        )
      ) :: Nil

    case a @ EncryptedAggregate(groupingExpressions, aggExpressions, child) =>
      EncryptedAggregateExec(groupingExpressions, aggExpressions, planLater(child)) :: Nil

    case Encrypt(isOblivious, child) =>
      EncryptExec(isOblivious, planLater(child)) :: Nil

    case EncryptedLocalRelation(output, plaintextData, isOblivious) =>
      EncryptedLocalTableScanExec(output, plaintextData, isOblivious) :: Nil

    case EncryptedBlockRDD(output, rdd, isOblivious) =>
      EncryptedBlockRDDScanExec(output, rdd, isOblivious) :: Nil

    // for rule-based optimization, we want to match sort(filter(sort()))
    // +- ObliviousSort [sourceIP#2676 ASC]
    //    +- ObliviousProject [sourceIP#2676, pageRank#2752, adRevenue#2679]
    //       +- ObliviousFilter -1
    //          +- ObliviousSortOpcode OP_SORT_COL3_IS_DUMMY_COL1

    case _ => Nil
  }
}
