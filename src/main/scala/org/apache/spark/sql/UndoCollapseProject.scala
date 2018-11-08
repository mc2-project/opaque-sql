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

package org.apache.spark.sql

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.Aggregate

object UndoCollapseProject {
  /**
   * Undo the CollapseProject rule if it collapsed Project(_, Aggregate) into Aggregate, because
   * our aggregator doesn't support complex expressions.
   *
   * This must be in the org.apache.spark.sql package because AggregateExpression is
   * package-private.
   */
  def separateProjectAndAgg(plan: Aggregate)
    : Option[(Seq[NamedExpression], Seq[NamedExpression])] = plan match {
    case Aggregate(_, aggExprs, _) =>
      // Action is only required if an aggregation expression contains operations beyond just
      // aggregation or copying input attributes to the output.
      val needsSeparation = aggExprs.exists {
        case _: AggregateExpression => false
        case Alias(_: AggregateExpression, _) => false
        case _: AttributeReference => false
        case Alias(_: AttributeReference, _) => false
        case _ => true
      }
      if (needsSeparation) {
        // Extract AggregateExpressions into a separate list and wrap them in Aliases, replacing
        // them with AttributeReferences in the output. Also extract AttributeReferences, since
        // these must be replicated to the aggregate expression list to avoid losing them.
        val pureAggExprs = new ArrayBuffer[NamedExpression]
        val pureProjectExprs: Seq[NamedExpression] =
          aggExprs.map(_.transform {
            case ae: AggregateExpression =>
              val newAE = Alias(ae, "_tmp")()
              pureAggExprs += newAE
              newAE.toAttribute
            case ref: AttributeReference =>
              pureAggExprs += ref
              ref
          }.asInstanceOf[NamedExpression])
        Some((pureProjectExprs, pureAggExprs))
      } else {
        None
      }
  }
}
