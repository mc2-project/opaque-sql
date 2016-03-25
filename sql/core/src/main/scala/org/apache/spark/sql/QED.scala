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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.types.DataType

import sun.misc.{BASE64Encoder, BASE64Decoder}

object QED {
  System.loadLibrary("SGXEnclave")

  val sgx = new SGXEnclave()
  val encoder = new BASE64Encoder()
  val decoder = new BASE64Decoder()

  val predicates = mutable.Map[Int, (InternalRow) => Boolean]()
  var nextPredicateId = 0

  // TODO
  def enclaveRegisterPredicate(pred: Expression, inputSchema: Seq[Attribute]): Int = {
    val curPredicateId = nextPredicateId
    nextPredicateId += 1
    predicates(curPredicateId) = GeneratePredicate.generate(pred, inputSchema)
    curPredicateId
  }

  // TODO
  def enclaveEvalPredicate(
      predOpcode: Int, row: InternalRow, schema: Seq[DataType]): Boolean = {
    predicates(predOpcode)(row)

    // turn base64 into binary representation first
    for (v <- row.toSeq(schema)) {
      
    }
  }

  def encodeData(value: Array[Byte]): String = {
    val encoded = encoder.encode(value)
    encoded
  }

  def decodeData(value: String): Array[Byte] = {
    val decoded = decoder.decodeBuffer(value)
    decoded
  }

  def genAndWriteData() = {
    // write this hard-coded set of columns to text file, in csv format
  }
}
