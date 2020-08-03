
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
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext


object JobVerficationEngine {
  var logEntries = ArrayBuffer[tuix.LogEntryChain]()

  def addLogEntryChain(logEntryChain: tuix.LogEntryChain) {
    logEntries += logEntryChain 
  }

  def verify(): Boolean = {
    // Check that all LogEntryChains have been added to logEntries
    // Piece together the sequence of operations / data movement
    // Retrieve the physical plan from df.explain()
    // Condense the physical plan to match ecall operations
    // Return whether everything checks out
  }
}
