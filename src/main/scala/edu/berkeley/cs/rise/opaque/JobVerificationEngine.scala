
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
import scala.collection.mutable.Map
import java.util.Arrays


object JobVerificationEngine {
  // An LogEntryChain object from each partition
  var logEntryChains = ArrayBuffer[tuix.LogEntryChain]()
  var sparkOperators = ArrayBuffer[String]()

  def addLogEntryChain(logEntryChain: tuix.LogEntryChain): Unit = {
    logEntryChains += logEntryChain 
  }

  def addExpectedOperator(operator: String): Unit = {
    sparkOperators += operator
  }

  def resetExpectedOperators(): Unit = {
    sparkOperators.clear
  }

  def verify(): Boolean = {
    // Check that all LogEntryChains have been added to logEntries
    // Piece together the sequence of operations / data movement
    println("Expected sequence of operators: " + sparkOperators)
    var numEcallsInLastPartition = -1
    for (logEntryChain <- logEntryChains) {
      // Check job ID
      val finalLogEntry = logEntryChain.currEntries(0)
      val numEcallsInJob = finalLogEntry.jobId + 1
      if (numEcallsInLastPartition == -1) {
        numEcallsInLastPartition = numEcallsInJob
      }
      if (numEcallsInJob != numEcallsInLastPartition) {
        throw new Exception("All partitions did not perform same number of ecalls")
      }
    }

  
    var numEcalls = numEcallsInLastPartition
    val numPartitions = logEntryChains.length
    println("Num Partitions: " + numPartitions)
    println("Num Ecalls: " + numEcalls)

    var executedAdjacencyMatrix = Array.ofDim[Int](numPartitions * (numEcalls + 1), numPartitions * (numEcalls + 1))
    var ecallSeq = ArrayBuffer[String]()

    // var mapEID = Map[Int, Int]()
    var this_partition = 0

    for (logEntryChain <- logEntryChains) {
      var prevOp = ""
      println("past entries length: " + logEntryChain.pastEntriesLength)
      for (i <- 0 until logEntryChain.pastEntriesLength) {
        val logEntry = logEntryChain.pastEntries(i)
        val op = logEntry.op
        val eid = logEntry.eid
        val jobId = logEntry.jobId

        val prev_partition = eid

        val row = prev_partition * numEcalls + jobId 
        val col = this_partition * numEcalls + jobId + 1

        executedAdjacencyMatrix(row)(col) = 1
        if (op != prevOp) {
          ecallSeq.append(op)
        }
        prevOp = op
      }

      println("Curr entries length: " + logEntryChain.currEntriesLength)
      for (i <- 0 until logEntryChain.currEntriesLength) {
        val logEntry = logEntryChain.currEntries(i)
        val op = logEntry.op
        val eid = logEntry.eid
        val jobId = logEntry.jobId

        val prev_partition = eid

        println("Prev partition: " + prev_partition)
        println("Job ID: " + jobId)

        val row = prev_partition * numEcalls + jobId 
        val col = this_partition * numEcalls + jobId + 1

        // println("Row: " + row + " Col: " + col)
        executedAdjacencyMatrix(row)(col) = 1
        println("Curr Entry Operation: " + op)
        if (op != prevOp) {
          ecallSeq.append(op)
        }
        prevOp = op
      }
      this_partition += 1
    }

    var expectedAdjacencyMatrix = Array.ofDim[Int](numPartitions * (numEcalls + 1), numPartitions * (numEcalls + 1))
    var expectedEcallSeq = ArrayBuffer[String]()
    for (operator <- sparkOperators) {
      if (operator == "EncryptedSortExec" && numPartitions == 1) {
        expectedEcallSeq.append("externalSort")
      } else if (operator == "EncryptedSortExec" && numPartitions > 1) {
        expectedEcallSeq.append("sample", "findRangeBounds", "partitionForSort", "externalSort")
      } else if (operator == "EncryptedProjectExec") {
        expectedEcallSeq.append("project")
      } else if (operator == "EncryptedFilterExec") {
        expectedEcallSeq.append("filter")
      } else if (operator == "EncryptedAggregateExec") {
        expectedEcallSeq.append("nonObliviousAggregateStep1", "nonObliviousAggregateStep2")
      } else if (operator == "EncryptedSortMergeJoinExec") {
        expectedEcallSeq.append("scanCollectLastPrimary", "nonObliviousSortMergeJoin")
      } else if (operator == "EncryptExec") {
        expectedEcallSeq.append("encrypt")
      } else {
        throw new Exception("Executed unknown operator") 
      }
    }

    if (!ecallSeq.sameElements(expectedEcallSeq)) {
      println("Ecall seq") 
      ecallSeq foreach { row => row foreach print; println }
      println("Expected Ecall Seq")
      expectedEcallSeq foreach { row => row foreach print; println }
      return false
    }

    for (i <- 0 until expectedEcallSeq.length) {
      // i represents the current ecall index
      val operator = expectedEcallSeq(i)
      if (operator == "project") {
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(j * numEcalls + i)(j * numEcalls + i + 1) = 1
        }
      } else if (operator == "filter") {
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(j * numEcalls + i)(j * numEcalls + i + 1) = 1
        }
      } else if (operator == "externalSort") {
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(j * numEcalls + i)(j * numEcalls + i + 1) = 1
        }
      } else if (operator == "sample") {
        for (j <- 0 until numPartitions) {
          // All EncryptedBlocks resulting from sample go to one worker
          // FIXME: which partition?
          expectedAdjacencyMatrix(j * numEcalls + i)(0 * numEcalls + i + 1) = 1
        }
      } else if (operator == "findRangeBounds") {
        // Broadcast from one partition (assumed to be partition 0) to all partitions
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(0 * numEcalls + i)(j * numEcalls + i + 1) = 1
        }
      } else if (operator == "partitionForSort") {
        // All to all shuffle
        for (j <- 0 until numPartitions) {
          for (k <- 0 until numPartitions) {
            expectedAdjacencyMatrix(j * numEcalls + i)(k * numEcalls + i + 1) = 1
          }
        }
      } else if (operator == "nonObliviousAggregateStep1") {
        // Blocks sent to prev and next partition
        for (j <- 0 until numPartitions) {
          var prev = j - 1
          var next = j + 1
          if (j == 0) {
            prev = 0
          } 
          if (j == numPartitions - 1) {
            next = numPartitions - 1
          }
          expectedAdjacencyMatrix(j * numEcalls + i)(prev * numEcalls + i + 1) = 1
          expectedAdjacencyMatrix(j* numEcalls + i)(next * numEcalls + i + 1) = 1
        }
      } else if (operator == "nonObliviousAggregateStep2") {
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(j * numEcalls + i)(0 * numEcalls + i + 1) = 1
        }
      } else if (operator == "scanCollectLastPrimary") {
        for (j <- 0 until numPartitions) {
          val next = j + 1
          expectedAdjacencyMatrix(j * numEcalls + i)(next * numEcalls + i + 1) = 1
        }
      } else if (operator == "nonObliviousSortMergeJoin") {
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(j * numEcalls + i)(j * numEcalls + i + 1) = 1
        }
      } else {
        throw new Exception("Job Verification Error creating expected adjacency matrix: operator not supported - " + operator)
      }
    }
  


    
    // Retrieve the physical plan from df.explain()
    // Condense the physical plan to match ecall operations
    // Return whether everything checks out
    println("Expected Adjacency Matrix: ")
    expectedAdjacencyMatrix foreach { row => row foreach print; println }

    println("Executed Adjacency Matrix: ")
    executedAdjacencyMatrix foreach { row => row foreach print; println }

    // if (expectedAdjacencyMatrix sameElements executedAdjacencyMatrix) {
    //   return true
    // } else {
    //   println("False")
    //   return false
    // }
    resetExpectedOperators()
    for (i <- 0 until numPartitions * (numEcalls + 1); j <- 0 until numPartitions * (numEcalls + 1)) {
      if (expectedAdjacencyMatrix(i)(j) != executedAdjacencyMatrix(i)(j)) {
        return false
      }
      // println(expectedAdjacencyMatrix(i)(j) + "==" + executedAdjacencyMatrix(i)(j))
    }
    return true
  }
}
