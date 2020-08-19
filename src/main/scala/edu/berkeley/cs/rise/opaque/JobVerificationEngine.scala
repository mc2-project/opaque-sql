
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

  def resetForNextJob(): Unit = {
    sparkOperators.clear
    logEntryChains.clear
  }

  def verify(): Boolean = {
    // Check that all LogEntryChains have been added to logEntries
    // Piece together the sequence of operations / data movement
    // println("===================Expected sequence of operators: " + sparkOperators)
    // println("Log Entry Chains Length: " + logEntryChains.length)
    if (sparkOperators.isEmpty) {
      resetForNextJob()
      return true
    }

    val numPartitions = logEntryChains.length

    // Count number of ecalls in "past entries" per partition and ensure they're the same
    var numEcallsInFirstPartition = -1
    var lastOpInPastEntry = ""
  
    var startingJobIdMap = Map[Int, Int]()
    var partitionId = 0

    var findRangeBoundsJobIds = ArrayBuffer[Int]()


    for (logEntryChain <- logEntryChains) {
      var startingJobId = -1
      var minJobId = 9999999
      var maxJobId = -9999999

      // var numExpectedFindRangeBoundsEcalls = 0

      var numEcallsLoggedInThisPartition = 0

      // We expect the same number of FindRangeBounds as PartitionForSorts

      println("===================new partition")
      for (i <- 0 until logEntryChain.pastEntriesLength) {
        val pastEntry = logEntryChain.pastEntries(i)
        if (pastEntry != Array.empty) {
          if (pastEntry.jobId < minJobId) {
            minJobId = pastEntry.jobId
          }
          if (pastEntry.jobId > maxJobId) {
            maxJobId = pastEntry.jobId
          }
          // if (partitionId == 0 && pastEntry.op == "findRangeBounds" && !findRangeBoundsJobIds.contains(pastEntry.jobId)) {
          //   // numExpectedFindRangeBoundsEcalls += 1
          //   findRangeBoundsJobIds.append(pastEntry.jobId)
          // }
          // println(partitionForSortJobIds)
          // println("Ecall: " + pastEntry.op + " at " + pastEntry.jobId)
        }
      }
      val latestJobId = logEntryChain.currEntries(0).jobId
      if (latestJobId < minJobId) {
        minJobId = latestJobId
      }
      if (latestJobId > maxJobId) {
        maxJobId = latestJobId
      }
      // println("Ecall: " + logEntryChain.currEntries(0).op)
      numEcallsLoggedInThisPartition = maxJobId - minJobId + 1
      startingJobId = minJobId
      // println("Starting job id: " + startingJobId)

      if (numEcallsInFirstPartition == -1) {
        numEcallsInFirstPartition = numEcallsLoggedInThisPartition
      }
      // println("findRangeBounds calls: " + findRangeBoundsJobIds.length)
      // if (partitionId > 0 && numEcallsInFirstPartition - findRangeBoundsJobIds.length != numEcallsLoggedInThisPartition) {
      if (numEcallsInFirstPartition != numEcallsLoggedInThisPartition) {
        println("This partition num ecalls: " + numEcallsLoggedInThisPartition)
        println("last partition num ecalls: " + numEcallsInFirstPartition)
        throw new Exception("All partitions did not perform same number of ecalls")
      }
      startingJobIdMap(partitionId) = startingJobId
      partitionId += 1
    }

    var numEcalls = numEcallsInFirstPartition 
    // println("Num Partitions: " + numPartitions)
    // println("Num Ecalls: " + numEcalls)

    val numEcallsPlusOne = numEcalls + 1
    var executedAdjacencyMatrix = Array.ofDim[Int](numPartitions * (numEcalls + 1), numPartitions * (numEcalls + 1))
    // var ecallSeq = ArrayBuffer[String]()
    var ecallSeq = Array.fill[String](numEcalls)("unknown")

    var this_partition = 0

    for (logEntryChain <- logEntryChains) {
      var prevOp = ""
      var prevJobId = -1
      // println("past entries length: " + logEntryChain.pastEntriesLength)
      for (i <- 0 until logEntryChain.pastEntriesLength) {
        val logEntry = logEntryChain.pastEntries(i)
        val op = logEntry.op
        // println("Logged Ecall: " + op)
        val eid = logEntry.eid
        // println("EID: " + eid)
        val jobId = logEntry.jobId
        val rcvEid = logEntry.rcvEid
        // println("Log Entry Job ID: " + logEntry.jobId)
        // println("starting job id: " + startingJobId)
        val ecallIndex = logEntry.jobId - startingJobIdMap(rcvEid)

        ecallSeq(ecallIndex) = op

        val prev_partition = eid
        // println("Prev partition: " + prev_partition)
        // println("This partition: " + this_partition)

        val row = prev_partition * (numEcallsPlusOne) + ecallIndex 
        val col = rcvEid * (numEcallsPlusOne) + ecallIndex + 1
        // println("Row: " + row + "Col: " + col)

        executedAdjacencyMatrix(row)(col) = 1
        // if (jobId != prevJobId) {
          // ecallSeq.append(op)
        // }
        // prevJobId = jobId
      }

      // println("Curr entries length: " + logEntryChain.currEntriesLength)
      for (i <- 0 until logEntryChain.currEntriesLength) {
        val logEntry = logEntryChain.currEntries(i)
        val op = logEntry.op
        // println("Ecall: " + op)
        val eid = logEntry.eid
        val jobId = logEntry.jobId
        // val this_partition = logEntry.rcvEid
        val ecallIndex = jobId - startingJobIdMap(this_partition)
        // println("Log Entry Job ID: " + logEntry.jobId)
        // println("Starting JOb ID: " + startingJobId)
        // println("Ecall index: " + ecallIndex)

        ecallSeq(ecallIndex) = op
        // println("Ecall index: " + ecallIndex)

        val prev_partition = eid

        // println("Prev partition: " + prev_partition)
        // println("This partition: " + this_partition)
        // println("Prev partition: " + prev_partition)

        // TODO: CHECK FOR BUG HERE WITH AGGREGATE
        val row = prev_partition * (numEcallsPlusOne) + ecallIndex 
        val col = this_partition * (numEcallsPlusOne) + ecallIndex + 1

        executedAdjacencyMatrix(row)(col) = 1
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
      // Below 4 lines for debugging
      println("Expected Ecall Seq")
      expectedEcallSeq foreach { row => row foreach print; println }
      println("Ecall seq") 
      ecallSeq foreach { row => row foreach print; println }
      return false
    }

    for (i <- 0 until expectedEcallSeq.length) {
      // i represents the current ecall index
      val operator = expectedEcallSeq(i)
      if (operator == "project") {
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
        }
      } else if (operator == "filter") {
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
        }
      } else if (operator == "externalSort") {
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
        }
      } else if (operator == "sample") {
        for (j <- 0 until numPartitions) {
          // All EncryptedBlocks resulting from sample go to one worker
          // FIXME: which partition?
          expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(0 * numEcallsPlusOne + i + 1) = 1
        }
      } else if (operator == "findRangeBounds") {
        // Broadcast from one partition (assumed to be partition 0) to all partitions
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(0 * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
        }
      } else if (operator == "partitionForSort") {
        // All to all shuffle
        for (j <- 0 until numPartitions) {
          for (k <- 0 until numPartitions) {
            expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(k * numEcallsPlusOne + i + 1) = 1
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
          expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(prev * numEcallsPlusOne + i + 1) = 1
          expectedAdjacencyMatrix(j* numEcallsPlusOne + i)(next * numEcallsPlusOne + i + 1) = 1
        }
      } else if (operator == "nonObliviousAggregateStep2") {
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
        }
      } else if (operator == "scanCollectLastPrimary") {
          // Blocks sent to next partition
        for (j <- 0 until numPartitions) {
          var next = j + 1
          if (j == numPartitions - 1) {
            next = numPartitions - 1
          }
          expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(next * numEcallsPlusOne + i + 1) = 1
        }
      } else if (operator == "nonObliviousSortMergeJoin") {
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
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

    for (i <- 0 until numPartitions * (numEcalls + 1); j <- 0 until numPartitions * (numEcalls + 1)) {
      if (expectedAdjacencyMatrix(i)(j) != executedAdjacencyMatrix(i)(j)) {
        return false
      }
    }
    return true
  }
}
