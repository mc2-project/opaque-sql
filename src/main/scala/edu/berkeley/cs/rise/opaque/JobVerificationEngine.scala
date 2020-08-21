
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
import scala.collection.mutable.Set
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
    if (sparkOperators.isEmpty) {
      return true
    }

    val numPartitions = logEntryChains.length
    var startingJobIdMap = Map[Int, Int]()

    var findRangeBoundsJobIds = ArrayBuffer[Int]()
    var perPartitionJobIds = Array.ofDim[Set[Int]](numPartitions)
    for (i <- 0 until numPartitions) {
      perPartitionJobIds(i) = Set[Int]()
    } 

    for (logEntryChain <- logEntryChains) {
      for (i <- 0 until logEntryChain.pastEntriesLength) {
        val pastEntry = logEntryChain.pastEntries(i)
        if (pastEntry != Array.empty) {
          val partitionOfOperation = pastEntry.sndEid
          perPartitionJobIds(partitionOfOperation).add(pastEntry.jobId)
        }
      }
      val latestJobId = logEntryChain.currEntries(0).jobId
      val partitionOfLastOperation = logEntryChain.currEntries(0).sndEid
      perPartitionJobIds(partitionOfLastOperation).add(latestJobId)
    }

    // Check that each partition performed the same number of ecalls
    var numEcallsInFirstPartition = -1
    for (i <- 0 until perPartitionJobIds.length) {
      val partition = perPartitionJobIds(i)
      val maxJobId = partition.max
      val minJobId = partition.min
      val numEcalls = maxJobId - minJobId + 1
      if (numEcallsInFirstPartition == -1) {
        numEcallsInFirstPartition = numEcalls
      }

      if (numEcalls != numEcallsInFirstPartition) {
        println("This partition num ecalls: " + numEcalls)
        println("last partition num ecalls: " + numEcallsInFirstPartition)
        throw new Exception("All partitions did not perform same number of ecalls")
      }
      startingJobIdMap(i) = minJobId
    }

    var numEcalls = numEcallsInFirstPartition 
    val numEcallsPlusOne = numEcalls + 1

    var executedAdjacencyMatrix = Array.ofDim[Int](numPartitions * (numEcalls + 1), numPartitions * (numEcalls + 1))
    var ecallSeq = Array.fill[String](numEcalls)("unknown")

    var this_partition = 0

    for (logEntryChain <- logEntryChains) {
      for (i <- 0 until logEntryChain.pastEntriesLength) {
        val logEntry = logEntryChain.pastEntries(i)
        val op = logEntry.op
        val sndEid = logEntry.sndEid
        val jobId = logEntry.jobId
        val rcvEid = logEntry.rcvEid
        val ecallIndex = logEntry.jobId - startingJobIdMap(rcvEid)

        ecallSeq(ecallIndex) = op

        val row = sndEid * (numEcallsPlusOne) + ecallIndex 
        val col = rcvEid * (numEcallsPlusOne) + ecallIndex + 1

        executedAdjacencyMatrix(row)(col) = 1
      }

      for (i <- 0 until logEntryChain.currEntriesLength) {
        val logEntry = logEntryChain.currEntries(i)
        val op = logEntry.op
        val sndEid = logEntry.sndEid
        val jobId = logEntry.jobId
        val ecallIndex = jobId - startingJobIdMap(this_partition)

        ecallSeq(ecallIndex) = op

        val row = sndEid * (numEcallsPlusOne) + ecallIndex 
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
        if (numPartitions == 1) {
          expectedAdjacencyMatrix(0 * numEcallsPlusOne + i)(0 * numEcallsPlusOne + i + 1) = 1
          expectedAdjacencyMatrix(0 * numEcallsPlusOne + i)(0 * numEcallsPlusOne + i + 1) = 1
        } else {
          for (j <- 0 until numPartitions) {
            var prev = j - 1
            var next = j + 1
            if (j > 0) {
              // Send block to prev partition
              expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(prev * numEcallsPlusOne + i + 1) = 1
            } 
            if (j < numPartitions - 1) {
              // Send block to next partition
              expectedAdjacencyMatrix(j* numEcallsPlusOne + i)(next * numEcallsPlusOne + i + 1) = 1
            }
          }
        }
      } else if (operator == "nonObliviousAggregateStep2") {
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
        }
      } else if (operator == "scanCollectLastPrimary") {
        // Blocks sent to next partition
        if (numPartitions == 1) {
          expectedAdjacencyMatrix(0 * numEcallsPlusOne + i)(0 * numEcallsPlusOne + i + 1) = 1
        } else {
          for (j <- 0 until numPartitions) {
            if (j < numPartitions - 1) {
              var next = j + 1
              expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(next * numEcallsPlusOne + i + 1) = 1
            }
          }
        }
      } else if (operator == "nonObliviousSortMergeJoin") {
        for (j <- 0 until numPartitions) {
          expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
        }
      } else {
        throw new Exception("Job Verification Error creating expected adjacency matrix: operator not supported - " + operator)
      }
    }

    for (i <- 0 until numPartitions * (numEcalls + 1); j <- 0 until numPartitions * (numEcalls + 1)) {
      if (expectedAdjacencyMatrix(i)(j) != executedAdjacencyMatrix(i)(j)) {
        println("Expected Adjacency Matrix: ")
        expectedAdjacencyMatrix foreach { row => row foreach print; println }

        println("Executed Adjacency Matrix: ")
        executedAdjacencyMatrix foreach { row => row foreach print; println }
        return false
      }
    }
    return true
  }
}
