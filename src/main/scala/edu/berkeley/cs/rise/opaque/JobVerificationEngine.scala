
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

class Crumb(val input_macs: ArrayBuffer[ArrayBuffer[Byte]],
            val num_input_macs: Int,
            val all_outputs_mac: ArrayBuffer[Byte],
            val ecall: Int) {

  // Checks if Crumb originates from same partition (?)
  override def equals(that: Any): Boolean = {
    that match {
      case that: Crumb => {
        input_macs == that.input_macs &&
        num_input_macs == that.num_input_macs &&
        all_outputs_mac == that.all_outputs_mac &&
        ecall == that.ecall
      }
      case _ => false
    }     
  }

  override def hashCode(): Int = {
    input_macs.hashCode ^ all_outputs_mac.hashCode
  }
}

class JobNode(var crumb: Crumb) {

  var outgoingNeighbors: ArrayBuffer[JobNode] = ArrayBuffer[JobNode]()
  
  def addOutgoingNeighbor(neighbor: JobNode) = {
    outgoingNeighbors.append(neighbor)
  }

  override def equals(that: Any): Boolean = {
    that match {
      case that: JobNode => {
        this.crumb == that.crumb
      }
      case _ => false
    }
  }

  override def hashCode(): Int = {
    this.crumb.hashCode
  }
}

object JobVerificationEngine {
  // An LogEntryChain object from each partition
  var logEntryChains = ArrayBuffer[tuix.LogEntryChain]()
  var sparkOperators = ArrayBuffer[String]()
  val ecallId = Map(
    1 -> "project",
    2 -> "filter",
    3 -> "sample",
    4 -> "findRangeBounds",
    5 -> "partitionForSort",
    6 -> "externalSort",
    7 -> "scanCollectLastPrimary",
    8 -> "nonObliviousSortMergeJoin",
    9 -> "nonObliviousAggregateStep1",
    10 -> "nonObliviousAggregateStep2",
    11 -> "countRowsPerPartition",
    12 -> "computeNumRowsPerPartition",
    13 -> "localLimit",
    14 -> "limitReturnRows"
  ).withDefaultValue("unknown")

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
    val OE_HMAC_SIZE = 32    
    val numPartitions = logEntryChains.length

    // Check that each partition performed the same number of ecalls and
    // initialize crumb set.
    var numEcallsInFirstPartition = -1
    val ecallSet = Set[Int]()
    val crumbSet = Set[Crumb]()
    val crumbMap = Map[ArrayBuffer[Byte], Crumb]()

    for (logEntryChain <- logEntryChains) {
      val logEntryChainEcalls = Set[Int]()
      var scanCollectLastPrimaryCalled = false
      for (i <- 0 until logEntryChain.pastEntriesLength) {
        val pastEntry = logEntryChain.pastEntries(i)
        val input_macs = ArrayBuffer[ArrayBuffer[Byte]]()
        for (j <- 0 until pastEntry.numInputMacs) {
          input_macs.append(ArrayBuffer[Byte]())
          for (k <- 0 until OE_HMAC_SIZE) {
            input_macs(j).append(pastEntry.inputMacs(j * OE_HMAC_SIZE + k).toByte)
          }
        }
        val all_outputs_mac = ArrayBuffer[Byte]()
        for (j <- 0 until pastEntry.allOutputsMacLength) {
          all_outputs_mac += pastEntry.allOutputsMac(j).toByte
        }
        val crumb = new Crumb(input_macs, pastEntry.numInputMacs,
                                  all_outputs_mac, pastEntry.ecall)
        val ecallNum = crumb.ecall
        if (ecallNum == 7) {
          scanCollectLastPrimaryCalled = true
        }
        ecallSet.add(ecallNum)
        logEntryChainEcalls.add(ecallNum)
        crumbSet.add(crumb)
        crumbMap(all_outputs_mac) = crumb
      }
      if (numEcallsInFirstPartition == -1) {
        numEcallsInFirstPartition = logEntryChainEcalls.size
      }
      if ( (numEcallsInFirstPartition != logEntryChainEcalls.size) && 
           (scanCollectLastPrimaryCalled && 
            numEcallsInFirstPartition + 1 != logEntryChainEcalls.size)
         ) {
        throw new Exception("All partitions did not perform same number of ecalls")
      }
    }
    val numEcalls = ecallSet.size
    val numEcallsPlusOne = numEcalls + 1
    
    // ===== testing =====
    // var crumbTotal = 0
    // for (logEntryChain <- logEntryChains) {
    //   crumbTotal += logEntryChain.pastEntriesLength
    // }
    // println(crumbTotal)
    // println(crumbSet.size)
    // println(crumbMap.size)
    // println("=====")
    // ====================

    // array size: numEcalls
    // map size: numPartitions
    val executedDAG = Map[Int, Map[Crumb, JobNode]]() 
    // Construct executed DAG
    for (crumb <- crumbSet) {
      if (!(executedDAG contains crumb.ecall)) {
        executedDag(crumb.ecall) = Map[Crumb, JobNode]()
      }
      if (!(executedDag(crumb.ecall) contains crumb)) {
        executedDag(crumb.ecall)(crumb) = JobNode(crumb)
      }
    }
    for (crumb <- crumbSet) {
      thisNode = executedDAG(crumb.ecall)(crumb)
      if (crumb.input_macs == ArrayBuffer[ArrayBuffer[Byte]]()) {
        // println("Starter partition detected")
      } else {
        // println(ecallId(crumb.ecall))
        for (i <- 0 until crumb.num_input_macs) {
          val parentCrumb = crumbMap(crumb.input_macs(i))
          val parentNode = executedDAG(crumb.ecall)(crumb)
          parentNode.addOutgoingNeighbor(thisNode)
        }
        // println("===")
      }
    }

    return true
    // val startingJobIdMap = Map[Int, Int]()
    // 
    // val perPartitionJobIds = Array.ofDim[Set[Int]](numPartitions)
    // for (i <- 0 until numPartitions) {
    //   perPartitionJobIds(i) = Set[Int]()
    // } 
    // for (logEntryChain <- logEntryChains) {
    //   for (i <- 0 until logEntryChain.pastEntriesLength) {
    //     val pastEntry = logEntryChain.pastEntries(i)
    //     val partitionOfOperation = pastEntry.sndPid
    //     perPartitionJobIds(partitionOfOperation).add(pastEntry.jobId)
    //   }
    //   val latestJobId = logEntryChain.currEntries(0).jobId
    //   val partitionOfLastOperation = logEntryChain.currEntries(0).sndPid
    //   perPartitionJobIds(partitionOfLastOperation).add(latestJobId)
    // }
    // 
    // // Check that each partition performed the same number of ecalls
    // var numEcallsInFirstPartition = -1
    // for (i <- 0 until perPartitionJobIds.length) {
    //   val partition = perPartitionJobIds(i)
    //   val maxJobId = partition.max
    //   val minJobId = partition.min
    //   val numEcalls = maxJobId - minJobId + 1
    //   if (numEcallsInFirstPartition == -1) {
    //     numEcallsInFirstPartition = numEcalls
    //   }
    // 
    //   if (numEcalls != numEcallsInFirstPartition) {
    //     // Below two lines for debugging
    //     // println("This partition num ecalls: " + numEcalls)
    //     // println("last partition num ecalls: " + numEcallsInFirstPartition)
    //     throw new Exception("All partitions did not perform same number of ecalls")
    //   }
    //   startingJobIdMap(i) = minJobId
    // }
    // 
    // val numEcalls = numEcallsInFirstPartition 
    // val numEcallsPlusOne = numEcalls + 1
    // 
    // val executedAdjacencyMatrix = Array.ofDim[Int](numPartitions * (numEcalls + 1), 
    //   numPartitions * (numEcalls + 1))
    // val ecallSeq = Array.fill[String](numEcalls)("unknown")
    // 
    // var this_partition = 0
    // 
    // for (logEntryChain <- logEntryChains) {
    //   for (i <- 0 until logEntryChain.pastEntriesLength) {
    //     val logEntry = logEntryChain.pastEntries(i)
    //     val ecall = ecallId(logEntry.ecall)
    //     val sndPid = logEntry.sndPid
    //     val jobId = logEntry.jobId
    //     val rcvPid = logEntry.rcvPid
    //     val ecallIndex = jobId - startingJobIdMap(rcvPid)
    // 
    //     ecallSeq(ecallIndex) = ecall
    // 
    //     val row = sndPid * (numEcallsPlusOne) + ecallIndex 
    //     val col = rcvPid * (numEcallsPlusOne) + ecallIndex + 1
    // 
    //     executedAdjacencyMatrix(row)(col) = 1
    //   }
    // 
    //   for (i <- 0 until logEntryChain.currEntriesLength) {
    //     val logEntry = logEntryChain.currEntries(i)
    //     val ecall = ecallId(logEntry.ecall)
    //     val sndPid = logEntry.sndPid
    //     val jobId = logEntry.jobId
    //     val ecallIndex = jobId - startingJobIdMap(this_partition)
    // 
    //     ecallSeq(ecallIndex) = ecall
    // 
    //     val row = sndPid * (numEcallsPlusOne) + ecallIndex 
    //     val col = this_partition * (numEcallsPlusOne) + ecallIndex + 1
    // 
    //     executedAdjacencyMatrix(row)(col) = 1
    //   }
    //   this_partition += 1
    // }
    // 
    // val expectedAdjacencyMatrix = Array.ofDim[Int](numPartitions * (numEcalls + 1), 
    //   numPartitions * (numEcalls + 1))
    // val expectedEcallSeq = ArrayBuffer[String]()
    // for (operator <- sparkOperators) {
    //   if (operator == "EncryptedSortExec" && numPartitions == 1) {
    //     expectedEcallSeq.append("externalSort")
    //   } else if (operator == "EncryptedSortExec" && numPartitions > 1) {
    //     expectedEcallSeq.append("sample", "findRangeBounds", "partitionForSort", "externalSort")
    //   } else if (operator == "EncryptedProjectExec") {
    //     expectedEcallSeq.append("project")
    //   } else if (operator == "EncryptedFilterExec") {
    //     expectedEcallSeq.append("filter")
    //   } else if (operator == "EncryptedAggregateExec") {
    //     expectedEcallSeq.append("nonObliviousAggregateStep1", "nonObliviousAggregateStep2")
    //   } else if (operator == "EncryptedSortMergeJoinExec") {
    //     expectedEcallSeq.append("scanCollectLastPrimary", "nonObliviousSortMergeJoin")
    //   } else if (operator == "EncryptedLocalLimitExec") {
    //     expectedEcallSeq.append("limitReturnRows")
    //   } else if (operator == "EncryptedGlobalLimitExec") {
    //     expectedEcallSeq.append("countRowsPerPartition", "computeNumRowsPerPartition", "limitReturnRows")
    //   } else {
    //     throw new Exception("Executed unknown operator") 
    //   }
    // }
    // 
    // if (!ecallSeq.sameElements(expectedEcallSeq)) {
    //   // Below 4 lines for debugging
    //   // println("===Expected Ecall Seq===")
    //   // expectedEcallSeq foreach { row => row foreach print; println }
    //   // println("===Ecall seq===") 
    //   // ecallSeq foreach { row => row foreach print; println }
    //   return false
    // }
    // 
    // for (i <- 0 until expectedEcallSeq.length) {
    //   // i represents the current ecall index
    //   val operator = expectedEcallSeq(i)
    //   if (operator == "project") {
    //     for (j <- 0 until numPartitions) {
    //       expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
    //     }
    //   } else if (operator == "filter") {
    //     for (j <- 0 until numPartitions) {
    //       expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
    //     }
    //   } else if (operator == "externalSort") {
    //     for (j <- 0 until numPartitions) {
    //       expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
    //     }
    //   } else if (operator == "sample") {
    //     for (j <- 0 until numPartitions) {
    //       // All EncryptedBlocks resulting from sample go to one worker
    //       expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(0 * numEcallsPlusOne + i + 1) = 1
    //     }
    //   } else if (operator == "findRangeBounds") {
    //     // Broadcast from one partition (assumed to be partition 0) to all partitions
    //     for (j <- 0 until numPartitions) {
    //       expectedAdjacencyMatrix(0 * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
    //     }
    //   } else if (operator == "partitionForSort") {
    //     // All to all shuffle
    //     for (j <- 0 until numPartitions) {
    //       for (k <- 0 until numPartitions) {
    //         expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(k * numEcallsPlusOne + i + 1) = 1
    //       }
    //     }
    //   } else if (operator == "nonObliviousAggregateStep1") {
    //     // Blocks sent to prev and next partition
    //     if (numPartitions == 1) {
    //       expectedAdjacencyMatrix(0 * numEcallsPlusOne + i)(0 * numEcallsPlusOne + i + 1) = 1
    //       expectedAdjacencyMatrix(0 * numEcallsPlusOne + i)(0 * numEcallsPlusOne + i + 1) = 1
    //     } else {
    //       for (j <- 0 until numPartitions) {
    //         val prev = j - 1
    //         val next = j + 1
    //         if (j > 0) {
    //           // Send block to prev partition
    //           expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(prev * numEcallsPlusOne + i + 1) = 1
    //         } 
    //         if (j < numPartitions - 1) {
    //           // Send block to next partition
    //           expectedAdjacencyMatrix(j* numEcallsPlusOne + i)(next * numEcallsPlusOne + i + 1) = 1
    //         }
    //       }
    //     }
    //   } else if (operator == "nonObliviousAggregateStep2") {
    //     for (j <- 0 until numPartitions) {
    //       expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
    //     }
    //   } else if (operator == "scanCollectLastPrimary") {
    //     // Blocks sent to next partition
    //     if (numPartitions == 1) {
    //       expectedAdjacencyMatrix(0 * numEcallsPlusOne + i)(0 * numEcallsPlusOne + i + 1) = 1
    //     } else {
    //       for (j <- 0 until numPartitions) {
    //         if (j < numPartitions - 1) {
    //           val next = j + 1
    //           expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(next * numEcallsPlusOne + i + 1) = 1
    //         }
    //       }
    //     }
    //   } else if (operator == "nonObliviousSortMergeJoin") {
    //     for (j <- 0 until numPartitions) {
    //       expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
    //     }
    //   } else if (operator == "countRowsPerPartition") {
    //     // Send from all partitions to partition 0
    //     for (j <- 0 until numPartitions) {
    //       expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(0 * numEcallsPlusOne + i + 1) = 1
    //     }
    //   } else if (operator == "computeNumRowsPerPartition") {
    //     // Broadcast from one partition (assumed to be partition 0) to all partitions
    //     for (j <- 0 until numPartitions) {
    //       expectedAdjacencyMatrix(0 * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
    //     }
    //   } else if (operator == "limitReturnRows") {
    //     for (j <- 0 until numPartitions) {
    //       expectedAdjacencyMatrix(j * numEcallsPlusOne + i)(j * numEcallsPlusOne + i + 1) = 1
    //     }
    //   } else {
    //     throw new Exception("Job Verification Error creating expected adjacency matrix: "
    //       + "operator not supported - " + operator)
    //   }
    // }
    // 
    // for (i <- 0 until numPartitions * (numEcalls + 1); 
    //      j <- 0 until numPartitions * (numEcalls + 1)) {
    //   if (expectedAdjacencyMatrix(i)(j) != executedAdjacencyMatrix(i)(j)) {
    //     // These two println for debugging purposes
    //     // println("Expected Adjacency Matrix: ")
    //     // expectedAdjacencyMatrix foreach { row => row foreach print; println }
    //     
    //     // println("Executed Adjacency Matrix: ")
    //     // executedAdjacencyMatrix foreach { row => row foreach print; println }
    //     return false
    //   }
    // }
  }
}
