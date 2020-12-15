
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
import scala.collection.mutable.Queue
import scala.collection.mutable.Set

// Wraps Crumb data specific to graph vertices and adds graph methods.
class JobNode(val inputMacs: ArrayBuffer[ArrayBuffer[Byte]] = ArrayBuffer[ArrayBuffer[Byte]](),
            val numInputMacs: Int = 0,
            val allOutputsMac: ArrayBuffer[Byte] = ArrayBuffer[Byte](),
            val ecall: Int = 0) {

  var outgoingNeighbors: ArrayBuffer[JobNode] = ArrayBuffer[JobNode]()
  var logMacs: ArrayBuffer[ArrayBuffer[Byte]] = ArrayBuffer[ArrayBuffer[Byte]]()

  def addOutgoingNeighbor(neighbor: JobNode) = {
    this.outgoingNeighbors.append(neighbor)
  }

  def addLogMac(logMac: ArrayBuffer[Byte]) = {
    this.logMacs.append(logMac)
  }

  // Run BFS on graph to get ecalls.
  def getEcalls(): ArrayBuffer[Int] = {
    val retval = ArrayBuffer[Int]()
    val queue = Queue[JobNode]()
    queue.enqueue(this)
    while (!queue.isEmpty) {
      val temp = queue.dequeue
      retval.append(temp.ecall)
      for (neighbor <- temp.outgoingNeighbors) {
        queue.enqueue(neighbor)
      }
    }
    return retval
  }

  // Checks if JobNodeData originates from same partition (?)
  override def equals(that: Any): Boolean = {
    that match {
      case that: JobNode => {
        inputMacs == that.inputMacs &&
        numInputMacs == that.numInputMacs &&
        allOutputsMac == that.allOutputsMac &&
        ecall == that.ecall
      }
      case _ => false
    }     
  }

  override def hashCode(): Int = {
    inputMacs.hashCode ^ allOutputsMac.hashCode
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
    val numPartitions = logEntryChains.size
    // Check that each partition performed the same number of ecalls and
    // initialize node set.
    var numEcallsInFirstPartition = -1
    // {all_outputs_mac -> jobNode}
    val outputsMap = Map[ArrayBuffer[Byte], JobNode]()
    for (logEntryChain <- logEntryChains) {
      val logEntryChainEcalls = Set[Int]()
      var scanCollectLastPrimaryCalled = false // Not called on first partition
      for (i <- 0 until logEntryChain.pastEntriesLength) {
        val pastEntry = logEntryChain.pastEntries(i)

        // Copy byte buffers
        val inputMacs = ArrayBuffer[ArrayBuffer[Byte]]()
        val logMac = ArrayBuffer[Byte]()
        val allOutputsMac = ArrayBuffer[Byte]()
        for (j <- 0 until pastEntry.numInputMacs) {
          inputMacs.append(ArrayBuffer[Byte]())
          for (k <- 0 until OE_HMAC_SIZE) {
            inputMacs(j).append(pastEntry.inputMacs(j * OE_HMAC_SIZE + k).toByte)
          }
        }
        for (j <- 0 until pastEntry.logMacLength) {
          logMac += pastEntry.logMac(i).toByte
        }
        for (j <- 0 until pastEntry.allOutputsMacLength) {
          allOutputsMac += pastEntry.allOutputsMac(j).toByte
        }

        // Create or update job node.
        if (!(outputsMap contains allOutputsMac)) {
          outputsMap(allOutputsMac) = new JobNode(inputMacs, pastEntry.numInputMacs,
                                  allOutputsMac, pastEntry.ecall)
        }
        val jobNode = outputsMap(allOutputsMac)
        jobNode.addLogMac(logMac)
        
        // Update ecall set.
        val ecallNum = jobNode.ecall
        if (ecallNum == 7) {
          scanCollectLastPrimaryCalled = true
        }
        logEntryChainEcalls.add(ecallNum)
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

    // Check allOutputsMac is computed correctly.
    for (node <- outputsMap.values) {
      // 
    }

    // Construct executed DAG
    // by setting parent JobNodes for each node.
    var rootNode = new JobNode()
    for (node <- outputsMap.values) {
      if (node.inputMacs == ArrayBuffer[ArrayBuffer[Byte]]()) {
        rootNode.addOutgoingNeighbor(node)
      } else {
        for (i <- 0 until node.numInputMacs) {
          val parentNode = outputsMap(node.inputMacs(i))
          parentNode.addOutgoingNeighbor(node)
        }
      }
    }

    return true
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
