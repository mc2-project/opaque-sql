
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

// Wraps Crumb data specific to graph vertices and adds graph methods.
class JobNode(val inputMacs: ArrayBuffer[ArrayBuffer[Byte]] = ArrayBuffer[ArrayBuffer[Byte]](),
            val numInputMacs: Int = 0,
            val allOutputsMac: ArrayBuffer[Byte] = ArrayBuffer[Byte](),
            var ecall: Int = 0) {

  var outgoingNeighbors: ArrayBuffer[JobNode] = ArrayBuffer[JobNode]()
  var logMacs: ArrayBuffer[ArrayBuffer[Byte]] = ArrayBuffer[ArrayBuffer[Byte]]()
  var isSource: Boolean = false
  var isSink: Boolean = false

  def addOutgoingNeighbor(neighbor: JobNode) = {
    this.outgoingNeighbors.append(neighbor)
  }

  def addLogMac(logMac: ArrayBuffer[Byte]) = {
    this.logMacs.append(logMac)
  }

  def setEcall(ecall: Int) = {
    this.ecall = ecall
  }

  def setSource() = {
    this.isSource = true
  }

  def setSink() = {
    this.isSink = true
  }

  // Compute and return a list of paths from this node to a sink node.
  def pathsToSink(): ArrayBuffer[List[Seq[Int]]] = {
    val retval = ArrayBuffer[List[Seq[Int]]]()
    if (this.isSink) {
      return retval
    }
    // This node is directly before the sink and has exactly one path to it
    // (the edge from this node to the sink).
    if (this.outgoingNeighbors.length == 1 && this.outgoingNeighbors(0).isSink) {
      return ArrayBuffer(List(Seq(this.ecall, 0)))
    }
    // Each neighbor has a list of paths to the sink -
    // For every path that exists, prepend the edge from this node to the neighbor.
    // Return all paths collected from all neighbors.
    for (neighbor <- this.outgoingNeighbors) {
      val pred = Seq(this.ecall, neighbor.ecall)
      val restPaths = neighbor.pathsToSink()
      for (restPath <- restPaths) {
        retval.append(pred +: restPath)
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

    // Set up map from allOutputsMAC --> JobNode.
    val outputsMap = Map[ArrayBuffer[Byte], JobNode]()
    for (logEntryChain <- logEntryChains) {
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
      }
    }

    // For each node, check that allOutputsMac is computed correctly.
    for (node <- outputsMap.values) {
      // assert (node.allOutputsMac == mac(concat(node.logMacs)))

      // Unclear what order to arrange log_macs to get the all_outputs_mac
      // Doing numEcalls * (numPartitions!) arrangements seems very bad.
      // See if we can do it more efficiently.
    }

    // Construct executed DAG by setting parent JobNodes for each node.
    val executedSourceNode = new JobNode()
    executedSourceNode.setSource
    val executedSinkNode = new JobNode()
    executedSinkNode.setSink
    for (node <- outputsMap.values) {
      if (node.inputMacs == ArrayBuffer[ArrayBuffer[Byte]]()) {
        executedSourceNode.addOutgoingNeighbor(node)
      } else {
        for (i <- 0 until node.numInputMacs) {
          val parentNode = outputsMap(node.inputMacs(i))
          parentNode.addOutgoingNeighbor(node)
        }
      }
    }
    for (node <- outputsMap.values) {
      if (node.outgoingNeighbors.length == 0) {
        node.addOutgoingNeighbor(executedSinkNode)
      }
    }

    // Construct expected DAG.
    val expectedDAG = ArrayBuffer[ArrayBuffer[JobNode]]()
    val expectedEcalls = ArrayBuffer[Int]()
    for (operator <- sparkOperators) {
      if (operator == "EncryptedSortExec" && numPartitions == 1) {
        // ("externalSort")
        expectedEcalls.append(6)
      } else if (operator == "EncryptedSortExec" && numPartitions > 1) {
        // ("sample", "findRangeBounds", "partitionForSort", "externalSort")
        expectedEcalls.append(3, 4, 5, 6)
      } else if (operator == "EncryptedProjectExec") {
        // ("project")
        expectedEcalls.append(1)
      } else if (operator == "EncryptedFilterExec") {
        // ("filter")
        expectedEcalls.append(2)
      } else if (operator == "EncryptedAggregateExec") {
        // ("nonObliviousAggregateStep1", "nonObliviousAggregateStep2")
        expectedEcalls.append(9, 10)
      } else if (operator == "EncryptedSortMergeJoinExec") {
        // ("scanCollectLastPrimary", "nonObliviousSortMergeJoin")
        expectedEcalls.append(7, 8)
      } else if (operator == "EncryptedLocalLimitExec") {
        // ("limitReturnRows")
        expectedEcalls.append(14)
      } else if (operator == "EncryptedGlobalLimitExec") {
        // ("countRowsPerPartition", "computeNumRowsPerPartition", "limitReturnRows")
        expectedEcalls.append(11, 12, 14)
      } else {
        throw new Exception("Executed unknown operator") 
      }
    }

    // Initialize job nodes.
    val expectedSourceNode = new JobNode()
    expectedSourceNode.setSource
    val expectedSinkNode = new JobNode()
    expectedSinkNode.setSink
    for (j <- 0 until numPartitions) {
      val partitionJobNodes = ArrayBuffer[JobNode]()
      expectedDAG.append(partitionJobNodes)
      for (i <- 0 until expectedEcalls.length) {
        val ecall = expectedEcalls(i)
        val jobNode = new JobNode()
        jobNode.setEcall(ecall)
        partitionJobNodes.append(jobNode)
        // Connect source node to starting ecall partitions.
        if (i == 0) {
          expectedSourceNode.addOutgoingNeighbor(jobNode)
        }
        // Connect ending ecall partitions to sink.
        if (i == expectedEcalls.length - 1) {
          jobNode.addOutgoingNeighbor(expectedSinkNode)
        }
      }
    }
    
    // Set outgoing neighbors for all nodes, except for the ones in the last ecall.
    for (i <- 0 until expectedEcalls.length - 1) {
      // i represents the current ecall index
      val operator = expectedEcalls(i)
      // project
      if (operator == 1) {
        for (j <- 0 until numPartitions) {
          expectedDAG(j)(i).addOutgoingNeighbor(expectedDAG(j)(i + 1))
        }
      // filter
      } else if (operator == 2) {
        for (j <- 0 until numPartitions) {
          expectedDAG(j)(i).addOutgoingNeighbor(expectedDAG(j)(i + 1))
        }
      // externalSort
      } else if (operator == 6) {
        for (j <- 0 until numPartitions) {
          expectedDAG(j)(i).addOutgoingNeighbor(expectedDAG(j)(i + 1))
        }
      // sample
      } else if (operator == 3) {
        for (j <- 0 until numPartitions) {
          // All EncryptedBlocks resulting from sample go to one worker
          expectedDAG(j)(i).addOutgoingNeighbor(expectedDAG(0)(i + 1))
        }
      // findRangeBounds
      } else if (operator == 4) {
        // Broadcast from one partition (assumed to be partition 0) to all partitions
        for (j <- 0 until numPartitions) {
          expectedDAG(0)(i).addOutgoingNeighbor(expectedDAG(j)(i + 1))
        }
      // partitionForSort
      } else if (operator == 5) {
        // All to all shuffle
        for (j <- 0 until numPartitions) {
          for (k <- 0 until numPartitions) {
            expectedDAG(j)(i).addOutgoingNeighbor(expectedDAG(k)(i + 1))
          }
        }
      // nonObliviousAggregateStep1
      } else if (operator == 9) {
        // Blocks sent to prev and next partition
        if (numPartitions == 1) {
          expectedDAG(0)(i).addOutgoingNeighbor(expectedDAG(0)(i + 1))
          expectedDAG(0)(i).addOutgoingNeighbor(expectedDAG(0)(i + 1))
        } else {
          for (j <- 0 until numPartitions) {
            val prev = j - 1
            val next = j + 1
            if (j > 0) {
              // Send block to prev partition
              expectedDAG(j)(i).addOutgoingNeighbor(expectedDAG(prev)(i + 1))
            } 
            if (j < numPartitions - 1) {
              // Send block to next partition
              expectedDAG(j)(i).addOutgoingNeighbor(expectedDAG(next)(i + 1))
            }
          }
        }
      // nonObliviousAggregateStep2
      } else if (operator == 10) {
        for (j <- 0 until numPartitions) {
          expectedDAG(j)(i).addOutgoingNeighbor(expectedDAG(j)(i + 1))
        }
      // scanCollectLastPrimary
      } else if (operator == 7) {
        // Blocks sent to next partition
        if (numPartitions == 1) {
          expectedDAG(0)(i).addOutgoingNeighbor(expectedDAG(0)(i + 1))
        } else {
          for (j <- 0 until numPartitions) {
            if (j < numPartitions - 1) {
              val next = j + 1
              expectedDAG(j)(i).addOutgoingNeighbor(expectedDAG(next)(i + 1))
            }
          }
        }
      // nonObliviousSortMergeJoin
      } else if (operator == 8) {
        for (j <- 0 until numPartitions) {
          expectedDAG(j)(i).addOutgoingNeighbor(expectedDAG(j)(i + 1))
        }
      // countRowsPerPartition
      } else if (operator == 11) {
        // Send from all partitions to partition 0
        for (j <- 0 until numPartitions) {
          expectedDAG(j)(i).addOutgoingNeighbor(expectedDAG(0)(i + 1))
        }
      // computeNumRowsPerPartition
      } else if (operator == 12) {
        // Broadcast from one partition (assumed to be partition 0) to all partitions
        for (j <- 0 until numPartitions) {
          expectedDAG(0)(i).addOutgoingNeighbor(expectedDAG(j)(i + 1))
        }
      // limitReturnRows
      } else if (operator == 14) {
        for (j <- 0 until numPartitions) {
          expectedDAG(j)(i).addOutgoingNeighbor(expectedDAG(j)(i + 1))
        }
      } else {
        throw new Exception("Job Verification Error creating expected DAG: "
          + "operator not supported - " + operator)
      }
    }
    val executedPathsToSink = executedSourceNode.pathsToSink
    val expectedPathsToSink = expectedSourceNode.pathsToSink
    print("DAGs equal: ")
    println(executedPathsToSink.toSet == expectedPathsToSink.toSet)
    return true
  }
}
