
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
import scala.collection.mutable.Stack

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlan

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
    if (this.outgoingNeighbors.length == 0 && !this.isSink) {
      throw new Exception("DAG is not well formed - non sink node has 0 outgoing neighbors.")
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

  def printNode() = {
    println("====")
    print("Ecall: ")
    println(this.ecall)
    print("Output: ")
    for (i <- 0 until this.allOutputsMac.length) {
      print(this.allOutputsMac(i))
    }
    println
    println("===")
  }
}

// Used in construction of expected DAG.
class OperatorNode(val operatorName: String = "") {
  var children: ArrayBuffer[OperatorNode] = ArrayBuffer[OperatorNode]()
  var parents: ArrayBuffer[OperatorNode] = ArrayBuffer[OperatorNode]()
  // Contains numPartitions * numEcalls job nodes.
  // numPartitions rows (outer array), numEcalls columns (inner array)
  var jobNodes: ArrayBuffer[ArrayBuffer[JobNode]] = ArrayBuffer[ArrayBuffer[JobNode]]() 

  def addChild(child: OperatorNode) = {
    this.children.append(child)
  }

  def addParent(parent: OperatorNode) = {
    this.parents.append(parent)
  }

  def isOrphan(): Boolean = {
    return this.parents.isEmpty
  }

  def printOperatorTree(offset: Int): Unit = {
    print(" "*offset)
    println(this.operatorName)
    for (child <- this.children) {
      child.printOperatorTree(offset + 4)
    }
  }

  def printOperatorTree(): Unit = {
    this.printOperatorTree(0)
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
    9 -> "nonObliviousAggregate",
    10 -> "countRowsPerPartition",
    11 -> "computeNumRowsPerPartition",
    12 -> "localLimit",
    13 -> "limitReturnRows"
  ).withDefaultValue("unknown")

  def pathsEqual(executedPaths: ArrayBuffer[List[Seq[Int]]],
                expectedPaths: ArrayBuffer[List[Seq[Int]]]): Boolean = {
    // Executed paths might contain extraneous paths from
    // MACs matching across ecalls if a block is unchanged from ecall to ecall (?)
    return expectedPaths.toSet.subsetOf(executedPaths.toSet)
  }

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

  def operatorDAGFromPlan(executedPlan: String): ArrayBuffer[OperatorNode] = {
    val lines = executedPlan.split("\n")

    // Superstrings must come before substrings, 
    // or inner the for loop will terminate when it finds an instance of the substring.
    // (eg. EncryptedSortMergeJoin before EncryptedSort)
    val possibleOperators = ArrayBuffer[String]("EncryptedProject", 
                                                "EncryptedSortMergeJoin", 
                                                "EncryptedSort", 
                                                "EncryptedFilter",
                                                "EncryptedAggregate",
                                                "EncryptedGlobalLimit",
                                                "EncryptedLocalLimit")
    val operatorStack = Stack[(Int, OperatorNode)]()
    val allOperatorNodes = ArrayBuffer[OperatorNode]()
    for (line <- lines) {
      // Only one operator per line, so terminate as soon as one is found so
      // no line creates two operator nodes because of superstring/substring instances.
      // eg. EncryptedSort and EncryptedSortMergeJoin
      var found = false
      for (sparkOperator <- possibleOperators) {
        if (!found) {
          val index = line indexOf sparkOperator
          if (index != -1) {
            found = true
            val newOperatorNode = new OperatorNode(sparkOperator)
            allOperatorNodes.append(newOperatorNode)
            if (operatorStack.isEmpty) {
              operatorStack.push( (index, newOperatorNode) )
            } else {
              if (index > operatorStack.top._1) {
                operatorStack.top._2.addParent(newOperatorNode)
                operatorStack.push( (index, newOperatorNode) )
              } else {
                while (index <= operatorStack.top._1) {
                  operatorStack.pop
                }
                operatorStack.top._2.addParent(newOperatorNode)
                operatorStack.push( (index, newOperatorNode) )
              }
            }        
          }
        }
      }
    }

    for (operatorNode <- allOperatorNodes) {
      for (parent <- operatorNode.parents) {
        parent.addChild(operatorNode)
      }
    }
    return allOperatorNodes
  }

  def linkEcalls(parentEcalls: ArrayBuffer[JobNode], childEcalls: ArrayBuffer[JobNode]): Unit = {
    if (parentEcalls.length != childEcalls.length) {
      println("Ecall lengths don't match! (linkEcalls)")
    }
    val numPartitions = parentEcalls.length
    val ecall = parentEcalls(0).ecall
    // project
    if (ecall == 1) {
      for (i <- 0 until numPartitions) {
        parentEcalls(i).addOutgoingNeighbor(childEcalls(i))
      }
    // filter
    } else if (ecall == 2) {
      for (i <- 0 until numPartitions) {
        parentEcalls(i).addOutgoingNeighbor(childEcalls(i))
      }
    // externalSort
    } else if (ecall == 6) {
      for (i <- 0 until numPartitions) {
        parentEcalls(i).addOutgoingNeighbor(childEcalls(i))
      }
    // sample
    } else if (ecall == 3) {
      for (i <- 0 until numPartitions) {
        parentEcalls(i).addOutgoingNeighbor(childEcalls(0))
      }
    // findRangeBounds
    } else if (ecall == 4) {
      for (i <- 0 until numPartitions) {
        parentEcalls(0).addOutgoingNeighbor(childEcalls(i))
      }
    // partitionForSort
    } else if (ecall == 5) {
      // All to all shuffle
      for (i <- 0 until numPartitions) {
        for (j <- 0 until numPartitions) {
          parentEcalls(i).addOutgoingNeighbor(childEcalls(j))
        }
      }
    // nonObliviousAggregate
    } else if (ecall == 9) {
      for (i <- 0 until numPartitions) {
        parentEcalls(i).addOutgoingNeighbor(childEcalls(i))
      }
    // nonObliviousSortMergeJoin
    } else if (ecall == 8) {
      for (i <- 0 until numPartitions) {
        parentEcalls(i).addOutgoingNeighbor(childEcalls(i))
      }
    // countRowsPerPartition
    } else if (ecall == 10) {
      // Send from all partitions to partition 0
      for (i <- 0 until numPartitions) {
        parentEcalls(i).addOutgoingNeighbor(childEcalls(0))
      }
    // computeNumRowsPerPartition
    } else if (ecall == 11) {
      // Broadcast from one partition (assumed to be partition 0) to all partitions
      for (i <- 0 until numPartitions) {
        parentEcalls(0).addOutgoingNeighbor(childEcalls(i))
      }
    // limitReturnRows
    } else if (ecall == 13) {
      for (i <- 0 until numPartitions) {
        parentEcalls(i).addOutgoingNeighbor(childEcalls(i))
      }
    } else {
      throw new Exception("Job Verification Error creating expected DAG: "
        + "ecall not supported - " + ecall)
    }
  }

  def getJobNodes(numPartitions: Int, operatorName: String): ArrayBuffer[ArrayBuffer[JobNode]] = {
    val jobNodes = ArrayBuffer[ArrayBuffer[JobNode]]() 
    val expectedEcalls = ArrayBuffer[Int]()
    if (operatorName == "EncryptedSort" && numPartitions == 1) {
      // ("externalSort")
      expectedEcalls.append(6)
    } else if (operatorName == "EncryptedSort" && numPartitions > 1) {
      // ("sample", "findRangeBounds", "partitionForSort", "externalSort")
      expectedEcalls.append(3, 4, 5, 6)
    } else if (operatorName == "EncryptedProject") {
      // ("project")
      expectedEcalls.append(1)
    } else if (operatorName == "EncryptedFilter") {
      // ("filter")
      expectedEcalls.append(2)
    } else if (operatorName == "EncryptedAggregate") {
      // ("nonObliviousAggregate")
      expectedEcalls.append(9)
    } else if (operatorName == "EncryptedSortMergeJoin") {
      // ("nonObliviousSortMergeJoin")
      expectedEcalls.append(8)
    } else if (operatorName == "EncryptedLocalLimit") {
      // ("limitReturnRows")
      expectedEcalls.append(13)
    } else if (operatorName == "EncryptedGlobalLimit") {
      // ("countRowsPerPartition", "computeNumRowsPerPartition", "limitReturnRows")
      expectedEcalls.append(10, 11, 13)
    } else {
      throw new Exception("Executed unknown operator: " + operatorName) 
    }
    for (ecallIdx <- 0 until expectedEcalls.length) {
      val ecall = expectedEcalls(ecallIdx)
      val ecallJobNodes = ArrayBuffer[JobNode]()
      jobNodes.append(ecallJobNodes)
      for (partitionIdx <- 0 until numPartitions) { 
        val jobNode = new JobNode()
        jobNode.setEcall(ecall)
        ecallJobNodes.append(jobNode)
      }
    }
    return jobNodes
  }

  def expectedDAGFromOperatorDAG(operatorNodes: ArrayBuffer[OperatorNode]): JobNode = {
    val source = new JobNode()
    val sink = new JobNode()
    source.setSource
    sink.setSink
    // For each node, create numPartitions * numEcalls jobnodes.
    for (node <- operatorNodes) {
      node.jobNodes = getJobNodes(logEntryChains.size, node.operatorName)
    }
    // Link all ecalls.
    for (node <- operatorNodes) {
      for (ecallIdx <- 0 until node.jobNodes.length) {
        if (ecallIdx == node.jobNodes.length - 1) {
          // last ecall of this operator, link to child operators if one exists.
          for (child <- node.children) {
            linkEcalls(node.jobNodes(ecallIdx), child.jobNodes(0))
          }
        } else {
          linkEcalls(node.jobNodes(ecallIdx), node.jobNodes(ecallIdx + 1))
        }
      }
    }
    // Set source and sink
    for (node <- operatorNodes) {
      if (node.isOrphan) {
        for (jobNode <- node.jobNodes(0)) {
          source.addOutgoingNeighbor(jobNode)
        }
      }
      if (node.children.isEmpty) {
        for (jobNode <- node.jobNodes(node.jobNodes.length - 1)) {
          jobNode.addOutgoingNeighbor(sink)
        }
      }
    }
    return source
  }

  def expectedDAGFromPlan(executedPlan: SparkPlan): JobNode = {
    val operatorDAGRoot = operatorDAGFromPlan(executedPlan.toString)
    expectedDAGFromOperatorDAG(operatorDAGRoot)
  }

  def verify(df: DataFrame): Boolean = {
    if (sparkOperators.isEmpty) {
      return true
    }
    val OE_HMAC_SIZE = 32    

    // Keep a set of nodes, since right now, the last nodes won't have outputs.
    val nodeSet = Set[JobNode]()
    // Set up map from allOutputsMAC --> JobNode.    
    val outputsMap = Map[ArrayBuffer[Byte], JobNode]()
    for (logEntryChain <- logEntryChains) {
      // Create job node for last ecall.
      val logEntry = logEntryChain.currEntries(0)
      val inputMacs = ArrayBuffer[ArrayBuffer[Byte]]()
      val allOutputsMac = ArrayBuffer[Byte]()
      // (TODO): add logMac and allOutputsMac to last crumb.
      for (j <- 0 until logEntry.numInputMacs) {
        inputMacs.append(ArrayBuffer[Byte]())
        for (k <- 0 until OE_HMAC_SIZE) {
          inputMacs(j).append(logEntry.inputMacs(j * OE_HMAC_SIZE + k).toByte)
        }
      }
      val lastJobNode = new JobNode(inputMacs, logEntry.numInputMacs,
                                    allOutputsMac, logEntry.ecall)
      nodeSet.add(lastJobNode)

      // Create job nodes for all ecalls before last for this partition.
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
        nodeSet.add(jobNode)
      }
    }

    // For each node, check that allOutputsMac is computed correctly.
    for (node <- nodeSet) {
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
    for (node <- nodeSet) {
      if (node.inputMacs == ArrayBuffer[ArrayBuffer[Byte]]()) {
        executedSourceNode.addOutgoingNeighbor(node)
      } else {
        for (i <- 0 until node.numInputMacs) {
          val parentNode = outputsMap(node.inputMacs(i))
          parentNode.addOutgoingNeighbor(node)
        }
      }
    }
    for (node <- nodeSet) {
      if (node.outgoingNeighbors.length == 0) {
        node.addOutgoingNeighbor(executedSinkNode)
      }
    }

    // ========================================== //

    // Get expected DAG
    val expectedSourceNode = expectedDAGFromPlan(df.queryExecution.executedPlan)

    val executedPathsToSink = executedSourceNode.pathsToSink
    val expectedPathsToSink = expectedSourceNode.pathsToSink
    val arePathsEqual = pathsEqual(executedPathsToSink, expectedPathsToSink)
    if (!arePathsEqual) {
      // println(executedPathsToSink.toString)
      // println(expectedPathsToSink.toString)
      println("===========DAGS NOT EQUAL===========")
    }
    return true 
  }
}
