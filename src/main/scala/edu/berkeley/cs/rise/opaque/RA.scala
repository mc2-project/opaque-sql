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

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

import edu.berkeley.cs.rise.opaque.execution.SP

// Performs remote attestation for all executors
// that have not been attested yet

object RA extends Logging {

  var numExecutors : Int = 1
  var loop : Boolean = true

  def initRA(sc: SparkContext): Unit = {

    if (!sc.isLocal) {
      numExecutors = sc.getConf.getInt("spark.executor.instances", -1)
    }
    val rdd = sc.parallelize(Seq.fill(numExecutors) { () }, numExecutors)

    val intelCert = Utils.findResource("AttestationReportSigningCACert.pem")
    val sp = new SP()

    sp.Init(Utils.sharedKey, intelCert)

    val numAttested = Utils.numAttested
    // Runs on executors
    val msg1s = rdd
      .mapPartitions { (_) =>
        val (eid, msg1) = Utils.generateReport()
        Iterator((eid, msg1))
      }
      .collect
      .toMap

    logInfo("Driver collected msg1s")

    // Runs on driver
    val msg2s = msg1s.collect { case (eid, Some(msg1: Array[Byte])) =>
      (eid, sp.ProcessEnclaveReport(msg1))
    }

    logInfo("Driver processed msg2s")

    // Runs on executors
    val attestationResults = rdd
      .mapPartitions { (_) =>
        val (enclave, eid) = Utils.finishAttestation(numAttested, msg2s)
        Iterator((eid, true))
      }
      .collect
      .toMap
  }

  def waitForExecutors(sc: SparkContext): Unit = {
    if (!sc.isLocal) {
      numExecutors = sc.getConf.getInt("spark.executor.instances", -1)
      val rdd = sc.parallelize(Seq.fill(numExecutors) { () }, numExecutors)
      while (
        rdd
          .mapPartitions { (_) =>
            val id = SparkEnv.get.executorId
            Iterator(id)
          }
          .collect
          .toSet
          .size < numExecutors
      ) {}
    }
    logInfo(s"All executors have started, numExecutors is ${numExecutors}")
  }

  // This function is executed in a loop that repeatedly tries to
  // call initRA if new enclaves are added
  // Periodically probe the workers using `startEnclave`
  def attestEnclaves(sc: SparkContext): Unit = {
    // Proactively initialize enclaves
    val rdd = sc.parallelize(Seq.fill(numExecutors) { () }, numExecutors)
    val numEnclavesAcc = Utils.numEnclaves
    rdd.mapPartitions{ (_) =>
      val eid = Utils.startEnclave(numEnclavesAcc)
      Iterator(eid)
    }.count

    if (Utils.numEnclaves.value != Utils.numAttested.value) {
      logInfo(
        s"RA.run: ${Utils.numEnclaves.value} unattested, ${Utils.numAttested.value} attested"
      )
      initRA(sc)
    }
    Thread.sleep(100)
  }

  def startThread(sc: SparkContext) : Unit = {
    val thread = new Thread {
      override def run: Unit = {
        while (loop) {
          RA.attestEnclaves(sc)
        }
      }
    }
    thread.start
  }

  def stopThread() : Unit = {
    loop = false
    Thread.sleep(5000)
  }

}
