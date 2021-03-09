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
  def initRA(sc: SparkContext): Unit = {

    var numExecutors = 1
    if (!sc.isLocal) {
      numExecutors = sc.getConf.getInt("spark.executor.instances", -1)
    }
    val rdd = sc.parallelize(Seq.fill(numExecutors) { () }, numExecutors)

    val intelCert = Utils.findResource("AttestationReportSigningCACert.pem")
    val sp = new SP()

    sp.Init(Utils.sharedKey, intelCert)

    val (numUnattested, numAttested) = Utils.getAttestationCounters()

    // Runs on executors
    val msg1s = rdd
      .mapPartitions { (_) =>
        val (eid, msg1) = Utils.generateReport()
        Iterator((eid, msg1))
      }
      .collect
      .toMap

    // Runs on driver
    val msg2s = msg1s.collect { case (eid, Some(msg1: Array[Byte])) =>
      (eid, sp.ProcessEnclaveReport(msg1))
    }

    // Runs on executors
    val attestationResults = rdd
      .mapPartitions { (_) =>
        val (enclave, eid) = Utils.finishAttestation(numAttested, msg2s)
        Iterator((eid, true))
      }
      .collect
      .toMap

    for ((_, ret) <- attestationResults) {
      if (!ret)
        throw new OpaqueException("Attestation failed")
    }

    logInfo("Attestation successfully completed")
  }

  def run(sc: SparkContext): Unit = {
    while (true) {
      // A loop that repeatedly tries to call initRA if new enclaves are added
      if (Utils.numUnattested.value != Utils.numAttested.value) {
        logInfo(
          s"RA.run: ${Utils.numUnattested.value} unattested, ${Utils.numAttested.value} attested"
        )
        initRA(sc)
      }
      Thread.sleep(10)
    }
  }
}
