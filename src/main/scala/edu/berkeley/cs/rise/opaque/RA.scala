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

import edu.berkeley.cs.rise.opaque.execution.SGXEnclave
import edu.berkeley.cs.rise.opaque.execution.SP

// Performs remote attestation for all executors
// that have not been attested yet

object RA extends Logging {
  def performRA(sc: SparkContext): Unit = {

    val rdd = sc.parallelize(Seq.fill(sc.defaultParallelism) {()}, sc.defaultParallelism)
    val intelCert = Utils.findResource("AttestationReportSigningCACert.pem")
    val sp = new SP()

    sp.Init(Utils.sharedKey, intelCert)

    // Runs on executors
    val msg1s = rdd.mapPartitionsWithIndex { (i, _) =>
      val (enclave, eid) = Utils.initEnclave()
      val msg1 = enclave.GenerateReport(eid)
      Iterator((i, (eid, msg1)))
    }.collect.toMap

    // Runs on driver
    val msg2s = msg1s.mapValues{case (eid, msg1) => (eid, sp.ProcessEnclaveReport(msg1))}.map(identity)

    // Runs on executors
    val attestationResults = rdd.mapPartitionsWithIndex { (i, _) =>
      val (eid, msg2) = msg2s(i)
      if (msg2 != null) { // Shared key has not been set for this executor
        val enclave = new SGXEnclave()
        enclave.FinishAttestation(eid, msg2)
      }
      Iterator((i, true))
    }.collect.toMap

    for ((_, ret) <- attestationResults) {
      if (!ret)
        throw new OpaqueException("Attestation failed")
    }
  }
}
