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

// Helper to handle remote attestation
// 

object RA extends Logging {
  def initRA(sc: SparkContext): Unit = {

    val rdd = sc.parallelize(Seq.fill(sc.defaultParallelism) {()}, 3)
    val intelCert = Utils.findResource("AttestationReportSigningCACert.pem")
    val sp = new SP()

    sp.Init(Utils.sharedKey, intelCert)

    val msg1s = rdd.mapPartitionsWithIndex { (i, _) =>
      val (enclave, eid) = Utils.initEnclave()
      val msg1 = enclave.GenerateReport(eid)
      Iterator((i, (enclave, eid, msg1)))
    }.collect.toMap

    val msg2s = msg1s.mapValues{case (enclave, eid, msg1) => (enclave, eid, sp.ProcessEnclaveReport(msg1))}.map(identity)
    println(sc.defaultParallelism)
    println(rdd.getNumPartitions)
    println(msg2s)

    val attestationResults = rdd.mapPartitionsWithIndex { (i, _) =>
      val (enclave, eid, msg2) = msg2s.get(i).get
      Utils.initEnclave()._1.FinishAttestation(eid, msg2)
      Iterator((eid, true))
    }.collect.toMap

    for ((_, ret) <- attestationResults) {
      if (!ret)
        throw new OpaqueException("Attestation failed")
    }
  }
}
