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
import java.nio.file.{Files, Paths}

import edu.berkeley.cs.rise.opaque.execution.SP
import edu.berkeley.cs.rise.opaque.execution.SGXEnclave

// Performs remote attestation for all executors
// that have not been attested yet

object RA extends Logging {
  def initRA(sc: SparkContext): Unit = {

    // All executors need to be initialized before attestation can occur
    var numExecutors = 1
    if (!sc.isLocal) {
      numExecutors = sc.getConf.getInt("spark.executor.instances", -1)
      while (!sc.isLocal && sc.getExecutorMemoryStatus.size < numExecutors) {}
    }

    val rdd = sc.parallelize(Seq.fill(numExecutors) {()}, numExecutors)
    val intelCert = Utils.findResource("AttestationReportSigningCACert.pem")

    // FIXME: hardcoded path
    val userCert = scala.io.Source.fromFile("/home/opaque/opaque/user1.crt").mkString

    val testKey: Array[Byte] = "Opaque deve key2".getBytes("UTF-8")
    
    Utils.addClientKey(testKey)

    val sp = new SP()

    sp.Init(Utils.clientKey, intelCert, userCert, testKey)

    println(Utils)
    println(rdd.getNumPartitions)

    // Runs on executors
    val msg1s = rdd.mapPartitions { (_) =>
      val (enclave, eid) = Utils.initEnclave()
     
      // Print utils and enclave address to ascertain different enclaves
      println(Utils)
      println(eid)

      val msg1 = enclave.GenerateReport(eid) 
      Iterator((eid, msg1))
    }.collect.toMap

    // Runs on driver
    val msg2s = msg1s.map{case (eid, msg1) => (eid, sp.ProcessEnclaveReport(msg1))}
    msg1s.map{case (eid, msg1) => (eid, print(eid + "\n"))}

    // Runs on executors
    val attestationResults = rdd.mapPartitions { (_) =>
      val (enclave, eid) = Utils.initEnclave()
      val msg2 = msg2s(eid)
      enclave.FinishAttestation(eid, msg2)
      Iterator((eid, true))
    }.collect.toMap

    for ((_, ret) <- attestationResults) {
      if (!ret)
        throw new OpaqueException("Attestation failed")
    }
  }
}
