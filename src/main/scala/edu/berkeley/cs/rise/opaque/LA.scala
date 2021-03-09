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

import Array.concat

// Helper to handle enclave "local attestation" and determine shared key

object LA extends Logging {
  def initLA(sc: SparkContext): Unit = {

    var numExecutors = 1
    if (!sc.isLocal) {
      // For now, I am hard-coding this value to 2, because it does not seem like Spark is picking the value up
      numExecutors = sc.getConf.getInt("spark.executor.instances", 2)
      while (!sc.isLocal && sc.getExecutorMemoryStatus.size < numExecutors) {}
    }

    val rdd = sc.makeRDD(Seq.fill(numExecutors) { () })

    // Obtain public keys
    val msg1s = rdd.mapPartitionsWithIndex { (i, _) =>
      val (enclave, eid) = Utils.initEnclave()
      val msg1 = enclave.GetPublicKey(eid) 
      Iterator((eid, msg1))
    }.collect.toMap

    // Combine all public keys into one large array
    var pkArray = Array[Byte]()
    for ((k,v) <- msg1s) {
      pkArray = concat(pkArray, v)
    }
    
    // Send list of public keys to enclaves
    val encryptedResults = rdd.context.parallelize(Array(pkArray), 1).map { publicKeys =>
      val (enclave, eid) = Utils.initEnclave()
      enclave.GetListEncrypted(eid, publicKeys)
    }.first()


    // Send encrypted secret key to all enclaves
    val msg3s = msg1s.map{case (eid, _) => (eid, encryptedResults)}

    val setSharedKeyResults = rdd.mapPartitionsWithIndex { (_, _) =>
      val (enclave, eid) = Utils.initEnclave()
      enclave.FinishSharedKey(eid, msg3s(eid))
      Iterator((eid, true))
    }.collect.toMap

    for ((_, ret) <- setSharedKeyResults) {
      if (!ret)
        throw new OpaqueException("Failed to set shared key")
    }
  }
}
