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
import edu.berkeley.cs.rise.opaque.execution.SGXEnclave

import Array.concat
import scala.util.Random

// Helper to handle enclave "local attestation" and determine shared key

object LA extends Logging {
  def initLA(sc: SparkContext): Unit = {

    val rdd = sc.makeRDD(Seq.fill(sc.defaultParallelism) { () })

    // Test print Utils.
    println(Utils)
    // Obtain public keys
    val msg1s = rdd.mapPartitionsWithIndex { (i, _) =>
      val (enclave, eid) = Utils.initEnclave()
     
      // Print utils and enclave address to ascertain different enclaves
      println(Utils)
      println(eid)

      val msg1 = enclave.GetPublicKey(eid) 
      Iterator((eid, msg1))
    }.collect.toMap

    // Combine all public keys into one large array
    var pkArray = Array[Byte]()
    for ((k,v) <- msg1s) concat(pkArray, v)

    val msg2s = msg1s.map{case (eid, msg1) => (eid, pkArray)}

    // Send list of public keys to enclaves
    val encryptedResults = rdd.mapPartitionsWithIndex { (_, _) =>
      val (enclave, eid) = Utils.initEnclave()
      val msg2 = enclave.GetListEncrypted(eid, msg2s(eid))
      Iterator((eid, msg2))
    }.collect.toMap

    // Pick a random encrypted list from the map and send it to all the enclaves
    val random = new Random
    val skArray = encryptedResults.values.toList(random.nextInt(encryptedResults.size))
    val msg3s = msg1s.map{case (eid, _) => (eid, skArray)}

    val setSharedKeyResults = rdd.mapPartitionsWithIndex { (_, _) =>
      val (enclave, eid) = Utils.initEnclave()
      val msg2 = enclave.FinishSharedKey(eid, msg3s(eid))
      Iterator((eid, true))
    }.collect.toMap

    for ((_, ret) <- setSharedKeyResults) {
      if (!ret)
        throw new OpaqueException("Failed to set shared key")
    }
  }
}
