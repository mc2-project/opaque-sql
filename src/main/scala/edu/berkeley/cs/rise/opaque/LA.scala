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

// Helper to handle enclave "local attestation" and determine shared key

object LA extends Logging {
  def initLA(sc: SparkContext): Unit = {

    // Hard-coded to be 2 for now
    val rdd = sc.makeRDD(Seq.fill(2) { () })

    // Test print Utils.
    println("LA: " + Utils)
    // Obtain public keys
    val msg1s = rdd.mapPartitionsWithIndex { (i, _) =>
      val (enclave, eid) = Utils.initEnclave()
     
      // Print utils and enclave address to ascertain different enclaves
      println("LA: " + Utils)
      println("LA: " + eid)

      val msg1 = enclave.GetPublicKey(eid) 
      Iterator((eid, msg1))
    }.collect.toMap

    // Combine all public keys into one large array
    var pkArray = Array[Byte]()
    for ((k,v) <- msg1s) {
      pkArray = concat(pkArray, v)
      for (byte <- v) print(byte.toChar)
      println()
    }

//    val msg2s = msg1s.map{case (eid, msg1) => (eid, pkArray)}
    
    // Send list of public keys to enclaves
    val encryptedResults = rdd.context.parallelize(Array(pkArray), 1).map { publicKeys =>
      val (enclave, eid) = Utils.initEnclave()
      println("LA: " + eid)
      enclave.GetListEncrypted(eid, publicKeys)
    }.first()

    println("Before encrypted results print")
    for (byte <- encryptedResults) print(byte.toChar)
    println("After encrypted results print")

    // Send encrypted secret key to all enclaves
    val msg3s = msg1s.map{case (eid, _) => (eid, encryptedResults)}
//    msg1s.map{case (eid, _) => println("msg1s keys: " + eid)}
//    msg3s.map{case (eid, _) => println("msg3s keys: " + eid)}

    val setSharedKeyResults = rdd.mapPartitionsWithIndex { (_, _) =>
      val (enclave, eid) = Utils.initEnclave()
      println("LA - set shared key: " + eid)
      enclave.FinishSharedKey(eid, msg3s(eid))
      println("LA - after set shared key: " + eid)
      Iterator((eid, true))
    }.collect.toMap

    for ((_, ret) <- setSharedKeyResults) {
      if (!ret)
        throw new OpaqueException("Failed to set shared key")
    }
  }
}
