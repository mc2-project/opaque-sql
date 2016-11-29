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

import java.nio.ByteBuffer
import java.nio.ByteOrder

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLDate
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import edu.berkeley.cs.rise.opaque.execution.ColumnType
import edu.berkeley.cs.rise.opaque.execution.OpaqueOperatorExec
import edu.berkeley.cs.rise.opaque.execution.SGXEnclave
import edu.berkeley.cs.rise.opaque.execution.SP
import edu.berkeley.cs.rise.opaque.logical.ConvertToOpaqueOperators
import edu.berkeley.cs.rise.opaque.logical.EncryptLocalRelation

/*
 * Remote attestation code
 *
 */

object RA {

  def loadLibrary() = {
    if (System.getenv("LIBSGXENCLAVE_PATH") == null) {
      throw new Exception("Set LIBSGXENCLAVE_PATH")
    }
    System.load(System.getenv("LIBSGXENCLAVE_PATH"))
  }

  def loadMasterLibrary() = {
    if (System.getenv("LIBSGX_SP_PATH") == null) {
      throw new Exception("Set LIBSGX_SP_PATH")
    }
    System.load(System.getenv("LIBSGX_SP_PATH"))

  }

  def getEPID(data: Iterator[_]): Iterator[(Array[Byte], Boolean)] = {
    loadLibrary()
    println("getEPID")
    this.synchronized {
      val enclave = new SGXEnclave()
      if (!Utils.attested) {
        val epid = enclave.RemoteAttestation0()
        Iterator((epid, Utils.attested))
      } else {
        val epid = new Array[Byte](0)
        Iterator((epid, Utils.attested))
      }
    }
  }

  def getMsg1(data: Iterator[_]): Iterator[(Array[Byte], Boolean)] = {
    loadLibrary()
    this.synchronized {
      // first, need to start the enclave
      val (enclave, eid) = Utils.initEnclave()

      if (!Utils.attested) {
        val msg1 = enclave.RemoteAttestation1(eid)
        Iterator((msg1, Utils.attested))
      } else {
        val msg1 = new Array[Byte](0)
        Iterator((msg1, Utils.attested))
      }

    }
  }

  def getMsg3(index: Int, data: Iterator[_], msg2: Array[Byte]): Iterator[(Array[Byte], Boolean)] = {
    loadLibrary()
    println("getMsg3 called")
    this.synchronized {
      val (enclave, eid) = Utils.initEnclave()
      println("synchronized getMsg3 called")

      if (Utils.attested) {
        val msg3 = new Array[Byte](0)
        Iterator((msg3, Utils.attested))
      } else {
        val msg3 = enclave.RemoteAttestation2(eid, msg2)
        Iterator((msg3, Utils.attested))
      }
    }
  }

  def finalAttest(index: Int, data: Iterator[_], attestResult:Array[Byte]): Iterator[Boolean]= {
    loadLibrary()
    println("finalAttest called")
    this.synchronized {
      val (enclave, eid) = Utils.initEnclave()
      println(s"synchronized finalAttest called ${Utils.attested}")
      if (!Utils.attested) {
        enclave.RemoteAttestation3(eid, attestResult)
        Utils.attested = true
      } else {
        println("finalAttest: already attested")
      }
    }
    Iterator(true)
  }

  // this should be called from master!
  def initRA(data: RDD[_]) = {


    // numPartitions = number of machines
    val numPartitions = data.getNumPartitions

    loadLibrary()
    loadMasterLibrary()

    val master = new SP()
    val enclave = new SGXEnclave()

    println("Loaded libraries")

    // load master keys
    master.LoadKeys()

    println("Loaded public and private keys")

    // check EPIDs
    val EPIDInfo = data.mapPartitions{
      x => getEPID(x)
    }.collect

    println("Got EPIDs")

    //
    for (v <- EPIDInfo) {
      val epid = v._1
      val attested = v._2
      if (!attested) {
        master.SPProcMsg0(epid)
      }
    }

    println("Checked EPIDs")

    // // get msg1 from enclave

    val msg1 = data.mapPartitions { block => getMsg1(block) }.collect
    println("Got msg1")

    var msg2 = Array.fill[Array[Byte]](numPartitions)(new Array[Byte](0))

    for (index <- 0 until msg1.length) {
      val attested = msg1(index)._2
      if (attested) {
        msg2(index) = new Array[Byte](0)
      } else {
        val ret = master.SPProcMsg1(msg1(index)._1)
        msg2(index) = ret
      }
    }


    println("Sent msg2")

    val msg3 = data.mapPartitionsWithIndex {
      (index, data) =>
      getMsg3(index, data, msg2(index))
    }.collect

    println("Got msg3")

    // get attestation result from the master
    var attResult = Array.fill[Array[Byte]](numPartitions)(new Array[Byte](0))
    for (index <- 0 until msg3.length) {
      val attested = msg3(index)._2
      if (attested) {
        attResult(index) = new Array[Byte](0)
      } else {
        val ret = master.SPProcMsg3(msg3(index)._1)
        attResult(index) = ret
      }
    }

    println("Got attestation result")

    // send final attestation result to each enclave
    data.mapPartitionsWithIndex { (index, data) =>
      finalAttest(index, data, attResult(index))
    }.collect

    println("Sent attestation results; attestation DONE")

    // attestation done
  }

}
