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

import java.net._

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

import edu.berkeley.cs.rise.opaque.execution.SP

/*
 * Remote attestation code
 * Note: Opaque utilizes ONE enclave per *machine* for performance reasons.
 */

object RA extends Logging {

  def getIP(): String = {
    val localhost = InetAddress.getLocalHost
    val ipAddr = localhost.getHostAddress
    ipAddr
  }

  def getMsg1(index: Int, data: Iterator[_]): Iterator[(Array[Byte], Boolean, Boolean, String)] = {
    val ipAddr = getIP()
    this.synchronized {
      logTrace("getMsg1")
      // first, need to start the enclave
      val (enclave, eid) = Utils.initEnclave()

      if (!Utils.attested && !Utils.attesting_getmsg1) {
        val msg1 = enclave.RemoteAttestation1(eid)
        Utils.attesting_getmsg1 = true
        Iterator((msg1, Utils.attested, true, ipAddr))
      } else {
        val msg1 = new Array[Byte](0)
        Iterator((msg1, Utils.attested, false, ipAddr))
      }

    }
  }

  def getMsg3(
      index: Int,
      data: Iterator[_],
      msg2: Array[Byte],
      inputIPAddr: String)
    : Iterator[(Array[Byte], Boolean, Boolean, String)] = {
    val ipAddr = getIP()
    this.synchronized {
      logTrace("synchronized getMsg3 called")
      val (enclave, eid) = Utils.initEnclave()

      if (!Utils.attested && !Utils.attesting_getmsg3) {
        val msg3 = enclave.RemoteAttestation2(eid, msg2)
        Utils.attesting_getmsg3 = true
        Iterator((msg3, Utils.attested, true, ipAddr))
      } else {
        val msg3 = new Array[Byte](0)
        Iterator((msg3, Utils.attested, false, ipAddr))
      }
    }
  }

  def finalAttest(
      index: Int,
      data: Iterator[_],
      attestResult: Array[Byte],
      inputIPAddr: String)
    : Iterator[Boolean] = {
    this.synchronized {
      logTrace(s"synchronized finalAttest called ${Utils.attested}")
      val (enclave, eid) = Utils.initEnclave()
      if (!Utils.attested && !Utils.attesting_final_ra) {
        enclave.RemoteAttestation3(eid, attestResult)

        Utils.attested = true
        Utils.attesting_getepid = true
        Utils.attesting_getmsg1 = false
        Utils.attesting_getmsg3 = false
        Utils.attesting_final_ra = false
      }
    }
    Iterator(true)
  }

  // this should only be called from the master!
  def initRA(sc: SparkContext): Unit = {

    val rdd = sc.makeRDD(Seq.fill(sc.defaultParallelism) { () })

    val sp = new SP()
    sp.LoadKeys()

    val epids = rdd.mapPartitions { _ =>
      val (enclave, eid) = Utils.initEnclave()
      val epid = enclave.RemoteAttestation0(eid)
      Iterator(epid)
    }.collect

    for (epid <- epids) {
      sp.SPProcMsg0(epid)
    }

    val msg1s = rdd.mapPartitionsWithIndex { (i, _) =>
      val (enclave, eid) = Utils.initEnclave()
      val msg1 = enclave.RemoteAttestation1(eid)
      Iterator((i, msg1))
    }.collect.toMap

    val msg2s = msg1s.mapValues(msg1 => sp.SPProcMsg1(msg1))

    val msg3s = rdd.mapPartitionsWithIndex { (i, _) =>
      val (enclave, eid) = Utils.initEnclave()
      val msg3 = enclave.RemoteAttestation2(eid, msg2s(i))
      Iterator((i, msg3))
    }.collect.toMap

    ???

      // // get attestation result from the master
      // var attResult_dedup = Map[String, Array[Byte]]()
      // val attResult = Array.fill[(String, Array[Byte])](numPartitions)(("", new Array[Byte](0)))
      // for (index <- 0 until msg3.length) {
      //   val attested = msg3(index)._2
      //   val proc = msg3(index)._3
      //   val ipAddr = msg3(index)._4
      //   if (!attested && proc) {
      //     val ret = master.SPProcMsg3(msg3(index)._1)
      //     attResult_dedup += (ipAddr -> ret)
      //   }
      // }

      // for (index <- 0 until msg3.length) {
      //   val attested = msg3(index)._2
      //   val ipAddr = msg3(index)._4

      //   if (!attested) {
      //     attResult(index) = (ipAddr, attResult_dedup(ipAddr))
      //   }
      // }
      // logTrace("Got attestation result")

      // // send final attestation result to each enclave
      // data.mapPartitionsWithIndex { (index, data) =>
      //   finalAttest(index, data, attResult(index)._2, attResult(index)._1)
      // }.collect

      // logTrace("Sent attestation results; attestation DONE")

      // // attestation done
  }

}
