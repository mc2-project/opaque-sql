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

import opaque.protos.client._

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import com.google.protobuf.ByteString
import io.grpc.netty.NettyServerBuilder

import scala.concurrent.{ExecutionContext, Future}

// Performs remote attestation for all executors
// that have not been attested yet

object RA extends Logging {

  private class ClientToEnclaveImpl(rdd: RDD[Unit]) extends ClientToEnclaveGrpc.ClientToEnclave {

    val eids: Seq[Long] = Seq()

    override def getRemoteEvidence(attestationStatus: AttestationStatus) = {
      val status = attestationStatus.status
      if (status != 0) {
        throw new OpaqueException(
          "Should not generate new attestation evidence if client is already attested."
        )
      }

      // Collect evidence from the executors
      val evidences = rdd
        .mapPartitions { (_) =>
          // evidence is a serialized `oe_evidence_msg_t`
          val (eid, evidence) = Utils.generateEvidence()
          Iterator((eid, evidence))
        }
        .collect
        .toMap
      logInfo("Driver collected evidence reports from enclaves.")

      val protoEvidences = Evidences(evidences.map { case (eid, evidence) =>
        evidence match {
          case Some(evidence) => {
            eids :+ eid
            ByteString.copyFrom(evidence)
          }
          case None =>
            throw new OpaqueException("At least one enclave failed attestion.")
        }
      }.toSeq)
      Future.successful(protoEvidences)
    }
    override def getFinalAttestationResult(encryptedKeys: EncryptedKeys) = {
      val keys = encryptedKeys.keys.map(key => key.toByteArray)
      val eidsToKey = eids.zip(keys).toMap

      // Send the asymmetrically encrypted shared key to any unattested enclave
      rdd.mapPartitions { (_) =>
        val (enclave, eid) = Utils.finishAttestation(numAttested, eidsToKey)
        Iterator((eid, true))
      }

      // No error occured, return attestation passed to the client
      val status = AttestationStatus(1)
      Future.successful(status)
    }
  }

  val numAttested: LongAccumulator = Utils.numAttested
  var numExecutors: Int = 1
  var loop: Boolean = true

  def initRA(sc: SparkContext): Unit = {

    if (!sc.isLocal) {
      numExecutors = sc.getConf.getInt("spark.executor.instances", -1)
    }
    val rdd = Some(sc.parallelize(Seq.fill(numExecutors) { () }, numExecutors))

    // Start gRPC server to listen for attestation inquiries
    val port = 50051
    val server = NettyServerBuilder
      .forPort(port)
      .addService(
        ClientToEnclaveGrpc.bindService(new ClientToEnclaveImpl(rdd.get), ExecutionContext.global)
      )
      .build
      .start
    logInfo(s"gRPC: Attestation Server started, listening on port ${port}.")
    sys.addShutdownHook {
      System.err.println("gRPC: Shutting down gRPC server since JVM is shutting down.")
      server.shutdown()
      System.err.println("gRPC: Server shut down.")
    }
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
  //
  // Important: `testing` should only be set to true during testing
  def attestEnclaves(sc: SparkContext, testing: Boolean = false): Unit = {
    // Proactively initialize enclaves
    val rdd = sc.parallelize(Seq.fill(numExecutors) { () }, numExecutors)
    val numEnclavesAcc = Utils.numEnclaves
    rdd.mapPartitions { (_) =>
      val eid = Utils.startEnclave(numEnclavesAcc, testing)
      Iterator(eid)
    }.count

    if (!testing) {
      if (Utils.numEnclaves.value != Utils.numAttested.value) {
        logInfo(
          s"RA.run: ${Utils.numEnclaves.value} unattested, ${Utils.numAttested.value} attested"
        )
        initRA(sc)
      }
      Thread.sleep(100)
    }
  }

  def startThread(sc: SparkContext, testing: Boolean = false): Unit = {
    val thread = new Thread {
      override def run: Unit = {
        while (loop) {
          RA.attestEnclaves(sc, testing)
        }
      }
    }
    thread.start
  }

  def stopThread(): Unit = {
    loop = false
    Thread.sleep(5000)
  }

}
