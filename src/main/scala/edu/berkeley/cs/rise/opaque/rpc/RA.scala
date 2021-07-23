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

package edu.berkeley.cs.rise.opaque.rpc

import opaque.protos.client._

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.OpaqueException

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

import com.google.protobuf.ByteString
import io.grpc.netty.NettyServerBuilder

import java.io.ByteArrayOutputStream

import scala.concurrent.{ExecutionContext, Future}
import scala.Console

// Performs remote attestation for all executors
// that have not been attested yet.

// NOTE: no part of this file should be used in tests.
// This is because the multi-partition tests that run with
// local-cluster[*,*,*] DO NOT include the fat jar that contains
// the gRPC dependency, and Spark will not be able to find those files.

object RA extends Logging {

  private class ClientToEnclaveImpl(rdd: RDD[Unit]) extends ClientToEnclaveGrpc.ClientToEnclave {

    var eids: Seq[Long] = Seq()

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
        eids = eids :+ eid
        ByteString.copyFrom(evidence)
      }.toSeq)
      Future.successful(protoEvidences)
    }
    override def getFinalAttestationResult(encryptedKeys: EncryptedKeys) = {
      val keys = encryptedKeys.keys.map(key => key.toByteArray)
      val eidsToKey = eids.zip(keys).toMap

      // Send the asymmetrically encrypted shared key to any unattested enclave
      rdd.mapPartitions { (_) =>
        val (enclave, eid) = Utils.finishAttestation(Utils.numAttested, eidsToKey)
        Iterator((eid, true))
      }.collect

      // No error occured, return attestation passed to the client
      val status = AttestationStatus(1)
      Future.successful(status)
    }
  }

  var loop: Boolean = true

  def initRAListener(sc: SparkContext, rdd: RDD[Unit]): Unit = {

    // Start gRPC server to listen for attestation inquiries
    val port = 50051
    // Need to use netty
    // See https://scalapb.github.io/docs/grpc/#grpc-netty-issues
    val server = NettyServerBuilder
      .forPort(port)
      .addService(
        ClientToEnclaveGrpc.bindService(new ClientToEnclaveImpl(rdd), ExecutionContext.global)
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

  // This function is executed in a loop that repeatedly tries to
  // call initRA if new enclaves are added
  // Periodically probe the workers using `startEnclave`
  //
  // Important: `testing` should only be set to true during testing
  def attestEnclaves(sc: SparkContext, rdd: RDD[Unit]): Unit = {
    // Proactively initialize enclaves
    val numEnclavesAcc = Utils.numEnclaves
    rdd.mapPartitions { (_) =>
      val eid = Utils.startEnclave(numEnclavesAcc)
      Iterator(eid)
    }.count
    if (Utils.numEnclaves.value != Utils.numAttested.value) {
      // TODO: Need to create an RPC call to the client to reattest if any executor
      // fails for whatever reason.
    }
    Thread.sleep(100)
  }

  def startThread(sc: SparkContext, rdd: RDD[Unit]): Unit = {
    val thread = new Thread {
      override def run: Unit = {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        while (loop) {
          RA.attestEnclaves(sc, rdd)
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
