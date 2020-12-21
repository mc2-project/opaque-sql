/*
 * Copyright 2015, Google Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *
 *    * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package edu.berkeley.cs.rise.opaque

import com.google.protobuf.ByteString

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import ra.{HelloRequest, GreeterGrpc, ConfirmRequest, RARequest, KeyRequest}
import ra.GreeterGrpc.GreeterBlockingStub
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

import edu.berkeley.cs.rise.opaque.execution.SP

/**
 * [[https://github.com/grpc/grpc-java/blob/v0.15.0/examples/src/main/java/io/grpc/examples/helloworld/HelloWorldClient.java]]
 */
object HelloWorldClient {
  def apply(host: String, port: Int): HelloWorldClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = GreeterGrpc.blockingStub(channel)

    val intelCert = Utils.findResource("AttestationReportSigningCACert.pem")
    val userCert = scala.io.Source.fromFile("/home/opaque/opaque/user1.crt").mkString
    val keyShare: Array[Byte] = "Opaque key share".getBytes("UTF-8")

    val sp = new SP()
    sp.Init(Utils.clientKey, intelCert, userCert, keyShare)
    
    new HelloWorldClient(channel, blockingStub, sp)
  }

  def main(args: Array[String]): Unit = {
    val client = HelloWorldClient("localhost", 50051)
    try {
      val user = args.headOption.getOrElse("world")
      client.getRA(user)
    } finally {
      client.shutdown()
    }
  }
}

class HelloWorldClient private(
  private val channel: ManagedChannel,
  private val blockingStub: GreeterBlockingStub,
  private val serviceProvider: SP
) {
  private[this] val logger = Logger.getLogger(classOf[HelloWorldClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  /** Say hello to server. */
  def greet(name: String): Unit = {
    logger.info("Will try to greet " + name + " ...")
    val request = HelloRequest(name = name)
    try {
      val response = blockingStub.sayHello(request)
      logger.info("Greeting: " + response.message)
      if (response.message != "") {
        val bytes: Array[Byte] = Array(192.toByte, 168.toByte, 1.toByte, 9.toByte)
        val byteString: ByteString = ByteString.copyFrom(bytes)

        val request2 = ConfirmRequest(success = true, clientInfo = byteString)
        val response2 = blockingStub.sayConfirm(request2)
        logger.info(response2.message)
      }      

    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }

  def getRA(name: String): Unit = {
    val request = RARequest(name = name)
    try {
      val response = blockingStub.getRA(request)
      if (response.success) {

        val msg2 = serviceProvider.ProcessEnclaveReport(response.report.toByteArray())
        
        val byteString: ByteString = ByteString.copyFrom(msg2)

        val request2 = KeyRequest(nonNull = true, key = byteString)
        val response2 = blockingStub.sendKey(request2)
	if (response2.success) {
	  println("Attestation passes")
        }
      }
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }
}
