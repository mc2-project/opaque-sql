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

import java.util.logging.Logger

import io.grpc.{Server, ServerBuilder}
import ra.{GreeterGrpc, HelloRequest, HelloReply, ConfirmRequest, ConfirmReply, RARequest, RAReply, KeyRequest, KeyReply}

import scala.concurrent.{ExecutionContext, Future}
import org.apache.spark.sql.SparkSession

/**
 * [[https://github.com/grpc/grpc-java/blob/v0.15.0/examples/src/main/java/io/grpc/examples/helloworld/HelloWorldServer.java]]
 */
object HelloWorldServer {

  private val logger = Logger.getLogger(classOf[HelloWorldServer].getName)

  def main(args: Array[String]): Unit = {

    val server = new HelloWorldServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }

  private val port = 50051
}

class HelloWorldServer(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null
  private[this] var spark: SparkSession = null

  private def start(): Unit = {

    // initialize SparkSession and ensure remote attestation works
    spark = SparkSession.builder()
      .appName("QEDBenchmark")
      .getOrCreate()
    Utils.initSQLContext(spark.sqlContext)

    // initialize server
    server = ServerBuilder.forPort(HelloWorldServer.port).addService(GreeterGrpc.bindService(new GreeterImpl, executionContext)).build.start
    HelloWorldServer.logger.info("Server started, listening on " + HelloWorldServer.port)

    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class GreeterImpl extends GreeterGrpc.Greeter {
    override def sayHello(req: HelloRequest) = {
      val reply = HelloReply(message = "Hello " + req.name)
      Future.successful(reply)
    }

    override def sayConfirm(req: ConfirmRequest) = {
      if (req.success) {
        println(req.clientInfo)
        val reply = ConfirmReply(message = "Received data")
        Future.successful(reply)
      } else {
        println("Failed to receive data or client did not confirm")
        val reply = ConfirmReply(message = "Stopped")
        Future.successful(reply)
      }
    }       

    override def getRA(req: RARequest) = {
        val (enclave, eid) = Utils.initEnclave()
        val msg1 = enclave.GenerateReport(eid)

        val byteString: ByteString = ByteString.copyFrom(msg1)

        val reply = RAReply(report = byteString, success = true)
        Future.successful(reply)
    }

    override def sendKey(req: KeyRequest) = {
	if (!req.nonNull) {
          val reply = KeyReply(success = false)
          Future.successful(reply)
        }
	val (enclave, eid) = Utils.initEnclave()
        enclave.FinishAttestation(eid, req.key.toByteArray)
        val reply = KeyReply(success = true)
        Future.successful(reply)
    }
  }
}

