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

import opaque.protos.listener._

import io.grpc.Server
import io.grpc.netty.NettyServerBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.tools.nsc.interpreter.Results._

/*
 * RPC listener responsible for receiving Scala code as strings and passing it to the REPL
 * handled by OpaqueILoop.
 */
object Listener {

  def main(args: Array[String]): Unit = {
    IntpHandler.initializeIntp()
    val server = new Listener(ExecutionContext.global, 50052)
    server.start()
    server.blockUntilShutdown()
  }
}

class Listener(executionContext: ExecutionContext, port: Int) {

  private[this] var server: Server = null

  private class ListenerImpl extends ListenerGrpc.Listener {
    override def receiveQuery(req: QueryRequest) = {
      val query = req.request
      val (output, result) = IntpHandler.run(query)
      val status = result match {
        case Success =>
          Status(0, "")
        case _ =>
          Status(1, f"OpaqueSQLError: ${output}")
      }
      val reply = QueryResult(output, Some(status))
      Future.successful(reply)
    }
  }

  private def start(): Unit = {
    server = NettyServerBuilder
      .forPort(port)
      .addService(ListenerGrpc.bindService(new ListenerImpl, executionContext))
      .build
      .start
    println(s"gRPC: Query Server started, listening on port ${port}")
    sys.addShutdownHook {
      System.err.println("gRPC: Shutting down gRPC server since JVM is shutting down.")
      this.stop()
      System.err.println("gRPC: Server shut down.")
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
}
