package edu.berkeley.cs.rise.opaque

import com.google.protobuf.ByteString

import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}

import ra.Ra
import ra.GreeterGrpc
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

import edu.berkeley.cs.rise.opaque.execution.SP

/**
 * [[https://github.com/grpc/grpc-java/blob/v0.15.0/examples/src/main/java/io/grpc/examples/helloworld/HelloWorldClient.java]]
 */
object OpaqueClient {

  def apply(host: String, port: Int): OpaqueClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = GreeterGrpc.newBlockingStub(channel)

    val intelCert = Utils.findResource("AttestationReportSigningCACert.pem")
    val userCert = scala.io.Source.fromFile("/home/opaque/opaque/user1.crt").mkString
    val keyShare: Array[Byte] = "Opaque key share".getBytes("UTF-8")
    val clientKey: Array[Byte] = "Opaque key share".getBytes("UTF-8")

    Utils.addClientKey(clientKey)

    val sp = new SP()

    sp.Init(Utils.clientKey, intelCert, userCert, keyShare)

    new OpaqueClient(channel, blockingStub, sp)
  }

  def main(args: Array[String]): Unit = {
    val client = OpaqueClient("localhost", 50051)
    try {
      val user = args.headOption.getOrElse("world")
      client.getRA(user)
      client.sendTestQuery()
    } finally {
      client.shutdown()
    }
  }
}

class OpaqueClient private(
  private val channel: ManagedChannel,
  private val blockingStub: GreeterGrpc.GreeterBlockingStub,
  private val serviceProvider: SP
) {
  private[this] val logger = Logger.getLogger(classOf[OpaqueClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  /** Say hello to server. */
  def greet(name: String): Unit = {
    logger.info("Will try to greet " + name + " ...")
    val request = Ra.HelloRequest.newBuilder().setName(name).build();
    try {
      val response = blockingStub.sayHello(request)
      logger.info("Greeting: " + response.getMessage())
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }

  def getRA(name: String): Unit = {
    val request = Ra.RARequest
	.newBuilder()
	.setName(name)
	.build()
    try {
      val response = blockingStub.getRA(request)
      if (response.getSuccess()) {
	// Verify the RA report is correct and return the client key if so
        val msg2 = serviceProvider.ProcessEnclaveReport(response.getReport().toByteArray())
        val byteString: ByteString = ByteString.copyFrom(msg2)

	// Building the key response
        val request2 = Ra.KeyRequest.newBuilder()
	  .setNonNull(true)
	  .setKey(byteString)
	  .build()
        val response2 = blockingStub.sendKey(request2)
        if (response2.getSuccess()) {
          println("Attestation passes")
        }
      }
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }

  def sendTestQuery(): Unit = {
    val testQuery = "SELECT * from Test"
    val request = Ra.QueryRequest.newBuilder()
      .setSqlQuery(testQuery)
      .build()
    val response = blockingStub.sendQuery(request)
    if (response.getSuccess()) {
      println(response.getDataCount()) // Should be 256
      println(response.getData(0)) // Should be 0, 0, 1
      println(response.getData(1)) // Should be 1, 2, 1
    } 
  } 
}

