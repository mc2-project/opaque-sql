package edu.berkeley.cs.rise.opaque

import java.util.logging.Logger

import com.google.protobuf.ByteString

import io.grpc.{Server, ServerBuilder}

import ra.Ra
import ra.GreeterGrpc

import scala.concurrent.{ExecutionContext, Future}
import org.apache.spark.sql.SparkSession

import implicits._

/**
 * [[https://github.com/grpc/grpc-java/blob/v0.15.0/examples/src/main/java/io/grpc/examples/helloworld/HelloWorldServer.java]]
 */
object OpaqueServer {
  private val logger = Logger.getLogger(classOf[OpaqueServer].getName)

  def main(args: Array[String]): Unit = {
    val server = new OpaqueServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
    println("Print hello world server")
  }

  private val port = 50051
}

class OpaqueServer(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null
  private[this] var spark: SparkSession = null

  private def start(): Unit = {

    // initialize SparkSession and ensure remote attestation works
    spark = SparkSession.builder()
      .appName("QEDBenchmark")
      .getOrCreate()
    Utils.initSQLContext(spark.sqlContext)

    // Create spark.sql table for testing purposes
    val data = for (i <- 0 until 256) yield (i, i*2, 1)
    val df = spark.createDataFrame(data).toDF("first", "second", "third")
    val dfEncrypted = df.encrypted
    dfEncrypted.createTempView("Test")

    // Create rpc Server and make it listen
    server = ServerBuilder.forPort(OpaqueServer.port).addService(new GreeterImpl()).build.start
    OpaqueServer.logger.info("Server started, listening on " + OpaqueServer.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
      
      // Drop test table
      spark.catalog.dropTempView("Test")
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

  private class GreeterImpl extends GreeterGrpc.GreeterImplBase {
    override def sayHello(req: Ra.HelloRequest, responseObserver: io.grpc.stub.StreamObserver[Ra.HelloReply]) = {
      val reply = Ra.HelloReply.newBuilder().setMessage("Hello " + req.getName()).build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }

    override def getRA(req: Ra.RARequest, responseObserver: io.grpc.stub.StreamObserver[Ra.RAReply]) = {
      
      val (enclave, eid) = Utils.initEnclave()
      val msg1 = enclave.GenerateReport(eid)

      val byteString: ByteString = ByteString.copyFrom(msg1)

      val reply = Ra.RAReply.newBuilder()
	.setReport(byteString)
	.setSuccess(true)
	.build();

      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }

    override def sendQuery(req: Ra.QueryRequest, responseObserver: io.grpc.stub.StreamObserver[Ra.QueryReply]) = {
      var reply = Ra.QueryReply.newBuilder().build();
      try {
	// Prepare items for setting data reply
        val result = spark.sql(req.getSqlQuery())
        val resultArray = result.collect()
        val replyBuilder = Ra.QueryReply.newBuilder().setSuccess(true)

	for (row <- resultArray) {
	  replyBuilder.addData(row.mkString(", "))
        }
        reply = replyBuilder.build();
      } catch {
        case _ : Throwable => println("Invalid SQL Query")
        reply = Ra.QueryReply.newBuilder()
	  .setSuccess(false)
	  .build()
      } finally {
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      }
    }

    override def sendKey(req: Ra.KeyRequest, responseObserver: io.grpc.stub.StreamObserver[Ra.KeyReply]) = {
      if (req.getNonNull()) {

	// Obtain key from message and store
	val (enclave, eid) = Utils.initEnclave()
	enclave.FinishAttestation(eid, req.getKey().toByteArray())

        val reply = Ra.KeyReply.newBuilder()
	  .setSuccess(true)
	  .build();
	responseObserver.onNext(reply);
        responseObserver.onCompleted();
      } else {
	println("Failed to receive data or client did not confirm")
	val reply = Ra.KeyReply.newBuilder()
          .setSuccess(false) 
          .build();
	responseObserver.onNext(reply);
        responseObserver.onCompleted();
      }
    }
  }
}
