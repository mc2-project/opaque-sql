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

package edu.berkeley.cs.rise.opaque.grpc

import edu.berkeley.cs.rise.opaque.OpaqueSuiteBase
import edu.berkeley.cs.rise.opaque.MultiplePartitionSparkSession
import edu.berkeley.cs.rise.opaque.SinglePartitionSparkSession

import org.apache.spark.sql.SparkSession

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.tpch.TPCH

import org.scalatest.BeforeAndAfterAll

trait gRPCSuite extends OpaqueSuiteBase with BeforeAndAfterAll { self =>

  // TODO: Hardcoded paths to program, jar, and master ip
  val listenerSub = os.proc("python3", "src/main/grpc/OpaqueRPCListener.py", "-j", 
      "/home/opaque/opaque/target/scala-2.12/opaque_2.12-0.1.jar", "-m", "spark://eric:7077").spawn()
  
  // Placeholder program. To be changed later in beforeAll
  var clientSub = os.proc("python3", "-u", "-c", "print(0)").spawn()

  override def beforeAll(): Unit = {
    // Initialize client here as listener takes time to start
    Thread.sleep(15000)
    clientSub = os.proc("python3", "src/main/grpc/OpaqueClient.py").spawn()

    // Necessary inputs and imports
    clientSub.stdin.writeLine("import edu.berkeley.cs.rise.opaque.tpch")
    clientSub.stdin.flush()
    println("first: " + clientSub.stdout.readLine())    
    println("second: " + clientSub.stdout.readLine())

    clientSub.stdin.writeLine("import edu.berkeley.cs.rise.opaque.{SecurityLevel, Insecure, Encrypted}")
    clientSub.stdin.flush()
    println("first: " + clientSub.stdout.readLine())
    println("second: " + clientSub.stdout.readLine())

    clientSub.stdin.writeLine("""def size = "sf_001" """)
    clientSub.stdin.flush()
    println("first: " + clientSub.stdout.readLine())
    println("second: " + clientSub.stdout.readLine())

    clientSub.stdin.writeLine("def numPartitions = 3")
    clientSub.stdin.flush()
    println("first: " + clientSub.stdout.readLine())
    println("second: " + clientSub.stdout.readLine())
 
    clientSub.stdin.writeLine("""def tpch = new edu.berkeley.cs.rise.opaque.tpch.TPCH(spark.sqlContext, size, "file://")""")
    clientSub.stdin.flush()
    println("first: " + clientSub.stdout.readLine())
    println("second: " + clientSub.stdout.readLine())
  }

  override def afterAll(): Unit = {
    if (clientSub.isAlive()) {clientSub.destroy()}
    if (listenerSub.isAlive()) {listenerSub.destroy()}
  }

  def runTests() = {
    for (queryNum <- TPCH.supportedQueries) {
      val testStr = s"TPC-H $queryNum"
      test(testStr) {
        val queryStr = s"tpch.query($queryNum, Encrypted, numPartitions).collect"
        clientSub.stdin.writeLine(queryStr)
        clientSub.stdin.flush()

        // Wait for completion. 
        // 1st line is result of format: 'opaque> res#: ...'
        // 2nd line is new line
        // Occasionally some other line will pop up, so we check to make sure that doesn't happen
        var first = clientSub.stdout.readLine()
        while (first == null || !first.contains("res")) {
          first = clientSub.stdout.readLine()
        }

        println("first: " + first)
        println("second: " + clientSub.stdout.readLine())

        // TODO: Currently no post-verification merged so just runnning it on emptyDataFrame
        clientSub.stdin.writeLine("""edu.berkeley.cs.rise.opaque.Utils.postVerifyAndPrint(spark.emptyDataFrame, "user1")""")
        clientSub.stdin.flush()
        
        // Wait for finish
        clientSub.stdout.readLine()
        clientSub.stdout.readLine()

        assert(true) 
      }
    }
  }
}

// Honestly do not need to extend as this suite doesn't use 'spark' or 'numPartitions'.
// However, it does use FunSuite.
class MultiPartition_gRPCSuite extends gRPCSuite with MultiplePartitionSparkSession {
  runTests()
}
