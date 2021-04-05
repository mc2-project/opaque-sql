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

import org.apache.spark.SparkConf

import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.interpreter.IR.Result
import scala.tools.nsc.GenericRunnerSettings
import scala.Console

import java.io._

/* Handler to simplify writing to an instance of OpaqueILoop. */
object IntpHandler {

  private val out = new StringWriter()

  /* We need to include the jars provided to Spark in the new IMain's classpath. */
  val sparkJars = new SparkConf().get("spark.jars", "").replace(",", ":")

  val settings = new GenericRunnerSettings(msg => Console.err.println(msg))
  settings.classpath.value = sys.props("java.class.path").concat(":").concat(sparkJars)

  val intp = new IMain(settings, new PrintWriter(out))
  initializeIntp()

  /* Initial commands for the interpreter to run. */
  def initializeIntp() = {
    intp.initializeSynchronous()
    val initializationCommands = Seq(
      """
        @transient val spark = if (org.apache.spark.repl.Main.sparkSession != null) {
            org.apache.spark.repl.Main.sparkSession
        } else {
            org.apache.spark.repl.Main.createSparkSession()
        }
        @transient val sc = {
        val _sc = spark.sparkContext
        if (_sc.getConf.getBoolean("spark.ui.reverseProxy", false)) {
            val proxyUrl = _sc.getConf.get("spark.ui.reverseProxyUrl", null)
            if (proxyUrl != null) {
            println(
                s"Spark Context Web UI is available at ${proxyUrl}/proxy/${_sc.applicationId}")
            } else {
            println(s"Spark Context Web UI is available at Spark Master Public URL")
            }
        } else {
            _sc.uiWebUrl.foreach {
            webUrl => println(s"Spark context Web UI available at ${webUrl}")
            }
        }
        println("Spark context available as 'sc' " +
            s"(master = ${_sc.master}, app id = ${_sc.applicationId}).")
        println("Spark session available as 'spark'.")
        _sc
        }
        """,
      "import org.apache.spark.SparkContext._",
      "import spark.implicits._",
      "import spark.sql",
      "import org.apache.spark.sql.functions._",
      /* Opaque SQL specific commands */
      "@transient val sqlContext = spark.sqlContext",
      // Use dummy key for attestation for now.
      "spark.conf.set(\"spark.opaque.testing.enableSharedKey\", \"true\")",
      """
        import edu.berkeley.cs.rise.opaque.implicits._
        try { 
          edu.berkeley.cs.rise.opaque.Utils.initSQLContext(sqlContext) 
        } catch {
          case _ : Throwable =>
        }
        """
    )
    run(initializationCommands)
  }

  def run(input: String): (String, Result) = this.synchronized {
    out.getBuffer.setLength(0)
    val res = intp.interpret(input)
    val output = out.getBuffer.toString
    (output, res)
  }
  def run(lines: Seq[String]): (String, Result) = run(lines.map(_ + "\n").mkString)
}
