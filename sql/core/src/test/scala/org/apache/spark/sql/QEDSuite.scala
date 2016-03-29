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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.functions.lit
import scala.util.Random
import org.scalatest.Tag
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.nio.ByteBuffer


// Exclude SGX tests with build/sbt sql/test:test-only org.apache.spark.sql.QEDSuite -- -l org.apache.spark.sql.SGXTest
object SGXTest extends Tag("org.apache.spark.sql.SGXTest")

class QEDSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  val enclave =
    try {
      System.load("/home/wzheng/sparksgx/libSGXEnclave.so")
      new SGXEnclave()
    } catch {
      case e: UnsatisfiedLinkError =>
        println(e)
        null
    }

  test("filter+show") {
    val data = Seq(("hello", 2), ("world", 1))
    val words = sparkContext.makeRDD(data).toDF("word", "count")
    val filtered = words.encFilter($"count" > lit(1))
    filtered.explain(true)
    assert(words.collect === data.map(Row.fromTuple))
    assert(filtered.collect === data.filter(_._2 > 1).map(Row.fromTuple))
  }

  test("JNIEncrypt", SGXTest) {

    def byteArrayToString(x: Array[Byte]) = {
      val loc = x.indexOf(0)
      if (-1 == loc)
        new String(x)
      else if (0 == loc)
        ""
      else
        new String(x, 0, loc, "UTF-8") // or appropriate encoding
    }

    val eid = enclave.StartEnclave()

    // Test encryption and decryption

    val plaintext = "Hello world!1234"
    val plaintext_bytes = plaintext.getBytes
    val ciphertext = enclave.Encrypt(eid, plaintext_bytes)

    val decrypted = enclave.Decrypt(eid, ciphertext)

    println("decrypted's length is " + decrypted.length)

    assert(plaintext_bytes.length == decrypted.length)

    for (idx <- 0 to plaintext_bytes.length - 1) {
      assert(plaintext_bytes(idx) == decrypted(idx))
    }

    enclave.StopEnclave(eid)
  }

  test("JNIObliviousSort", SGXTest) {

    val eid = enclave.StartEnclave()

    val len = 1000
    var number_list = (1 to len).toList
    val random_number_list = Random.shuffle(number_list).toArray

    val sorted = enclave.ObliviousSort(eid, random_number_list)

    for (i <- 0 to random_number_list.length - 1) {
      assert(number_list(i) == sorted(i))
    }

    enclave.StopEnclave(eid)

  }

  test("JNIFilterSingleRow", SGXTest) {
    val eid = enclave.StartEnclave()

    val filter_number : Int = 5
    val filter_number_bytes = ByteBuffer.allocate(4).putInt(filter_number).array();

    assert(filter_number_bytes.length == 4)

    val ret = enclave.Filter(eid, 0, filter_number_bytes)
    assert(ret == true)

    enclave.StopEnclave(eid)
  }

}
