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
import java.nio.ByteOrder


// Exclude SGX tests with build/sbt sql/test:test-only org.apache.spark.sql.QEDSuite -- -l org.apache.spark.sql.SGXTest
object SGXTest extends Tag("org.apache.spark.sql.SGXTest")

class QEDSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  val enclave = {
    System.load(System.getenv("LIBSGXENCLAVE_PATH"))
    new SGXEnclave()
  }

  test("encFilter") {
    val (enclave, eid) = QED.initEnclave()
    val data = Seq(("hello", 1), ("world", 4), ("foo", 2))
    val encrypted = data.map {
      case (word, count) =>
        (QED.encrypt(enclave, eid, word), QED.encrypt(enclave, eid, count))
    }
    def decrypt(rows: Array[Row]): Array[(String, Int)] = {
      rows.map {
        case Row(wordEnc: Array[Byte], countEnc: Array[Byte]) =>
          (QED.decrypt[String](enclave, eid, wordEnc), QED.decrypt[Int](enclave, eid, countEnc))
      }
    }

    val words = sparkContext.makeRDD(encrypted).toDF("word", "count")
    assert(decrypt(words.collect) === data)

    val filtered = words.encFilter($"count") // TODO: make enc versions of each operator
    assert(decrypt(filtered.collect) === data.filter(_._2 > 3))
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

    val filter_number : Int = 1233
    val buffer = ByteBuffer.allocate(9)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val t : Byte = 1

    buffer.put(t)
    buffer.putInt(4) // for length
    buffer.putInt(filter_number)

    buffer.flip()
    val byte_array = new Array[Byte](buffer.limit())
    buffer.get(byte_array)

    println("Bytes array's length " + byte_array.length)


    // then encrypt this buffer
    val enc_bytes = enclave.Encrypt(eid, byte_array)

    val new_buffer = ByteBuffer.allocate(1024)
    new_buffer.order(ByteOrder.LITTLE_ENDIAN)

    val enc_buffer_size = enc_bytes.length
    new_buffer.putInt(1) // for number of columns
    new_buffer.putInt(enc_buffer_size)
    new_buffer.put(enc_bytes)

    new_buffer.flip()
    val enc_byte_array = new Array[Byte](new_buffer.limit())
    new_buffer.get(enc_byte_array)

    val ret = enclave.Filter(eid, -1, enc_byte_array)

    enclave.StopEnclave(eid)
  }

}
