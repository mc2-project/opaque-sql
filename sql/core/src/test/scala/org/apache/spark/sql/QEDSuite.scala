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

    println("Starting JNIObliviousSort")

    val eid = enclave.StartEnclave()

    val len = 20
    var number_list = (1 to len).toList
    val random_number_list = Random.shuffle(number_list).toArray

    // encrypt these numbers
    val enc_size = (12 + 16 + 4 + 4) * len
    val single_enc_size = 12 + 16 + 4
    val buffer = ByteBuffer.allocate(enc_size)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    val temp_buffer = ByteBuffer.allocate(4)
    temp_buffer.order(ByteOrder.LITTLE_ENDIAN)
    val temp_array = Array.fill[Byte](4)(0)

    for (v <- random_number_list) {
      temp_buffer.clear()
      temp_buffer.putInt(v)
      temp_buffer.flip()
      temp_buffer.get(temp_array)

      val enc_bytes = enclave.Encrypt(eid, temp_array)
      if (enc_bytes.length != single_enc_size) {
        println("enc_bytes' length is " + enc_bytes.length)
        assert(enc_bytes.length == single_enc_size)
      }

      buffer.putInt(single_enc_size)
      buffer.put(enc_bytes)
    }

    buffer.flip()
    val enc_data = new Array[Byte](buffer.limit())
    buffer.get(enc_data)

    val enc_sorted = enclave.ObliviousSort(eid, 1, enc_data, 0, len)

    // decrypt enc_sorted
    var low_index = 4
    var sorted_numbers = Array.fill[Int](len)(0)

    for (i <- 1 to len) {
      val enc_number_bytes = enc_sorted.slice(low_index, single_enc_size + low_index)
      //println("slice is from " + low_index + " to " + low_index + single_enc_size)
      assert(enc_number_bytes.length == single_enc_size)

      val dec_number_bytes = enclave.Decrypt(eid, enc_number_bytes)
      temp_buffer.clear()
      val buf = ByteBuffer.wrap(dec_number_bytes)
      buf.order(ByteOrder.LITTLE_ENDIAN)
      val dec_number = buf.getInt
      sorted_numbers(i - 1) = dec_number

      low_index += single_enc_size + 4
    }

    for (i <- 1 to len) {
      println(sorted_numbers(i - 1))
      assert(number_list(i-1) == sorted_numbers(i-1))
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
