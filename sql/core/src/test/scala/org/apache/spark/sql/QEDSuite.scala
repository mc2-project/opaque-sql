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

  def byte_to_int(array: Array[Byte], index: Int) = {
    val int_bytes = array.slice(index, index + 4)
    val buf = ByteBuffer.wrap(int_bytes)
    buf.order(ByteOrder.LITTLE_ENDIAN)
    buf.getInt
  }

  def byte_to_string(array: Array[Byte], index: Int, length: Int) = {
    val string_bytes = array.slice(index, index + length)
    new String(string_bytes)
  }

  val enclave = {
    System.load(System.getenv("LIBSGXENCLAVE_PATH"))
    new SGXEnclave()
  }

  /*
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

  test("JNIObliviousSort1", SGXTest) {

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

  test("JNIObliviousSortRow", SGXTest) {

    val eid = enclave.StartEnclave()

    val data = Seq(("test1", 10), ("hello", 1), ("world", 4), ("foo", 2), ("test2", 3) )

    val encrypted = data.map {
      case (word, count) =>
        (QED.encrypt(enclave, eid, word), QED.encrypt(enclave, eid, count))
    }

    var total_size = 0
    for (v <- encrypted) {
      // for num columns
      total_size += 4
      total_size += 4 * 2
      total_size += v._1.length
      total_size += v._2.length

      println("v1's length is " + v._1.length + ", v2's length is " + v._2.length)
    }

    val buffer = ByteBuffer.allocate(total_size)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    for (v <- encrypted) {
      buffer.putInt(2)
      buffer.putInt(v._1.length)
      buffer.put(v._1)
      buffer.putInt(v._2.length)
      buffer.put(v._2)
    }


    buffer.flip()
    val encrypted_data = new Array[Byte](buffer.limit())
    buffer.get(encrypted_data)

    println("Encrypted_data's size is " + encrypted_data.length)

    val sorted_encrypted_data = enclave.ObliviousSort(eid, 2, encrypted_data, 0, data.length)

    var low_index = 0
    for (i <- 1 to data.length) {
      val num_cols = byte_to_int(sorted_encrypted_data, low_index)
      low_index += 4

      println("num_cols is " + num_cols)

      for (c <- 1 to num_cols) {
        val enc_value_len = byte_to_int(sorted_encrypted_data, low_index)
        //println("enc_value_len is " + enc_value_len)
        low_index += 4
        val enc_value = sorted_encrypted_data.slice(low_index, low_index + enc_value_len)
        val dec_value = enclave.Decrypt(eid, enc_value)
        //println("Size of dec_value: " + dec_value.length)

        // for (v <- dec_value) {
        //   print(v + " - " )
        // }
        // println("")


        if (dec_value(0) == 1.toByte) {
          println("Row " + i + ", column " + c + ": " + byte_to_int(dec_value, 5))
        } else if (dec_value(0) == 2.toByte) {
          println("Row " + i + ", column " + c + ": " + byte_to_string(dec_value, 5, byte_to_int(dec_value, 1)))
        }

        low_index += enc_value_len
      }
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


  test("JNIAggregation", SGXTest) {

    val eid = enclave.StartEnclave()

    def encrypt_and_serialize(data: Seq[(Int, String, Int)]): Array[Byte] = {
      val encrypted = data.map {
        case (identifier, word, count) =>
          (QED.encrypt(enclave, eid, identifier),
            QED.encrypt(enclave, eid, word),
            QED.encrypt(enclave, eid, count))
      }

      var total_size = 0
      for (row <- encrypted) {
        // for num columns
        total_size += 4
        total_size += 4
        total_size += row._1.length
        total_size += 4
        total_size += row._2.length
        total_size += 4
        total_size += row._3.length
      }

      val buffer = ByteBuffer.allocate(total_size)
      buffer.order(ByteOrder.LITTLE_ENDIAN)

      for (row <- encrypted) {
        buffer.putInt(3)
        buffer.putInt(row._1.length)
        buffer.put(row._1)
        buffer.putInt(row._2.length)
        buffer.put(row._2)
        buffer.putInt(row._3.length)
        buffer.put(row._3)
      }

      buffer.flip()
      val encrypted_data = new Array[Byte](buffer.limit())
      buffer.get(encrypted_data)

      encrypted_data
    }

    // make sure this is sorted data, i.e. sorted on the aggregation attribute
    val data_1 = Seq((1, "A", 1), (2, "A", 1), (3, "B", 1), (4, "B", 1), (5, "C", 1))
    val data_2 = Seq((6, "C", 1), (7, "C", 1), (8, "C", 1), (9, "C", 1), (10, "C", 1))
    val data_3 = Seq((11, "C", 1), (12, "D", 1), (13, "E", 1), (14, "F", 1), (15, "G", 1))

    val enc_data1 = encrypt_and_serialize(data_1)
    val enc_data2 = encrypt_and_serialize(data_2)
    val enc_data3 = encrypt_and_serialize(data_3)

    // should input dummy row
    val agg_size = 4 + 12 + 16 + 4 + 4 + 2048 + 128
    val agg_row1 = Array.fill[Byte](agg_size)(0)
    val agg_row2 = Array.fill[Byte](agg_size)(0)
    val agg_row3 = Array.fill[Byte](agg_size)(0)

    val ret_agg_row1 = enclave.Aggregate(eid, 1, enc_data1, data_1.length, agg_row1)
    val ret_agg_row2 = enclave.Aggregate(eid, 1, enc_data2, data_2.length, agg_row2)
    val ret_agg_row3 = enclave.Aggregate(eid, 1, enc_data3, data_3.length, agg_row3)

    // aggregate the agg_row's together
    val agg_row_buffer = ByteBuffer.allocate(ret_agg_row1.length + ret_agg_row2.length + ret_agg_row3.length)
    agg_row_buffer.order(ByteOrder.LITTLE_ENDIAN)

    agg_row_buffer.put(ret_agg_row1)
    agg_row_buffer.put(ret_agg_row2)
    agg_row_buffer.put(ret_agg_row3)

    agg_row_buffer.flip()
    val agg_row_value = new Array[Byte](agg_row_buffer.limit())
    agg_row_buffer.get(agg_row_value)

    val step_2_values = enclave.ProcessBoundary(eid, 1, agg_row_value, 3)

    // split these values
    val new_agg_row1 = step_2_values.slice(0, agg_size)
    val new_agg_row2 = step_2_values.slice(agg_size, agg_size * 2)
    val new_agg_row3 = step_2_values.slice(agg_size * 2, agg_size * 3)

    val partial_result_1 = enclave.Aggregate(eid, 101, enc_data1, data_1.length, new_agg_row1)
    val partial_result_2 = enclave.Aggregate(eid, 101, enc_data2, data_2.length, new_agg_row2)
    val partial_result_3 = enclave.Aggregate(eid, 101, enc_data3, data_3.length, new_agg_row3)

    enclave.StopEnclave(eid)
  }
   */

  test("JNIJoin", SGXTest) {
    val eid = enclave.StartEnclave()

    def encrypt_and_serialize(data: Seq[(Int, String, Int)]): Array[Byte] = {
      val encrypted = data.map {
        case (identifier, word, count) =>
          (QED.encrypt(enclave, eid, identifier),
            QED.encrypt(enclave, eid, word),
            QED.encrypt(enclave, eid, count))
      }

      var total_size = 0
      for (row <- encrypted) {
        // for num columns
        total_size += 4
        total_size += 4
        total_size += row._1.length
        total_size += 4
        total_size += row._2.length
        total_size += 4
        total_size += row._3.length
      }

      val buffer = ByteBuffer.allocate(total_size)
      buffer.order(ByteOrder.LITTLE_ENDIAN)

      for (row <- encrypted) {
        buffer.putInt(3)
        buffer.putInt(row._1.length)
        buffer.put(row._1)
        buffer.putInt(row._2.length)
        buffer.put(row._2)
        buffer.putInt(row._3.length)
        buffer.put(row._3)
      }

      buffer.flip()
      val encrypted_data = new Array[Byte](buffer.limit())
      buffer.get(encrypted_data)

      encrypted_data
    }

    val table_p_data = Seq((1, "A", 10), (2, "C", 20), (3, "D", 30))
    val table_f_data = Seq((100, "A", 1), (100, "A", 2), (0, "B", 1), (200, "C", 1), (200, "C", 2), (300, "D", 1))

    val buffer = ByteBuffer.allocate(128)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val enc_table_p_id = new Array[Byte](8)
    val enc_table_f_id = new Array[Byte](8)

    for (i <- 1 to 8) {
      buffer.put("a".getBytes)
    }
    buffer.flip()
    buffer.get(enc_table_p_id)

    buffer.clear()

    for (i <- 1 to 8) {
      buffer.put("b".getBytes)
    }
    buffer.flip()
    buffer.get(enc_table_f_id)

    val enc_table_p = encrypt_and_serialize(table_p_data)
    val enc_table_f = encrypt_and_serialize(table_f_data)

    val processed_table_p = enclave.JoinSortPreprocess(eid, 3, enc_table_p_id, enc_table_p, table_p_data.length)
    val processed_table_f = enclave.JoinSortPreprocess(eid, 3, enc_table_f_id, enc_table_f, table_f_data.length)

    // merge the two buffers together
    val processed_rows = processed_table_p ++ processed_table_f

    println("processed_rows' length is " + processed_rows.length)

    val sorted_rows = enclave.ObliviousSort(eid, 3, processed_rows, 0, table_p_data.length + table_f_data.length)

    //val join_row = enclave.ScanCollectLastPrimary(eid, 3, sorted_rows, table_p_data.length + table_f_data.length);

    val joined_rows = enclave.SortMergeJoin(eid, 3, sorted_rows, table_p_data.length + table_f_data.length)
    

    enclave.StopEnclave(eid)
  }

}
