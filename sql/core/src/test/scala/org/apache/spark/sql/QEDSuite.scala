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

import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.util.Random

import oblivious_sort.ObliviousSort
import org.apache.spark.unsafe.types.UTF8String

import org.apache.spark.sql.QEDOpcode._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

class QEDSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  import QED.time

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

  val (enclave, eid) = QED.initEnclave()

  test("big data 1 - spark sql") {
    QEDBenchmark.bd1SparkSQL(sqlContext, "tiny")
  }

  test("big data 1") {
    QEDBenchmark.bd1Opaque(sqlContext, "tiny")
  }

  test("big data 2 - spark sql") {
    QEDBenchmark.bd2SparkSQL(sqlContext, "tiny")
  }

  test("big data 2") {
    QEDBenchmark.bd2Opaque(sqlContext, "tiny")
  }

  test("big data 3 - spark sql") {
    QEDBenchmark.bd3SparkSQL(sqlContext, "tiny")
  }

  ignore("big data 3") {
    QEDBenchmark.bd3Opaque(sqlContext, "tiny")
  }

  test("columnsort padding") {
    val data = Random.shuffle((0 until 3).map(x => (x.toString, x)).toSeq)
    val encData = QED.encrypt2(data).map {
      case (str, x) => InternalRow(str, x).encSerialize
    }
    val sorted = ObliviousSort.ColumnSort(
      sparkContext, sparkContext.makeRDD(encData, 1), OP_SORT_COL2)
      .map(row => Row(QED.parseRow(row): _*)).collect
    assert(QED.decrypt2[String, Int](sorted) === data.sortBy(_._2))
  }

  test("encFilter") {
    val data = for (i <- 0 until 256) yield ("foo", i)
    val words = sparkContext.makeRDD(QED.encrypt2(data), 1).toDF("word", "count")
    assert(QED.decrypt2(words.collect) === data)

    val filtered = words.encFilter(OP_FILTER_COL2_GT3)
    assert(QED.decrypt2[String, Int](filtered.collect).sorted === data.filter(_._2 > 3).sorted)
  }

  test("encFilter on date") {
    import java.sql.Date
    val dates = List("1975-01-01", "1980-01-01", "1980-03-02", "1980-04-01", "1990-01-01")
    val filteredDates = List("1980-01-01", "1980-03-02", "1980-04-01")
    val data = sqlContext.createDataFrame(
      sparkContext.makeRDD(dates.map(d =>
        Row(DateTimeUtils.toJavaDate(DateTimeUtils.stringToDate(UTF8String.fromString(d)).get))), 1),
      StructType(Seq(StructField("date", DateType))))
    val filtered = data.filter($"date" >= lit("1980-01-01"))
      .filter($"date" <= lit("1980-04-01"))
    assert(filtered.collect.map(_.get(0).toString).sorted === filteredDates.sorted)

    val encDates = data.mapPartitions { iter =>
      val (enclave, eid) = QED.initEnclave()
      iter.map {
        case Row(d: java.sql.Date) => QED.encrypt(enclave, eid, d)
      }
    }.toDF("date")
    val encFiltered = encDates.encFilter(OP_FILTER_COL1_DATE_BETWEEN_1980_01_01_AND_1980_04_01)
    assert(QED.decrypt1[java.sql.Date](encFiltered.collect).map(_.toString).sorted ===
      filteredDates.sorted)
  }

  test("encPermute") {
    val array = (0 until 256).toArray
    val permuted =
      sqlContext.createDataFrame(
        sparkContext.makeRDD(QED.encrypt1(array).map(Row(_)), 1),
        StructType(List(StructField("x", BinaryType, true))))
        .encPermute().collect
    assert(QED.decrypt1[Int](permuted) !== array)
    assert(QED.decrypt1[Int](permuted).sorted === array)
  }

  test("encAggregate") {
    def abc(i: Int): String = (i % 3) match {
      case 0 => "A"
      case 1 => "B"
      case 2 => "C"
    }
    val data = for (i <- 0 until 256) yield (i, abc(i), 1)
    val words = sparkContext.makeRDD(QED.encrypt3(data), 1).toDF("id", "word", "count")

    val summed = words.encAggregate($"word", $"count".as("totalCount"))
    assert(QED.decrypt2[String, Int](summed.collect) ===
      data.map(p => (p._2, p._3)).groupBy(_._1).mapValues(_.map(_._2).sum).toSeq.sorted)
  }

  test("encAggregate on multiple columns") {
    def abc(i: Int): String = (i % 3) match {
      case 0 => "A"
      case 1 => "B"
      case 2 => "C"
    }
    val data = for (i <- 0 until 256) yield (abc(i), 1, 1.0f)
    val words = sparkContext.makeRDD(QED.encrypt3(data), 1).toDF("str", "x", "y")

    val summed = words.encAggregate($"str", $"x".as("avgX"), $"y".as("totalY"))
    assert(QED.decrypt3[String, Int, Float](summed.collect) ===
      data.groupBy(_._1).mapValues(group =>
        (group.map(_._2).sum / group.map(_._2).size, group.map(_._3).sum))
      .toSeq.map { case (str, (avgX, avgY)) => (str, avgX, avgY) }.sorted)
  }

  test("encSort") {
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x)).toSeq)
    val sorted = time("Enc sorting: ") {
      sparkContext.makeRDD(QED.encrypt2(data), 1).toDF("str", "x").encSort($"x").collect
    }
    assert(QED.decrypt2[String, Int](sorted) === data.sortBy(_._2))
  }

  test("encSort by float") {
    val data = Random.shuffle((0 until 256).map(x => (x.toString, x.toFloat)).toSeq)
    val sorted = sparkContext.makeRDD(QED.encrypt2(data), 1).toDF("str", "x").encSort($"x").collect
    assert(QED.decrypt2[String, Float](sorted) === data.sortBy(_._2))
  }

  test("encJoin") {
    val p_data = for (i <- 1 to 16) yield (i, i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield (i, (i % 16).toString, i * 10)
    val p = sparkContext.makeRDD(QED.encrypt3(p_data), 1).toDF("id", "pk", "x")
    val f = sparkContext.makeRDD(QED.encrypt3(f_data), 1).toDF("id", "fk", "x")
    val joined = p.encJoin(f, $"pk", $"fk").collect
    val expectedJoin =
      for {
        (p_id, pk, p_x) <- p_data
        (f_id, fk, f_x) <- f_data
        if pk == fk
      } yield (p_id, pk, p_x, f_id, f_x)
    assert(QED.decrypt5[Int, String, Int, Int, Int](joined).toSet === expectedJoin.toSet)
  }

  ignore("encJoin on column 1") {
    val p_data = for (i <- 1 to 16) yield (i.toString, i * 10)
    val f_data = for (i <- 1 to 256 - 16) yield ((i % 16).toString, (i * 10).toString, i.toFloat)
    val p = sparkContext.makeRDD(QED.encrypt2(p_data), 1).toDF("pk", "x")
    val f = sparkContext.makeRDD(QED.encrypt3(f_data), 1).toDF("fk", "x", "y")
    val joined = p.encJoin(f, $"pk", $"fk").collect
    val expectedJoin =
      for {
        (pk, p_x) <- p_data
        (fk, f_x, f_y) <- f_data
        if pk == fk
      } yield (pk, p_x, f_x, f_y)
    assert(QED.decrypt4[String, Int, String, Float](joined).toSet === expectedJoin.toSet)
  }

  test("encProject") {
    def encrypt2[A, B](rows: Seq[(A, B)]): Seq[(Array[Byte], Array[Byte])] = {
      val (enclave, eid) = QED.initEnclave()
      rows.map {
        case (a, b) =>
          (QED.encrypt(enclave, eid, a, Some(QEDColumnType.IP_TYPE)),
            QED.encrypt(enclave, eid, b))
      }
    }
    val data = for (i <- 0 until 256) yield ("%03d".format(i) * 3, i.toFloat)
    val rdd = sparkContext.makeRDD(encrypt2(data), 1).toDF("str", "x")
    val proj = rdd.encProject(/*substring($"str", 0, 3)*/$"str", $"x")
    assert(QED.decrypt2(proj.collect) === data.map { case (str, x) => (str.substring(0, 3), x) })
  }

  test("JNIEncrypt") {

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

    // println("decrypted's length is " + decrypted.length)

    assert(plaintext_bytes.length == decrypted.length)

    for (idx <- 0 to plaintext_bytes.length - 1) {
      assert(plaintext_bytes(idx) == decrypted(idx))
    }

    enclave.StopEnclave(eid)
  }

  test("JNIObliviousSort1") {

    // val eid = enclave.StartEnclave()

    // val len = 20
    // var number_list = (1 to len).toList
    // val random_number_list = Random.shuffle(number_list).toArray

    // // encrypt these numbers
    // val enc_size = (12 + 16 + 4 + 4) * len
    // val single_enc_size = 12 + 16 + 4
    // val buffer = ByteBuffer.allocate(enc_size)
    // buffer.order(ByteOrder.LITTLE_ENDIAN)

    // val temp_buffer = ByteBuffer.allocate(4)
    // temp_buffer.order(ByteOrder.LITTLE_ENDIAN)
    // val temp_array = Array.fill[Byte](4)(0)

    // for (v <- random_number_list) {
    //   temp_buffer.clear()
    //   temp_buffer.putInt(v)
    //   temp_buffer.flip()
    //   temp_buffer.get(temp_array)

    //   val enc_bytes = enclave.Encrypt(eid, temp_array)
    //   if (enc_bytes.length != single_enc_size) {
    //     // println("enc_bytes' length is " + enc_bytes.length)
    //     assert(enc_bytes.length == single_enc_size)
    //   }

    //   buffer.putInt(single_enc_size)
    //   buffer.put(enc_bytes)
    // }

    // buffer.flip()
    // val enc_data = new Array[Byte](buffer.limit())
    // buffer.get(enc_data)

    // val enc_sorted = enclave.ObliviousSort(eid, OP_SORT_INTEGERS_TEST.value, enc_data, 0, len)

    // // decrypt enc_sorted
    // var low_index = 4
    // var sorted_numbers = Array.fill[Int](len)(0)

    // for (i <- 1 to len) {
    //   val enc_number_bytes = enc_sorted.slice(low_index, single_enc_size + low_index)
    //   //println("slice is from " + low_index + " to " + low_index + single_enc_size)
    //   assert(enc_number_bytes.length == single_enc_size)

    //   val dec_number_bytes = enclave.Decrypt(eid, enc_number_bytes)
    //   temp_buffer.clear()
    //   val buf = ByteBuffer.wrap(dec_number_bytes)
    //   buf.order(ByteOrder.LITTLE_ENDIAN)
    //   val dec_number = buf.getInt
    //   sorted_numbers(i - 1) = dec_number

    //   low_index += single_enc_size + 4
    // }

    // for (i <- 1 to len) {
    //   // println(sorted_numbers(i - 1))
    //   assert(number_list(i-1) == sorted_numbers(i-1))
    // }

    // enclave.StopEnclave(eid)

  }


  test("JNIObliviousSortRow") {

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

      // println("v1's length is " + v._1.length + ", v2's length is " + v._2.length)
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

    // println("Encrypted_data's size is " + encrypted_data.length)

    val sorted_encrypted_data = enclave.ObliviousSort(eid, OP_SORT_COL2.value, encrypted_data, 0, data.length)

    var low_index = 0
    for (i <- 1 to data.length) {
      val num_cols = byte_to_int(sorted_encrypted_data, low_index)
      low_index += 4

      // println("num_cols is " + num_cols)
      assert(num_cols == 2)

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


        // if (dec_value(0) == 1.toByte) {
        //   println("Row " + i + ", column " + c + ": " + byte_to_int(dec_value, 5))
        // } else if (dec_value(0) == 2.toByte) {
        //   println("Row " + i + ", column " + c + ": " + byte_to_string(dec_value, 5, byte_to_int(dec_value, 1)))
        // } else {
        //   println("Unidentifiable type!\n");
        // }

        low_index += enc_value_len
      }
    }
    assert(
      QED.decrypt2[String, Int](
        QED.parseRows(sorted_encrypted_data).map(fields => Row(fields: _*)).toSeq)
        === data.sortBy(_._2))

    enclave.StopEnclave(eid)

  }


  test("JNIFilterSingleRow") {
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

    // println("Bytes array's length " + byte_array.length)

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

    val ret = enclave.Filter(eid, OP_FILTER_TEST.value, enc_byte_array)

    enclave.StopEnclave(eid)
  }

  test("JNIAggregation") {

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
    val data_1 = Seq((1, "A", 1), (2, "A", 1), (3, "A", 1), (4, "A", 1), (5, "A", 1))
    val data_2 = Seq((6, "B", 1), (7, "B", 1), (8, "B", 1), (9, "C", 1), (10, "C", 1))

    val enc_data1 = encrypt_and_serialize(data_1)
    val enc_data2 = encrypt_and_serialize(data_2)

    // should input dummy row
    val agg_size = 4 + 12 + 16 + 4 + 4 + 2048 + 128
    val agg_row1 = Array.fill[Byte](agg_size)(0)
    val agg_row2 = Array.fill[Byte](agg_size)(0)

    val ret_agg_row1 = enclave.Aggregate(
      eid, OP_GROUPBY_COL2_SUM_COL3_STEP1.value, enc_data1, data_1.length, agg_row1)
    val ret_agg_row2 = enclave.Aggregate(
      eid, OP_GROUPBY_COL2_SUM_COL3_STEP1.value, enc_data2, data_2.length, agg_row2)

    // aggregate the agg_row's together
    val agg_row_buffer = ByteBuffer.allocate(ret_agg_row1.length + ret_agg_row2.length)
    agg_row_buffer.order(ByteOrder.LITTLE_ENDIAN)

    agg_row_buffer.put(ret_agg_row1)
    agg_row_buffer.put(ret_agg_row2)

    agg_row_buffer.flip()
    val agg_row_value = new Array[Byte](agg_row_buffer.limit())
    agg_row_buffer.get(agg_row_value)

    val step_2_values = enclave.ProcessBoundary(
      eid, OP_GROUPBY_COL2_SUM_COL3_STEP1.value, agg_row_value, 2)

    // split these values
    val slices = QED.splitBytes(step_2_values, 2)
    val new_agg_row1 = slices(0)
    val new_agg_row2 = slices(1)
    //val new_agg_row3 = slices(2)

    val partial_result_1 = enclave.Aggregate(
      eid, OP_GROUPBY_COL2_SUM_COL3_STEP2.value, enc_data1, data_1.length, new_agg_row1)
    val partial_result_2 = enclave.Aggregate(
      eid, OP_GROUPBY_COL2_SUM_COL3_STEP2.value, enc_data2, data_2.length, new_agg_row2)

    enclave.StopEnclave(eid)
  }

  test("JNIJoin") {
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
    val table_p_id = new Array[Byte](8)
    val table_f_id = new Array[Byte](8)

    for (i <- 1 to 8) {
      buffer.put("a".getBytes)
    }
    buffer.flip()
    buffer.get(table_p_id)

    buffer.clear()

    for (i <- 1 to 8) {
      buffer.put("b".getBytes)
    }
    buffer.flip()
    buffer.get(table_f_id)

    val enc_table_p = encrypt_and_serialize(table_p_data)
    val enc_table_f = encrypt_and_serialize(table_f_data)

    val processed_table_p = enclave.JoinSortPreprocess(
      eid, OP_JOIN_COL2.value, table_p_id, enc_table_p, table_p_data.length)
    val processed_table_f = enclave.JoinSortPreprocess(
      eid, OP_JOIN_COL2.value, table_f_id, enc_table_f, table_f_data.length)

    // merge the two buffers together
    val processed_rows = processed_table_p ++ processed_table_f

    // println("processed_rows' length is " + processed_rows.length)

    val sorted_rows = enclave.ObliviousSort(
      eid, OP_JOIN_COL2.value, processed_rows, 0, table_p_data.length + table_f_data.length)

    val join_row = enclave.ScanCollectLastPrimary(
      eid, OP_JOIN_COL2.value, sorted_rows, table_p_data.length + table_f_data.length);

    val processed_join_row = enclave.ProcessJoinBoundary(
      eid, OP_JOIN_COL2.value, join_row, 1)

    val joined_rows = enclave.SortMergeJoin(eid, OP_JOIN_COL2.value, sorted_rows,
      table_p_data.length + table_f_data.length, processed_join_row)

    enclave.StopEnclave(eid)
  }


  test("JNIRandomID") {
    val eid = enclave.StartEnclave()

    for (v <- 1 to 10) {
      val buf = enclave.RandomID(eid)
      val integer = QED.decrypt[Int](enclave, eid, buf)
      println("Integer is " + integer)
    }

    enclave.StopEnclave(eid)
  }
}
