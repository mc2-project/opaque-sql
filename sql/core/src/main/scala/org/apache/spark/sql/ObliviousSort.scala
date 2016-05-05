package oblivious_sort

import java.lang.ThreadLocal
import java.net.URLEncoder
import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.SynchronizedSet
import scala.math.BigInt
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.QED
import org.apache.spark.sql.QEDOpcode._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.storage.StorageLevel

object ObliviousSort extends java.io.Serializable {

  // TODO(ankurdave): Use SparkContext to determine these
  val NumMachines = 2
  val NumCores = 1
  val Multiplier = 8 // TODO: fix bug when this is 1

  def time[A](desc: String)(f: => A): A = {
    val start = System.nanoTime
    val result = f
    // println(s"$desc: ${(System.nanoTime - start) / 1000000.0} ms")
    result
  }

  class Value(r: Int, c: Int, v: Array[Byte]) extends java.io.Serializable {
    def this(r: Int, c: Int) = this(r, c, new Array[Byte](0))
    var row: Int = r
    var column: Int = c
    var value: Array[Byte] = v
  }

  def log2(i: Int): Int = {
    math.ceil((math.log(i) / math.log(2)).toInt).toInt
  }

  // this function performs an oblivious sort on an array of (column, (row, value))
  def OSortSingleMachine_WithIndex(
      values: Array[Value], low_idx: Int, len: Int, opcode: Int) = {

    val valuesSlice = values.slice(low_idx, low_idx + len)

    // Concatenate encrypted rows into a buffer
    val nonEmptyRows = valuesSlice.map(_.value).filter(_.length != 0)
    val concatRows = QED.concatByteArrays(nonEmptyRows)

    // Sort rows in enclave
    val (enclave, eid) = QED.initEnclave()
    val allRowsSorted =
      if (nonEmptyRows.nonEmpty) {
        enclave.ObliviousSort(eid, opcode, concatRows, 0, nonEmptyRows.length)
      }
      else Array.empty[Byte]
    // enclave.StopEnclave(eid)

    // Copy rows back into values
    val sortedRowIter =
      if (nonEmptyRows.nonEmpty) {
        if (opcode == OP_JOIN_COL2.value) {
          // Row format is nonstandard but rows are guaranteed to be the same length, so we can split
          // them evenly
          QED.splitBytes(allRowsSorted, nonEmptyRows.length).iterator
        } else {
          // Rows may be different lengths but row format is standard, so we must parse each row
          QED.readRows(allRowsSorted)
        }
      } else {
        Iterator.empty
      }
    for (row <- valuesSlice) {
      if (sortedRowIter.hasNext) {
        row.value = sortedRowIter.next()
      } else {
        row.value = new Array[Byte](0)
      }
    }
  }

  def Transpose(value: Value, r: Int, s: Int): Unit = {
    val row = value.row
    val column = value.column

    val idx = (column - 1) * r + row
    val new_row = (idx - 1) / s + 1
    val new_column = (idx + s - 1) % s + 1
    //println("Transpose: (" + row + ", " + column + ")" + " --> (" + new_row + ", " + new_column + ")")

    value.row = new_row
    value.column = new_column
  }

  def ColumnSortParFunction1(index: Int, it: Iterator[(Array[Byte], Int)],
      numPartitions: Int, r: Int, s: Int, opcode: Int): Iterator[Value] = {

    val rounds = s / numPartitions
    var ret_result = Array.empty[Value]
    time("Column sort 1-- array allocation time") {
      ret_result = Array.fill[Value](r * rounds)(new Value(0, 0))
    }

    // println("Ret_result's array size is " + (r * rounds))
    // println("Total number of rounds: " + rounds)

    var counter = 0
    for (v <- it) {
      ret_result(counter).value = v._1
      counter += 1
    }

    val array_len = counter / (s / numPartitions)

    time("Column sort, step 1, total sort time") {
      for (rnd <- 0 to rounds - 1) {

        time("Column sort, step 1") {
          OSortSingleMachine_WithIndex(ret_result, rnd * array_len, array_len, opcode)
        }

        // add result to ret_result
        val column = index * rounds + rnd  + 1
        //println("Index is " + index + ", Column: " + column)

        for (idx <- 0 to r - 1) {

          val index = rnd * array_len + idx

          ret_result(index).column = column
          ret_result(index).row = idx + 1
          Transpose(ret_result(index), r, s)

        }
      }
    }

    ret_result.iterator
  }

  def ColumnSortStep3(
      key: (Int, Iterable[(Int, Array[Byte])]), r: Int, s: Int, opcode: Int): Iterator[Value] = {

    var len = 0
    var i = 0

    for (iter <- 0 to r - 1) {
      // output = (value, row, column)
      val old_column = key._1
      val old_row = i + 1

      val idx = (old_row - 1) * s + old_column
      val new_row = (idx + r - 1) % r + 1
      val new_column = (idx - 1) / r + 1

      if ((new_column % (NumCores * Multiplier) == 0 && new_column != s) 
        || (new_column % (NumCores * Multiplier) == 1 && new_column != 1)) {
        len += 1
      }

      len += 1

      i += 1
    }

    var ret_result = Array.fill[Value](len)(new Value(0, 0))
    var counter = 0

    for (v <- key._2) {
      ret_result(counter).value = v._2
      //println("ret_result's value for col " + ret_result(counter).column + ": " + ret_result(counter).value)
      counter += 1
    }

    // println(s"Filled counter $counter vs. len $len")

    time("Column sort, step 3") {
      OSortSingleMachine_WithIndex(ret_result, 0, r, opcode)
    }


    // append result with row and column
    i = 0
    var additional_index = r
    for (idx <- 0 to r - 1) {
      // output = (value, row, column)
      val old_column = key._1
      val old_row = i + 1

      val index = (old_row - 1) * s + old_column
      val new_row = (index + r - 1) % r + 1
      val new_column = (index - 1) / r + 1

      val final_column = (new_column - 1) / (NumCores * Multiplier) + 1
      val final_row = new_column

      //println("[(col, row)] (" + new_column + ", " + new_row + ") --> (" + final_column + ", " + final_row + ")")

      if (new_column % (Multiplier * NumCores) == 0 && new_column != s && final_column < NumMachines) {
        ret_result(additional_index).column = final_column + 1
        ret_result(additional_index).row = final_row
        ret_result(additional_index).value = ret_result(idx).value
        //println("[(col, row)] (" + new_column + ", " + new_row + ") --> (" + (final_column + 1) + ", " + final_row + ")")
        additional_index += 1
      } else if (new_column % (Multiplier * NumCores) == 1 && new_column != 1 && final_column > 1) {
        ret_result(additional_index).column = final_column - 1
        ret_result(additional_index).row = final_row
        ret_result(additional_index).value = ret_result(idx).value
        //println("[(col, row)] (" + new_column + ", " + new_row + ") --> (" + (final_column - 1) + ", " + final_row + ")")
        additional_index += 1
      }

      ret_result(idx).column = final_column
      ret_result(idx).row = final_row

      i += 1
    }

    ret_result.iterator
  }

  def ColumnSortFinal(key: (Int, Iterable[(Int, Array[Byte])]), r: Int, s: Int, opcode: Int)
    : Iterator[(Int, (Int, Array[Byte]))] = {

    var result = Array.empty[Value]
    var num_columns = 0
    var min_col = 0

    // println("ColumnSortFinal: column is " + key._1 + ", length is " + key._2.toArray.length)

    if (key._1 == 1 || key._1 == NumMachines) {
      result = Array.fill[Value](r * Multiplier * NumCores + r)(new Value(0, 0))
      num_columns = Multiplier * NumCores + 1
      if (key._1 == 1) {
        min_col = 1
      } else {
        min_col = Multiplier * NumCores * (NumMachines - 1)
      }
    } else {
      result = Array.fill[Value](r * Multiplier * NumCores + 2 * r)(new Value(0, 0))
      num_columns = Multiplier * NumCores + 2
      min_col = Multiplier * NumCores * (key._1 - 1) 
    }


    var counter = Map[Int, Int]()

    time("Single threaded allocation") {
      for (v <- key._2) {
        val col = v._1
        if (!counter.contains(col)) {
          counter(col) = 0
        }
        
        val index = counter(col)
        //if (key._1 == 1) {
        val offset = (col - min_col) * r
        result(index + offset).column = v._1
        result(index + offset).column = index + 1
        result(index + offset).value = v._2
        //}

        counter(col) += 1
      }
    }

    // run column sort in parallel

    time("First sort") {
      val threads = for (i <- 1 to NumCores) yield new Thread() {
        override def run() {
          var offset = r
          if (key._1 == 1) {
            offset = 0
          }

          if (i == NumCores && key._1 < NumMachines) {
            // also sort the last piece
            OSortSingleMachine_WithIndex(result, NumCores * (Multiplier * r), r, opcode)
            // println("[" + key._1 + " - 1] Sorting from " + NumCores * (Multiplier * r) + " for len " + r)
          } else if (i == 1 && key._1 > 1) {
            // also want to sort the first piece
            OSortSingleMachine_WithIndex(result, 0, r, opcode)
            // println("[" + key._1 + " - 2] Sorting from 0 for len" + r)
          }

          OSortSingleMachine_WithIndex(result, (i - 1) * (Multiplier * r) + offset, Multiplier * r, opcode)
          // println("[" + key._1 + " - 3] Sorting " + (i - 1) * (Multiplier * r) + offset + " for len " +  Multiplier * r)
        }
      }

      for (t <- threads) {
        t.start()
      }

      for (t <- threads) {
        t.join()
      }
    }

    time("Second sort") {
      val threads_2 = for (i <- 1 to NumCores + 1) yield new Thread() {
        override def run() {
          var offset = 0
          if (key._1 == 1) {
            offset = -1 * r
          }
          if (!(key._1 == 1 && i == 1) && !(key._1 == NumMachines && i == NumCores + 1)) {
            val low_index = (i - 1) * (Multiplier * r) + offset
            // println("Sorting array from " + low_index + " for length " + 2 * r + ", for column " + key._1 + ", total length: " + result.length)
            OSortSingleMachine_WithIndex(result, low_index, 2 * r, opcode)
          }
        }
      }

      for (t <- threads_2) {
        t.start()
      }

      for (t <- threads_2) {
        t.join()
      }
    }

    var final_result = ArrayBuffer.empty[(Int, (Int, Array[Byte]))]
    var final_offset = r
    if (key._1 == 1) {
      final_offset = 0
    }

    var begin_index = r
    if (key._1 == 1) {
      begin_index = 0
    }

    val end_index = r * Multiplier * NumCores + begin_index - 1
    //println("Result's length is " + result.length + ", end_index is " + end_index)

    var final_counter = 0
    for (idx <- begin_index to end_index) {
      val new_col = (final_counter) / r + (key._1 - 1) * (NumCores * Multiplier) + 1
      val new_row = (final_counter) % r + 1

      final_result += ((new_col, (new_row, result(idx).value)))
      final_counter += 1
      //println("[" + new_col + ", " + new_row + "]: " + result(idx).value + ", old column is " + key._1)
    }

    // println("Final result's length is " + final_result.length)

    final_result.iterator

  }

  // this sorting algorithm is taken from "Tight Bounds on the Complexity of Parallel Sorting"
  def ColumnSort(
      sc: SparkContext, data: RDD[Array[Byte]], opcode: Int, r_input: Int = 0, s_input: Int = 0)
    : RDD[Array[Byte]] = {

    // let len be N
    // divide N into r * s, where s is the number of machines, and r is the size of the 
    // constraints: s | r; r >= 2 * (s-1)^2

    data.cache()

    val len = data.count

    var s = s_input
    var r = r_input

    if (r_input == 0 && s_input == 0) {
      s = Multiplier * NumMachines * NumCores
      r = (math.ceil(len * 1.0 / s)).toInt
    } 

    // println("s is " + s + ", r is " + r)

    if (r < 2 * math.pow(s, 2).toInt) {
      println(s"Padding r from $r to ${2 * math.pow(s, 2).toInt}. s=$s, len=$len")
      r = 2 * math.pow(s, 2).toInt
    }

    val padded =
      if (len != r * s) {
        println(s"Padding len from $len to ${r*s}. r=$r, s=$s")
        assert(r * s > len)
        val firstPartitionSize =
          data.mapPartitionsWithIndex((index, iter) =>
            if (index == 0) Iterator(iter.size) else Iterator(0)).collect.sum
        val paddingSize = r * s - len.toInt
        data.mapPartitionsWithIndex((index, iter) =>
          if (index == 0) iter.padTo(firstPartitionSize + paddingSize, new Array[Byte](0))
          else iter)
      } else {
        data
      }

    val numPartitions = NumCores * NumMachines
    val par_data =
      time("prepartitioning") {
        val result = padded.zipWithIndex.map(t => (t._1, t._2.toInt))
          .groupBy((x: (Array[Byte], Int)) => x._2 / r + 1, numPartitions).flatMap(_._2)
          .cache()
        result.count
        result
      }

    val par_data_1_2 = par_data.mapPartitionsWithIndex((index, x) =>
      ColumnSortParFunction1(index, x, NumCores * NumMachines, r, s, opcode))

    /* Alternative */
    val par_data_intermediate = par_data_1_2.map(x => (x.column, (x.row, x.value)))
      .groupByKey(numPartitions).flatMap(x => ColumnSortStep3(x, r, s, opcode))
    val par_data_i2 = par_data_intermediate.map(x => (x.column, (x.row, x.value)))
      .groupByKey(numPartitions).flatMap(x => ColumnSortFinal(x, r, s, opcode))
    val par_data_final =
      time("final partition sorting") {
        val result = par_data_i2
          .map(v => ((v._1, v._2._1), v._2._2))
          .sortByKey()
          .cache()
        result.count
        result
      }
    /* End Alternative */

    par_data_final.map(_._2).filter(_.nonEmpty)
  }

  def GenRandomData(offset: Int, len: Int): Seq[(Int, Int)] ={
    val r = Random
    var inp = Array.fill[(Int, Int)](len)(0, 0)

    for (i <- 0 to len - 1) {
      inp(i) = (r.nextInt(), offset * len + i)
    }

    inp
  }

}
