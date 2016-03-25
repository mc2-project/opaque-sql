package oblivious_sort

import scala.math.BigInt
import scala.util.Random
import scala.math._
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.Map
import scala.collection.mutable.SynchronizedSet
import java.lang.ThreadLocal
import java.net.URLEncoder
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object ObliviousSort extends java.io.Serializable {

  //val AccessKey = Config.ACCESS_KEY
  //val SecretKey = Config.SECRET_KEY
  //val Prefix = "s3n://$AccessKey:$SecretKey@oblivious-comp/"
  val Prefix = "s3n://oblivious-comp/"

  val NumMachines = 32
  val NumCores = 2
  val Multiplier = 8

  def time[A](desc: String)(f: => A): A = {
    val start = System.nanoTime
    val result = f
    println(s"$desc: ${(System.nanoTime - start) / 1000000.0} ms")
    result
  }

  class Value(r: Int, c: Int, v: Int) extends java.io.Serializable {
    var row: Int = r
    var column: Int = c
    var value: Int = v

  }


  def BitonicSortReduce(key: ((Int, Int), Iterable[(Int, Int)])): ((Int, Int), (Int, Int)) = {
    // pairwise comparison
    val list = key._2.toArray
    assert(list.length == 2)

    println("Key: " + key)
    println("List:" + list)

    val min_idx = min(list(0)._2, list(1)._2)
    val max_idx = max(list(0)._2, list(1)._2)
    var result = (list(0), list(1))

    if (list(0)._1 > list(1)._1) {
      result = ((list(1)._1, min_idx), (list(0)._1, max_idx))
    } else {
      result = ((list(0)._1, min_idx), (list(1)._1, max_idx))
    }

    result
  }

  def BitonicSortMap(inp: (Int, Int), internal_stage: Int, total_stages: Int): (Int, Int) = {
    // map each KV pair to a new key
    // calculate the pair in this stage

    //println("BitonicSort: stage " + internal_stage + ", total stages " + total_stages)

    var reduceKey = (0, 0)
    var idx = inp._2
    var pair_idx = 0

    if (internal_stage == total_stages) {

      idx = inp._2
      val mod_size = math.pow(2, internal_stage).toInt
      pair_idx = idx / mod_size * mod_size + (mod_size - idx % mod_size - 1)

    } else {

      idx = inp._2
      val mod_size = math.pow(2, internal_stage).toInt
      val offset_size = math.pow(2, internal_stage - 1).toInt
      pair_idx = idx / mod_size * mod_size + (idx + offset_size) % mod_size
    }

    reduceKey = (idx, pair_idx)
    if (idx > pair_idx) {
      reduceKey = (pair_idx, idx)
    }

    //println(inp + "'s reduce key is " + reduceKey)
    reduceKey
  }

  def Expand(pair: ((Int, Int), (Int, Int))): Seq[(Int, Int)] = {
    List(pair._1, pair._2)
  }

  def Merger(inp: RDD[(Int, Int)], stage: Int): RDD[(Int, Int)] = {
    // each merger consists of several stages as well
    // merger i has i stages

    var current_data = inp
    for (i <- stage to 1 by -1) {
      //println("Merge: stage is " + stage + ", i is " + i)
      current_data = current_data
        .map(x => (BitonicSortMap(x, i, stage), x))
        .groupByKey
        .map(x => BitonicSortReduce(x))
        .flatMap(x => Expand(x))
    }

    current_data

  }

  def log2(i: Int): Int = {
    math.ceil((math.log(i) / math.log(2)).toInt).toInt
  }

  def OSort(sc: SparkContext, path: String, len: Int) = {

    // val number_list_len = 32
    // var random_number_list = (0 to number_list_len).toList
    // var number_list = Array.fill[(Int, Int)](number_list_len)((0, 0))

    // val random_number_list_ = Random.shuffle(random_number_list)

    // for (i <- 0 to number_list_len - 1) {
    //   val num = random_number_list_(i)
    //   number_list(i) = (num, i)
    //   println(number_list(i))
    // }


    // val par_data = sc.parallelize(number_list)

    val par_data = sc.objectFile[(Int, Int)](Prefix + path)

    var final_data = par_data
    for (stage <- 1 to log2(len)) {
      //println("OSort stage " + stage)
      final_data = Merger(final_data, stage)
    }

    val l = final_data.map(_.swap).sortByKey().collect

    for (s <- l) {
      //println(s)
    }
  }

/*
        for (i <- 0 to len - 1) {

          var idx = i
          var pair_idx = i

          if (stage_i == stage) {
            val mod_size = math.pow(2, stage_i).toInt
            pair_idx = idx / mod_size * mod_size + (mod_size - idx % mod_size - 1)

          } else {
            val mod_size = math.pow(2, stage_i).toInt
            val offset_size = math.pow(2, stage_i - 1).toInt
            pair_idx = idx / mod_size * mod_size + (idx + offset_size) % mod_size
          }

          // compare and swap keys in index 
          val min_idx = math.min(idx, pair_idx)
          val max_idx = math.max(idx, pair_idx)

          val min_val = math.min(values(idx), values(pair_idx))
          val max_val = math.max(values(idx), values(pair_idx))

          values(min_idx) = min_val
          values(max_idx) = max_val

        }
*/

  def OSortSingleMachine(input: Iterator[Int]): Seq[Int] = {
    // this performs an oblivious sort on an array of integers
    var values = ArrayBuffer.empty[Int]

    for (v <- input) {
      values += v
    }

    val len = values.length
    val log_len = log2(len) + 1

    // sort in two loops

    println("Array's length is " + len + ", log_len is " + log_len)
    var swaps = 0

    for (stage <- 1 to log_len) {
      for (stage_i <- stage to 1 by -1) {

        val part_size = math.pow(2, stage_i).toInt

        if (stage_i == stage) {

          for (i <- 0 to len - 1 by part_size) {
            for (j <- 1 to part_size / 2) {

              val idx = i + j - 1
              val pair_idx = i + part_size - j

              if (pair_idx < len) {

                val min_val = math.min(values(idx), values(pair_idx))
                val max_val = math.max(values(idx), values(pair_idx))

                if (min_val != values(idx)) {
                  swaps += 1
                }

                values(idx) = min_val
                values(pair_idx) = max_val
              }

            }
          }

        } else {

          for (i <- 0 to len - 1 by part_size) {
            for (j <- 1 to part_size / 2) {
              val idx = i + j - 1
              val pair_idx = idx + part_size / 2

              if (pair_idx < len) {

                val min_val = math.min(values(idx), values(pair_idx))
                val max_val = math.max(values(idx), values(pair_idx))

                if (min_val != values(idx)) {
                  swaps += 1
                }

                values(idx) = min_val
                values(pair_idx) = max_val
              }

            }
          }

        }
      }
    }

    println("Total swaps: " + swaps)

    values
  }

  def OSortSingleMachine2(input: Iterable[(Int, Int)], len: Int): Seq[Int] = {
    // this performs an oblivious sort on an array of integers
    var values = Array.fill[Int](len)(0)
    var idx = 0

    for (v <- input) {
      values(idx) = v._2
      idx += 1
    }

    //val len = values.length
    val log_len = log2(len) + 1

    // sort in two loops

    println("Array's length is " + len)
    var swaps = 0

    for (stage <- 1 to log_len) {
      for (stage_i <- stage to 1 by -1) {

        val part_size = math.pow(2, stage_i).toInt

        if (stage_i == stage) {

          for (i <- 0 to len - 1 by part_size) {
            for (j <- 1 to part_size / 2) {

              val idx = i + j - 1
              val pair_idx = i + part_size - j

              if (pair_idx < len) {

                val min_val = math.min(values(idx), values(pair_idx))
                val max_val = math.max(values(idx), values(pair_idx))

                if (min_val != values(idx)) {
                  swaps += 1
                }

                values(idx) = min_val
                values(pair_idx) = max_val
              }

            }
          }

        } else {

          for (i <- 0 to len - 1 by part_size) {
            for (j <- 1 to part_size / 2) {
              val idx = i + j - 1
              val pair_idx = idx + part_size / 2

              if (pair_idx < len) {

                val min_val = math.min(values(idx), values(pair_idx))
                val max_val = math.max(values(idx), values(pair_idx))

                if (min_val != values(idx)) {
                  swaps += 1
                }

                values(idx) = min_val
                values(pair_idx) = max_val
              }

            }
          }

        }
      }
    }

    println("Total swaps: " + swaps)

    values
  }


  // this function performs an oblivious sort on an array of (column, (row, value))
  def OSortSingleMachine_WithIndex(values: Array[Value], low_idx: Int, len: Int) = {

    val log_len = log2(len) + 1
    val offset = low_idx

    // sort in two loops

    println("Array's length is " + len)
    var swaps = 0
    var min_val = 0
    var max_val = 0

    for (stage <- 1 to log_len) {
      for (stage_i <- stage to 1 by -1) {

        val part_size = math.pow(2, stage_i).toInt

        if (stage_i == stage) {

          for (i <- offset to (offset + len - 1) by part_size) {
            for (j <- 1 to part_size / 2) {

              val idx = i + j - 1
              val pair_idx = i + part_size - j

              if (pair_idx < offset + len) {

                min_val = math.min(values(idx).value, values(pair_idx).value)
                max_val = math.max(values(idx).value, values(pair_idx).value)

                if (min_val != values(idx).value) {
                  swaps += 1
                }

                values(idx).value = min_val
                values(pair_idx).value = max_val
              }

            }
          }

        } else {

          for (i <- offset to (offset + len - 1) by part_size) {
            for (j <- 1 to part_size / 2) {
              val idx = i + j - 1
              val pair_idx = idx + part_size / 2

              if (pair_idx < offset + len) {

                min_val = math.min(values(idx).value, values(pair_idx).value)
                max_val = math.max(values(idx).value, values(pair_idx).value)

                if (min_val != values(idx).value) {
                  swaps += 1
                }

                values(idx).value = min_val
                values(pair_idx).value = max_val

              }

            }
          }

        }
      }
    }

    println("Total swaps: " + swaps)
  }


  def OSortSingleMachineStep5_Step7(
    values: Array[Value],
    lower_slice: Array[Value],
    higher_slice: Array[Value],
    r: Int,
    s: Int
  ) = {
    // lower_slice's length or higher_slice's length could be 0

    assert(lower_slice.length == 0 || lower_slice.length == r)
    assert(higher_slice.length == 0 || higher_slice.length == r)

    // sort the values first
    time("Middle value sort") {
      OSortSingleMachine_WithIndex(values, 0, values.length)

      // for (idx <- 0 to Multiplier - 1) {
      //   OSortSingleMachine_WithIndex(values, idx * r, r)
      //   println("Sorting from " + idx * r)
      // }

      // for (idx <- 0 to Multiplier - 2) {
      //   OSortSingleMachine_WithIndex(values, idx * r + r / 2, r)
      //   println("Second sorting from " + (idx * r + r / 2))
      // }

    }

    // for (idx <- 0 to values.length - 1) {
    //   if (idx < values.length - 1) {
    //     println(values(idx).value)
    //     if (values(idx).value > values(idx + 1).value) {
    //       println("WRONG! idx is " + idx)
    //     }
    //     //assert(values(idx).value < values(idx + 1).value)
    //   }
    // }

    // then sort each slice
    if (lower_slice.length > 0) {
      time("lower slice sort") {
        OSortSingleMachine_WithIndex(lower_slice, 0, lower_slice.length)

        // then merge lower slice with values
        for (i <- 0 to r - 1) {

          val idx = i
          val pair_idx = r - i - 1

          val min_val = math.min(lower_slice(idx).value, values(pair_idx).value)
          val max_val = math.max(lower_slice(idx).value, values(pair_idx).value)

          lower_slice(idx).value = min_val
          values(pair_idx).value = max_val

        }

        // sort just the first r numbers
        OSortSingleMachine_WithIndex(values, 0, r)
      }

    }

    if (higher_slice.length > 0) {
      time("higher slice sort") {
        OSortSingleMachine_WithIndex(higher_slice, 0, higher_slice.length)

        val offset = r * (Multiplier - 1)

        // merge higher slice with values
        for (i <- 0 to r - 1) {

          val idx = i + offset
          val pair_idx = r - i - 1

          val min_val = math.min(higher_slice(pair_idx).value, values(idx).value)
          val max_val = math.max(higher_slice(pair_idx).value, values(idx).value)

          values(idx).value = min_val
          higher_slice(pair_idx).value = max_val

        }

        // sort just the last r numbers
        OSortSingleMachine_WithIndex(values, r * (Multiplier - 1), r)
      }

    }

    // for (v <- values) {
    //   println(v)
    // }

  }

  def Transpose(value: Value, r: Int, s: Int) = {
    val row = value.row
    val column = value.column

    val idx = (column - 1) * r + row
    val new_row = (idx - 1) / s + 1
    val new_column = (idx + s - 1) % s + 1
    //println("Transpose: (" + row + ", " + column + ")" + " --> (" + new_row + ", " + new_column + ")")

    value.row = new_row
    value.column = new_column
  }

  def Untranspose(value: Value, r: Int, s: Int) = {
    val row = value.row
    val column = value.column

    val idx = (row - 1) * s + column
    val new_row = (idx + r - 1) % r + 1
    val new_column = (idx - 1) / r + 1

    value.row = new_row
    value.column = new_column
  }

  def ShiftDown(value: Value, r: Int, s: Int) = {

    val row = value.row
    val column = value.column

    val idx = (column - 1) * r + row
    val new_idx = (idx + r / 2 - 1) % (r * s) + 1

    val new_row = (new_idx + r - 1) % r + 1
    val new_column = (new_idx - 1) / r + 1

    value.row = new_row
    value.column = new_column
  }

  def ShiftUp(value: Value, r: Int, s: Int) = {

    // when shifting up, there's this special condition that the top half of first column should stay there,
    // and bottom half of first column should go to the end

    val row = value.row
    val column = value.column

    if (column == 1) {

      if (row > r / 2) {

        val new_row = row
        val new_column = s

        value.row = new_row
        value.column = new_column
      }

    }  else {

      val idx = (column - 1) * r + row
      val new_idx = (idx + r * s - r / 2 - 1) % (r * s) + 1

      val new_row = (new_idx + r - 1) % r + 1
      val new_column = (new_idx - 1) / r + 1

      value.row = new_row
      value.column = new_column
    }

  }


  def Flip(v: (Int, Int, Int)): (Int, (Int, Int)) = {
    (v._3, (v._2, v._1))
  } 

  // Int, Int, Int => value, row, column
  def ColumnSortParFunction(key: (Int, Iterable[(Int, Int)]), r: Int, s: Int, f: ((Int, Int, Int), Int, Int) => ((Int, Int, Int))): Seq[(Int, (Int, Int))] = {

    // pre-allocate result array
    var ret_result = Array.fill[(Int, (Int, Int))](r)((0, (0, 0)))
    // fill in the array with current numbers
    var i = 0
    for (v <- key._2) {
      ret_result(i) = (key._1, (v._1, v._2))
      i += 1
    }

    // time("Column sort") {
    //   OSortSingleMachine_WithIndex(ret_result, 0, r)
    // }

    // append result with row and column
    i = 0
    for (v <- ret_result) {
      // output = (value, row, column)
      val x = f((v._2._2, i+1, key._1), r, s)

      // output = (column (row, result))
      ret_result(i) = Flip(x)
      i += 1
    }

    ret_result
  }

  def ColumnSortParFunction1(index: Int, it: Iterator[(Int, Int)], 
    numPartitions: Int, r: Int, s: Int): Iterator[Value] = {

    val rounds = s / numPartitions
    var ret_result = Array.empty[Value]
    time("Column sort 1-- array allocation time") {
      ret_result = Array.fill[Value](r * rounds)(new Value(0, 0, 0))
    }

    println("Ret_result's array size is " + (r * rounds))
    println("Total number of rounds: " + rounds)

    var counter = 0
    for (v <- it) {
      ret_result(counter).value = v._1
      counter += 1
    }

    val array_len = counter / (s / numPartitions)

    time("Column sort, step 1, total sort time") {
      for (rnd <- 0 to rounds - 1) {

        time("Column sort, step 1") {
          OSortSingleMachine_WithIndex(ret_result, rnd * array_len, array_len)
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

  def ColumnSortStep3(key: (Int, Iterable[(Int, Int)]), r: Int, s: Int): Iterator[Value] = {

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

    var ret_result = Array.fill[Value](len)(new Value(0, 0, 0))
    var counter = 0

    for (v <- key._2) {
      ret_result(counter).value = v._2
      //println("ret_result's value for col " + ret_result(counter).column + ": " + ret_result(counter).value)
      counter += 1
    }

    time("Column sort, step 3") {
      OSortSingleMachine_WithIndex(ret_result, 0, r)
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

  def ColumnSortStep5_Step7(key: (Int, Iterable[(Int, Int)]), r: Int, s: Int): Iterator[(Int, (Int, Int))] = {

    var len = r * Multiplier + 2 * r

    var final_result = Array.fill[Value](r * Multiplier)(new Value(0, 0, 0))
    var counters = Array.fill[Int](Multiplier)(0)

    if (key._1 == 1) {
      // only has one higher slice
      var lower_slice = Array.fill[Value](0)(new Value(0, 0, 0))
      var higher_slice = Array.fill[Value](r)(new Value(0, 0, 0))
      var higher_slice_idx = 0
      var final_result_idx = 0

      for (v <- key._2) {
        if (v._1 == r * s + 1) {
          //higher_slice(higher_slice_idx) = (key._1, (higher_slice_idx + 1, v._2))
          higher_slice(higher_slice_idx).column = key._1
          higher_slice(higher_slice_idx).row = higher_slice_idx + 1
          higher_slice(higher_slice_idx).value = v._2
          higher_slice_idx += 1
        } else {
          //final_result(final_result_idx) = (key._1, (final_result_idx + 1, v._2))

          val col_index = (v._1-1) % Multiplier
          val offset = col_index * r
          val real_index = counters(col_index) + offset

          final_result(real_index).column = key._1
          final_result(real_index).row = final_result_idx + 1
          final_result(real_index).value = v._2

          counters(col_index) += 1
          final_result_idx += 1
        }
      }

      time("Column sort step5, step7") {
        OSortSingleMachineStep5_Step7(final_result, lower_slice, higher_slice, r, s)
      }

    } else if (key._1 == s / Multiplier) {
      // only has one lower slice
      var lower_slice = Array.fill[Value](r)(new Value(0, 0, 0))
      var higher_slice = Array.fill[Value](0)(new Value(0, 0, 0))
      var lower_slice_idx = 0
      var final_result_idx = 0

      for (v <- key._2) {
        if (v._1 == -1) {
          //lower_slice(lower_slice_idx) = (key._1, (lower_slice_idx + 1, v.value))
          lower_slice(lower_slice_idx).column = key._1
          lower_slice(lower_slice_idx).row = lower_slice_idx + 1
          lower_slice(lower_slice_idx).value = v._2
          lower_slice_idx += 1
        } else {
          //final_result(final_result_idx) = (key._1, (final_result_idx + 1, v.value))

          val col_index = (v._1-1) % Multiplier
          val offset = col_index * r
          val real_index = counters(col_index) + offset

          final_result(real_index).column = key._1
          final_result(real_index).row = final_result_idx + 1
          final_result(real_index).value = v._2

          counters(col_index) += 1
          final_result_idx += 1
        }
      }

      time("Column sort step5, step7") {
        OSortSingleMachineStep5_Step7(final_result, lower_slice, higher_slice, r, s)
      }

    } else {
      // has two slices

      var lower_slice = Array.fill[Value](r)(new Value(0, 0, 0))
      var higher_slice = Array.fill[Value](r)(new Value(0, 0, 0))
      var lower_slice_idx = 0
      var higher_slice_idx = 0
      var final_result_idx = 0

      for (v <- key._2) {
        if (v._1 == -1) {
          //lower_slice(lower_slice_idx) = (key._1, (lower_slice_idx + 1, v.value))
          lower_slice(lower_slice_idx).column = key._1
          lower_slice(lower_slice_idx).row = lower_slice_idx + 1
          lower_slice(lower_slice_idx).value = v._2
          lower_slice_idx += 1
        } else if (v._1 == r * s + 1) {
          //higher_slice(higher_slice_idx) = (key._1, (higher_slice_idx + 1, v.value))
          higher_slice(higher_slice_idx).column = key._1
          higher_slice(higher_slice_idx).row = higher_slice_idx + 1
          higher_slice(higher_slice_idx).value = v._2
          higher_slice_idx += 1
        } else {
          //final_result(final_result_idx) = (key._1, (final_result_idx + 1, v.value))

          val col_index = (v._1-1) % Multiplier
          val offset = col_index * r
          val real_index = counters(col_index) + offset

          final_result(real_index).column = key._1
          final_result(real_index).row = higher_slice_idx + 1
          final_result(real_index).value = v._2

          counters(col_index) += 1
          final_result_idx += 1
        }
      }

      time("Column sort step5, step7") {
        OSortSingleMachineStep5_Step7(final_result, lower_slice, higher_slice, r, s)
      }
    }

    var ret_result = ArrayBuffer.empty[(Int, (Int, Int))]

    for (idx <- 1 to r * Multiplier) {
      val new_col = (idx - 1) / r + (key._1 - 1) * Multiplier + 1
      val new_row = (idx - 1) % r + 1
      
      ret_result += ((new_col, (new_row, final_result(idx - 1).value)))
    }

    println("Final result's length is " + final_result.length)

    ret_result.iterator
  }


  def ColumnSortFinal(key: (Int, Iterable[(Int, Int)]), r: Int, s: Int): Iterator[(Int, (Int, Int))] = {
    var result = Array.empty[Value]
    var num_columns = 0
    var min_col = 0

    println("ColumnSortFinal: column is " + key._1 + ", length is " + key._2.toArray.length)

    if (key._1 == 1 || key._1 == NumMachines) {
      result = Array.fill[Value](r * Multiplier * NumCores + r)(new Value(0, 0, 0))
      num_columns = Multiplier * NumCores + 1
      if (key._1 == 1) {
        min_col = 1
      } else {
        min_col = Multiplier * NumCores * (NumMachines - 1)
      }
    } else {
      result = Array.fill[Value](r * Multiplier * NumCores + 2 * r)(new Value(0, 0, 0))
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
            OSortSingleMachine_WithIndex(result, NumCores * (Multiplier * r), r)
            println("[" + key._1 + " - 1] Sorting from " + NumCores * (Multiplier * r) + " for len " + r)
          } else if (i == 1 && key._1 > 1) {
            // also want to sort the first piece
            OSortSingleMachine_WithIndex(result, 0, r)
            println("[" + key._1 + " - 2] Sorting from 0 for len" + r)
          }

          OSortSingleMachine_WithIndex(result, (i - 1) * (Multiplier * r) + offset, Multiplier * r)
          println("[" + key._1 + " - 3] Sorting " + (i - 1) * (Multiplier * r) + offset + " for len " +  Multiplier * r)
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
            println("Sorting array from " + low_index + " for length " + 2 * r + ", for column " + key._1 + ", total length: " + result.length)
            OSortSingleMachine_WithIndex(result, low_index, 2 * r)
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

    var final_result = ArrayBuffer.empty[(Int, (Int, Int))]
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

    println("Final result's length is " + final_result.length)

    final_result.iterator

  }


  def GetFileData(sc: SparkContext, path: String): RDD[(Int, Int)] = {
    val data = sc.objectFile[(Int, Int)](Prefix + path)
    data
  }

  // this sorting algorithm is taken from "Tight Bounds on the Complexity of Parallel Sorting"
  def ColumnSort(sc: SparkContext, data: RDD[(Int, Int)], r_input: Int = 0, s_input: Int = 0): RDD[(Int, (Int, Int))] = {
    // let len be N
    // divide N into r * s, where s is the number of machines, and r is the size of the 
    // constraints: s | r; r >= 2 * (s-1)^2

    val len = data.count

    var s = s_input
    var r = r_input

    if (r_input == 0 && s_input == 0) {
      s = Multiplier * NumMachines * NumCores
      r = (math.ceil(len * 1.0 / s)).toInt
    } 

    println("s is " + s + ", r is " + r)


    // TODO: for now do an assert; in the future, need to pad with dummy values
    assert(r >= 2 * math.pow(s, 2).toInt)

    // // read data
    // val par_data = data.map(x => 
    //   { 
    //     val row = ((x._2 + r) % r + 1).toInt
    //     val column = ((x._2) / r + 1).toInt
    //     //println("Transform into matrix: " + "(" + x._1 + ", " + row + ", " + column + ")")
    //     (column, (row, x._1))
    //   }
    // )

    // // this algorithm runs in 8 steps
    // // step 1: sort each column
    // // step 2: transpose
    // val par_data_1_2 = par_data.groupByKey(s).flatMap(x => ColumnSortParFunction(x, r, s, Transpose))

    // Alternative step 1
    //val par_data = data

    //val num_partitions = par_data.partitions.size
    //val num_partitions = 32

    val par_data = data.repartition(NumCores * NumMachines)
    par_data.count
    //val par_data = data.coalesce(NumMachines)
    //val num_partitions = par_data.partitions.size

    val par_data_1_2 = par_data.mapPartitionsWithIndex((index, x) => ColumnSortParFunction1(index, x, NumCores * NumMachines, r, s))

    // // step 2: transpose
    // val par_data_2 = par_data_1.map(x => {
    //   val begin = System.nanoTime
    //   val ret = Flip(Transpose(x, r, s))
    //   timer += ((System.nanoTime - begin) / 1000000.0)
    //   ret
    // })


    // // step 3: sort each column
    // // step 4: un-transpose
    // val par_data_3_4 = par_data_1_2.groupByKey(s).flatMap(x => ColumnSortParFunction(x, r, s, Untranspose))
    
    // // step 5: sort each column, again
    // // step 6: shift down by 1/2 * r
    // val par_data_5_6 = par_data_3_4.groupByKey(s).flatMap(x => ColumnSortParFunction(x, r, s, ShiftDown))


    // // step 7: final column sort!
    // // step 8: shift back
    // val par_data_final = par_data_5_6.groupByKey(s).flatMap(x => ColumnSortParFunction(x, r, s, ShiftUp))


    /* Alternative */
    val par_data_intermediate = par_data_1_2.map(x => (x.column, (x.row, x.value))).groupByKey(s).flatMap(x => ColumnSortStep3(x, r, s))
    //val par_data_final = par_data_intermediate.map(x => (x.column, (x.row, x.value))).groupByKey(s).flatMap(x => ColumnSortStep5_Step7(x, r, s))
    val par_data_final = par_data_intermediate.map(x => (x.column, (x.row, x.value))).groupByKey(s).flatMap(x => ColumnSortFinal(x, r, s))
    /* End Alternative */

    val count = par_data_final.count

    par_data_final
  }

  def RegularSort(sc: SparkContext, path: String) = {
    val par_data = sc.objectFile[(Int, Int)](Prefix + path).cache
    val sum = par_data.count
    val data = par_data.sortByKey(true, NumMachines * NumCores)
    val count = data.count
  }

  def GenRandomData(offset: Int, len: Int): Seq[(Int, Int)] ={
    val r = Random
    var inp = Array.fill[(Int, Int)](len)(0, 0)

    for (i <- 0 to len - 1) {
      inp(i) = (r.nextInt(), offset * len + i)
    }

    inp
  }

  def WriteRandomDataS3(sc: SparkContext, len: Int, path: String) {

    val par = NumMachines * NumCores
    val data = sc.parallelize((1 to par).toList, par).flatMap(x => GenRandomData(x-1, len / par))
    println("Total data size: " + data.count)

    data.saveAsObjectFile(Prefix + path)
  }


  def main(args: Array[String]) {
    var conf = new SparkConf(false)
    val sc = new SparkContext()

    // Benchmark regular sort and oblivious sort

    for (i <- 1 to 1) {
      println("Run " + i)
      //WriteRandomDataS3(sc, 1024 * 1024 * 1024, "test_1B_2_" + i)

      // time("Regular sort") {
      //   RegularSort(sc, "test_1B_" + i)
      // }

      time("ColumnSort") {
        val rdd = GetFileData(sc, "test_1B_2_" + i)
        rdd.count
        ColumnSort(sc, rdd)
      }

      println("")
    }

    //val random_number_list_ = List(6, 14, 10, 3, 5, 15, 4, 1, 8, 11, 12, 7, 13, 9, 2, 0)
  }

}

