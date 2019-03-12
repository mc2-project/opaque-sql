package edu.berkeley.cs.rise.opaque.expressions

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._

/**
 * Spark SQL UDAF to enable elementwise summation of Array[Double].
 *
 * From https://gist.github.com/mrchristine/4f77885dab668a39c063b44b4ae71582.
 */
class VectorSum extends UserDefinedAggregateFunction {
  private def addArray(agg: Array[Double], arr: Array[Double]): Unit = {
    var i = 0
    while (i < arr.length) {
      agg(i) = agg(i) + arr(i)
      i += 1
    }
  }

  // function to determine if the current array size is large enough to old the record size.
  // if not, it will resize the array and return a new copy
  private def ensureArraySize(agg: Array[Double], size: Int): Array[Double] = {
    if(size > agg.length) {
      val newAgg = new Array[Double](size)
      Array.copy(agg, 0, newAgg, 0, agg.length)
      newAgg
    } else {
      agg
    }
  }

  override def inputSchema: StructType =
    new StructType().add("arr", ArrayType(DoubleType, false))

  override def bufferSchema: StructType =
    new StructType().add("arr", ArrayType(DoubleType, false))

  override def dataType: DataType = ArrayType(DoubleType, false)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, Array[Double]())
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)) {
      val arr = input.getAs[Seq[Double]](0)
      val agg: Array[Double] = ensureArraySize(buffer.getSeq[Double](0).toArray, arr.size)
      addArray(agg, arr.toArray)
      buffer.update(0, agg.toSeq)
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val agg2: Array[Double] = buffer2.getSeq[Double](0).toArray
    val agg1: Array[Double] = ensureArraySize(buffer1.getSeq[Double](0).toArray, agg2.length)
    addArray(agg1, agg2)
    buffer1.update(0, agg1.toSeq)
  }

  def evaluate(buffer: Row): Array[Double] =
    buffer.getSeq[Double](0).toArray
}
