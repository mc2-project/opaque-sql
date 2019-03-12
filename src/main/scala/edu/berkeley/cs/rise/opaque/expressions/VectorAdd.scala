package edu.berkeley.cs.rise.opaque.expressions

import breeze.linalg.DenseVector
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.BinaryOperator
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DoubleType

object VectorAdd {
  def vectoradd(v1: Column, v2: Column): Column =
    new Column(VectorAdd(v1.expr, v2.expr))
}

case class VectorAdd(left: Expression, right: Expression)
    extends BinaryOperator with NullIntolerant with CodegenFallback {

  override def dataType: DataType = left.dataType

  override def inputType = DataTypes.createArrayType(DoubleType)

  override def symbol: String = "+"

  override def sqlOperator: String = "$plus"

  protected override def nullSafeEval(input1: Any, input2: Any): ArrayData = {
    val v1 = input1.asInstanceOf[ArrayData].toDoubleArray
    val v2 = input2.asInstanceOf[ArrayData].toDoubleArray
    ArrayData.toArrayData ((new DenseVector(v1) + new DenseVector(v2)).toArray)
  }
}
