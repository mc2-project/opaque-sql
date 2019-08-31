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

object DotProduct {
  def dot(v1: Column, v2: Column): Column =
    new Column(DotProduct(v1.expr, v2.expr))
}

case class DotProduct(left: Expression, right: Expression)
    extends BinaryOperator with NullIntolerant with CodegenFallback {

  override def dataType: DataType = DoubleType

  override def inputType = DataTypes.createArrayType(DoubleType)

  override def symbol: String = "dot"

  override def sqlOperator: String = "$dot"

  protected override def nullSafeEval(input1: Any, input2: Any): Double = {
    val v1 = input1.asInstanceOf[ArrayData].toDoubleArray
    val v2 = input2.asInstanceOf[ArrayData].toDoubleArray
    new DenseVector(v1).dot(new DenseVector(v2))
  }
}
