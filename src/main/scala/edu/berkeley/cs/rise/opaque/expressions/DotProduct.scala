package edu.berkeley.cs.rise.opaque.expressions

import breeze.linalg.DenseVector
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.BinaryOperator
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
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

  protected override def nullSafeEval(input1: Any, input2: Any): Any =
    new DenseVector(input1.asInstanceOf[Seq[Double]].toArray)
      .dot(new DenseVector(input2.asInstanceOf[Seq[Double]].toArray))
}
