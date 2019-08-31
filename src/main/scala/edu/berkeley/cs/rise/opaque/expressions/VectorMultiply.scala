package edu.berkeley.cs.rise.opaque.expressions

import breeze.linalg.DenseVector
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.BinaryExpression
import org.apache.spark.sql.catalyst.expressions.ExpectsInputTypes
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DoubleType

object VectorMultiply {
  def vectormultiply(v: Column, c: Column): Column =
    new Column(VectorMultiply(v.expr, c.expr))
}

case class VectorMultiply(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant with CodegenFallback with ExpectsInputTypes {

  override def dataType: DataType = left.dataType

  override def inputTypes = Seq(DataTypes.createArrayType(DoubleType), DoubleType)

  protected override def nullSafeEval(input1: Any, input2: Any): ArrayData = {
    val v = input1.asInstanceOf[ArrayData].toDoubleArray
    val c = input2.asInstanceOf[Double]
    ArrayData.toArrayData((new DenseVector(v) * c).toArray)
  }
}
