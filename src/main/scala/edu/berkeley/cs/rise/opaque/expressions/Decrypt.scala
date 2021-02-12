package edu.berkeley.cs.rise.opaque.expressions

import edu.berkeley.cs.rise.opaque.Utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

object Decrypt {
  def decrypt(v: Column, dataType: DataType): Column = new Column(Decrypt(v.expr, dataType))
}

// TODO: write expression description
case class Decrypt(child: Expression, outputDataType: DataType)
    extends UnaryExpression with NullIntolerant with CodegenFallback {

  override def dataType: DataType = outputDataType

  protected override def nullSafeEval(input: Any): Any = {
    // This function is implemented so that we can test against Spark
    val v = input.asInstanceOf[UTF8String].toString
    Utils.decryptScalar(v)
  }
}
