package edu.berkeley.cs.rise.opaque.expressions

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.BinaryOperator
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DoubleType

object Decrypt {
  def decrypt(v: Column): Column = new Column(Decrypt(v.expr))
}

case class Decrypt(child: Expression, dataType: DataType)
    extends UnaryOperator with NullIntolerant with CodegenFallback {

  override def dataType: DataType = dataType

  override def inputType = StringType

  override def symbol: String = "decrypt"

  override def sqlOperator: String = "decrypt"

  protected override def nullSafeEval(input: StringType): Any  = {
    // TODO: Implement this function so that we can test against Spark
    val v = input.asInstanceOf[StringType]
    Utils.decrypt(v)
  }
}
