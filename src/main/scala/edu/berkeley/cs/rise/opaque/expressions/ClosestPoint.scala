package edu.berkeley.cs.rise.opaque.expressions

import breeze.linalg.DenseVector
import breeze.linalg.squaredDistance
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

object ClosestPoint {
  def closestPoint(point: Column, centroids: Column): Column =
    new Column(ClosestPoint(point.expr, centroids.expr))
}

case class ClosestPoint(left: Expression, right: Expression)
    extends BinaryExpression with NullIntolerant with CodegenFallback with ExpectsInputTypes {

  override def dataType: DataType = left.dataType

  override def inputTypes = Seq(
    DataTypes.createArrayType(DoubleType),
    DataTypes.createArrayType(DataTypes.createArrayType(DoubleType)))

  protected override def nullSafeEval(input1: Any, input2: Any): ArrayData = {
    val point = new DenseVector(input1.asInstanceOf[ArrayData].toDoubleArray)
    val centroids = input2.asInstanceOf[ArrayData]
      .toArray[ArrayData](DataTypes.createArrayType(DoubleType))
      .map(centroid => new DenseVector(centroid.toDoubleArray))

    var bestIndex = 0
    var bestDist = Double.PositiveInfinity

    for (i <- 0 until centroids.length) {
      val tempDist = squaredDistance(point, centroids(i))
      if (tempDist < bestDist) {
        bestDist = tempDist
        bestIndex = i
      }
    }

    ArrayData.toArrayData(centroids(bestIndex).toArray)
  }
}
