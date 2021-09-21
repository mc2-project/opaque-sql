package edu.berkeley.cs.rise.opaque.execution.udfs

import edu.berkeley.cs.rise.opaque.expressions.DotProduct.dot
import edu.berkeley.cs.rise.opaque.expressions.VectorAdd.vectoradd
import edu.berkeley.cs.rise.opaque.expressions.VectorMultiply.vectormultiply
import edu.berkeley.cs.rise.opaque.expressions.VectorSum

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import java.util.Random

object LogisticRegression {

  // This fails on some input column types:
  // `Caused by: edu.berkeley.cs.rise.opaque.OpaqueException:
  // N4tuix8SubtractE can't operate on values of different types (DoubleField and IntegerField)`
  def normalize(xColumns: Seq[String], unprocessed: DataFrame): DataFrame = {
    var normalizedUnprocessed = unprocessed
    for (colName <- xColumns) {
      val mean =
        normalizedUnprocessed.groupBy().agg(avg(colName).as(colName + "-mean"))
      val meanOfSquared = normalizedUnprocessed
        .groupBy()
        .agg(avg(col(colName) * col(colName)).as(colName + "-mean_of_squared"))
      val stddev = mean
        .join(meanOfSquared)
        .select(
          sqrt(
            col(colName + "-mean_of_squared") - col(colName + "-mean") * col(colName + "-mean")
          ).as(colName + "-stddev")
        )
      normalizedUnprocessed = normalizedUnprocessed
        .join(mean)
        .join(stddev)
        .withColumn(
          colName + "-normalized",
          (col(colName) - col(colName + "-mean")) / col(colName + "-stddev")
        )
      normalizedUnprocessed.explain
      normalizedUnprocessed =
        normalizedUnprocessed.drop(colName).drop(colName + "-mean").drop(colName + "-stddev")
      normalizedUnprocessed =
        normalizedUnprocessed.withColumn(colName, col(colName + "-normalized"))
      normalizedUnprocessed = normalizedUnprocessed.drop(colName + "-normalized")
    }
    normalizedUnprocessed
  }

  // Stack all input rows to the regression as an array type
  def prepare(
      xColumns: Seq[String],
      yColumn: Option[String],
      unprocessed: DataFrame
  ): DataFrame = {
    val otherColumns = unprocessed.columns.toSet.diff(xColumns.toSet).toSet
    yColumn match {
      case Some(yColumn) =>
        val transformedColumns = (otherColumns.toSet - yColumn).toSeq.map { name =>
          col(name)
        } ++ Seq(array(xColumns.map { name => col(name) }: _*).as("x")) ++ Seq(
          col(yColumn).as("y")
        )
        unprocessed.select(transformedColumns: _*)
      case None =>
        val transformedColumns = otherColumns.toSeq.map { name =>
          col(name)
        } ++ Seq(array(xColumns.map { name => col(name) }: _*).as("x"))
        unprocessed.select(transformedColumns: _*)
    }
  }

  // Based on algorithm found in last page of
  // https://people.eecs.berkeley.edu/~jrs/189/lec/10.pdf
  // Not entirely correct: some (typically unnormalized) input cause
  // gradients to turn to 0 and the weights to no longer be updated
  def train(
      spark: SparkSession,
      D: Int,
      A: Double,
      ITERATIONS: Int,
      data: DataFrame
  ): DataFrame = {
    import spark.implicits._
    val rand = new Random(42)

    val vectorsum = new VectorSum

    var total =
      data.withColumn("w", lit(Array.fill(D) { 2 * rand.nextDouble - 1 }))

    for (i <- 0 until ITERATIONS) {
      total = total.withColumn("xw", dot($"x", $"w"))
      total = total.withColumn("s(xw)", lit(1.0d) / (lit(1.0d) + exp(-$"xw")))
      total = total.withColumn("y - s(xw)", $"y" - $"s(xw)")
      val grad = total
        .withColumn("x^T(y - s(wx))", vectormultiply($"x", $"y - s(xw)"))
        .groupBy("w") // $w column is the same in every row
        .agg(vectorsum($"x^T(y - s(wx))"))
        .withColumnRenamed("vectorsum(x^T(y - s(wx)))", "grad")
      val grad_reg =
        grad
          .withColumn("grad_reg", vectormultiply($"grad", lit(A)))
          .select($"grad_reg")
      total = total
        .join(grad_reg)
        .withColumn("w_new", vectoradd($"w", $"grad_reg"))
      total = total.drop($"w").drop("grad_reg")
      total = total.withColumnRenamed("w_new", "w")
    }
    total.select($"w").limit(1)
  }

  def predict(spark: SparkSession, w: DataFrame, data: DataFrame): DataFrame = {
    import spark.implicits._
    val dotProduct = w.join(data).withColumn("dot", dot($"w", $"x"))
    val sigmoid =
      dotProduct.withColumn("y_hat", (lit(1.0d) / (lit(1.0d) + exp(-$"dot"))).cast(IntegerType))
    sigmoid.drop("w", "x", "dot")
  }
}
