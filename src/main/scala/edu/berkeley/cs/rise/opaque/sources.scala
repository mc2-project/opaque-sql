package edu.berkeley.cs.rise.opaque

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType

import edu.berkeley.cs.rise.opaque.execution.Block
import edu.berkeley.cs.rise.opaque.execution.OpaqueOperatorExec

class EncryptedSource extends SchemaRelationProvider with CreatableRelationProvider {
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType): BaseRelation = {
    EncryptedScan(parameters("path"), schema, isOblivious(parameters))(
      sqlContext.sparkSession)
  }

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame): BaseRelation = {
    val blocks: RDD[Block] = data.queryExecution.executedPlan.asInstanceOf[OpaqueOperatorExec]
      .executeBlocked()
    blocks.map(block => (0, block.bytes)).saveAsSequenceFile(parameters("path"))
    EncryptedScan(parameters("path"), data.schema, isOblivious(parameters))(
      sqlContext.sparkSession)
  }

  private def isOblivious(parameters: Map[String, String]): Boolean = {
    parameters.get("oblivious") match {
      case Some("true") => true
      case _ => false
    }
  }
}

case class EncryptedScan(
    path: String,
    override val schema: StructType,
    val isOblivious: Boolean)(
    @transient val sparkSession: SparkSession)
  extends BaseRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def needConversion: Boolean = false

  def buildBlockedScan(): RDD[Block] = sparkSession.sparkContext
    .sequenceFile[Int, Array[Byte]](path).map {
      case (_, bytes) => Block(bytes)
    }
}
