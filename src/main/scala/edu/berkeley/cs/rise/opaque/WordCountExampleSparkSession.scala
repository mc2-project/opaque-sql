package edu.berkeley.cs.rise.opaque

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.scalactic._
import spark.jobserver.SparkSessionJob
import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}

import scala.util.Try

object WordCountExampleSparkSession extends SparkSessionJob {
  type JobData = Seq[String]
  type JobOutput = collection.Map[String, Long]

  override def runJob(sparkSession: SparkSession, runtime: JobEnvironment, data: JobData): JobOutput =
    sparkSession.sparkContext.parallelize(data).countByValue

  override def validate(sparkSession: SparkSession, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = {
    Try(config.getString("input.string").split(" ").toSeq)
      .map(words => Good(words))
      .getOrElse(Bad(One(SingleProblem("No input.string param"))))
  }
}
