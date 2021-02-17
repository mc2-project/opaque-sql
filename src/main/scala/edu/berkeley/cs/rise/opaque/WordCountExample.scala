package edu.berkeley.cs.rise.opaque

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalactic._

import scala.util.Try
import spark.jobserver.api.{SparkJob => NewSparkJob, _}

object WordCountExample extends NewSparkJob {
  type JobData = Seq[String]
  type JobOutput = collection.Map[String, Long]

  def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
    sc.parallelize(data).countByValue
  }

  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
    JobData Or Every[ValidationProblem] = {
    Try(config.getString("input.string").split(" ").toSeq)
      .map(words => Good(words))
      .getOrElse(Bad(One(SingleProblem("No input.string param"))))
  }
}
