name := "opaque"

version := "0.1"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.11.8"

spName := "amplab/opaque"

sparkVersion := "2.0.0"

sparkComponents ++= Seq("core", "sql", "catalyst")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

parallelExecution := false

// This fixes a class loader problem with scala.Tuple2 class, scala-2.11, Spark 2.x
fork in Test := true

// This and the next line fix a problem with forked run: https://github.com/scalatest/scalatest/issues/770
javaOptions in Test ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m", "-XX:MaxPermSize=384m")

// Enclave C++ build
val enclaveBuildTask = TaskKey[Unit]("enclaveBuild", "Builds the C++ enclave code")

enclaveBuildTask := {
  import sys.process._
  val ret = Seq("src/enclave/build.sh").!
  if (ret != 0) error("C++ build failed.")
}

baseDirectory in enclaveBuildTask := (baseDirectory in ThisBuild).value

compile in Compile <<= (compile in Compile).dependsOn(enclaveBuildTask)

watchSources <++= (baseDirectory in ThisBuild) map { (base: File) =>
  ((base / "src/enclave") ** (("*.cpp" || "*.h" || "*.tcc" || "*.edl" || "*.fbs")
    -- "Enclave_u.h"
    -- "Enclave_t.h"
    -- "*_generated.h")).get
}
