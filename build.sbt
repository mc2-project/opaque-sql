name := "opaque"

version := "0.1"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.11.8"

spName := "amplab/opaque"

sparkVersion := "2.0.2"

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
  if (ret != 0) sys.error("C++ build failed.")
  IO.copyFile(
    baseDirectory.value / "src" / "enclave" / "libSGXEnclave.so",
    baseDirectory.value / "libSGXEnclave.so")
  IO.copyFile(
    baseDirectory.value / "src" / "enclave" / "enclave.signed.so",
    baseDirectory.value / "enclave.signed.so")
  IO.copyFile(
    baseDirectory.value / "src" / "enclave" / "libservice_provider.so",
    baseDirectory.value / "libservice_provider.so")
}

baseDirectory in enclaveBuildTask := (baseDirectory in ThisBuild).value

compile in Compile := { (compile in Compile).dependsOn(enclaveBuildTask).value }

// Watch the enclave C++ files
watchSources ++=
  ((baseDirectory.value / "src/enclave") ** (("*.cpp" || "*.h" || "*.tcc" || "*.edl")
      -- "Enclave_u.h"
      -- "Enclave_t.h"
      -- "key.cpp")).get

// Synthesize test data
val synthTestDataTask = TaskKey[Unit]("synthTestData", "Synthesizes test data")

synthTestDataTask := {
  val diseaseDataFiles =
    for {
      diseaseDir <- (baseDirectory.value / "data" / "disease").get
      name <- Seq("disease.csv", "gene.csv", "treatment.csv", "patient-125.csv")
    } yield new File(diseaseDir, name)
  if (!diseaseDataFiles.forall(_.exists)) {
    import sys.process._
    val ret = Seq("data/disease/synth-disease-data").!
    if (ret != 0) sys.error("Failed to synthesize test data.")
  }
}

test in Test := { (test in Test).dependsOn(synthTestDataTask).value }
