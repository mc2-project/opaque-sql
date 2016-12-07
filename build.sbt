name := "opaque"

version := "0.1"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.11.8"

spName := "amplab/opaque"

sparkVersion := "2.0.0"

sparkComponents ++= Seq("core", "sql", "catalyst")

val flatbuffersVersion = "1.3.0.1"

libraryDependencies += "com.github.davidmoten" % "flatbuffers-java" % flatbuffersVersion

libraryDependencies += (
  "com.github.davidmoten" % "flatbuffers-compiler" % flatbuffersVersion
    artifacts(Artifact("flatbuffers-compiler", "tar.gz", "tar.gz", "distribution-linux")))

classpathTypes += "tar.gz"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

parallelExecution := false

// This fixes a class loader problem with scala.Tuple2 class, scala-2.11, Spark 2.x
fork in Test := true

// This and the next line fix a problem with forked run: https://github.com/scalatest/scalatest/issues/770
javaOptions in Test ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m", "-XX:MaxPermSize=384m")

// Enclave C++ build
val enclaveBuildTask = TaskKey[Unit]("enclaveBuild", "Builds the C++ enclave code")

enclaveBuildTask := {
  compileFlatbuffersTask.value // Enclave build depends on the generated C++ headers
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

// Flatbuffers header file generation
val compileFlatbuffersTask = TaskKey[Seq[File]](
  "compileFlatbuffers", "Generates headers from Flatbuffers")

compileFlatbuffersTask := {
  val cppOutDir = sourceManaged.value / "flatbuffers" / "gen-cpp"
  val javaOutDir = sourceManaged.value / "flatbuffers" / "gen-java"

  import sys.process._
  val targzName = s"flatbuffers-compiler-$flatbuffersVersion-distribution-linux.tar.gz"
  val targz = (dependencyClasspath in Compile).value.files.find(_.getName == targzName).get
  IO.withTemporaryDirectory { tmp =>
    Seq("tar", "xzf", targz.getPath, "-C", tmp.getPath).!
    val flatc = (tmp / "bin/flatc").getPath
    val flatbuffers = ((baseDirectory.value / "src/flatbuffers") ** "*.fbs").get
    for (file <- flatbuffers) {
      streams.value.log.info(s"Generating flatbuffers for ${file}")
      if (Seq(flatc, "--cpp", "-o", cppOutDir.getPath, file.getPath).! != 0
        || Seq(flatc, "--java", "-o", javaOutDir.getPath, file.getPath).! != 0) {
        sys.error("Flatbuffers build failed.")
      }
    }
  }
  (javaOutDir ** "*.java").get
}

sourceGenerators in Compile += compileFlatbuffersTask.taskValue

// Watch the enclave C++ files
watchSources ++=
  ((baseDirectory.value / "src/enclave") ** (("*.cpp" || "*.h" || "*.tcc" || "*.edl")
      -- "Enclave_u.h"
      -- "Enclave_t.h"
      -- "key.cpp")).get

// Watch the Flatbuffer schemas
watchSources ++=
  ((baseDirectory.value / "src/flatbuffers") ** "*.fbs").get
