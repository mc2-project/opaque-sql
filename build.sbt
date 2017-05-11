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

scalacOptions ++= Seq("-g:vars")
javaOptions ++= Seq("-Xdebug", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8000")
// This and the next line fix a problem with forked run: https://github.com/scalatest/scalatest/issues/770
javaOptions in Test ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m", "-XX:MaxPermSize=384m")

// Flatbuffers library dependency
val fetchFlatbuffersLibTask = TaskKey[File](
  "fetchFlatbuffersLib", "Fetches and builds the Flatbuffers library, returning its location.")

val flatbuffersVersion = "1.6.0"

fetchFlatbuffersLibTask := {
  val flatbuffersSource = target.value / "flatbuffers" / s"flatbuffers-$flatbuffersVersion"
  if (!flatbuffersSource.exists) {
    // Fetch flatbuffers from Github
    streams.value.log.info(s"Fetching Flatbuffers")
    val flatbuffersUrl = new java.net.URL(
      s"https://github.com/google/flatbuffers/archive/v$flatbuffersVersion.zip")
    IO.unzipURL(flatbuffersUrl, flatbuffersSource.getParentFile)
  }
  val flatc = flatbuffersSource / "flatc"
  if (!flatc.exists) {
    // Build flatbuffers with cmake
    import sys.process._
    streams.value.log.info(s"Building Flatbuffers")
    if (Process(Seq(
      "cmake", "-G", "Unix Makefiles",
      "-DFLATBUFFERS_BUILD_TESTS=OFF",
      "-DFLATBUFFERS_BUILD_FLATLIB=OFF",
      "-DFLATBUFFERS_BUILD_FLATHASH=OFF",
      "-DFLATBUFFERS_BUILD_FLATC=ON"), flatbuffersSource).! != 0
      || Process(Seq("make"), flatbuffersSource).! != 0) {
      sys.error("Flatbuffers library build failed.")
    }
  }
  flatbuffersSource
}

unmanagedSources in Compile ++= ((fetchFlatbuffersLibTask.value / "java") ** "*.java").get

// Flatbuffers header file generation
val buildFlatbuffersTask = TaskKey[Seq[File]](
  "buildFlatbuffers", "Generates headers from Flatbuffers interface files.")

buildFlatbuffersTask := {
  import sys.process._

  val flatc = fetchFlatbuffersLibTask.value / "flatc"

  val cppOutDir = sourceManaged.value / "flatbuffers" / "gen-cpp"
  val javaOutDir = sourceManaged.value / "flatbuffers" / "gen-java"

  val flatbuffers = ((baseDirectory.value / "src/flatbuffers") ** "*.fbs").get
  // Only regenerate Flatbuffers headers if any .fbs file changed, indicated by their latest
  // modification time being newer than all generated headers. We do this because regenerating
  // Flatbuffers headers causes a full enclave rebuild, which is slow.
  val fbsLastMod = flatbuffers.map(_.lastModified).max
  val gen = (cppOutDir +++ javaOutDir).get

  if (gen.isEmpty || fbsLastMod > gen.map(_.lastModified).max) {
    for (fbs <- flatbuffers) {
      streams.value.log.info(s"Generating flatbuffers for ${fbs}")
      if (Seq(flatc.getPath, "--cpp", "-o", cppOutDir.getPath, fbs.getPath).! != 0
        || Seq(flatc.getPath, "--java", "-o", javaOutDir.getPath, fbs.getPath).! != 0) {
        sys.error("Flatbuffers build failed.")
      }
    }
  }

  (javaOutDir ** "*.java").get
}

sourceGenerators in Compile += buildFlatbuffersTask.taskValue

// Enclave C++ build
val enclaveBuildTask = TaskKey[Unit]("enclaveBuild", "Builds the C++ enclave code")

enclaveBuildTask := {
  buildFlatbuffersTask.value // Enclave build depends on the generated C++ headers
  import sys.process._
  val enclaveSourceDir = baseDirectory.value / "src" / "enclave"
  val enclaveBuildDir = target.value / "enclave"
  enclaveBuildDir.mkdir()
  val cmakeResult =
    Process(Seq("cmake", "-G", "Unix Makefiles", enclaveSourceDir.getPath), enclaveBuildDir).!
  if (cmakeResult != 0) sys.error("C++ build failed.")
  val buildResult = Process(Seq("make"), enclaveBuildDir).!
  if (buildResult != 0) sys.error("C++ build failed.")
}

baseDirectory in enclaveBuildTask := (baseDirectory in ThisBuild).value

compile in Compile := { (compile in Compile).dependsOn(enclaveBuildTask).value }

// Watch the enclave C++ files
watchSources ++=
  ((baseDirectory.value / "src/enclave") ** (
    ("*.cpp" || "*.h" || "*.tcc" || "*.edl" || "CMakeLists.txt") -- ".*")).get

// Watch the Flatbuffer schemas
watchSources ++=
  ((baseDirectory.value / "src/flatbuffers") ** "*.fbs").get

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
