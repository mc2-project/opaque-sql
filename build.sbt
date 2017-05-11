name := "opaque"

version := "0.1"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.11.8"

spName := "amplab/opaque"

sparkVersion := "2.0.2"

sparkComponents ++= Seq("core", "sql", "catalyst")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

val flatbuffersVersion = "1.6.0"

parallelExecution := false

fork in Test := true

scalacOptions ++= Seq("-g:vars")
javaOptions ++= Seq("-Xdebug", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8000")
javaOptions in Test ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m", "-XX:MaxPermSize=384m")

val cppBuildType = SettingKey[BuildType]("cppBuildType", "...description...")

cppBuildType := Release

val fetchFlatbuffersLibTask = TaskKey[File]("fetchFlatbuffersLib",
  "Fetches and builds the Flatbuffers library, returning its location.")

unmanagedSources in Compile ++= ((fetchFlatbuffersLibTask.value / "java") ** "*.java").get

val buildFlatbuffersTask = TaskKey[Seq[File]]("buildFlatbuffers",
  "Generates Java and C++ sources from Flatbuffers interface files, returning the Java sources.")

sourceGenerators in Compile += buildFlatbuffersTask.taskValue

val enclaveBuildTask = TaskKey[File]("enclaveBuild",
  "Builds the C++ enclave code, returning the shared library directory.")

baseDirectory in enclaveBuildTask := (baseDirectory in ThisBuild).value

compile in Compile := { (compile in Compile).dependsOn(enclaveBuildTask).value }

javaOptions += "-Djava.library.path=" + enclaveBuildTask.value

javaOptions += "-Dopaque.enclave.trusted.signed.path=" + (
  enclaveBuildTask.value / "libenclave_trusted_signed.so")

// Watch the enclave C++ files
watchSources ++=
  ((sourceDirectory.value / "enclave") ** (
    ("*.cpp" || "*.h" || "*.tcc" || "*.edl" || "CMakeLists.txt") -- ".*")).get

// Watch the Flatbuffer schemas
watchSources ++=
  ((sourceDirectory.value / "flatbuffers") ** "*.fbs").get

val synthTestDataTask = TaskKey[Seq[File]]("synthTestData",
  "Synthesizes test data, returning the generated files.")

resourceGenerators in Test += synthTestDataTask.taskValue

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

// Flatbuffers header file generation

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

enclaveBuildTask := {
  buildFlatbuffersTask.value // Enclave build depends on the generated C++ headers
  import sys.process._
  val enclaveSourceDir = baseDirectory.value / "src" / "enclave"
  val enclaveBuildDir = target.value / "enclave"
  enclaveBuildDir.mkdir()
  val cmakeResult =
    Process(Seq(
      "cmake",
      s"-DCMAKE_INSTALL_PREFIX:PATH=${enclaveBuildDir.getPath}",
      s"-DCMAKE_BUILD_TYPE=${cppBuildType.value}",
      enclaveSourceDir.getPath), enclaveBuildDir).!
  if (cmakeResult != 0) sys.error("C++ build failed.")
  val buildResult = Process(Seq("make"), enclaveBuildDir).!
  if (buildResult != 0) sys.error("C++ build failed.")
  val installResult = Process(Seq("make", "install"), enclaveBuildDir).!
  if (installResult != 0) sys.error("C++ build failed.")
  enclaveBuildDir
}

synthTestDataTask := {
  val diseaseDataFiles =
    for {
      diseaseDir <- ((resourceManaged in Test).value / "data" / "disease").get
      name <- Seq("disease.csv", "gene.csv", "treatment.csv", "patient-125.csv")
    } yield new File(diseaseDir, name)
  if (!diseaseDataFiles.forall(_.exists)) {
    import sys.process._
    val ret = Seq("data/disease/synth-disease-data").!
    if (ret != 0) sys.error("Failed to synthesize test data.")
  }
  diseaseDataFiles
}
