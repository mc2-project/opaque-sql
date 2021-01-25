name := "opaque"

version := "0.1"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.12.10"

spName := "amplab/opaque"

sparkVersion := "3.0.0"

sparkComponents ++= Seq("core", "sql", "catalyst")

libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

val flatbuffersVersion = "1.7.0"

concurrentRestrictions in Global := Seq(
  Tags.limit(Tags.Test, 1))

fork in Test := true
fork in run := true

testOptions in Test += Tests.Argument("-oF")
javaOptions in Test ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m")
javaOptions in run ++= Seq(
  "-Xmx2048m", "-XX:ReservedCodeCacheSize=384m", "-Dspark.master=local[1]")

// Include Spark dependency for `build/sbt run`, though it is marked as "provided" for use with
// spark-submit. From
// https://github.com/sbt/sbt-assembly/blob/4a211b329bf31d9d5f0fae67ea4252896d8a4a4d/README.md
run in Compile := Defaults.runTask(
  fullClasspath in Compile,
  mainClass in (Compile, run),
  runner in (Compile, run)).evaluated

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfuture",
  "-Xlint:_",
  "-Ywarn-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Ywarn-unused-import"
)

scalacOptions in (Compile, console) := Seq.empty

val flatbuffersGenCppDir = SettingKey[File]("flatbuffersGenCppDir",
  "Location of Flatbuffers generated C++ files.")

flatbuffersGenCppDir := sourceManaged.value / "flatbuffers" / "gen-cpp"

val buildType = SettingKey[BuildType]("buildType",
  "Release, Debug, or Profile.")

buildType := Release

scalacOptions ++= { if (buildType.value == Debug) Seq("-g:vars") else Nil }
javaOptions ++= { if (buildType.value == Debug) Seq("-Xdebug", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8000") else Nil }

val fetchFlatbuffersLibTask = TaskKey[File]("fetchFlatbuffersLib",
  "Fetches and builds the Flatbuffers library, returning its location.")

unmanagedSources in Compile ++= ((fetchFlatbuffersLibTask.value / "java") ** "*.java").get

val buildFlatbuffersTask = TaskKey[Seq[File]]("buildFlatbuffers",
  "Generates Java and C++ sources from Flatbuffers interface files, returning the Java sources.")

sourceGenerators in Compile += buildFlatbuffersTask.taskValue

val enclaveBuildTask = TaskKey[File]("enclaveBuild",
  "Builds the C++ enclave code, returning the directory containing the resulting shared libraries.")

baseDirectory in enclaveBuildTask := (baseDirectory in ThisBuild).value

compile in Compile := { (compile in Compile).dependsOn(enclaveBuildTask).value }

val copyEnclaveLibrariesToResourcesTask = TaskKey[Seq[File]]("copyEnclaveLibrariesToResources",
  "Copies the enclave libraries to the managed resources directory, returning the copied files.")

resourceGenerators in Compile += copyEnclaveLibrariesToResourcesTask.taskValue

// Add the managed resource directory to the resource classpath so we can find libraries at runtime
managedResourceDirectories in Compile += resourceManaged.value

val fetchIntelAttestationReportSigningCACertTask = TaskKey[Seq[File]](
  "fetchIntelAttestationReportSigningCACert",
  "Fetches and decompresses the Intel IAS SGX Report Signing CA file, required for "
    + "remote attestation.")

resourceGenerators in Compile += fetchIntelAttestationReportSigningCACertTask.taskValue

// Watch the enclave C++ files
watchSources ++=
  ((sourceDirectory.value / "enclave") ** (
    ("*.cpp" || "*.c" || "*.h" || "*.tcc" || "*.edl" || "CMakeLists.txt") -- ".*")).get

// Watch the Flatbuffer schemas
watchSources ++=
  ((sourceDirectory.value / "flatbuffers") ** "*.fbs").get

val synthTestDataTask = TaskKey[Unit]("synthTestData",
  "Synthesizes test data.")

test in Test := { (test in Test).dependsOn(synthTestDataTask).value }

val sgxGdbTask = TaskKey[Unit]("sgx-gdb-task",
  "Runs OpaqueSinglePartitionSuite under the sgx-gdb debugger.")

def sgxGdbCommand = Command.command("sgx-gdb") { state =>
  val extracted = Project extract state
  val newState = extracted.append(Seq(buildType := Debug), state)
  Project.extract(newState).runTask(sgxGdbTask, newState)
  state
}

commands += sgxGdbCommand

initialCommands in console :=
  """
    |import org.apache.spark.SparkContext
    |import org.apache.spark.sql.SQLContext
    |import org.apache.spark.sql.catalyst.analysis._
    |import org.apache.spark.sql.catalyst.dsl._
    |import org.apache.spark.sql.catalyst.errors._
    |import org.apache.spark.sql.catalyst.expressions._
    |import org.apache.spark.sql.catalyst.plans.logical._
    |import org.apache.spark.sql.catalyst.rules._
    |import org.apache.spark.sql.catalyst.util._
    |import org.apache.spark.sql.execution
    |import org.apache.spark.sql.functions._
    |import org.apache.spark.sql.types._
    |import org.apache.log4j.Level
    |import org.apache.log4j.LogManager
    |
    |LogManager.getLogger("org.apache.spark").setLevel(Level.WARN)
    |LogManager.getLogger("org.apache.spark.executor.Executor").setLevel(Level.WARN)
    |
    |val spark = (org.apache.spark.sql.SparkSession.builder()
    |  .master("local")
    |  .appName("Opaque shell")
    |  .getOrCreate())
    |val sc = spark.sparkContext
    |val sqlContext = spark.sqlContext
    |
    |import spark.implicits._
    |
    |import edu.berkeley.cs.rise.opaque.implicits._
    |edu.berkeley.cs.rise.opaque.Utils.initSQLContext(sqlContext)
  """.stripMargin

cleanupCommands in console := "spark.stop()"

sgxGdbTask := {
  (compile in Test).value
  Process(Seq(
    "sgx-gdb", "java",
    "-x",
    ((baseDirectory in ThisBuild).value / "project" / "resources" / "run-tests.gdb").getPath),
    None,
    "CLASSPATH" -> (fullClasspath in Test).value.map(_.data.getPath).mkString(":")).!<
}

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
    val nproc = java.lang.Runtime.getRuntime.availableProcessors
    if (Process(Seq(
      "cmake", "-G", "Unix Makefiles",
      "-DFLATBUFFERS_BUILD_TESTS=OFF",
      "-DFLATBUFFERS_BUILD_FLATLIB=OFF",
      "-DFLATBUFFERS_BUILD_FLATHASH=OFF",
      "-DFLATBUFFERS_BUILD_FLATC=ON"), flatbuffersSource).! != 0
      || Process(Seq("make", "-j" + nproc), flatbuffersSource).! != 0) {
      sys.error("Flatbuffers library build failed.")
    }
  }
  flatbuffersSource
}

// Flatbuffers header file generation

buildFlatbuffersTask := {
  import sys.process._

  val flatc = fetchFlatbuffersLibTask.value / "flatc"

  val javaOutDir = sourceManaged.value / "flatbuffers" / "gen-java"

  val flatbuffers = ((sourceDirectory.value / "flatbuffers") ** "*.fbs").get
  // Only regenerate Flatbuffers headers if any .fbs file changed, indicated by their latest
  // modification time being newer than all generated headers. We do this because regenerating
  // Flatbuffers headers causes a full enclave rebuild, which is slow.
  val fbsLastMod = flatbuffers.map(_.lastModified).max
  val gen = (flatbuffersGenCppDir.value ** "*.h" +++ javaOutDir ** "*.java").get

  if (gen.isEmpty || fbsLastMod > gen.map(_.lastModified).max) {
    for (fbs <- flatbuffers) {
      streams.value.log.info(s"Generating flatbuffers for ${fbs}")
      if (Seq(flatc.getPath, "--cpp", "--gen-mutable", "-o", flatbuffersGenCppDir.value.getPath, fbs.getPath).! != 0
        || Seq(flatc.getPath, "--java", "-o", javaOutDir.getPath, fbs.getPath).! != 0) {
        sys.error("Flatbuffers build failed.")
      }
    }
  }

  (javaOutDir ** "*.java").get
}

nativePlatform := {
  try {
    val lines = Process("uname -sm").lines
    if (lines.length == 0) {
      sys.error("Error occured trying to run `uname`")
    }
    // uname -sm returns "<kernel> <hardware name>"
    val parts = lines.head.split(" ")
    if (parts.length != 2) {
      sys.error("'uname -sm' returned unexpected string: " + lines.head)
    } else {
      val arch = parts(1).toLowerCase.replaceAll("\\s", "")
      val kernel = parts(0).toLowerCase.replaceAll("\\s", "")
      arch + "-" + kernel
    }
  } catch {
    case ex: Exception =>
      sLog.value.error("Error trying to determine platform.")
      sLog.value.warn("Cannot determine platform! It will be set to 'unknown'.")
      "unknown-unknown"
  }
}

enclaveBuildTask := {
  buildFlatbuffersTask.value // Enclave build depends on the generated C++ headers
  import sys.process._
  val enclaveSourceDir = baseDirectory.value / "src" / "enclave"
  val enclaveBuildDir = target.value / "enclave"
  enclaveBuildDir.mkdirs()
  val cmakeResult =
    Process(Seq(
      "cmake",
      s"-DCMAKE_INSTALL_PREFIX:PATH=${enclaveBuildDir.getPath}",
      s"-DCMAKE_BUILD_TYPE=${buildType.value}",
      s"-DFLATBUFFERS_LIB_DIR=${(fetchFlatbuffersLibTask.value / "include").getPath}",
      s"-DFLATBUFFERS_GEN_CPP_DIR=${flatbuffersGenCppDir.value.getPath}",
      enclaveSourceDir.getPath), enclaveBuildDir).!
  if (cmakeResult != 0) sys.error("C++ build failed.")
  val nproc = java.lang.Runtime.getRuntime.availableProcessors
  val buildResult = Process(Seq("make", "-j" + nproc), enclaveBuildDir).!
  if (buildResult != 0) sys.error("C++ build failed.")
  val installResult = Process(Seq("make", "install"), enclaveBuildDir).!
  if (installResult != 0) sys.error("C++ build failed.")
  enclaveBuildDir / "lib"
}

copyEnclaveLibrariesToResourcesTask := {
  val libraries = (enclaveBuildTask.value ** "*.so").get
  val mappings: Seq[(File, String)] =
    libraries pair rebase(enclaveBuildTask.value, s"/native/${nativePlatform.value}")
  val resources: Seq[File] = for ((file, path) <- mappings) yield {
    val resource = resourceManaged.value / path
    IO.copyFile(file, resource)
    resource
  }
  resources
}

fetchIntelAttestationReportSigningCACertTask := {
  val cert = resourceManaged.value / "AttestationReportSigningCACert.pem"
  if (!cert.exists) {
    streams.value.log.info(s"Fetching Intel Attestation report signing CA certificate")
    val certUrl = new java.net.URL(
      s"https://software.intel.com/sites/default/files/managed/7b/de/RK_PUB.zip")
    IO.unzipURL(certUrl, cert.getParentFile)
  }
  Seq(cert)
}

synthTestDataTask := {
  val diseaseDataFiles =
    for {
      diseaseDir <- (baseDirectory.value / "data" / "disease").get
      name <- Seq("disease.csv", "gene.csv", "treatment.csv", "patient-125.csv")
    } yield new File(diseaseDir, name)

  val tpchDir = baseDirectory.value / "data" / "tpch" / "sf_small"
  tpchDir.mkdirs()
  val tpchDataFiles =
    for {
      name <- Seq(
        "customer.tbl", "lineitem.tbl", "nation.tbl", "orders.tbl", "partsupp.tbl", "part.tbl",
        "region.tbl", "supplier.tbl")
    } yield new File(tpchDir, name)

  if (!diseaseDataFiles.forall(_.exists)) {
    import sys.process._
    val ret = Seq("data/disease/synth-disease-data").!
    if (ret != 0) sys.error("Failed to synthesize disease test data.")
  }

  if (!tpchDataFiles.forall(_.exists)) {
    import sys.process._
    val ret = Seq("data/tpch/synth-tpch-data").!
    if (ret != 0) sys.error("Failed to synthesize TPC-H test data.")
  }
}
