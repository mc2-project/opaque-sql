/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.rise.opaque

import java.io.File
import java.io.FileNotFoundException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.SecureRandom
import java.util.UUID

import javax.crypto._
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

import scala.collection.mutable.ArrayBuilder

import com.google.flatbuffers.FlatBufferBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Contains
import org.apache.spark.sql.catalyst.expressions.DateAdd
import org.apache.spark.sql.catalyst.expressions.DateAddInterval
import org.apache.spark.sql.catalyst.expressions.Descending
import org.apache.spark.sql.catalyst.expressions.Divide
import org.apache.spark.sql.catalyst.expressions.EndsWith
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.Exp
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.catalyst.expressions.LessThan
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Like
import org.apache.spark.sql.catalyst.expressions.Multiply
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.sql.catalyst.expressions.CreateArray
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.Or
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.StartsWith
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.catalyst.expressions.TimeAdd
import org.apache.spark.sql.catalyst.expressions.UnaryMinus
import org.apache.spark.sql.catalyst.expressions.Upper
import org.apache.spark.sql.catalyst.expressions.Year
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.Cross
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.NaturalJoin
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.plans.UsingJoin
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.unsafe.types.UTF8String

import edu.berkeley.cs.rise.opaque.execution.Block
import edu.berkeley.cs.rise.opaque.execution.OpaqueOperatorExec
import edu.berkeley.cs.rise.opaque.execution.SGXEnclave
import edu.berkeley.cs.rise.opaque.expressions.ClosestPoint
import edu.berkeley.cs.rise.opaque.expressions.DotProduct
import edu.berkeley.cs.rise.opaque.expressions.VectorAdd
import edu.berkeley.cs.rise.opaque.expressions.VectorMultiply
import edu.berkeley.cs.rise.opaque.expressions.VectorSum
import edu.berkeley.cs.rise.opaque.logical.ConvertToOpaqueOperators
import edu.berkeley.cs.rise.opaque.logical.EncryptLocalRelation
// import edu.berkeley.cs.rise.opaque.JobVerificationEngine

object Utils extends Logging {
  private val perf: Boolean = System.getenv("SGX_PERF") == "1"

  def time[A](desc: String)(f: => A): A = {
    val start = System.nanoTime
    val result = f
    if (perf) {
      logInfo(s"$desc: ${(System.nanoTime - start) / 1000000.0} ms")
    }
    result
  }

  def logPerf(message: String): Unit = {
    if (perf) {
      logInfo(message)
    }
  }

  /**
   * Retry `fn`, which may throw an OpaqueException, up to n times.
   *
   * From https://stackoverflow.com/a/7931459.
   */
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    import scala.util.{Try, Success, Failure}
    Try { fn  } match {
      case Success(x) => x
      case Failure(e) if n > 1 => retry(n - 1)(fn)
      case Failure(e) => throw e
    }
  }

  private def jsonSerialize(x: Any): String = (x: @unchecked) match {
    case x: Int => x.toString
    case x: Double => x.toString
    case x: Boolean => x.toString
    case x: String => s""""$x""""
    case x: Option[_] => x match {
      case Some(x) => jsonSerialize(x)
      case None => "null"
    }
    case x: Map[_, _] => x.map {
      case (k, v) => s"${jsonSerialize(k)}: ${jsonSerialize(v)}"
    }.mkString("{", ", ", "}")
  }

  def timeBenchmark[A](benchmarkAttrs: (String, Any)*)(f: => A): A = {
    val start = System.nanoTime
    val result = f
    val timeMs = (System.nanoTime - start) / 1000000.0
    val attrs = benchmarkAttrs.toMap + (
      "time" -> timeMs,
      "sgx" -> (if (System.getenv("SGX_MODE") == "HW") "hw" else "sim"))
    logInfo(jsonSerialize(attrs))
    result
  }

  def findLibraryAsResource(libraryName: String): String = {
    // Derived from sbt-jni: macros/src/main/scala/ch/jodersky/jni/annotations.scala
    import java.nio.file.{Files, Path}
    val lib = System.mapLibraryName(libraryName)
    val tmp: Path = Files.createTempDirectory("jni-")
    val plat: String = {
      val line = try {
        scala.sys.process.Process("uname -sm").lineStream.head
      } catch {
        case ex: Exception => sys.error("Error running `uname` command")
      }
      val parts = line.split(" ")
      if (parts.length != 2) {
        sys.error("Could not determine platform: 'uname -sm' returned unexpected string: " + line)
      } else {
        val arch = parts(1).toLowerCase.replaceAll("\\s", "")
        val kernel = parts(0).toLowerCase.replaceAll("\\s", "")
        arch + "-" + kernel
      }
    }
    val resourcePath: String = s"/native/$plat/$lib"
    val resourceStream = Option(getClass.getResourceAsStream(resourcePath)) match {
      case Some(s) => s
      case None => throw new UnsatisfiedLinkError(
        "Native library " + lib + " (" + resourcePath + ") cannot be found on the classpath.")
    }
    val extractedPath = tmp.resolve(lib)
    try {
      Files.copy(resourceStream, extractedPath)
    } catch {
      case ex: Exception => throw new UnsatisfiedLinkError(
        "Error while extracting native library: " + ex)
    }
    extractedPath.toAbsolutePath.toString
  }

  def findResource(resourceName: String): String = {
    import java.nio.file.{Files, Path}
    val tmp: Path = Files.createTempDirectory("jni-")
    val resourcePath: String = s"/$resourceName"
    val resourceStream = Option(getClass.getResourceAsStream(resourcePath)) match {
      case Some(s) => s
      case None => throw new FileNotFoundException(
        s"Resource $resourcePath cannot be found on the classpath.")
    }
    val extractedPath = tmp.resolve(resourceName)
    Files.copy(resourceStream, extractedPath)
    extractedPath.toAbsolutePath.toString
  }

  def createTempDir(): File = {
    val dir = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID.toString)
    dir.mkdirs()
    dir.getCanonicalFile
  }

  def deleteRecursively(file: File): Unit = {
    for (contents <- Option(file.listFiles); f <- contents) {
      deleteRecursively(f)
      f.delete()
    }
  }

  def initEnclave(): (SGXEnclave, Long) = {
    this.synchronized {
      if (eid == 0L) {
        val enclave = new SGXEnclave()
        val path = findLibraryAsResource("enclave_trusted_signed")
        eid = enclave.StartEnclave(path)
        logInfo("Starting an enclave")
        (enclave, eid)
      } else {
        val enclave = new SGXEnclave()
        (enclave, eid)
      }
    }
  }

  final val GCM_IV_LENGTH = 12 
  final val GCM_KEY_LENGTH = 16
  final val GCM_TAG_LENGTH = 16

  /**
   * Symmetric key used to encrypt row data. This key is securely sent to the enclaves if
   * attestation succeeds. For development, we use a hardcoded key. You should change it.
   */
  val sharedKey: Array[Byte] = "Opaque devel key".getBytes("UTF-8")
  assert(sharedKey.size == GCM_KEY_LENGTH)
  
  def encrypt(data: Array[Byte]): Array[Byte] = {
    val random = SecureRandom.getInstance("SHA1PRNG")
    val cipherKey = new SecretKeySpec(sharedKey, "AES")
    val iv = new Array[Byte](GCM_IV_LENGTH)
    random.nextBytes(iv)
    val spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv)
    val cipher = Cipher.getInstance("AES/GCM/NoPadding", "SunJCE")
    cipher.init(Cipher.ENCRYPT_MODE, cipherKey, spec)
    val cipherText = cipher.doFinal(data)    
    iv ++ cipherText
  }
  
  def decrypt(data: Array[Byte]): Array[Byte] = {
    val cipherKey = new SecretKeySpec(sharedKey, "AES")
    val iv = data.take(GCM_IV_LENGTH)
    val cipherText = data.drop(GCM_IV_LENGTH)
    val cipher = Cipher.getInstance("AES/GCM/NoPadding", "SunJCE")
    cipher.init(Cipher.DECRYPT_MODE, cipherKey, new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv))
    cipher.doFinal(cipherText)
  }

  var eid = 0L
  var attested : Boolean = false
  var attesting_getepid : Boolean = false
  var attesting_getmsg1 : Boolean = false
  var attesting_getmsg3 : Boolean = false
  var attesting_final_ra : Boolean = false

  def initSQLContext(sqlContext: SQLContext): Unit = {
    sqlContext.experimental.extraOptimizations =
      (Seq(EncryptLocalRelation, ConvertToOpaqueOperators) ++
        sqlContext.experimental.extraOptimizations)
    sqlContext.experimental.extraStrategies =
      (Seq(OpaqueOperators) ++
        sqlContext.experimental.extraStrategies)
    RA.initRA(sqlContext.sparkContext)
  }

  def concatByteArrays(arrays: Array[Array[Byte]]): Array[Byte] = {
    arrays match {
      case Array() => Array.empty
      case Array(bytes) => bytes
      case _ =>
        val totalBytes = arrays.map(_.length.toLong).sum
        assert(totalBytes < Int.MaxValue)
        val buf = ByteBuffer.allocate(totalBytes.toInt)
        buf.order(ByteOrder.LITTLE_ENDIAN)
        for (a <- arrays) {
          buf.put(a)
        }
        buf.flip()
        val all = new Array[Byte](buf.limit())
        buf.get(all)
        all
    }
  }

  def splitBytes(bytes: Array[Byte], numSplits: Int): Array[Array[Byte]] = {
    val splitSize = bytes.length / numSplits
    assert(numSplits * splitSize == bytes.length)
    bytes.grouped(splitSize).toArray
  }

  def ensureCached[T](
      rdd: RDD[T], storageLevel: StorageLevel): RDD[T] = {
    if (rdd.getStorageLevel == StorageLevel.NONE) {
      rdd.persist(storageLevel)
    } else {
      rdd
    }
  }

  def ensureCached[T](rdd: RDD[T]): RDD[T] = ensureCached(rdd, StorageLevel.MEMORY_ONLY)

  def ensureCached[T](
      ds: Dataset[T], storageLevel: StorageLevel): Dataset[T] = {
    if (ds.storageLevel == StorageLevel.NONE) {
      ds.persist(storageLevel)
    } else {
      ds
    }
  }

  def ensureCached[T](ds: Dataset[T]): Dataset[T] = ensureCached(ds, StorageLevel.MEMORY_ONLY)

  def force(ds: Dataset[_]): Unit = {
    val rdd: RDD[_] = ds.queryExecution.executedPlan match {
      case p: OpaqueOperatorExec => p.executeBlocked()
      case p => p.execute()
    }
    rdd.foreach(x => {})
  }



  def flatbuffersCreateField(
      builder: FlatBufferBuilder, value: Any, dataType: DataType, isNull: Boolean): Int = {
    (value, dataType) match {
      case (b: Boolean, BooleanType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.BooleanField,
          tuix.BooleanField.createBooleanField(builder, b),
          isNull)
      case (null, BooleanType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.BooleanField,
          tuix.BooleanField.createBooleanField(builder, false),
          isNull)
      case (x: Int, IntegerType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.IntegerField,
          tuix.IntegerField.createIntegerField(builder, x),
          isNull)
      case (null, IntegerType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.IntegerField,
          tuix.IntegerField.createIntegerField(builder, 0),
          isNull)
      case (x: Long, LongType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.LongField,
          tuix.LongField.createLongField(builder, x),
          isNull)
      case (null, LongType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.LongField,
          tuix.LongField.createLongField(builder, 0L),
          isNull)
      case (x: Float, FloatType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.FloatField,
          tuix.FloatField.createFloatField(builder, x),
          isNull)
      case (null, FloatType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.FloatField,
          tuix.FloatField.createFloatField(builder, 0),
          isNull)
      case (x: Double, DoubleType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.DoubleField,
          tuix.DoubleField.createDoubleField(builder, x),
          isNull)
      case (null, DoubleType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.DoubleField,
          tuix.DoubleField.createDoubleField(builder, 0.0),
          isNull)
      case (x: Int, DateType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.DateField,
          tuix.DateField.createDateField(builder, x),
          isNull)
      case (null, DateType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.DateField,
          tuix.DateField.createDateField(builder, 0),
          isNull)
      case (x: Array[Byte], BinaryType) =>
        val length = x.size
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.BinaryField,
          tuix.BinaryField.createBinaryField(
            builder,
            tuix.BinaryField.createValueVector(builder, x),
            length),
          isNull)
      case (null, BinaryType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.BinaryField,
          tuix.BinaryField.createBinaryField(
            builder,
            tuix.BinaryField.createValueVector(builder, Array.empty),
            0),
          isNull)
      case (x: Byte, ByteType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.ByteField,
          tuix.ByteField.createByteField(builder, x),
          isNull)
      case (null, ByteType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.ByteField,
          tuix.ByteField.createByteField(builder, 0),
          isNull)
      case (x: CalendarInterval, CalendarIntervalType) =>
        val months = x.months
        val days = x.days
        val microseconds = x.microseconds
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.CalendarIntervalField,
          tuix.CalendarIntervalField.createCalendarIntervalField(builder, months, days, microseconds),
          isNull)
      case (null, CalendarIntervalType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.CalendarIntervalField,
          tuix.CalendarIntervalField.createCalendarIntervalField(builder, 0, 0, 0L),
          isNull)
      case (x: Byte, NullType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.NullField,
          tuix.NullField.createNullField(builder, x),
          isNull)
      case (null, NullType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.NullField,
          tuix.NullField.createNullField(builder, 0),
          isNull)
      case (x: Short, ShortType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.ShortField,
          tuix.ShortField.createShortField(builder, x),
          isNull)
      case (null, ShortType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.ShortField,
          tuix.ShortField.createShortField(builder, 0),
          isNull)
      case (x: Long, TimestampType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.TimestampField,
          tuix.TimestampField.createTimestampField(builder, x),
          isNull)
      case (null, TimestampType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.TimestampField,
          tuix.TimestampField.createTimestampField(builder, 0),
          isNull)
      case (x: ArrayData, ArrayType(elementType, containsNull)) =>
        // Iterate through each element in x and turn it into Field type
        val fieldsArray = new ArrayBuilder.ofInt
        for (i <- 0 until x.numElements) {
          val field = flatbuffersCreateField(builder, x.get(i, elementType), elementType, isNull)
          fieldsArray += field
        }
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.ArrayField,
          tuix.ArrayField.createArrayField(
            builder,
            tuix.ArrayField.createValueVector(builder, fieldsArray.result)),
          isNull)
      case (null, ArrayType(elementType, containsNull)) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.ArrayField,
          tuix.ArrayField.createArrayField(
            builder,
            tuix.ArrayField.createValueVector(builder, Array.empty)),
          isNull)
      case (x: MapData, MapType(keyType, valueType, valueContainsNull)) =>
        val keys = new ArrayBuilder.ofInt()
        val values = new ArrayBuilder.ofInt()
        for (i <- 0 until x.numElements) {
          keys += flatbuffersCreateField(
            builder, x.keyArray.get(i, keyType), keyType, isNull)
          values += flatbuffersCreateField(
            builder, x.valueArray.get(i, valueType), valueType, isNull)
        }
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.MapField,
          tuix.MapField.createMapField(
            builder,
            tuix.MapField.createKeysVector(builder, keys.result),
            tuix.MapField.createValuesVector(builder, values.result)),
          isNull)
      case (null, MapType(keyType, valueType, valueContainsNull)) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.MapField,
          tuix.MapField.createMapField(
            builder,
            tuix.MapField.createKeysVector(builder, Array.empty),
            tuix.MapField.createValuesVector(builder, Array.empty)),
          isNull)
      case (s: UTF8String, StringType) =>
        val utf8 = s.getBytes()
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.StringField,
          tuix.StringField.createStringField(
            builder,
            // TODO: pad strings to upper bound for obliviousness
            tuix.StringField.createValueVector(builder, utf8),
            utf8.length),
          isNull)
      case (null, StringType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.StringField,
          tuix.StringField.createStringField(
            builder,
            // TODO: pad strings to upper bound for obliviousness
            tuix.StringField.createValueVector(builder, Array.empty),
            0),
          isNull)
    }
  }

  def flatbuffersExtractFieldValue(f: tuix.Field): Any = {
    if (f.isNull()) {
      null
    } else {
      val fieldUnionType = f.valueType
      fieldUnionType match {
        case tuix.FieldUnion.BooleanField =>
          f.value(new tuix.BooleanField).asInstanceOf[tuix.BooleanField].value
        case tuix.FieldUnion.IntegerField =>
          f.value(new tuix.IntegerField).asInstanceOf[tuix.IntegerField].value
        case tuix.FieldUnion.LongField =>
          f.value(new tuix.LongField).asInstanceOf[tuix.LongField].value
        case tuix.FieldUnion.FloatField =>
          f.value(new tuix.FloatField).asInstanceOf[tuix.FloatField].value
        case tuix.FieldUnion.DoubleField =>
          f.value(new tuix.DoubleField).asInstanceOf[tuix.DoubleField].value
        case tuix.FieldUnion.DateField =>
          f.value(new tuix.DateField).asInstanceOf[tuix.DateField].value
        case tuix.FieldUnion.StringField =>
          val stringField = f.value(new tuix.StringField).asInstanceOf[tuix.StringField]
          val sBytes = new Array[Byte](stringField.length.toInt)
          stringField.valueAsByteBuffer.get(sBytes)
          UTF8String.fromBytes(sBytes)
        case tuix.FieldUnion.BinaryField =>
          val binaryField = f.value(new tuix.BinaryField).asInstanceOf[tuix.BinaryField]
          val length = binaryField.length
          val bBytes = new Array[Byte](length.toInt)
          binaryField.valueAsByteBuffer.get(bBytes)
          bBytes
        case tuix.FieldUnion.ByteField =>
          f.value(new tuix.ByteField).asInstanceOf[tuix.ByteField].value
        case tuix.FieldUnion.CalendarIntervalField =>
          val calendarIntervalField =
            f.value(new tuix.CalendarIntervalField).asInstanceOf[tuix.CalendarIntervalField]
          val months = calendarIntervalField.months
          val days = calendarIntervalField.days
          val microseconds = calendarIntervalField.microseconds
          new CalendarInterval(months, days, microseconds)
        case tuix.FieldUnion.NullField =>
          f.value(new tuix.NullField).asInstanceOf[tuix.NullField].value
        case tuix.FieldUnion.ShortField =>
          f.value(new tuix.ShortField).asInstanceOf[tuix.ShortField].value
        case tuix.FieldUnion.TimestampField =>
          f.value(new tuix.TimestampField).asInstanceOf[tuix.TimestampField].value
        case tuix.FieldUnion.ArrayField =>
          val arrField = f.value(new tuix.ArrayField).asInstanceOf[tuix.ArrayField]
          val arr = new Array[Any](arrField.valueLength)
          for (i <- 0 until arrField.valueLength) {
            arr(i) =
              if (!arrField.value(i).isNull()) {
                flatbuffersExtractFieldValue(arrField.value(i))
              } else {
                null
              }
          }
          ArrayData.toArrayData(arr)
        case tuix.FieldUnion.MapField =>
          val mapField = f.value(new tuix.MapField).asInstanceOf[tuix.MapField]
          val keys = new Array[Any](mapField.keysLength)
          val values = new Array[Any](mapField.valuesLength)
          for (i <- 0 until mapField.keysLength) {
            keys(i) = flatbuffersExtractFieldValue(mapField.keys(i))
            values(i) = flatbuffersExtractFieldValue(mapField.values(i))
          }
          ArrayBasedMapData(keys, values)
      }
    }
  }

  val MaxBlockSize = 1000

  /**
   * Encrypts the given Spark SQL [[InternalRow]]s into a [[Block]] (a serialized
   * tuix.EncryptedBlocks).
   *
   * If `useEnclave` is true, it will attempt to use the local enclave. Otherwise, it will attempt
   * to use the local encryption key, which is intended to be available only on the driver, not the
   * workers.
   */
  def encryptInternalRowsFlatbuffers(
      rows: Seq[InternalRow],
      types: Seq[DataType],
      useEnclave: Boolean): Block = {
    // For the encrypted blocks
    val builder2 = new FlatBufferBuilder
    val encryptedBlockOffsets = ArrayBuilder.make[Int]

    // 1. Serialize the rows as plaintext using tuix.Rows
    var builder = new FlatBufferBuilder
    var rowsOffsets = ArrayBuilder.make[Int]

    def finishBlock(): Unit = {
      val rowsOffsetsArray = rowsOffsets.result
      builder.finish(
        tuix.Rows.createRows(
          builder,
          tuix.Rows.createRowsVector(
            builder,
            rowsOffsetsArray)))
      val plaintext = builder.sizedByteArray()

      // 2. Encrypt the row data and put it into a tuix.EncryptedBlock
      val ciphertext =
        if (useEnclave) {
          val (enclave, eid) = initEnclave()
          enclave.Encrypt(eid, plaintext)
        } else {
          encrypt(plaintext)
        }

      encryptedBlockOffsets += tuix.EncryptedBlock.createEncryptedBlock(
        builder2,
        rowsOffsetsArray.size,
        tuix.EncryptedBlock.createEncRowsVector(builder2, ciphertext))

      builder = new FlatBufferBuilder
      rowsOffsets = ArrayBuilder.make[Int]
    }

    for (row <- rows) {
      rowsOffsets += tuix.Row.createRow(
        builder,
        tuix.Row.createFieldValuesVector(
          builder,
          row.toSeq(types).zip(types).zipWithIndex.map {
            case ((value, dataType), i) =>
              flatbuffersCreateField(builder, value, dataType, row.isNullAt(i))
          }.toArray),
        false)

      if (builder.offset() > MaxBlockSize) {
        finishBlock()
      }
    }
    if (builder.offset() > 0) {
      finishBlock()
    }

    // 3. Put the tuix.EncryptedBlock objects into a tuix.EncryptedBlocks
    builder2.finish(
      tuix.EncryptedBlocks.createEncryptedBlocks(
        builder2,
        tuix.EncryptedBlocks.createBlocksVector(
          builder2,
          encryptedBlockOffsets.result),
        tuix.LogEntryChain.createLogEntryChain(builder2,
          tuix.LogEntryChain.createCurrEntriesVector(builder2, Array.empty),
          tuix.LogEntryChain.createPastEntriesVector(builder2, Array.empty),
          tuix.LogEntryChain.createNumPastEntriesVector(builder2, Array.empty)),
        tuix.EncryptedBlocks.createLogMacVector(builder2, Array.empty),
        tuix.EncryptedBlocks.createAllOutputsMacVector(builder2, Array.empty)
      ))
    val encryptedBlockBytes = builder2.sizedByteArray()

    // 4. Wrap the serialized tuix.EncryptedBlocks in a Scala Block object
    Block(encryptedBlockBytes)
  }

  /**
   * Decrypts the given [[Block]] (a serialized tuix.EncryptedBlocks) and returns the rows within as
   * Spark SQL [[InternalRow]]s.
   *
   * This function can only be called from the driver. The decryption key will not be available on
   * the workers.
   */
  def decryptBlockFlatbuffers(block: Block): Seq[InternalRow] = {
    // 4. Extract the serialized tuix.EncryptedBlocks from the Scala Block object
    val buf = ByteBuffer.wrap(block.bytes)

    // 3. Deserialize the tuix.EncryptedBlocks to get the encrypted rows
    val encryptedBlocks = tuix.EncryptedBlocks.getRootAsEncryptedBlocks(buf)

    (for (i <- 0 until encryptedBlocks.blocksLength) yield {
      val encryptedBlock = encryptedBlocks.blocks(i)
      val ciphertextBuf = encryptedBlock.encRowsAsByteBuffer
      val ciphertext = new Array[Byte](ciphertextBuf.remaining)
      ciphertextBuf.get(ciphertext)

      // 2. Decrypt the row data
      val plaintext = decrypt(ciphertext)

      // 1. Deserialize the tuix.Rows and return them as Scala InternalRow objects
      val rows = tuix.Rows.getRootAsRows(ByteBuffer.wrap(plaintext))
      for (j <- 0 until rows.rowsLength) yield {
        val row = rows.rows(j)
        assert(!row.isDummy)
        InternalRow.fromSeq(
          for (k <- 0 until row.fieldValuesLength) yield {
            val field: Any =
              if (!row.fieldValues(k).isNull()) {
                flatbuffersExtractFieldValue(row.fieldValues(k))
              } else {
                null
              }
            field
          })
      }
    }).flatten
  }

  def addBlockForVerification(block: Block): Unit = {
    val buf = ByteBuffer.wrap(block.bytes)
    val encryptedBlocks = tuix.EncryptedBlocks.getRootAsEncryptedBlocks(buf)
    val blockLog = encryptedBlocks.log
    JobVerificationEngine.addLogEntryChain(blockLog)
  }

  def verifyJob(): Boolean = {
    return JobVerificationEngine.verify()
  }

  def treeFold[BaseType <: TreeNode[BaseType], B](
    tree: BaseType)(op: (Seq[B], BaseType) => B): B = {
    val fromChildren: Seq[B] = tree.children.map(c => treeFold(c)(op))
    op(fromChildren, tree)
  }

  /** Serialize an Expression into a tuix.Expr. Returns the offset of the written tuix.Expr. */
  def flatbuffersSerializeExpression(
    builder: FlatBufferBuilder, expr: Expression, input: Seq[Attribute]): Int = {
    treeFold[Expression, Int](expr) {
      (childrenOffsets, expr) => (expr, childrenOffsets) match {
        case (ar: AttributeReference, Nil) if input.exists(_.semanticEquals(ar)) =>
          val colNum = input.indexWhere(_.semanticEquals(ar))
          assert(colNum != -1)
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Col,
            tuix.Col.createCol(builder, colNum))

        case (Literal(value, dataType), Nil) =>
          val valueOffset = flatbuffersCreateField(builder, value, dataType, (value == null))
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Literal,
            tuix.Literal.createLiteral(builder, valueOffset))

        case (Alias(child, _), Seq(childOffset)) =>
          // TODO: Use an expression for aliases so we can refer to them elsewhere in the expression
          // tree. For now we just ignore them when evaluating expressions.
          childOffset

        case (Cast(child, dataType, _), Seq(childOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Cast,
            tuix.Cast.createCast(
              builder,
              childOffset,
              dataType match {
                case IntegerType => tuix.ColType.IntegerType
                case LongType => tuix.ColType.LongType
                case FloatType => tuix.ColType.FloatType
                case DoubleType => tuix.ColType.DoubleType
                case StringType => tuix.ColType.StringType
              }))

        // Arithmetic
        case (Add(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Add,
            tuix.Add.createAdd(
              builder, leftOffset, rightOffset))

        case (Subtract(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Subtract,
            tuix.Subtract.createSubtract(
              builder, leftOffset, rightOffset))

        case (Multiply(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Multiply,
            tuix.Multiply.createMultiply(
              builder, leftOffset, rightOffset))

        case (Divide(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Divide,
            tuix.Divide.createDivide(
              builder, leftOffset, rightOffset))

        case (UnaryMinus(child), Seq(childOffset)) =>
          // Implement UnaryMinus(child) as Subtract(Literal(0), child)
          val zeroOffset = flatbuffersSerializeExpression(
            builder, Cast(Literal(0), child.dataType), input)

          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Subtract,
            tuix.Subtract.createSubtract(
              builder, zeroOffset, childOffset))

        // Predicates
        case (And(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.And,
            tuix.And.createAnd(
              builder, leftOffset, rightOffset))

        case (Or(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Or,
            tuix.Or.createOr(
              builder, leftOffset, rightOffset))

        case (Not(child), Seq(childOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Not,
            tuix.Not.createNot(
              builder, childOffset))

        case (LessThan(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.LessThan,
            tuix.LessThan.createLessThan(
              builder, leftOffset, rightOffset))

        case (LessThanOrEqual(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.LessThanOrEqual,
            tuix.LessThanOrEqual.createLessThanOrEqual(
              builder, leftOffset, rightOffset))

        case (GreaterThan(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.GreaterThan,
            tuix.GreaterThan.createGreaterThan(
              builder, leftOffset, rightOffset))

        case (GreaterThanOrEqual(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.GreaterThanOrEqual,
            tuix.GreaterThanOrEqual.createGreaterThanOrEqual(
              builder, leftOffset, rightOffset))

        case (EqualTo(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.EqualTo,
            tuix.EqualTo.createEqualTo(
              builder, leftOffset, rightOffset))

        // String expressions
        case (Substring(str, pos, len), Seq(strOffset, posOffset, lenOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Substring,
            tuix.Substring.createSubstring(
              builder, strOffset, posOffset, lenOffset))
        
        case (Like(left, right, escapeChar), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Like,
            tuix.Like.createLike(
              builder, leftOffset, rightOffset))

        case (StartsWith(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.StartsWith,
            tuix.StartsWith.createStartsWith(
              builder, leftOffset, rightOffset))

        case (EndsWith(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.EndsWith,
            tuix.EndsWith.createEndsWith(
              builder, leftOffset, rightOffset))

        // Conditional expressions
        case (If(predicate, trueValue, falseValue), Seq(predOffset, trueOffset, falseOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.If,
            tuix.If.createIf(
              builder, predOffset, trueOffset, falseOffset))

        case (CaseWhen(Seq((predicate, trueValue)), falseValue), Seq(predOffset, trueOffset, falseOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.If,
            tuix.If.createIf(
              builder, predOffset, trueOffset, falseOffset))

        case (CaseWhen(branches, elseValue), childrenOffsets) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.CaseWhen,
            tuix.CaseWhen.createCaseWhen(
              builder,
              tuix.CaseWhen.createChildrenVector(
                builder,
                childrenOffsets.toArray)))

        // Null expressions
        case (IsNull(child), Seq(childOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.IsNull,
            tuix.IsNull.createIsNull(
              builder, childOffset))

        case (IsNotNull(child), Seq(childOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Not,
            tuix.Not.createNot(
              builder,
              tuix.Expr.createExpr(
                builder,
                tuix.ExprUnion.IsNull,
                tuix.IsNull.createIsNull(
                  builder, childOffset))))

        case (Contains(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Contains,
            tuix.Contains.createContains(
              builder, leftOffset, rightOffset))

        // Time expressions
        case (Year(child), Seq(childOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Year,
            tuix.Year.createYear(
              builder, childOffset))

        case (DateAdd(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.DateAdd,
            tuix.DateAdd.createDateAdd(
              builder, leftOffset, rightOffset))

        case (DateAddInterval(left, right, _, _), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.DateAddInterval,
            tuix.DateAddInterval.createDateAddInterval(
              builder, leftOffset, rightOffset))

        // Math expressions
        case (Exp(child), Seq(childOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Exp,
            tuix.Exp.createExp(
              builder, childOffset))

        // Complex type creation
        case (ca @ CreateArray(children, false), childrenOffsets) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.CreateArray,
            tuix.CreateArray.createCreateArray(
              builder,
              tuix.CreateArray.createChildrenVector(
                builder,
                childrenOffsets.toArray)))

        // Opaque UDFs
        case (VectorAdd(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.VectorAdd,
            tuix.VectorAdd.createVectorAdd(
              builder, leftOffset, rightOffset))

        case (VectorMultiply(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.VectorMultiply,
            tuix.VectorMultiply.createVectorMultiply(
              builder, leftOffset, rightOffset))

        case (DotProduct(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.DotProduct,
            tuix.DotProduct.createDotProduct(
              builder, leftOffset, rightOffset))

        case (Upper(child), Seq(childOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.Upper,
            tuix.Upper.createUpper(
              builder, childOffset))   

        case (ClosestPoint(left, right), Seq(leftOffset, rightOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.ClosestPoint,
            tuix.ClosestPoint.createClosestPoint(
              builder, leftOffset, rightOffset))
      }
    }
  }

  def serializeFilterExpression(condition: Expression, input: Seq[Attribute]): Array[Byte] = {
    val builder = new FlatBufferBuilder
    builder.finish(
      tuix.FilterExpr.createFilterExpr(
        builder,
        flatbuffersSerializeExpression(builder, condition, input)))
    builder.sizedByteArray()
  }

  def serializeProjectList(
    projectList: Seq[NamedExpression], input: Seq[Attribute]): Array[Byte] = {
    val builder = new FlatBufferBuilder
    builder.finish(
      tuix.ProjectExpr.createProjectExpr(
        builder,
        tuix.ProjectExpr.createProjectListVector(
          builder,
          projectList.map(expr => flatbuffersSerializeExpression(builder, expr, input)).toArray)))
    builder.sizedByteArray()
  }

  def serializeSortOrder(
    sortOrder: Seq[SortOrder], input: Seq[Attribute]): Array[Byte] = {
    val builder = new FlatBufferBuilder
    builder.finish(
      tuix.SortExpr.createSortExpr(
        builder,
        tuix.SortExpr.createSortOrderVector(
          builder,
          sortOrder.map(o =>
            tuix.SortOrder.createSortOrder(
              builder,
              flatbuffersSerializeExpression(builder, o.child, input),
              o.direction match {
                case Ascending => tuix.SortDirection.Ascending
                case Descending => tuix.SortDirection.Descending
              })).toArray)))
    builder.sizedByteArray()
  }

  def serializeJoinExpression(
    joinType: JoinType, leftKeys: Seq[Expression], rightKeys: Seq[Expression],
    leftSchema: Seq[Attribute], rightSchema: Seq[Attribute]): Array[Byte] = {
    val builder = new FlatBufferBuilder
    builder.finish(
      tuix.JoinExpr.createJoinExpr(
        builder,
        joinType match {
          case Inner => tuix.JoinType.Inner
          case FullOuter => tuix.JoinType.FullOuter
          case LeftOuter => tuix.JoinType.LeftOuter
          case RightOuter => tuix.JoinType.RightOuter
          case LeftSemi => tuix.JoinType.LeftSemi
          case LeftAnti => tuix.JoinType.LeftAnti
          case Cross => tuix.JoinType.Cross
          // scalastyle:off
          case ExistenceJoin(_) => ???
          case NaturalJoin(_) => ???
          case UsingJoin(_, _) => ???
          // scalastyle:on
        },
        tuix.JoinExpr.createLeftKeysVector(
          builder,
          leftKeys.map(e => flatbuffersSerializeExpression(builder, e, leftSchema)).toArray),
        tuix.JoinExpr.createRightKeysVector(
          builder,
          rightKeys.map(e => flatbuffersSerializeExpression(builder, e, rightSchema)).toArray)))
    builder.sizedByteArray()
  }

  def serializeAggOp(
    groupingExpressions: Seq[NamedExpression],
    aggExpressions: Seq[AggregateExpression],
    input: Seq[Attribute]): Array[Byte] = {

    // The output of agg operator contains both the grouping columns and the aggregate values.
    // To avoid the need for special handling of the grouping columns, we transform the grouping expressions
    // into AggregateExpressions that collect the first seen value.
    val aggGroupingExpressions = groupingExpressions.map {
      case e: NamedExpression => AggregateExpression(First(e, Literal(false)), Complete, false)
    }
    val aggregateExpressions = aggGroupingExpressions ++ aggExpressions

    val aggSchema = aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    // For aggregation, we concatenate the current aggregate row with the new input row and run
    // the update expressions as a projection to obtain a new aggregate row. concatSchema
    // describes the schema of the temporary concatenated row.
    val concatSchema = aggSchema ++ input

    val builder = new FlatBufferBuilder
    builder.finish(
      tuix.AggregateOp.createAggregateOp(
        builder,
        tuix.AggregateOp.createGroupingExpressionsVector(
          builder,
          groupingExpressions.map(e => flatbuffersSerializeExpression(builder, e, input)).toArray),
        tuix.AggregateOp.createAggregateExpressionsVector(
          builder,
          aggregateExpressions
            .map(e => serializeAggExpression(builder, e, input, aggSchema, concatSchema))
            .toArray)))
    builder.sizedByteArray()
  }

  /**
   * Serialize an AggregateExpression into a tuix.AggregateExpr. Returns the offset of the written
   * tuix.AggregateExpr.
   */
  def serializeAggExpression(
    builder: FlatBufferBuilder,
    e: AggregateExpression,
    input: Seq[Attribute],
    aggSchema: Seq[Attribute],
    concatSchema: Seq[Attribute]): Int = {
    (e.aggregateFunction: @unchecked) match {

      case avg @ Average(child)  =>
        val sum = avg.aggBufferAttributes(0)
        val count = avg.aggBufferAttributes(1)
        val dataType = child.dataType

        val sumInitValue = Literal.default(dataType)
        val countInitValue = Literal(0L)
        // TODO: support DecimalType to match Spark SQL behavior

        val (updateExprs: Seq[Expression], evaluateExprs: Seq[Expression]) = e.mode match {
          case Partial => {
            val sumUpdateExpr = Add(
              sum,
              If(IsNull(child),
                Literal.default(dataType),
                Cast(child, dataType)))
            val countUpdateExpr = If(IsNull(child), count, Add(count, Literal(1L)))
            (Seq(sumUpdateExpr, countUpdateExpr), Seq(sum, count))
          }
          case Final => {
            val sumUpdateExpr = Add(sum, avg.inputAggBufferAttributes(0))
            val countUpdateExpr = Add(count, avg.inputAggBufferAttributes(1))
            val evalExpr = If(EqualTo(count, Literal(0L)),
              Literal.create(null, DoubleType),
              Divide(Cast(sum, DoubleType), Cast(count, DoubleType)))
            (Seq(sumUpdateExpr, countUpdateExpr), Seq(evalExpr))
          }
          case Complete => {
            val sumUpdateExpr = Add(
              sum,
              If(IsNull(child), Cast(Literal(0), dataType), Cast(child, dataType)))
            val countUpdateExpr = If(IsNull(child), count, Add(count, Literal(1L)))
            val evalExpr = Divide(Cast(sum, DoubleType), Cast(count, DoubleType))
            (Seq(sumUpdateExpr, countUpdateExpr), Seq(evalExpr))
          }
          case _ => 
        }

        tuix.AggregateExpr.createAggregateExpr(
          builder,
          tuix.AggregateExpr.createInitialValuesVector(
            builder,
            Array(
              /* sum = */ flatbuffersSerializeExpression(builder, sumInitValue, input),
              /* count = */ flatbuffersSerializeExpression(builder, countInitValue, input))),
          tuix.AggregateExpr.createUpdateExprsVector(
            builder,
            updateExprs.map(e => flatbuffersSerializeExpression(builder, e, concatSchema)).toArray),
          tuix.AggregateExpr.createEvaluateExprsVector(
            builder,
            evaluateExprs.map(e => flatbuffersSerializeExpression(builder, e, aggSchema)).toArray)
        )

      case c @ Count(children) =>
        val count = c.aggBufferAttributes(0)
        // COUNT(*) should count NULL values
        // COUNT(expr) should return the number or rows for which the supplied expressions are non-NULL

        val (updateExprs: Seq[Expression], evaluateExprs: Seq[Expression]) = e.mode match {
          case Partial => {
            val nullableChildren = children.filter(_.nullable)
            val countUpdateExpr = nullableChildren.isEmpty match {
              case true => Add(count, Literal(1L))
              case false => If(nullableChildren.map(IsNull).reduce(Or), count, Add(count, Literal(1L)))
             }
            (Seq(countUpdateExpr), Seq(count))
          }
          case Final => {
            val countUpdateExpr = Add(count, c.inputAggBufferAttributes(0))
            (Seq(countUpdateExpr), Seq(count))
          }
          case Complete => {
            val countUpdateExpr = Add(count, Literal(1L))
            (Seq(countUpdateExpr), Seq(count))
          }
          case _ => 
        }

        tuix.AggregateExpr.createAggregateExpr(
          builder,
          tuix.AggregateExpr.createInitialValuesVector(
            builder,
            Array(
              /* count = */ flatbuffersSerializeExpression(builder, Literal(0L), input))),
          tuix.AggregateExpr.createUpdateExprsVector(
            builder,
            updateExprs.map(e => flatbuffersSerializeExpression(builder, e, concatSchema)).toArray),
          tuix.AggregateExpr.createEvaluateExprsVector(
            builder,
            evaluateExprs.map(e => flatbuffersSerializeExpression(builder, e, aggSchema)).toArray)
        )

      case f @ First(child, Literal(false, BooleanType)) =>
        val first = f.aggBufferAttributes(0)
        val valueSet = f.aggBufferAttributes(1)

        val (updateExprs, evaluateExprs) = e.mode match {
          case Partial => {
            val firstUpdateExpr = If(valueSet, first, child)
              val valueSetUpdateExpr = Literal(true)
            (Seq(firstUpdateExpr, valueSetUpdateExpr), Seq(first, valueSet))
          }
          case Final => {
            val firstUpdateExpr = If(valueSet, first, f.inputAggBufferAttributes(0))
            val valueSetUpdateExpr = Or(valueSet, f.inputAggBufferAttributes(1))
            (Seq(firstUpdateExpr, valueSetUpdateExpr), Seq(first))
          }
          case Complete => {
            val firstUpdateExpr = If(valueSet, first, child)
            val valueSetUpdateExpr = Literal(true)
            (Seq(firstUpdateExpr, valueSetUpdateExpr), Seq(first))
          }
        }

        // TODO: support aggregating null values
        tuix.AggregateExpr.createAggregateExpr(
          builder,
          tuix.AggregateExpr.createInitialValuesVector(
            builder,
            Array(
              /* first = */ flatbuffersSerializeExpression(
                builder, Literal.create(null, child.dataType), input),
              /* valueSet = */ flatbuffersSerializeExpression(builder, Literal(false), input))),
          tuix.AggregateExpr.createUpdateExprsVector(
            builder,
            updateExprs.map(e => flatbuffersSerializeExpression(builder, e, concatSchema)).toArray),
          tuix.AggregateExpr.createEvaluateExprsVector(
            builder,
            evaluateExprs.map(e => flatbuffersSerializeExpression(builder, e, aggSchema)).toArray))

      case l @ Last(child, Literal(false, BooleanType)) =>
        val last = l.aggBufferAttributes(0)
        val valueSet = l.aggBufferAttributes(1)

        val (updateExprs, evaluateExprs) = e.mode match {
          case Partial => {
            val lastUpdateExpr = child
            val valueSetUpdateExpr = Literal(true)
            (Seq(lastUpdateExpr, valueSetUpdateExpr), Seq(last, valueSet))
          }
          case Final => {
            val lastUpdateExpr = If(l.inputAggBufferAttributes(1), l.inputAggBufferAttributes(0), last)
            val valueSetUpdateExpr = Or(l.inputAggBufferAttributes(1), valueSet)
            (Seq(lastUpdateExpr, valueSetUpdateExpr), Seq(last))
          }
          case Complete => {
            val lastUpdateExpr = child
            val valueSetUpdateExpr = Literal(true)
            (Seq(lastUpdateExpr, valueSetUpdateExpr), Seq(last))
          }
        }

        // TODO: support aggregating null values
        tuix.AggregateExpr.createAggregateExpr(
          builder,
          tuix.AggregateExpr.createInitialValuesVector(
            builder,
            Array(
              /* last = */ flatbuffersSerializeExpression(
                builder, Literal.create(null, child.dataType), input),
              /* valueSet = */ flatbuffersSerializeExpression(builder, Literal(false), input))),
          tuix.AggregateExpr.createUpdateExprsVector(
            builder,
            updateExprs.map(e => flatbuffersSerializeExpression(builder, e, concatSchema)).toArray),
          tuix.AggregateExpr.createEvaluateExprsVector(
            builder,
            evaluateExprs.map(e => flatbuffersSerializeExpression(builder, e, aggSchema)).toArray))

      case m @ Max(child) =>
        val max = m.aggBufferAttributes(0)

        val (updateExprs, evaluateExprs) = e.mode match {
          case Partial => {
            val maxUpdateExpr = If(Or(IsNull(max), GreaterThan(child, max)), child, max)
            (Seq(maxUpdateExpr), Seq(max))
          }
          case Final => {
            val maxUpdateExpr = If(Or(IsNull(max),
              GreaterThan(m.inputAggBufferAttributes(0), max)), m.inputAggBufferAttributes(0), max)
            (Seq(maxUpdateExpr), Seq(max))
          }
          case Complete => {
            val maxUpdateExpr = child
            (Seq(maxUpdateExpr), Seq(max))
          }
        }

        tuix.AggregateExpr.createAggregateExpr(
          builder,
          tuix.AggregateExpr.createInitialValuesVector(
            builder,
            Array(
              /* max = */ flatbuffersSerializeExpression(
                builder, Literal.create(null, child.dataType), input))),
          tuix.AggregateExpr.createUpdateExprsVector(
            builder,
            updateExprs.map(e => flatbuffersSerializeExpression(builder, e, concatSchema)).toArray),
          tuix.AggregateExpr.createEvaluateExprsVector(
            builder,
            evaluateExprs.map(e => flatbuffersSerializeExpression(builder, e, aggSchema)).toArray))

      case m @ Min(child) =>
        val min = m.aggBufferAttributes(0)

        val (updateExprs, evaluateExprs) = e.mode match {
          case Partial => {
            val minUpdateExpr = If(Or(IsNull(min), LessThan(child, min)), child, min)
              (Seq(minUpdateExpr), Seq(min))
          }
          case Final => {
            val minUpdateExpr = If(Or(IsNull(min),
              LessThan(m.inputAggBufferAttributes(0), min)), m.inputAggBufferAttributes(0), min)
              (Seq(minUpdateExpr), Seq(min))
          }
          case Complete => {
            val minUpdateExpr = child
            (Seq(minUpdateExpr), Seq(min))
          }
        }

        tuix.AggregateExpr.createAggregateExpr(
          builder,
          tuix.AggregateExpr.createInitialValuesVector(
            builder,
            Array(
              /* min = */ flatbuffersSerializeExpression(
                builder, Literal.create(null, child.dataType), input))),
          tuix.AggregateExpr.createUpdateExprsVector(
            builder,
            updateExprs.map(e => flatbuffersSerializeExpression(builder, e, concatSchema)).toArray),
          tuix.AggregateExpr.createEvaluateExprsVector(
            builder,
            evaluateExprs.map(e => flatbuffersSerializeExpression(builder, e, aggSchema)).toArray))

      case s @ Sum(child) =>
        val sum = s.aggBufferAttributes(0)
        val sumDataType = s.dataType
        // If any value is not NULL, return a non-NULL value
        // If all values are NULL, return NULL

        val initValue = Literal.create(null, sumDataType)
        val (updateExprs, evaluateExprs) = e.mode match {
          case Partial => {
            val partialSum = Add(If(IsNull(sum), Literal.default(sumDataType), sum), Cast(child, sumDataType))
            val sumUpdateExpr = If(IsNull(partialSum), sum, partialSum)
            (Seq(sumUpdateExpr), Seq(sum))
          }
          case Final => {
            val partialSum = Add(If(IsNull(sum), Literal.default(sumDataType), sum), s.inputAggBufferAttributes(0))
            val sumUpdateExpr = If(IsNull(partialSum), sum, partialSum)
            (Seq(sumUpdateExpr), Seq(sum))
          }
          case Complete => {
            val sumUpdateExpr = Add(If(IsNull(sum), Literal.default(sumDataType), sum), Cast(child, sumDataType))
            (Seq(sumUpdateExpr), Seq(sum))
          }
        }

        tuix.AggregateExpr.createAggregateExpr(
          builder,
          tuix.AggregateExpr.createInitialValuesVector(
            builder,
            Array(
              /* sum = */ flatbuffersSerializeExpression(
                builder, initValue, input))),
          tuix.AggregateExpr.createUpdateExprsVector(
            builder,
            updateExprs.map(e => flatbuffersSerializeExpression(builder, e, concatSchema)).toArray),
          tuix.AggregateExpr.createEvaluateExprsVector(
            builder,
            evaluateExprs.map(e => flatbuffersSerializeExpression(builder, e, aggSchema)).toArray))

      case vs @ ScalaUDAF(Seq(child), _: VectorSum, _, _) =>
        val sum = vs.aggBufferAttributes(0)

        val sumDataType = vs.dataType

        val (updateExprs, evaluateExprs) = e.mode match {
          case Partial => {
            val vectorSumUpdateExpr = VectorAdd(sum, child)
            (Seq(vectorSumUpdateExpr), Seq(sum))
          }
          case Final => {
            val vectorSumUpdateExpr = VectorAdd(sum, vs.inputAggBufferAttributes(0))
            (Seq(vectorSumUpdateExpr), Seq(sum))
          }
          case Complete => {
            val vectorSumUpdateExpr = VectorAdd(sum, child)
            (Seq(vectorSumUpdateExpr), Seq(sum))
          }
        }

        // TODO: support aggregating null values
        tuix.AggregateExpr.createAggregateExpr(
          builder,
          tuix.AggregateExpr.createInitialValuesVector(
            builder,
            Array(
              /* sum = */ flatbuffersSerializeExpression(
                builder, Literal(Array[Double]()), input))),
          tuix.AggregateExpr.createUpdateExprsVector(
            builder,
            updateExprs.map(e => flatbuffersSerializeExpression(builder, e, concatSchema)).toArray),
          tuix.AggregateExpr.createEvaluateExprsVector(
            builder,
            evaluateExprs.map(e => flatbuffersSerializeExpression(builder, e, aggSchema)).toArray))
    }
  }

  def concatEncryptedBlocks(blocks: Seq[Block]): Block = {
    // Input: sequence of EncryptedBlocks
    // This gets a list of all EncryptedBlock
    val allBlocks = for {
      block <- blocks
      encryptedBlocks = tuix.EncryptedBlocks.getRootAsEncryptedBlocks(ByteBuffer.wrap(block.bytes))
      i <- 0 until encryptedBlocks.blocksLength
    } yield encryptedBlocks.blocks(i)

    val allLogMacs = for {
      block <- blocks
      encryptedBlocks = tuix.EncryptedBlocks.getRootAsEncryptedBlocks(ByteBuffer.wrap(block.bytes))
      i <- 0 until encryptedBlocks.logMacLength
    } yield encryptedBlocks.logMac(i)

    // For tuix::Mac EncryptedBlocks.all_outputs_mac
    // val allAllOutputsMacs = for {
    //   block <- blocks
    //   encryptedBlocks = tuix.EncryptedBlocks.getRootAsEncryptedBlocks(ByteBuffer.wrap(block.bytes))
    //   i <- 0 until encryptedBlocks.allOutputsMacLength
    // } yield encryptedBlocks.allOutputsMac(i)

    val allAllOutputsMacs = for {
      block <- blocks
      encryptedBlocks = tuix.EncryptedBlocks.getRootAsEncryptedBlocks(ByteBuffer.wrap(block.bytes))
      i <- 0 until encryptedBlocks.allOutputsMacLength
    } yield encryptedBlocks.allOutputsMac(i).toByte

    val allLogEntryChains = for {
      block <- blocks
      encryptedBlocks = tuix.EncryptedBlocks.getRootAsEncryptedBlocks(ByteBuffer.wrap(block.bytes))
    } yield encryptedBlocks.log

    val allCurrLogEntries = for {
      logEntryChain <- allLogEntryChains
      i <- 0 until logEntryChain.currEntriesLength
    } yield logEntryChain.currEntries(i)

    val allPastLogEntries = for {
      logEntryChain <- allLogEntryChains
      i <- 0 until logEntryChain.pastEntriesLength
    } yield logEntryChain.pastEntries(i)

    val numPastEntriesList = for {
      logEntryChain <- allLogEntryChains
    } yield logEntryChain.numPastEntries(0)

    val builder = new FlatBufferBuilder
    builder.finish(
      tuix.EncryptedBlocks.createEncryptedBlocks(
        builder, tuix.EncryptedBlocks.createBlocksVector(builder, allBlocks.map { encryptedBlock =>
          val encRows = new Array[Byte](encryptedBlock.encRowsLength)
          encryptedBlock.encRowsAsByteBuffer.get(encRows)
          tuix.EncryptedBlock.createEncryptedBlock(
            builder,
            encryptedBlock.numRows,
            tuix.EncryptedBlock.createEncRowsVector(builder, encRows))
        }.toArray), 
        tuix.LogEntryChain.createLogEntryChain(
          builder, 
          tuix.LogEntryChain.createCurrEntriesVector(builder, allCurrLogEntries.map { currLogEntry =>
            val macLst = new Array[Byte](currLogEntry.macLstLength)
            currLogEntry.macLstAsByteBuffer.get(macLst)
            val macLstMac = new Array[Byte](currLogEntry.macLstMacLength)
            currLogEntry.macLstMacAsByteBuffer.get(macLstMac)
            val inputMacs = new Array[Byte](currLogEntry.inputMacsLength)
            currLogEntry.inputMacsAsByteBuffer.get(inputMacs)
            tuix.LogEntry.createLogEntry(
              builder,
              currLogEntry.ecall,
              currLogEntry.numMacs,
              tuix.LogEntry.createMacLstVector(builder, macLst),
              tuix.LogEntry.createMacLstMacVector(builder, macLstMac),
              tuix.LogEntry.createInputMacsVector(builder, inputMacs),
              currLogEntry.numInputMacs)
          }.toArray),
          tuix.LogEntryChain.createPastEntriesVector(builder, allPastLogEntries.map { crumb =>
            val inputMacs = new Array[Byte](crumb.inputMacsLength)
            crumb.inputMacsAsByteBuffer.get(inputMacs)
            val allOutputsMac = new Array[Byte](crumb.allOutputsMacLength)
            crumb.allOutputsMacAsByteBuffer.get(allOutputsMac)
            val logMac = new Array[Byte](crumb.logMacLength)
            crumb.logMacAsByteBuffer.get(logMac)
            tuix.Crumb.createCrumb(
              builder,
              tuix.Crumb.createInputMacsVector(builder, inputMacs),
              crumb.numInputMacs,
              tuix.Crumb.createAllOutputsMacVector(builder, allOutputsMac),
              crumb.ecall,
              tuix.Crumb.createLogMacVector(builder, logMac))
          }.toArray),
          tuix.LogEntryChain.createNumPastEntriesVector(builder, numPastEntriesList.toArray)
        ),
      tuix.EncryptedBlocks.createLogMacVector(builder, allLogMacs.map { logMac =>
          val mac = new Array[Byte](logMac.macLength)
          logMac.macAsByteBuffer.get(mac)
          tuix.Mac.createMac(builder, tuix.Mac.createMacVector(builder, mac))
        }.toArray),
      // tuix.EncryptedBlocks.createAllOutputsMacVector(builder, allAllOutputsMacs.map { allOutputsMac =>
      //     val mac = new Array[Byte](allOutputsMac.macLength)
      //     allOutputsMac.macAsByteBuffer.get(mac)
      //     tuix.Mac.createMac(builder, tuix.Mac.createMacVector(builder, mac))
      //   }.toArray)
      tuix.EncryptedBlocks.createAllOutputsMacVector(builder, allAllOutputsMacs.toArray)
      ))
    Block(builder.sizedByteArray())
  }

  def emptyBlock: Block = {
    val builder = new FlatBufferBuilder
    builder.finish(
      tuix.EncryptedBlocks.createEncryptedBlocks(
        builder, 
        tuix.EncryptedBlocks.createBlocksVector(builder, Array.empty), 
        tuix.LogEntryChain.createLogEntryChain(builder,
          tuix.LogEntryChain.createCurrEntriesVector(builder, Array.empty),
          tuix.LogEntryChain.createPastEntriesVector(builder, Array.empty),
          tuix.LogEntryChain.createNumPastEntriesVector(builder, Array.empty)),
        tuix.EncryptedBlocks.createLogMacVector(builder, Array.empty),
        tuix.EncryptedBlocks.createAllOutputsMacVector(builder, Array.empty)))
    Block(builder.sizedByteArray())
  }

  // def emptyBlock(block: Block): Block = {
  //   val builder = new FlatBufferBuilder
  //   val encryptedBlocks = tuix.EncryptedBlocks.getRootAsEncryptedBlocks(ByteBuffer.wrap(block.bytes))
  //   val pastLogEntries = for {
  //     i <- 0 until encryptedBlocks.log.pastEntriesLength
  //   } yield encryptedBlocks.log.pastEntries(i)
  // 
  //   val currLogEntry = encryptedBlocks.log.currEntries(0)
  // 
  //   val logEntries = pastLogEntries :+ currLogEntry
  // 
  //   val numPastEntries = encryptedBlocks.log.numPastEntries(0)
  // 
  //   builder.finish(
  //     tuix.EncryptedBlocks.createEncryptedBlocks(
  //       builder, 
  //       tuix.EncryptedBlocks.createBlocksVector(builder, Array.empty), 
  //       tuix.LogEntryChain.createLogEntryChain(builder,
  //         tuix.LogEntryChain.createCurrEntriesVector(builder, 
  //           Array.empty),
  //       tuix.LogEntryChain.createPastEntriesVector(builder, logEntries.map { logEntry =>
  //         tuix.LogEntry.createLogEntry(
  //           builder,
  //           logEntry.ecall,
  //           logEntry.sndPid,
  //           logEntry.rcvPid,
  //           logEntry.jobId,
  //           0,
  //           tuix.LogEntry.createMacLstVector(builder, Array.empty),
  //           tuix.LogEntry.createMacLstMacVector(builder, Array.empty)
  //       )}.toArray),
  //       tuix.LogEntryChain.createNumPastEntriesVector(builder, Array(numPastEntries))),
  //     tuix.EncryptedBlocks.createLogMacVector(builder, Array.empty)))
  //   Block(builder.sizedByteArray())
  // }
}
