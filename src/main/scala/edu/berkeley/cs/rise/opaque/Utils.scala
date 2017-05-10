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

import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.collection.mutable.ArrayBuilder

import com.google.flatbuffers.FlatBufferBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Descending
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Multiply
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.catalyst.expressions.Subtract
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
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLDate
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String

import edu.berkeley.cs.rise.opaque.execution.Block
import edu.berkeley.cs.rise.opaque.execution.OpaqueOperatorExec
import edu.berkeley.cs.rise.opaque.execution.SGXEnclave
import edu.berkeley.cs.rise.opaque.logical.ConvertToOpaqueOperators
import edu.berkeley.cs.rise.opaque.logical.EncryptLocalRelation

object Utils {
  private val perf: Boolean = System.getenv("SGX_PERF") == "1"

  def time[A](desc: String)(f: => A): A = {
    val start = System.nanoTime
    val result = f
    if (perf) {
      println(s"$desc: ${(System.nanoTime - start) / 1000000.0} ms")
    }
    result
  }

  def logPerf(message: String): Unit = {
    if (perf) {
      println(message)
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
    println(jsonSerialize(attrs))
    result
  }

  def initEnclave(): (SGXEnclave, Long) = {
    this.synchronized {
      if (eid == 0L) {
        if (System.getenv("LIBSGXENCLAVE_PATH") == null) {
          throw new Exception("Set LIBSGXENCLAVE_PATH")
        }
        System.load(System.getenv("LIBSGXENCLAVE_PATH"))
        val enclave = new SGXEnclave()
        eid = enclave.StartEnclave()
        println("Starting an enclave")
        (enclave, eid)
      } else {
        val enclave = new SGXEnclave()
        (enclave, eid)
      }
    }
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
      rdd: RDD[T], storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): RDD[T] = {
    if (rdd.getStorageLevel == StorageLevel.NONE) {
      rdd.persist(storageLevel)
    } else {
      rdd
    }
  }

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
      case (x: Int, IntegerType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.IntegerField,
          tuix.IntegerField.createIntegerField(builder, x),
          isNull)
      case (x: Long, LongType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.LongField,
          tuix.LongField.createLongField(builder, x),
          isNull)
      case (x: Float, FloatType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.FloatField,
          tuix.FloatField.createFloatField(builder, x),
          isNull)
      case (x: Double, DoubleType) =>
        tuix.Field.createField(
          builder,
          tuix.FieldUnion.DoubleField,
          tuix.DoubleField.createDoubleField(builder, x),
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
        case tuix.FieldUnion.StringField =>
          val stringField = f.value(new tuix.StringField).asInstanceOf[tuix.StringField]
          val sBytes = new Array[Byte](stringField.length.toInt)
          stringField.valueAsByteBuffer.get(sBytes)
          UTF8String.fromBytes(sBytes)
      }
    }
  }

  val MaxBlockSize = 1000

  def encryptInternalRowsFlatbuffers(rows: Seq[InternalRow], types: Seq[DataType]): Block = {
    // For the encrypted blocks
    val builder2 = new FlatBufferBuilder
    var encryptedBlockOffsets = ArrayBuilder.make[Int]

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
      val (enclave, eid) = initEnclave()
      val ciphertext = enclave.Encrypt(eid, plaintext)

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
          encryptedBlockOffsets.result)))
    val encryptedBlockBytes = builder2.sizedByteArray()

    // 4. Wrap the serialized tuix.EncryptedBlocks in a Scala Block object
    Block(encryptedBlockBytes)
  }

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
      val (enclave, eid) = initEnclave()
      val plaintext = enclave.Decrypt(eid, ciphertext)

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

        // Predicates
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

        // Conditional expressions
        case (If(predicate, trueValue, falseValue), Seq(predOffset, trueOffset, falseOffset)) =>
          tuix.Expr.createExpr(
            builder,
            tuix.ExprUnion.If,
            tuix.If.createIf(
              builder, predOffset, trueOffset, falseOffset))
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
          case ExistenceJoin(_) => ???
          case NaturalJoin(_) => ???
          case UsingJoin(_, _) => ???
        },
        tuix.JoinExpr.createLeftKeysVector(
          builder,
          leftKeys.map(e => flatbuffersSerializeExpression(builder, e, leftSchema)).toArray),
        tuix.JoinExpr.createRightKeysVector(
          builder,
          rightKeys.map(e => flatbuffersSerializeExpression(builder, e, rightSchema)).toArray)))
    builder.sizedByteArray()
  }

  def concatEncryptedBlocks(blocks: Seq[Block]): Block = {
    val allBlocks = for {
      block <- blocks
      encryptedBlocks = tuix.EncryptedBlocks.getRootAsEncryptedBlocks(ByteBuffer.wrap(block.bytes))
      i <- 0 until encryptedBlocks.blocksLength
    } yield encryptedBlocks.blocks(i)

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
        }.toArray)))
    Block(builder.sizedByteArray())
  }
}
