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

package edu.berkeley.cs.rise.opaque.execution

import scala.collection.mutable.ArrayBuffer

import edu.berkeley.cs.rise.opaque.Utils
import edu.berkeley.cs.rise.opaque.RA
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import Opcode._

trait LeafExecNode extends SparkPlan {
  override final def children: Seq[SparkPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

trait UnaryExecNode extends SparkPlan {
  def child: SparkPlan

  override final def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

trait BinaryExecNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override final def children: Seq[SparkPlan] = Seq(left, right)
}

case class EncryptedLocalTableScanExec(
    output: Seq[Attribute],
    plaintextData: Seq[InternalRow],
    override val isOblivious: Boolean)
  extends LeafExecNode with OpaqueOperatorExec {

  private val unsafeRows: Array[InternalRow] = {
    val proj = UnsafeProjection.create(output, output)
    plaintextData.map(r => proj(r).copy()).toArray
  }

  override def executeBlocked(): RDD[Block] = {
    sqlContext.sparkContext.parallelize(Utils.encryptInternalRows(unsafeRows, output.map(_.dataType)))
      .mapPartitions { rowIter =>
        val serRows = rowIter.map(Utils.fieldsToRow).toArray
        Iterator(Block(Utils.createBlock(serRows, false), serRows.length))
      }
  }
}

case class EncryptExec(
    override val isOblivious: Boolean,
    child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): RDD[Block] = {
    child.execute().mapPartitions { rowIter =>
      val serRows = Utils.encryptInternalRows(rowIter.toSeq, output.map(_.dataType))
        .map(Utils.fieldsToRow).toArray
      Iterator(Block(Utils.createBlock(serRows, false), serRows.length))
    }
  }
}

case class EncryptedBlockRDDScanExec(
    output: Seq[Attribute],
    rdd: RDD[Block],
    override val isOblivious: Boolean)
  extends LeafExecNode with OpaqueOperatorExec {

  override def executeBlocked(): RDD[Block] = {
    Utils.ensureCached(rdd)
    Utils.time("force child of BlockRDDScan") { rdd.count }
    rdd
  }
}

case class Block(bytes: Array[Byte], numRows: Int) extends Serializable

trait OpaqueOperatorExec extends SparkPlan {
  def executeBlocked(): RDD[Block]

  def isOblivious: Boolean = children.exists(_.find {
    case p: OpaqueOperatorExec => p.isOblivious
    case _ => false
  }.nonEmpty)

  override def doExecute() = {
    sqlContext.sparkContext.emptyRDD
    // throw new UnsupportedOperationException("use executeBlocked")
  }

  override def executeCollect(): Array[InternalRow] = {
    executeBlocked().collect().flatMap { block =>
      Utils.decryptN(
        Utils.splitBlock(block.bytes, block.numRows, false)
          .map(serRow => Utils.parseRow(serRow)).toSeq)
        .map(rowSeq => InternalRow.fromSeq(rowSeq))
    }
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    if (n == 0) {
      return new Array[InternalRow](0)
    }

    val childRDD = executeBlocked()

    val buf = new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * n * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val left = n - buf.size
      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val sc = sqlContext.sparkContext
      val res = sc.runJob(childRDD,
        (it: Iterator[Block]) => if (it.hasNext) Some(it.next()) else None, p)

      res.foreach {
        case Some(block) =>
          buf ++= Utils.decryptN(
            Utils.splitBlock(block.bytes, block.numRows, false)
              .map(serRow => Utils.parseRow(serRow)).toSeq)
            .map(rowSeq => InternalRow.fromSeq(rowSeq))
        case None => {}
      }

      partsScanned += p.size
    }

    if (buf.size > n) {
      buf.take(n).toArray
    } else {
      buf.toArray
    }
  }
}

/**
 * An extractor that matches expressions that represent a column of the input attributes (i.e., if
 * the expression is a direct reference to the column, or it is derived solely from the column). To
 * use this extractor, create an object deriving from this trait and provide a value for `input`.
 */
trait ColumnNumberMatcher extends Serializable {
  def input: Seq[Attribute]
  def unapply(expr: Expression): Option[(Int, DataType)] =
    if (expr.references.size == 1) {
      val attr = expr.references.head
      val colNum = input.indexWhere(attr.semanticEquals(_))
      if (colNum != -1) {
        Some(Tuple2(colNum + 1, attr.dataType))
      } else {
        None
      }
    } else {
      None
    }
}

case class ObliviousProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  private object Col extends ColumnNumberMatcher {
    override def input: Seq[Attribute] = child.output
  }

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def executeBlocked() = {
    import Opcode._
    val opcode = projectList match {
      case Seq(
        Alias(Substring(Col(1, _), Literal(0, IntegerType), Literal(8, IntegerType)), _),
        Col(4, _)) =>
        OP_BD2
      case Seq(Col(4, _), Alias(Multiply(Col(2, _), Col(5, _)), _)) =>
        OP_PROJECT_PAGERANK_WEIGHT_RANK
      case Seq(Col(1, _),
        Alias(Add(
          Literal(0.15, DoubleType),
          Multiply(Literal(0.85, DoubleType), Col(2, _))), _)) =>
        OP_PROJECT_PAGERANK_APPLY_INCOMING_RANK
      case Seq(Col(2, _), Col(3, _), Col(4, _)) if child.output.size == 4 =>
        OP_PROJECT_DROP_COL1
      case Seq(Col(2, _), Col(3, _)) if child.output.size == 3 =>
        OP_PROJECT_DROP_COL1
      case Seq(Col(1, _)) if child.output.size == 2 =>
        OP_PROJECT_DROP_COL2
      case Seq(Col(1, _), Col(2, _)) if child.output.size == 3 =>
        OP_PROJECT_DROP_COL3
      case Seq(Col(2, _), Col(1, _), Col(3, _)) if child.output.size == 3 =>
        OP_PROJECT_SWAP_COL1_COL2
      case Seq(Col(1, _), Col(3, _), Col(2, _)) if child.output.size == 3 =>
        OP_PROJECT_SWAP_COL2_COL3
      case Seq(Col(4, _), Col(2, _), Col(5, _)) if child.output.size == 5 =>
        OP_PROJECT_COL4_COL2_COL5
      case Seq(Col(2, _), Col(1, _), Col(4, _)) if child.output.size == 9 =>
        OP_PROJECT_COL2_COL1_COL4
      case Seq(Col(2, _), Col(4, _)) if child.output.size == 4 =>
        OP_PROJECT_COL2_COL4
      case Seq(Col(3, _)) if child.output.size == 3 =>
        OP_PROJECT_COL3
      case Seq(Col(1, _), Alias(Add(Col(2, _), Literal(1, IntegerType)), _)) =>
        OP_PROJECT_COL2_ADD_1
      case Seq(Col(2, _), Col(5, _),
        Alias(Subtract(
          Multiply(Col(9, _), Subtract(Literal(1.0, FloatType), Col(10, _))),
          Multiply(Col(7, _), Col(8, _))), _)) =>
        OP_PROJECT_TPCH9GENERIC
      case Seq(Col(4, _), Col(2, _),
        Alias(Subtract(
          Multiply(Col(9, _), Subtract(Literal(1.0, FloatType), Col(10, _))),
          Multiply(Col(7, _), Col(8, _))), _)) =>
        OP_PROJECT_TPCH9OPAQUE
      case Seq(Col(1, _), Alias(Year(Col(2, DateType)), _)) =>
        OP_PROJECT_TPCH9_ORDER_YEAR
      case Seq(
        Alias(Multiply(Col(1, FloatType), Col(1, FloatType)), _),
        Alias(Multiply(Col(1, FloatType), Col(2, FloatType)), _),
        Alias(Multiply(Col(2, FloatType), Col(2, FloatType)), _),
        Alias(Multiply(Col(1, FloatType), Col(3, FloatType)), _),
        Alias(Multiply(Col(2, FloatType), Col(3, FloatType)), _)
      ) =>
          OP_PROJECT_LS
      case _ =>
        throw new Exception(
          s"ObliviousProjectExec: unknown project list $projectList.\n" +
            s"Input: ${child.output}.\n" +
            s"Types: ${child.output.map(_.dataType)}")
    }
    val execRDD = child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    Utils.ensureCached(execRDD)
    Utils.time("force child of project") { execRDD.count }
    RA.initRA(execRDD)

    execRDD.map { block =>
      val (enclave, eid) = Utils.initEnclave()
      val serResult = enclave.Project(eid, 0, 0, opcode.value, block.bytes, block.numRows)
      Block(serResult, block.numRows)
    }
  }
}

case class ObliviousFilterExec(instruction: Any, child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  private object Col extends ColumnNumberMatcher {
    override def input: Seq[Attribute] = child.output
  }

  override def output: Seq[Attribute] = child.output

  override def executeBlocked(): RDD[Block] = {
    import Opcode._
    instruction match {
      case opcode: Opcode => ObliviousFilterExec.filterBlocks(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), opcode)
      case condition: Expression => {
        val op = condition match {
          case IsNotNull(_) =>
            // TODO: null handling. For now we assume nothing is null, because we can't represent nulls
            // in the encrypted format. EDIT BY ERIC: No longer a safe assumption. Sometimes condition will be null,
            // as in the case of a an ObliviousFilterExec within ObliviousAggregateExec
            return child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
          case GreaterThan(Col(2, _), Literal(3, IntegerType)) =>
            OP_FILTER_COL2_GT3
          case And(
            IsNotNull(Col(2, _)),
            GreaterThan(Col(2, _), Literal(1000, IntegerType))) =>
            OP_BD1
          case And(
            And(
              And(
                IsNotNull(Col(3, _)),
                GreaterThanOrEqual(Cast(Col(3, _), StringType), Literal(start, StringType))),
              LessThanOrEqual(Cast(Col(3, _), StringType), Literal(end, StringType))),
            IsNotNull(Col(2, _)))
              if start == UTF8String.fromString("1980-01-01")
              && end == UTF8String.fromString("1980-04-01") =>
            OP_FILTER_COL3_DATE_BETWEEN_1980_01_01_AND_1980_04_01
          case Contains(Col(2, _), Literal(maroon, StringType))
              if maroon == UTF8String.fromString("maroon") =>
            OP_FILTER_COL2_CONTAINS_MAROON
          case GreaterThan(Col(4, _), Literal(25, _)) =>
            OP_FILTER_COL4_GT_25
          case GreaterThan(Col(4, _), Literal(40, _)) =>
            OP_FILTER_COL4_GT_40
          case GreaterThan(Col(4, _), Literal(45, _)) =>
            OP_FILTER_COL4_GT_45
          case _ =>
            throw new Exception(
              s"ObliviousFilterExec: unknown condition $condition.\n" +
                s"Input: ${child.output}.\n" +
                s"Types: ${child.output.map(_.dataType)}")
      }
        ObliviousFilterExec.filterBlocks(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), op)
      }
      case _ => throw new IllegalArgumentException("Must have an opcode or expression passed in as the first argument.")
    }
  }
}

object ObliviousFilterExec extends java.io.Serializable {
  def filterBlocks(execRDD: RDD[Block], opcode: Opcode): RDD[Block] = {
    Utils.ensureCached(execRDD)
    RA.initRA(execRDD)
    execRDD.map { block =>
      val (enclave, eid) = Utils.initEnclave()
      val numOutputRows = new MutableInteger
      val filtered = enclave.Filter(
        eid, 0, 0, opcode.value, block.bytes, block.numRows, numOutputRows)
      val returned_block = Block(filtered, numOutputRows.value)
      returned_block
    }
  }
}

case class ObliviousPermuteExec(child: SparkPlan) extends UnaryExecNode with OpaqueOperatorExec {
  override def output: Seq[Attribute] = child.output

  override def executeBlocked() = {
    val execRDD = child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    Utils.ensureCached(execRDD)
    RA.initRA(execRDD)

    val rowsWithRandomIds = execRDD.map { block =>
      val (enclave, eid) = Utils.initEnclave()
      val serResult = enclave.Project(
        eid, 0, 0, OP_PROJECT_ADD_RANDOM_ID.value, block.bytes, block.numRows)
      Block(serResult, block.numRows)
    }
    
    ObliviousSortExec.sortBlocks(rowsWithRandomIds, OP_SORT_COL1).map { block =>
      val (enclave, eid) = Utils.initEnclave()
      val serResult = enclave.Project(
        eid, 0, 0, OP_PROJECT_DROP_COL1.value, block.bytes, block.numRows)
      Block(serResult, block.numRows)
    }
  }
}


case class ObliviousAggregateExec(
    groupingExpressions: Seq[Expression],
    aggExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  import Utils.time

  private object Col extends ColumnNumberMatcher {
    override def input: Seq[Attribute] = child.output
  }

  override def producedAttributes: AttributeSet =
    AttributeSet(aggExpressions) -- AttributeSet(groupingExpressions)

  override def output: Seq[Attribute] = aggExpressions.map(_.toAttribute)

  def getOpcodes(): 
    (Opcode, Opcode, Opcode, Opcode) = {
      import Opcode._

      val (aggStep1Opcode, aggStep2Opcode, aggDummySortOpcode, aggDummyFilterOpcode) =
        (groupingExpressions, aggExpressions) match {
          case (Seq(Col(1, _)), Seq(Col(1, _),
            Alias(AggregateExpression(Sum(Col(2, IntegerType)), _, false, _), _))) =>
            (OP_GROUPBY_COL1_SUM_COL2_INT_STEP1,
              OP_GROUPBY_COL1_SUM_COL2_INT_STEP2,
              OP_SORT_COL2_IS_DUMMY_COL1,
              OP_FILTER_NOT_DUMMY)

          case (Seq(Col(1, _)), Seq(Col(1, _),
            Alias(AggregateExpression(Sum(Col(2, FloatType)), _, false, _), _))) =>
            (OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP1,
              OP_GROUPBY_COL1_SUM_COL2_FLOAT_STEP2,
              OP_SORT_COL2_IS_DUMMY_COL1,
              OP_FILTER_NOT_DUMMY)

          case (Seq(Col(1, _)), Seq(Col(1, _),
            Alias(AggregateExpression(Min(Col(2, IntegerType)), _, false, _), _))) =>
            (OP_GROUPBY_COL1_MIN_COL2_INT_STEP1,
              OP_GROUPBY_COL1_MIN_COL2_INT_STEP2,
              OP_SORT_COL2_IS_DUMMY_COL1,
              OP_FILTER_NOT_DUMMY)

          case (Seq(Col(2, _)), Seq(Col(2, _),
            Alias(AggregateExpression(Sum(Col(3, IntegerType)), _, false, _), _))) =>
            (OP_GROUPBY_COL2_SUM_COL3_INT_STEP1,
              OP_GROUPBY_COL2_SUM_COL3_INT_STEP2,
              OP_SORT_COL2_IS_DUMMY_COL1,
              OP_FILTER_NOT_DUMMY)

          case (Seq(Col(1, _)), Seq(Col(1, _),
            Alias(AggregateExpression(Sum(Col(3, FloatType)), _, false, _), _),
            Alias(AggregateExpression(Average(Col(2, IntegerType)), _, false, _), _))) =>
            (OP_GROUPBY_COL1_SUM_COL3_FLOAT_AVG_COL2_INT_STEP1,
              OP_GROUPBY_COL1_SUM_COL3_FLOAT_AVG_COL2_INT_STEP2,
              OP_SORT_COL2_IS_DUMMY_COL1,
              OP_FILTER_NOT_DUMMY)

          case (Seq(Col(1, _), Col(2, _)), Seq(Col(1, _), Col(2, _),
            Alias(AggregateExpression(Sum(Col(3, FloatType)), _, false, _), _))) =>
            (OP_GROUPBY_COL1_COL2_SUM_COL3_FLOAT_STEP1,
              OP_GROUPBY_COL1_COL2_SUM_COL3_FLOAT_STEP2,
              OP_SORT_COL2_IS_DUMMY_COL1,
              OP_FILTER_NOT_DUMMY)

          case (Seq(), Seq(
            Alias(AggregateExpression(Sum(Col(1, IntegerType)), _, false, _), _))) =>
            (OP_SUM_COL1_INTEGER,
              OP_SUM_COL1_INTEGER,
              OP_SORT_COL2_IS_DUMMY_COL1,
              OP_FILTER_NOT_DUMMY)

          case (Seq(), Seq(
            Alias(AggregateExpression(Sum(Col(1, FloatType)), _, false, _), _),
            Alias(AggregateExpression(Sum(Col(2, FloatType)), _, false, _), _),
            Alias(AggregateExpression(Sum(Col(3, FloatType)), _, false, _), _),
            Alias(AggregateExpression(Sum(Col(4, FloatType)), _, false, _), _),
            Alias(AggregateExpression(Sum(Col(5, FloatType)), _, false, _), _)
          )) =>
            (OP_SUM_LS,
              OP_SUM_LS_2,
              OP_SORT_COL2_IS_DUMMY_COL1,
              OP_FILTER_NOT_DUMMY)

        case _ =>
          throw new Exception(
            s"ObliviousAggregateExec: unknown grouping expressions $groupingExpressions, " +
              s"aggregation expressions $aggExpressions.\n" +
              s"Input: ${child.output}.\n" +
              s"Types: ${child.output.map(_.dataType)}")
        }
      (aggStep1Opcode, aggStep2Opcode, aggDummySortOpcode, aggDummyFilterOpcode)
    }

  override def executeBlocked(): RDD[Block] = {
    import Opcode._
    val (aggStep1Opcode, aggStep2Opcode, aggDummySortOpcode, aggDummyFilterOpcode) = getOpcodes()
    val childRDD = child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    Utils.ensureCached(childRDD)

    if (childRDD.map(_.numRows).sum == 0) {
       return childRDD
    }

    if (aggStep1Opcode == OP_SUM_COL1_INTEGER || aggStep1Opcode == OP_SUM_LS) {
      RA.initRA(childRDD)
      // Do local global aggregates
      val aggregates = childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        val numOutputRows = new MutableInteger
        val resultBytes = enclave.GlobalAggregate(
          eid, 0, 0, aggStep1Opcode.value, block.bytes, block.numRows, numOutputRows)
        resultBytes
      }

      Utils.ensureCached(aggregates)
      val (enclave, eid) = Utils.initEnclave()
      val aggregatesCollected = aggregates.collect
      // Collect and run GlobalAggregate again
      val finalOutputRows = new MutableInteger
      val result = enclave.GlobalAggregate(eid, 0, 0, aggStep2Opcode.value,
        Utils.concatByteArrays(aggregatesCollected), aggregatesCollected.length, finalOutputRows)
      assert(finalOutputRows.value == 1)

      var a = new Array[Block](1)
      a(0) = Block(result, finalOutputRows.value)

      return sparkContext.parallelize(a, 1)
    }

    time("aggregate - force child") { childRDD.count }
    // Process boundaries
    RA.initRA(childRDD)
    val boundaries = childRDD.map { block =>
      val (enclave, eid) = Utils.initEnclave()
      val boundary = enclave.AggregateStep1(
        eid, 0, 0, aggStep1Opcode.value, block.bytes, block.numRows)
      // enclave.StopEnclave(eid)
      boundary
    }

    val boundariesCollected = time("aggregate - step 1") { boundaries.collect }
    if (boundariesCollected.forall(_.isEmpty)) {
      return sqlContext.sparkContext.emptyRDD[Block]
    }
    val (enclave, eid) = Utils.initEnclave()
    val processedBoundariesConcat = time("aggregate - ProcessBoundary") {
      enclave.ProcessBoundary(
        eid, aggStep1Opcode.value,
        Utils.concatByteArrays(boundariesCollected), boundariesCollected.length)
    }

    // Send processed boundaries to partitions and generate a mix of partial and final aggregates
    val processedBoundaries = Utils.splitBytes(processedBoundariesConcat, boundariesCollected.length)
    val processedBoundariesRDD = sparkContext.parallelize(processedBoundaries, childRDD.partitions.length)
    val partialAggregates = childRDD.zipPartitions(processedBoundariesRDD) {
      (blockIter, boundaryIter) =>
        val blockArray = blockIter.toArray
        assert(blockArray.length == 1)
        val block = blockArray.head
        val boundaryArray = boundaryIter.toArray
        assert(boundaryArray.length == 1)
        val boundaryRecord = boundaryArray.head
        val (enclave, eid) = Utils.initEnclave()
        assert(block.numRows > 0)
        val partialAgg = enclave.AggregateStep2(
          eid, 0, 0, aggStep2Opcode.value, block.bytes, block.numRows, boundaryRecord)
        assert(partialAgg.nonEmpty,
          s"enclave.AggregateStep2($eid, 0, 0, $aggStep2Opcode, ${block.bytes.length}, ${block.numRows}, ${boundaryRecord.length}) returned empty result")
        Iterator(Block(partialAgg, block.numRows))
    }

    Utils.ensureCached(partialAggregates)
    time("aggregate - step 2") { partialAggregates.count }
    partialAggregates
  }
}

case class EncryptedAggregateExec(
    groupingExpressions: Seq[Expression],
    aggExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  import Utils.time

  private object Col extends ColumnNumberMatcher {
    override def input: Seq[Attribute] = child.output
  }

  override def producedAttributes: AttributeSet =
    AttributeSet(aggExpressions) -- AttributeSet(groupingExpressions)

  override def output: Seq[Attribute] = aggExpressions.map(_.toAttribute)

  override def executeBlocked(): RDD[Block] = {
    import Opcode._
    val aggOpcode =
      (groupingExpressions, aggExpressions) match {
        case (Seq(Col(1, _)), Seq(Col(1, _),
          Alias(AggregateExpression(Sum(Col(2, IntegerType)), _, false, _), _))) =>
          OP_GROUPBY_COL1_SUM_COL2_INT

        case (Seq(Col(1, _)), Seq(Col(1, _),
          Alias(AggregateExpression(Sum(Col(2, FloatType)), _, false, _), _))) =>
          OP_GROUPBY_COL1_SUM_COL2_FLOAT

        case (Seq(Col(1, _)), Seq(Col(1, _),
          Alias(AggregateExpression(Min(Col(2, IntegerType)), _, false, _), _))) =>
          OP_GROUPBY_COL1_MIN_COL2_INT

        case (Seq(Col(1, _)), Seq(Col(1, _),
          Alias(AggregateExpression(Sum(Col(3, FloatType)), _, false, _), _),
          Alias(AggregateExpression(Average(Col(2, IntegerType)), _, false, _), _))) =>
          OP_GROUPBY_COL1_SUM_COL3_FLOAT_AVG_COL2_INT

        case (Seq(), Seq(
          Alias(AggregateExpression(Sum(Col(1, IntegerType)), _, false, _), _))) =>
          OP_SUM_COL1_INTEGER

        case (Seq(), Seq(
          Alias(AggregateExpression(Sum(Col(1, FloatType)), _, false, _), _),
          Alias(AggregateExpression(Sum(Col(2, FloatType)), _, false, _), _),
          Alias(AggregateExpression(Sum(Col(3, FloatType)), _, false, _), _),
          Alias(AggregateExpression(Sum(Col(4, FloatType)), _, false, _), _),
          Alias(AggregateExpression(Sum(Col(5, FloatType)), _, false, _), _)
        )) =>
          OP_SUM_LS

      case _ =>
        throw new Exception(
          s"EncryptedAggregateExec: unknown grouping expressions $groupingExpressions, " +
            s"aggregation expressions $aggExpressions.\n" +
            s"Input: ${child.output}.\n" +
            s"Types: ${child.output.map(_.dataType)}")
      }

    val childRDD = child.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    Utils.ensureCached(childRDD)

    if (aggOpcode == OP_SUM_COL1_INTEGER || aggOpcode == OP_SUM_LS) {
      RA.initRA(childRDD)
      // Do local global aggregates
      val aggregates = childRDD.map { block =>
        val (enclave, eid) = Utils.initEnclave()
        val numOutputRows = new MutableInteger
        val resultBytes = enclave.GlobalAggregate(
          eid, 0, 0, aggOpcode.value, block.bytes, block.numRows, numOutputRows)
        resultBytes
      }

      var aggOpcode2 = aggOpcode
      if (aggOpcode == OP_SUM_LS) {
        aggOpcode2 = OP_SUM_LS_2
      }

      Utils.ensureCached(aggregates)
      val (enclave, eid) = Utils.initEnclave()
      val aggregatesCollected = aggregates.collect
      // Collect and run GlobalAggregate again
      val finalOutputRows = new MutableInteger
      val result = enclave.GlobalAggregate(eid, 0, 0, aggOpcode2.value,
        Utils.concatByteArrays(aggregatesCollected), aggregatesCollected.length, finalOutputRows)
      assert(finalOutputRows.value == 1)

      var a = new Array[Block](1)
      a(0) = Block(result, finalOutputRows.value)

      return sparkContext.parallelize(a, 1)
    }

    time("aggregate - force child") { childRDD.count }
    RA.initRA(childRDD)
    // Process boundaries
    val aggregates = childRDD.map { block =>
      val (enclave, eid) = Utils.initEnclave()
      val numOutputRows = new MutableInteger
      val resultBytes = enclave.NonObliviousAggregate(
        eid, 0, 0, aggOpcode.value, block.bytes, block.numRows, numOutputRows)
      Block(resultBytes, numOutputRows.value)
    }
    Utils.ensureCached(aggregates)
    aggregates.count
    aggregates
  }
}

case class ObliviousSortMergeJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    condition: Option[Expression])
  extends BinaryExecNode with OpaqueOperatorExec {

  import Utils.time

  override def output: Seq[Attribute] =
    left.output ++ right.output

  override def executeBlocked() = {
    import Opcode._
    val (joinOpcode, dummySortOpcode, dummyFilterOpcode) =
      OpaqueJoinUtils.getOpcodes(left.output, right.output, leftKeys, rightKeys, condition)

    val leftRDD = left.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    val rightRDD = right.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    Utils.ensureCached(leftRDD)
    time("Force left child of ObliviousSortMergeJoinExec") { leftRDD.count }
    Utils.ensureCached(rightRDD)
    time("Force right child of ObliviousSortMergeJoinExec") { rightRDD.count }

    RA.initRA(leftRDD)

    val processed = leftRDD.zipPartitions(rightRDD) { (leftBlockIter, rightBlockIter) =>
      val (enclave, eid) = Utils.initEnclave()

      val leftBlockArray = leftBlockIter.toArray
      assert(leftBlockArray.length == 1)
      val leftBlock = leftBlockArray.head

      val rightBlockArray = rightBlockIter.toArray
      assert(rightBlockArray.length == 1)
      val rightBlock = rightBlockArray.head

      val processed = enclave.JoinSortPreprocess(
        eid, 0, 0, joinOpcode.value, leftBlock.bytes, leftBlock.numRows,
        rightBlock.bytes, rightBlock.numRows)

      Iterator(Block(processed, leftBlock.numRows + rightBlock.numRows))
    }
    Utils.ensureCached(processed)
    time("join - preprocess") { processed.count }

    val sorted = time("join - sort") {
      val result = ObliviousSortExec.sortBlocks(processed, joinOpcode)
      Utils.ensureCached(result)
      result.count
      result
    }

    val lastPrimaryRows = sorted.map { block =>
      val (enclave, eid) = Utils.initEnclave()
      enclave.ScanCollectLastPrimary(eid, joinOpcode.value, block.bytes, block.numRows)
    }

    val lastPrimaryRowsCollected = time("join - collect last primary") { lastPrimaryRows.collect }
    val (enclave, eid) = Utils.initEnclave()
    val processedJoinRows = time("join - process boundary") {
      enclave.ProcessJoinBoundary(
        eid, joinOpcode.value, Utils.concatByteArrays(lastPrimaryRowsCollected),
        lastPrimaryRowsCollected.length)
    }

    val processedJoinRowsSplit = Utils.readVerifiedRows(processedJoinRows).toArray
    assert(processedJoinRowsSplit.length == sorted.partitions.length)
    val processedJoinRowsRDD =
      sparkContext.parallelize(processedJoinRowsSplit, sorted.partitions.length)

    val joined = sorted.zipPartitions(processedJoinRowsRDD) { (blockIter, joinRowIter) =>
      val block = blockIter.next()
      assert(!blockIter.hasNext)
      val joinRow = joinRowIter.next()
      assert(!joinRowIter.hasNext)
      val (enclave, eid) = Utils.initEnclave()
      val joined = enclave.SortMergeJoin(
        eid, 0, 0, joinOpcode.value, block.bytes, block.numRows, joinRow)
      Iterator(Block(joined, block.numRows))
    }
    Utils.ensureCached(joined)
    time("join - sort merge join") { joined.count }

    val joinedWithRandomIds = joined.map { block =>
      val (enclave, eid) = Utils.initEnclave()
      val serResult = enclave.Project(
        eid, 0, 0, OP_PROJECT_ADD_RANDOM_ID.value, block.bytes, block.numRows)
      Block(serResult, block.numRows)
    }
    val permuted = ObliviousSortExec.sortBlocks(joinedWithRandomIds, OP_SORT_COL1).map { block =>
      val (enclave, eid) = Utils.initEnclave()
      val serResult = enclave.Project(
        eid, 0, 0, OP_PROJECT_DROP_COL1.value, block.bytes, block.numRows)
      Block(serResult, block.numRows)
    }

    val nonDummy = permuted.map { block =>
      val (enclave, eid) = Utils.initEnclave()
      val numOutputRows = new MutableInteger
      val filtered = enclave.Filter(
        eid, 0, 0, dummyFilterOpcode.value, block.bytes, block.numRows, numOutputRows)
      Block(filtered, numOutputRows.value)
    }
    Utils.ensureCached(nonDummy)
    time("join - filter dummies") { nonDummy.count }
    nonDummy
  }
}

private object OpaqueJoinUtils {
  /** Given the join information, return (joinOpcode, dummySortOpcode, dummyFilterOpcode). */
  def getOpcodes(
      leftOutput: Seq[Attribute], rightOutput: Seq[Attribute], leftKeys: Seq[Expression],
      rightKeys: Seq[Expression], condition: Option[Expression])
      : (Opcode, Opcode, Opcode) = {

    import Opcode._

    object LeftCol extends ColumnNumberMatcher {
      override def input: Seq[Attribute] = leftOutput
    }
    object RightCol extends ColumnNumberMatcher {
      override def input: Seq[Attribute] = rightOutput
    }

    val info = (leftOutput.map(_.dataType), rightOutput.map(_.dataType),
      leftKeys, rightKeys, condition)
    val (joinOpcode, dummySortOpcode, dummyFilterOpcode) = info match {
      case (Seq(StringType, IntegerType), Seq(StringType, StringType, FloatType),
        Seq(LeftCol(1, _)), Seq(RightCol(1, _)), None) =>
        (OP_JOIN_COL1, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, StringType, IntegerType), Seq(IntegerType, StringType, IntegerType),
        Seq(LeftCol(2, _)), Seq(RightCol(2, _)), None) =>
        (OP_JOIN_COL2, OP_SORT_COL4_IS_DUMMY_COL2, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, FloatType), Seq(IntegerType, IntegerType, FloatType),
        Seq(LeftCol(1, _)), Seq(RightCol(1, _)), None) =>
        (OP_JOIN_PAGERANK, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, StringType),
        Seq(IntegerType, IntegerType, IntegerType, IntegerType, IntegerType, FloatType,
          IntegerType, FloatType, FloatType),
        Seq(LeftCol(1, _)), Seq(RightCol(2, _)), None) =>
        (OP_JOIN_TPCH9GENERIC_NATION, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, IntegerType),
        Seq(IntegerType, IntegerType, IntegerType, IntegerType, FloatType, IntegerType,
          FloatType, FloatType),
        Seq(LeftCol(1, _)), Seq(RightCol(4, _)), None) =>
        (OP_JOIN_TPCH9GENERIC_SUPPLIER, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, IntegerType),
        Seq(IntegerType, IntegerType, FloatType, IntegerType, IntegerType, FloatType, FloatType),
        Seq(LeftCol(1, _)), Seq(RightCol(4, _)), None) =>
        (OP_JOIN_TPCH9GENERIC_ORDERS, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, IntegerType, FloatType),
        Seq(IntegerType, IntegerType, IntegerType, IntegerType, FloatType, FloatType),
        Seq(LeftCol(2, _), LeftCol(1, _)), Seq(RightCol(3, _), RightCol(1, _)), None) =>
        (OP_JOIN_TPCH9GENERIC_PARTSUPP, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType),
        Seq(IntegerType, IntegerType, IntegerType, IntegerType, FloatType, FloatType),
        Seq(LeftCol(1, _)), Seq(RightCol(2, _)), None) =>
        (OP_JOIN_TPCH9GENERIC_PART_LINEITEM, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, IntegerType),
        Seq(IntegerType, StringType, IntegerType, IntegerType, FloatType, IntegerType,
          IntegerType, FloatType, FloatType),
        Seq(LeftCol(1, _)), Seq(RightCol(6, _)), None) =>
        (OP_JOIN_TPCH9OPAQUE_ORDERS, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, StringType, IntegerType, IntegerType, FloatType),
        Seq(IntegerType, IntegerType, IntegerType, IntegerType, FloatType, FloatType),
        Seq(LeftCol(3, _), LeftCol(4, _)), Seq(RightCol(3, _), RightCol(2, _)), None) =>
        (OP_JOIN_TPCH9OPAQUE_LINEITEM, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, StringType),
        Seq(IntegerType, IntegerType, IntegerType, FloatType),
        Seq(LeftCol(1, _)), Seq(RightCol(2, _)), None) =>
        (OP_JOIN_TPCH9OPAQUE_NATION, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, IntegerType),
        Seq(IntegerType, IntegerType, FloatType),
        Seq(LeftCol(1, _)), Seq(RightCol(2, _)), None) =>
        (OP_JOIN_TPCH9OPAQUE_SUPPLIER, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType),
        Seq(IntegerType, IntegerType, FloatType),
        Seq(LeftCol(1, _)), Seq(RightCol(1, _)), None) =>
        (OP_JOIN_TPCH9OPAQUE_PART_PARTSUPP, OP_SORT_COL3_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(StringType, IntegerType),
        Seq(StringType, IntegerType, StringType, IntegerType, StringType, StringType),
        Seq(LeftCol(1, _)), Seq(RightCol(1, _)), None) =>
        (OP_JOIN_DISEASEDEFAULT_TREATMENT, OP_SORT_COL2_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(StringType, IntegerType, StringType),
        Seq(IntegerType, StringType, StringType),
        Seq(LeftCol(1, _)), Seq(RightCol(2, _)), None) =>
        (OP_JOIN_DISEASEDEFAULT_PATIENT, OP_SORT_COL2_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(StringType, IntegerType, StringType, StringType, IntegerType),
        Seq(IntegerType, StringType, StringType),
        Seq(LeftCol(1, _)), Seq(RightCol(2, _)), None) =>
        (OP_JOIN_DISEASEOPAQUE_PATIENT, OP_SORT_COL2_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(StringType, IntegerType, StringType),
        Seq(StringType, IntegerType),
        Seq(LeftCol(1, _)), Seq(RightCol(1, _)), None) =>
        (OP_JOIN_DISEASEOPAQUE_TREATMENT, OP_SORT_COL2_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, StringType),
        Seq(StringType, IntegerType, StringType, IntegerType, StringType, StringType),
        Seq(LeftCol(1, _)), Seq(RightCol(2, _)), None) =>
        (OP_JOIN_GENEDEFAULT_GENE, OP_SORT_COL2_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, StringType),
        Seq(StringType, IntegerType, StringType),
        Seq(LeftCol(1, _)), Seq(RightCol(2, _)), None) =>
        (OP_JOIN_GENEOPAQUE_GENE, OP_SORT_COL2_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case (Seq(IntegerType, StringType, StringType, IntegerType, StringType),
        Seq(IntegerType, StringType, StringType),
        Seq(LeftCol(3, _)), Seq(RightCol(2, _)), None) =>
        (OP_JOIN_GENEOPAQUE_PATIENT, OP_SORT_COL2_IS_DUMMY_COL1, OP_FILTER_NOT_DUMMY)

      case _ =>
        throw new Exception(
          s"OpaqueJoinUtils: unknown left join keys $leftKeys, " +
            s"right join keys $rightKeys, condition $condition.\n" +
            s"Input: left $leftOutput, right $rightOutput.\n" +
            s"Types: left ${leftOutput.map(_.dataType)}, right ${rightOutput.map(_.dataType)}")
    }
    (joinOpcode, dummySortOpcode, dummyFilterOpcode)
  }
}

case class EncryptedSortMergeJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    condition: Option[Expression])
  extends BinaryExecNode with OpaqueOperatorExec {

  import Utils.time

  override def output: Seq[Attribute] =
    left.output ++ right.output

  override def executeBlocked() = {
    val (joinOpcode, dummySortOpcode, dummyFilterOpcode) =
      OpaqueJoinUtils.getOpcodes(left.output, right.output, leftKeys, rightKeys, condition)

    val leftRDD = left.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    val rightRDD = right.asInstanceOf[OpaqueOperatorExec].executeBlocked()
    Utils.ensureCached(leftRDD)
    time("Force left child of EncryptedSortMergeJoinExec") { leftRDD.count }
    Utils.ensureCached(rightRDD)
    time("Force right child of EncryptedSortMergeJoinExec") { rightRDD.count }

    RA.initRA(leftRDD)

    val processed = leftRDD.zipPartitions(rightRDD) { (leftBlockIter, rightBlockIter) =>
      val (enclave, eid) = Utils.initEnclave()

      val leftBlockArray = leftBlockIter.toArray
      assert(leftBlockArray.length == 1)
      val leftBlock = leftBlockArray.head

      val rightBlockArray = rightBlockIter.toArray
      assert(rightBlockArray.length == 1)
      val rightBlock = rightBlockArray.head

      val processed = enclave.JoinSortPreprocess(
        eid, 0, 0, joinOpcode.value, leftBlock.bytes, leftBlock.numRows,
        rightBlock.bytes, rightBlock.numRows)

      Iterator(Block(processed, leftBlock.numRows + rightBlock.numRows))
    }
    Utils.ensureCached(processed)
    time("join - preprocess") { processed.count }

    val sorted = time("join - sort") {
      val result = EncryptedSortExec.sort(processed, joinOpcode)
      Utils.ensureCached(result)
      result.count
      result
    }

    val joined = sorted.map { block =>
      val (enclave, eid) = Utils.initEnclave()
      val numOutputRows = new MutableInteger
      val joined = enclave.NonObliviousSortMergeJoin(
        eid, 0, 0, joinOpcode.value, block.bytes, block.numRows, numOutputRows)
      Block(joined, numOutputRows.value)
    }
    Utils.ensureCached(joined)
    time("join - sort merge join") { joined.count }

    joined
  }
}
