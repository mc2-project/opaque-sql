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


package org.apache.spark.sql

class MutableInteger(var value: Int = 0)

class SGXEnclave extends java.io.Serializable {
  @native def StartEnclave(): Long
  @native def StopEnclave(enclave_id: Long)

  @native def Project(
    eid: Long,
    index: Int,
    numPart: Int,
    op_code: Int,
    input_rows: Array[Byte],
    num_rows: Int
  ): Array[Byte]

  @native def Filter(
    enclave_id: Long, index: Int, numPart: Int,
    op_code: Int, rows: Array[Byte], num_rows: Int,
    num_output_rows: MutableInteger): Array[Byte]

  @native def Encrypt(
    enclave_id: Long, plaintext: Array[Byte]): Array[Byte]
  @native def Decrypt(
    enclave_id: Long, ciphertext: Array[Byte]): Array[Byte]

  @native def Test(eid: Long)

  @native def ObliviousSort(
    enclave_id: Long,
    op_code: Int,
    input: Array[Byte],
    offset: Int,
    num_items: Int
  ): Array[Byte]

  @native def Sample(
    enclave_id: Long,
    index: Int,
    numPart: Int,
    op_code: Int,
    input: Array[Byte],
    num_rows: Int,
    num_output_rows: MutableInteger
  ): Array[Byte]

  @native def FindRangeBounds(
    enclave_id: Long,
    op_code: Int,
    num_partitions: Int,
    input: Array[Byte],
    num_rows: Int
  ): Array[Byte]

  @native def PartitionForSort(
    enclave_id: Long,
    index: Int,
    num_part: Int,
    op_code: Int,
    num_partitions: Int,
    input: Array[Byte],
    num_rows: Int,
    boundary_rows: Array[Byte],
    offsets: Array[Int],
    rows_per_partition: Array[Int]
  ): Array[Byte]

  @native def ExternalSort(
    enclave_id: Long,
    index: Int,
    numPart: Int,
    op_code: Int,
    input: Array[Byte],
    num_items: Int
  ): Array[Byte]

  @native def AggregateStep1(
    eid: Long,
    index: Int, numPart: Int,
    op_code: Int,
    input_rows: Array[Byte],
    num_rows: Int): Array[Byte]

  @native def ProcessBoundary(
    eid: Long,
    op_code: Int,
    agg_rows: Array[Byte],
    num_agg_rows: Int
  ): Array[Byte]

  @native def AggregateStep2(
    eid: Long,
    index: Int, numPart: Int,
    op_code: Int,
    input_rows: Array[Byte],
    num_rows: Int,
    boundary_info_row: Array[Byte]): Array[Byte]

  @native def FinalAggregation(
    eid: Long,
    op_code: Int,
    rows: Array[Byte],
    num_rows: Int
  ): Array[Byte]

  @native def NonObliviousAggregate(
    eid: Long,
    index: Int,
    numPart: Int,
    op_code: Int,
    rows: Array[Byte],
    num_rows: Int,
    num_output_rows: MutableInteger
  ): Array[Byte]

  @native def JoinSortPreprocess(
    eid: Long,
    index: Int,
    numPart: Int,
    op_code: Int,
    primary_rows: Array[Byte],
    num_primary_rows: Int,
    foreign_rows: Array[Byte],
    num_foreign_rows: Int
  ): Array[Byte]

  @native def ScanCollectLastPrimary(
    eid: Long,
    op_code: Int,
    input_rows: Array[Byte],
    num_rows: Int
  ): Array[Byte]

  @native def ProcessJoinBoundary(
    eid: Long,
    op_code: Int,
    input_rows: Array[Byte],
    num_rows: Int
  ): Array[Byte]

  @native def SortMergeJoin(
    eid: Long,
    index: Int,
    numPart: Int,
    op_code: Int,
    input_rows: Array[Byte],
    num_rows: Int,
    join_row: Array[Byte]
  ): Array[Byte]

  @native def NonObliviousSortMergeJoin(
    eid: Long,
    index: Int,
    numPart: Int,
    op_code: Int,
    rows: Array[Byte],
    num_rows: Int,
    num_output_rows: MutableInteger
  ): Array[Byte]

  @native def EncryptAttribute(
    eid: Long,
    plaintext: Array[Byte]
  ): Array[Byte]

  @native def CreateBlock(
      eid: Long, rows: Array[Byte], numRows: Int, rowsAreJoinRows: Boolean): Array[Byte]

  @native def SplitBlock(
      eid: Long, block: Array[Byte], numRows: Int, rowsAreJoinRows: Boolean): Array[Byte]

  @native def EnclaveColumnSort(
    eid: Long,
    index: Int, numPart: Int,
    op_code: Int, round: Int, input: Array[Byte], r: Int, s: Int, column: Int, current_part: Int, num_part: Int, offset: Int) : Array[Byte]

  @native def CountNumRows(
    eid: Long, input_rows: Array[Byte]) : Int

  @native def ColumnSortFilter(
    eid: Long, op_code: Int, input_rows: Array[Byte], column: Int, offset: Int, num_rows: Int, ret_num_rows: MutableInteger): Array[Byte]
}
