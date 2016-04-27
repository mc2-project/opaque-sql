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

class SGXEnclave extends java.io.Serializable {
  @native def StartEnclave(): Long
  @native def StopEnclave(enclave_id: Long)

  @native def Filter(
    enclave_id: Long, op: Int, value1: Array[Byte]): Boolean

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

  // returns an encrypted random ID
  @native def RandomID(eid: Long): Array[Byte]

  @native def Aggregate(
    eid: Long,
    op_code: Int,
    input_rows: Array[Byte],
    num_rows: Int,
    agg_row: Array[Byte]): Array[Byte]

  @native def ProcessBoundary(
    eid: Long,
    op_code: Int,
    agg_rows: Array[Byte],
    num_agg_rows: Int
  ): Array[Byte]

  @native def FinalAggregation(
    eid: Long,
    op_code: Int,
    rows: Array[Byte],
    num_rows: Int
  ): Array[Byte]

  @native def JoinSortPreprocess(
    eid: Long,
    op_code: Int,
    enc_table_id: Array[Byte],
    input_rows: Array[Byte],
    num_rows: Int
  ): Array[Byte]

  @native def ScanCollectLastPrimary(
    eid: Long,
    op_code: Int,
    input_rows: Array[Byte],
    num_rows: Int
  ): Array[Byte]

  @native def SortMergeJoin(
    eid: Long,
    op_code: Int,
    input_rows: Array[Byte],
    num_rows: Int,
    join_row: Array[Byte]
  ): Array[Byte]

  @native def EncryptAttribute(
    eid: Long,
    plaintext: Array[Byte]
  ): Array[Byte];

}
