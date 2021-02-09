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

import ch.jodersky.jni.nativeLoader

@nativeLoader("enclave_jni")
class SGXEnclave extends java.io.Serializable {
  @native def StartEnclave(libraryPath: String): Long
  @native def StopEnclave(enclaveId: Long): Unit

  @native def Project(eid: Long, projectList: Array[Byte], input: Array[Byte]): Array[Byte]

  @native def Filter(eid: Long, condition: Array[Byte], input: Array[Byte]): Array[Byte]

  @native def Encrypt(eid: Long, plaintext: Array[Byte]): Array[Byte]
  @native def Decrypt(eid: Long, ciphertext: Array[Byte]): Array[Byte]

  @native def Sample(eid: Long, input: Array[Byte]): Array[Byte]
  @native def FindRangeBounds(
    eid: Long, order: Array[Byte], numPartitions: Int, input: Array[Byte]): Array[Byte]
  @native def PartitionForSort(
    eid: Long, order: Array[Byte], numPartitions: Int, input: Array[Byte],
    boundaries: Array[Byte]): Array[Array[Byte]]
  @native def ExternalSort(eid: Long, order: Array[Byte], input: Array[Byte]): Array[Byte]

  @native def ScanCollectLastPrimary(
    eid: Long, joinExpr: Array[Byte], input: Array[Byte]): Array[Byte]
  @native def NonObliviousSortMergeJoin(
    eid: Long, joinExpr: Array[Byte], input: Array[Byte], joinRow: Array[Byte]): Array[Byte]

  @native def NonObliviousAggregate(
    eid: Long, aggOp: Array[Byte], inputRows: Array[Byte], isPartial: Boolean): (Array[Byte])

  @native def CountRowsPerPartition(
    eid: Long, inputRows: Array[Byte]): Array[Byte]
  @native def ComputeNumRowsPerPartition(
    eid: Long, limit: Int, inputRows: Array[Byte]): Array[Byte]
  @native def LocalLimit(
    eid: Long, limit: Int, inputRows: Array[Byte]): Array[Byte]
  @native def LimitReturnRows(
    eid: Long, partitionID: Long, limits: Array[Byte], inputRows: Array[Byte]): Array[Byte]

  // Remote attestation, enclave side
  @native def GenerateReport(eid: Long): Array[Byte]
  @native def FinishAttestation(eid: Long, attResultInput: Array[Byte]): Unit
}
