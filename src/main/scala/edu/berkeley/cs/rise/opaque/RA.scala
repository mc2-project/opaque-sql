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

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

import edu.berkeley.cs.rise.opaque.execution.SP

object RA extends Logging {
  def initRA(sc: SparkContext): Unit = {

    val rdd = sc.makeRDD(Seq.fill(sc.defaultParallelism) { () })

    val intelCert = Utils.findResource("AttestationReportSigningCACert.pem")

    // FIXME: hardcoded path
    val userCert = scala.io.Source.fromFile("/home/chester/opaque/user1.crt").mkString

    // val userCert = """-----BEGIN CERTIFICATE-----
// MIIDpDCCAgwCFGUiLLjMglw1cxfSRsR2dX8nguLRMA0GCSqGSIb3DQEBCwUAMA8x
// DTALBgNVBAMMBHJvb3QwHhcNMjAwNTI5MDAzNjQ4WhcNMjAwNjI4MDAzNjQ4WjAQ
// MQ4wDAYDVQQDDAV1c2VyMTCCAaAwDQYJKoZIhvcNAQEBBQADggGNADCCAYgCggGB
// AMOb69fsVyiN4AQvW8nbqO49AjrBqvrqIjn/fa7aB26vt61yPymIuYpWwdsvUi/E
// vRbA/2TzACopyCEzVOxaa2ZwM96DB+JFajiJ6r/vPnoy11mHgEpGUfY0So08ewLV
// sMwDxSQu9uo4X516ti2WY8guLPy6TyM0JgMcHY8oggu8YS3NanxLDt/7+Q/gTkrK
// NnUU540Gap+VIYXf0lMMSlyxSzPKCYMuAtzrjXO6yjs9IJfGzXpVT0QHaurBy2nR
// m70x47xbh3CwNjenNfY2duHfzOMbOVC3YefJ9RWdSIEEpoPReqa8sjKPva1BSXgM
// JAqCYpT1duueK+pU1WXGnDsmUEnbq+qCMrEZnAtmdIIwLNEGROq4JhOogK5WAQo8
// H4FSYZy0Jn6qWv/Gs6GfR5oIA6gR76QkVRZQOXxuBO17pLHSkqQWxtb05dg+3qm2
// RUnhciNPg3kSb9hgGp+9Jp6IVtOXLyX/a2TEVekeAOXGptJWPPKNQ9qi7RmrZ8B2
// QwIBAzANBgkqhkiG9w0BAQsFAAOCAYEAUMb5/sWIIIWuzaLJC3GPoRhpGSIWeKX8
// bh2c7Kxuw1/K+w3wygCowihAcAMpcxZSaChhmgdQEfS6rkkzNoRK0qH0zMhVOMcI
// xWkABUbf27Yl4TAjsdFYr7+hUyNkM7rETFQAiCp+aph2iY5/zF4C7DTxCLSGa8X2
// HVEezyfFb5umAFTq/Iugi9WhvAPq9iuAEquZKZ50g7uAbLtmPGB7iKmBVC6VW1m0
// h6Gz8U0H03Fbg5navTN0Jdx7w86yyAHW4L2oQ545fR3lwEhjXz+Er3ABe4z6tGJO
// IcnDLw+vNS/CCCTbIec8ck1YJIg4jxkIZtlX3j5+4xaGjCoI7SwLYG0/tF9saDwI
// b1qf7h5kGAahpTnO7Cy6OQUf/UxdQNKRo4XYywXk0Ky1DYFlJNXsm7MsqR5sma+b
// eFtt2qH9wF8sdarKlvgmuaMlbuRIbU3y4dLhwNAzN714tOgudEclKASfwKJ1ix5V
// 4VtJgb7X/djNCwySOYlO6r8Stc+i0aIj
// -----END CERTIFICATE-----"""

    val keyShare: Array[Byte] = "Opaque key share".getBytes("UTF-8")

    val GCM_KEY_LENGTH = 16
    assert(keyShare.size == GCM_KEY_LENGTH)

    val sp = new SP()

    // Retry attestation a few times in case of transient failures
    Utils.retry(3) {
      sp.Init(Utils.sharedKey, intelCert, userCert, keyShare)

      val msg1s = rdd.mapPartitionsWithIndex { (i, _) =>
        val (enclave, eid) = Utils.initEnclave()
        val msg1 = enclave.RemoteAttestation1(eid)
        Iterator((i, msg1))
      }.collect.toMap

      val msg2s = msg1s.mapValues(msg1 => sp.SPProcMsg1(msg1)).map(identity)

      val statuses = rdd.mapPartitionsWithIndex { (i, _) =>
        val (enclave, eid) = Utils.initEnclave()
         enclave.RemoteAttestation3(eid, msg2s(i))
        Iterator((i, true))
      }.collect.toMap

      // TODO: some sort of assert that attestation passed
    }
  }
}
