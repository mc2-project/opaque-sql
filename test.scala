import edu.berkeley.cs.rise.opaque.implicits._

edu.berkeley.cs.rise.opaque.Utils.initSQLContext(spark.sqlContext)
val data = Seq(("foo", 1), ("bar", 2), ("baz", 3), ("boo", 4))
val df = spark.createDataFrame(data).toDF("word", "count")
// Encrypt with client key
val dfEncrypted2 = df.encrypted
// Enter enclave and filter
// val df2 = dfEncrypted.filter($"count" > lit(3))
// val df3 = dfEncrypted.filter($"count" > lit(4))
// Decrypt with client key
// dfEncrypted.show()
// df2.show()
// df3.show()
dfEncrypted2.write.format("edu.berkeley.cs.rise.opaque.EncryptedSource").save("dfEncrypted2")
