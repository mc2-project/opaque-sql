// This script tests loading from two dataframes, each encrypted with a different key
//
// import org.apache.spark.sql.types._
// 
// val df = (spark.read.format("edu.berkeley.cs.rise.opaque.EncryptedSource")
//   .schema(StructType(Seq(StructField("word", StringType), StructField("count", IntegerType))))
//   .load("dfEncrypted"))
// println("dfEncrypted")
// df.show()
// 
// val df2 = (spark.read.format("edu.berkeley.cs.rise.opaque.EncryptedSource")
//   .schema(StructType(Seq(StructField("word", StringType), StructField("count", IntegerType))))
//   .load("dfEncrypted2"))
// println("dfEncrypted2")
// df2.show()
