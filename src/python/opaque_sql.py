from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame, SparkSession, SQLContext

# Need to create a new SparkContext to use a dummy key for now.
sc = SparkContext.getOrCreate()
conf = sc.getConf().set("spark.opaque.testing.enableSharedKey", "true")
sc.stop()
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

def init_opaque_sql():
    sc = SparkContext.getOrCreate()
    sc._jvm.edu.berkeley.cs.rise.opaque.Utils.initOpaqueSQL(SparkSession(sc)._jsparkSession)

def encrypted(df):
    sc = SparkContext.getOrCreate()
    obj = sc._jvm.org.apache.spark.sql.OpaqueDatasetFunctions(df._jdf).encrypted()
    return DataFrame(obj, SQLContext(sc))
setattr(DataFrame, "encrypted", encrypted)
