from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame, SparkSession, SQLContext

def init_opaque_sql(testing=False):
    sc = SparkContext.getOrCreate()
    sc._jvm.edu.berkeley.cs.rise.opaque.Utils.initOpaqueSQL(SparkSession(sc)._jsparkSession, testing)

def encrypted(df):
    sc = SparkContext.getOrCreate()
    obj = sc._jvm.org.apache.spark.sql.OpaqueDatasetFunctions(df._jdf).encrypted()
    return DataFrame(obj, SQLContext(sc))
setattr(DataFrame, "encrypted", encrypted)
