from pyspark import SparkContext
from pyspark.sql import DataFrame, SQLContext

def init_sql_context():
    sc = SparkContext.getOrCreate()
    sc._jvm.edu.berkeley.cs.rise.opaque.Utils.initSQLContext(SQLContext(sc)._jsqlContext)

def encrypted(df):
    sc = SparkContext.getOrCreate()
    obj = sc._jvm.org.apache.spark.sql.OpaqueDatasetFunctions(df._jdf).encrypted()
    return DataFrame(obj, SQLContext(sc))
setattr(DataFrame, "encrypted", encrypted)
