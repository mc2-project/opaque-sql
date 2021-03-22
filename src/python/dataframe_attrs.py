from pyspark import SparkContext
from pyspark.sql import DataFrame, SQLContext

def encrypted(df):
    sc = SparkContext.getOrCreate()
    obj = sc._jvm.org.apache.spark.sql.OpaqueDatasetFunctions(df._jdf).encrypted()
    return DataFrame(obj, SQLContext(sc))

setattr(DataFrame, "encrypted", encrypted)
