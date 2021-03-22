from pyspark import SparkContext
from pyspark.sql import SQLContext

def init_sql_context():
    sc = SparkContext.getOrCreate()
    sc._jvm.edu.berkeley.cs.rise.opaque.Utils.initSQLContext(SQLContext(sc)._jsqlContext)
