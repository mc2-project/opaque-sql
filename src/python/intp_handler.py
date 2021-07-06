
from pyspark import SparkConf, SparkContext

from code import InteractiveInterpreter

# This function is run in the programatic interpreter to pass the jars from one Spark context
# to another
def set_jars(jars):
    jars_str = jars.split(",")
    for jar in jars_str:
        sc._jvm.addJar(jar)

intp = InteractiveInterpreter(locals=locals())
intp.runcode("import pyspark.shell")

# We need to include the jars provided to Spark in the new IMain's classpath.
sc = SparkContext.getOrCreate()
jars = sc.getConf().get("spark.jars", None)
if not jars:
  raise Exception("Need to add an assembly jar to spark-submit")

intp.runcode("from intp_handler import set_jars")
intp.runcode("set_jars(" + jars + ")")

intp.runcode("from opaque_sql import *")
intp.runcode("init_opaque_sql(testing=True)")
