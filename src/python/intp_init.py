# This file is intended to serve as a startup script for the programmatic interpreter
import pyspark.shell
from pyspark import SparkContext

from opaque_sql import *

def intp_init():
    sc = SparkContext.getOrCreate()
    jars = sc.getConf().get("spark.jars", None)
    if not jars:
        raise Exception("Need to add an assembly jar to spark-submit")

    init_opaque_sql()
