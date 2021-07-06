from code import InteractiveInterpreter

# This function is run in the programatic interpreter to pass the jars from one Spark context
# to another
def set_jars(self, jars):
    jars_str = jars.split(",")
    for jar in jars_str:
        sc._jvm.addJar(jar)

class IntpHandler:
    def __init__(self, jars):
        self.intp = InteractiveInterpreter(locals=locals())
        self.intp.runcode("import pyspark.shell")

        self.intp.runcode("from intp_handler import set_jars")
        self.intp.runcode("set_jars(" + jars + ")")

        self.intp.runcode("from opaque_sql import *")
        self.intp.runcode("init_opaque_sql(testing=True)")
    
    def run(self, code):
        self.intp.runcode(code)
