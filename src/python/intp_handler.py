import code
from code import InteractiveInterpreter

from pyspark import SparkConf, SparkContext

class IntpHandler:
    def __init__(self):
        self.intp = InteractiveInterpreter(locals=locals())
        self.intp.runcode("from intp_init import intp_init")
        self.intp.runcode("intp_init()")

    def run(self, code):
        self.intp.runcode(code)
