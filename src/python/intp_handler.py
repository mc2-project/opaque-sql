import code
import io
from code import InteractiveInterpreter
from contextlib import redirect_stdout, redirect_stderr

from pyspark import SparkConf, SparkContext

class IntpHandler:
    def __init__(self):
        self.intp = InteractiveInterpreter(locals=locals())
        self.intp.runcode("from intp_init import intp_init")
        self.intp.runcode("intp_init()")

    def run(self, source):
        out = io.StringIO()
        err = io.StringIO()
        with redirect_stdout(out) and redirect_stderr(err):
            compiled = code.compile(source)
            self.intp.runcode(compiled)
        out.getvalue(), err.getvalue()

