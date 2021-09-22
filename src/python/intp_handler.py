from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from traceback import print_exc

from opaque_sql import *

from pyspark.shell import *

class IntpHandler:
    def __init__(self):
        self.initialized = False

    def run(self, source):
        with StringIO() as out, redirect_stdout(out), \
                StringIO() as err, redirect_stderr(err):
            try:
                if not self.initialized:
                    exec("init_opaque_sql()")
                    self.initialized = True
                exec(source)
            except Exception as e:
                print_exc() # This goes to stderr
            finally:
                return out.getvalue(), err.getvalue()
