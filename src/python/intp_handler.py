import code
import io
from code import InteractiveInterpreter
from contextlib import redirect_stdout, redirect_stderr

from opaque_sql import *

class IntpHandler:
    def __init__(self):
        init_opaque_sql()

    def run(self, source):
        out = io.StringIO()
        err = io.StringIO()
        with redirect_stdout(out) and redirect_stderr(err):
            compiled = compile(source)
            exec(compiled)
        out.getvalue(), err.getvalue()

