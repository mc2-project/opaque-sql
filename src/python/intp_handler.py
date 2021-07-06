from code import InteractiveInterpreter

intp = InteractiveInterpreter(locals=locals())
intp.runcode("import pyspark.shell")
