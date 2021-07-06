from code import InteractiveInterpreter

intp = InteractiveInterpreter(locals=locals())
intp.runcode("import pyspark.shell")
intp.runcode("print(\"hello world!\")")
