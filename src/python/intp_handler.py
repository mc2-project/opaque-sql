from code import InteractiveInterpreter

intp = InteractiveInterpreter(locals=locals())
intp.runcode("print(\"hello world!\")")
