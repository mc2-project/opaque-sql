from protobuf import listener_pb2, listener_pb2_grpc
from intp_handler import IntpHandler
from pyspark import SparkConf, SparkContext

class Listener(listener_pb2_grpc.ListenerServicer): 

    def __init__(self, intp_handler, port):
      self.intp_handler = intp_handler
      self.port = port


if __name__ == "__main__":
    # We need to include the jars provided to Spark in the new IMain's classpath.
    sc = SparkContext.getOrCreate()
    jars = sc.getConf().get("spark.jars", None)
    if not jars:
      raise Exception("Need to add an assembly jar to spark-submit")

    # Create handler object that the main program will interact with
    intp_handler = IntpHandler(jars)

    listener = listener(intp_handler, 50052)
