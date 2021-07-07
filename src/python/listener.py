from pyspark import SparkConf, SparkContext

from protobuf import listener_pb2, listener_pb2_grpc
from intp_handler import IntpHandler

class Listener(listener_pb2_grpc.ListenerServicer):

    def __init__(self, intp_handler, port):
      self.intp_handler = intp_handler
      self.port = port


if __name__ == "__main__":
    # Create handler object that the main program will interact with
    intp_handler = IntpHandler()

    listener = Listener(intp_handler, 50052)
