from pyspark import SparkConf, SparkContext

import grpc
from concurrent import futures
from protobuf import listener_pb2, listener_pb2_grpc

from intp_handler import IntpHandler

class Listener(listener_pb2_grpc.ListenerServicer):

    def __init__(self):
        self.intp_handler = IntpHandler()
    
    def ReceiveQuery(self, request, context):
        source = request.request
        output, result = self.intp_handler.run(source)
        status = listener_pb2.Status(0, "")
        if result: # code threw an exception
            status = listener_pb2.Status(1, result)
        return listener_pb2.QueryResult(output, status)

if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    listener_pb2_grpc.add_ListenerServicer_to_server(Listener(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    server.wait_for_termination()
