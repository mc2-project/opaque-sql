import grpc
from concurrent import futures
from protobuf import listener_pb2, listener_pb2_grpc

from intp_handler import IntpHandler

class Listener(listener_pb2_grpc.ListenerServicer):

    def __init__(self):
        self.intp_handler = IntpHandler()
    
    def ReceiveQuery(self, request, context):
        source = request.request
        out, err = self.intp_handler.run(source)
        status = opaquesql_pb2.Status(status=0, exception="")
        if err: # If non-empty, user code threw an exception
            status = opaquesql_pb2.Status(status=1, exception="Opaque SQL returned an error: " + err)
        return opaquesql_pb2.QueryResult(result=out, status=status)

if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    listener_pb2_grpc.add_ListenerServicer_to_server(Listener(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    server.wait_for_termination()
