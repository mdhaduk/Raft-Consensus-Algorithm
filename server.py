import sys
import grpc
from concurrent import futures
import threading

import raft_pb2
import raft_pb2_grpc

class KeyValueStoreServicer(raft_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, server_id):
        self.server_id = server_id
        self.store = {}  # In-memory key-value store
        self.store_lock = threading.RLock()  # Thread safety

    def ping(self, request, context):
        return raft_pb2.GenericResponse(success=True)

    def GetState(self, request, context):
        return raft_pb2.State(term=0, isLeader=False)

    def Get(self, request, context):
        """Handle Get request from frontend
        Input: StringArg with .arg field containing the key
        Output: KeyValue with .key and .value fields
        """
        key = request.arg
        with self.store_lock:
            value = self.store.get(key, "")  # Empty string for missing keys
        return raft_pb2.KeyValue(key=key, value=value)

    def Put(self, request, context):
        """Handle Put request from frontend
        Input: KeyValue with .key and .value fields
        Output: GenericResponse with .success field
        """
        key = request.key
        value = request.value
        with self.store_lock:
            self.store[key] = value
        return raft_pb2.GenericResponse(success=True)

def serve(server_id):
    # calculate port then stop
    port = 9001 + server_id
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStoreServicer(server_id), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    server_id = int(sys.argv[1])
    serve(server_id)
