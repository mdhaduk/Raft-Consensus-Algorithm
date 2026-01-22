import atexit
import grpc
from concurrent import futures
import subprocess
import sys
import raft_pb2
import raft_pb2_grpc

class FrontEndServicer(raft_pb2_grpc.FrontEndServicer):
    def __init__(self):
        self.processes = {}

    def _start_server(self, server_id):
        proc = self.processes.get(server_id)
        
        #pre condition: if server id already exists AND is running
        if proc is not None and proc.poll() is None:
            return True, ""

        try:
            cmd = [sys.executable, "server.py", str(server_id)]
            proc = subprocess.Popen(cmd)
            self.processes[server_id] = proc
            return True, ""
        except Exception as exc:
            return False, str(exc)

    def StartRaft(self, request, context):
        for server_id in range(request.arg):
            ok, error = self._start_server(server_id)
            if not ok:
                return raft_pb2.Reply(wrongLeader=False, error=error)
        return raft_pb2.Reply(wrongLeader=False)

    def StartServer(self, request, context):
        ok, error = self._start_server(request.arg)
        if not ok:
            return raft_pb2.Reply(wrongLeader=False, error=error)
        return raft_pb2.Reply(wrongLeader=False)
    
    def Get(self, request, context):
        return raft_pb2.Reply(wrongLeader=True, error="Not implemented")

    def Put(self, request, context):
        return raft_pb2.Reply(wrongLeader=True, error="Not implemented")

    def stop_all(self):
        for proc in self.processes.values():
            if proc.poll() is None:
                proc.terminate()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10)) #concurrency, arbitrary # of threads
    raft_pb2_grpc.add_FrontEndServicer_to_server(FrontEndServicer(), server)
    server.add_insecure_port("[::]:8001")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
