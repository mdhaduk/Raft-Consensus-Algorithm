import grpc
from concurrent import futures
import subprocess

import raft_pb2
import raft_pb2_grpc


class FrontEndServicer(raft_pb2_grpc.FrontEndServicer):
    def __init__(self):
        self.server_processes = {}

    def _clean_servers(self):
        # Clean up existing servers
        for proc in self.server_processes.values():
            try:
                proc.terminate()
                proc.wait(timeout=2)
            except:
                pass
        self.server_processes.clear()

    def _start_servers(self, num_servers):
        # Start N servers with IDs 0 to N-1
        for server_id in range(num_servers):
            proc = subprocess.Popen(['python3', 'server.py', str(server_id)])
            self.server_processes[server_id] = proc
        

    def StartRaft(self, request, context):
        '''
        Start N servers (1-5) with IDs 0 to N-1. Clean up any persistent state first. Each server process must be killable by the test script. 
        
        :param self: Description
        :param request: Description
        :param context: Description
        '''
        num_servers = request.arg

        self._clean_servers()

        try:
            self._start_servers(num_servers)
        except Exception as e:
            self._clean_servers()
            return raft_pb2.Reply(wrongLeader=False, error=str(e))

        return raft_pb2.Reply(wrongLeader=False, error="")

    def StartServer(self, request, context):
        '''
        Start individual server by ID. Does not clean persistent state. Used for restarting crashed server. (A1)
        
        :param self: Description
        :param request: Description
        :param context: Description
        '''
        server_id = request.arg
        proc = subprocess.Popen(['python3', 'server.py', str(server_id)])
        self.server_processes[server_id] = proc
        return raft_pb2.Reply(wrongLeader=False, error="")

    def Get(self, request, context):
        return raft_pb2.Reply(wrongLeader=True, error="Not implemented")

    def Put(self, request, context):
        return raft_pb2.Reply(wrongLeader=True, error="Not implemented")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_FrontEndServicer_to_server(FrontEndServicer(), server)
    server.add_insecure_port('[::]:8001')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
