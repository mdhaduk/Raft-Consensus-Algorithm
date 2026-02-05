import atexit
import grpc
from concurrent import futures
import subprocess
import sys
import configparser
import raft_pb2
import raft_pb2_grpc

class FrontEndServicer(raft_pb2_grpc.FrontEndServicer):
    def __init__(self):
        self.processes = {}
        self.cached_server = None  # Cache discovered server for efficiency

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
    
    def _get_active_servers_from_config(self):
        """Read active servers from config.ini"""
        try:
            config = configparser.ConfigParser()
            config.read("config.ini")
            active_str = config.get("Servers", "active")
            return [int(x.strip()) for x in active_str.split(",")]
        except Exception:
            # fallback if config read fails
            return list(self.processes.keys())
    
    def ping_server(self, server_id):
        try:
            port = 9001 + server_id
            channel = grpc.insecure_channel(f"localhost:{port}")
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            stub.ping(raft_pb2.Empty())
            channel.close()
            return True
        except:
            return False

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


    def find_available_server(self):
        # try cached server first
        if self.cached_server is not None and self.ping_server(self.cached_server):
            return self.cached_server

        # cache miss or server unavailable - search for available server
        active_servers = self._get_active_servers_from_config()
        for server_id in active_servers:
            if self.ping_server(server_id):
                self.cached_server = server_id  # cache server
                return server_id

        self.cached_server = None  # clear cache if none available
        return None
    
    def forward_get_to_server(self, server_id, key):
        try:
            # calculate port for the server
            port = 9001 + server_id
            channel = grpc.insecure_channel(f"localhost:{port}")
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)

            # call the get method in server
            response = stub.Get(raft_pb2.StringArg(arg=key))
            channel.close()
            
            return True, response.value
        except Exception  as e:
            return False, str(e)

    def Get(self, request, context):
        server_id = self.find_available_server()
        if server_id is None:
            return raft_pb2.Reply(wrongLeader=True, error="No servers available")
        
        # Forward to server
        success, value = self.forward_get_to_server(server_id, request.key)
        if success:
            return raft_pb2.Reply(wrongLeader=False, value=value)
        else:
            return raft_pb2.Reply(wrongLeader=True, error="Server error")

    def forward_put_to_server(self, server_id, key, value):
        try:
            # calculate port for the server
            port = 9001 + server_id
            channel = grpc.insecure_channel(f"localhost:{port}")
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)

            # call the put method in server
            response = stub.Put(raft_pb2.KeyValue(key=key, value=value))
            channel.close()
            
            return True, ""
        except Exception  as e:
            return False, str(e)
        
    def Put(self, request, context):
        server_id = self.find_available_server()
        if server_id is None:
            return raft_pb2.Reply(wrongLeader=True, error="No servers available")
        
        # Forward to server
        success, error = self.forward_put_to_server(server_id, request.key, request.value)
        if success:
            return raft_pb2.Reply(wrongLeader=False)
        else:
            return raft_pb2.Reply(wrongLeader=True, error=error)

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
