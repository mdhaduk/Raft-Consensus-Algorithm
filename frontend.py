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
        self.config = self._load_config()
        self.cached_server = None 

    def _load_config(self):
        """Load configuration from config.ini"""
        config = configparser.ConfigParser()
        config.read('config.ini')
        return config

    def get_server_state(self, server_id):
        """Get state of a server by calling GetState RPC
        Returns: (success, term, is_leader)
        """
        try:
            base_port = int(self.config.get('Servers', 'base_port', fallback='9001'))
            addr = f"localhost:{base_port + server_id}"
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)

            response = stub.GetState(raft_pb2.Empty(), timeout=2)
            channel.close()
            return True, response.term, response.isLeader
        except:
            return False, None, None
    
    def find_leader_server(self):
        """Find current leader by checking GetState on all servers"""
        active_servers = self.get_active_servers_from_config()
        
        for server_id in active_servers:
            try:
                #get_server_state should call KeyValueStore via stub and return (success, term, is_leader)
                success, term, is_leader = self.get_server_state(server_id)
                if success and is_leader:
                    return server_id
            except:
                continue
        
        # No leader found, return any available server
        return self.find_available_server()

    def get_active_servers_from_config(self):
        """Get list of active server IDs from config.ini"""
        try:
            active_str = self.config.get('Servers', 'active')
            active_servers = [int(x.strip()) for x in active_str.split(',')]
            return active_servers
        except:
            return []

    def ping_server(self, server_id):
        """Check if a server is responsive by pinging it"""
        try:
            base_port = int(self.config.get('Servers', 'base_port', fallback='9001'))
            addr = f"localhost:{base_port + server_id}"
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)

            response = stub.ping(raft_pb2.Empty(), timeout=2)
            channel.close()
            return response.success
        except:
            return False

    def find_available_server(self):
        """Find first available server from active servers list"""
        # try cached server first
        if self.cached_server is not None and self.ping_server(self.cached_server):
            return self.cached_server

        active_servers = self.get_active_servers_from_config()
        for server_id in active_servers:
            # Check if server process exists and is running
            if server_id in self.processes:
                proc = self.processes[server_id]
                if proc is not None and proc.poll() is None:
                    if self.ping_server(server_id):
                        return server_id
        return None

    def forward_get_to_server(self, server_id, key):
        """Forward Get request to specific server
        Returns: (success, value)
        """
        try:
            base_port = int(self.config.get('Servers', 'base_port', fallback='9001'))
            addr = f"localhost:{base_port + server_id}"
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)

            # Convert key to StringArg for server
            request = raft_pb2.StringArg(arg=key)
            response = stub.Get(request, timeout=5)
            channel.close()

            return True, response.value
        except Exception as e:
            return False, str(e)

    def forward_put_to_server(self, server_id, key, value):
        """Forward Put request to specific server
        Returns: (success, error_message)
        """
        try:
            base_port = int(self.config.get('Servers', 'base_port', fallback='9001'))
            addr = f"localhost:{base_port + server_id}"
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)

            # Forward KeyValue to server
            request = raft_pb2.KeyValue(key=key, value=value)
            response = stub.Put(request, timeout=5)
            channel.close()

            if response.success:
                return True, ""
            else:
                return False, response.error
        except Exception as e:
            return False, str(e)

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
        """Handle client Get request
        Input: GetKey with .key, .clientId, .requestId
        Output: Reply with .wrongLeader, .error, .value
        """
        server_id = self.find_leader_server()
        if server_id is None:
            return raft_pb2.Reply(wrongLeader=True, error="No servers available")

        # Forward to server
        success, value = self.forward_get_to_server(server_id, request.key)
        if success:
            return raft_pb2.Reply(wrongLeader=False, value=value)
        else:
            return raft_pb2.Reply(wrongLeader=True, error=f"Server error: {value}")

    def Put(self, request, context):
        """Handle client Put request
        Input: KeyValue with .key, .value, .clientId, .requestId
        Output: Reply with .wrongLeader, .error
        """
        server_id = self.find_leader_server()
        if server_id is None:
            return raft_pb2.Reply(wrongLeader=True, error="No servers available")

        # Forward to server
        success, error = self.forward_put_to_server(server_id, request.key, request.value)
        if success:
            return raft_pb2.Reply(wrongLeader=False)
        else:
            return raft_pb2.Reply(wrongLeader=True, error=f"Server error: {error}")

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
