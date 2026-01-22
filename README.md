Workflow

1. raft.proto defines the contracts for message and service types --> langauge agnostic 
2. use raft.proto to generate the raft_pb2.py and raft_pb2_grpc.py; these files
have the base classes for client/server and client stubs --> can be generated in variety of supported languages
3. frontend.py implements the service base classes defined in generated base protobuf files for the (also uses the base classes for the message types)
4. server.py does the same for its base classes
5. a1_tests.py is the client, frontend/server are gRPC servers... RPC calls made from client to server,
frontend is used to spawn the servers... that is all (Assignment one)
    -> How does a1_tests.py make RPC calls? It creates a client stub then creates a channel to the port on which
    frontend/server is running, then makes calls based on the APIs defined by, again, the proto file