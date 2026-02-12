import sys
import grpc
from concurrent import futures
import threading
import configparser
import random

import raft_pb2
import raft_pb2_grpc

class KeyValueStoreServicer(raft_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, server_id):
        self.server_id = server_id
        self.store = {}  # Assignment 2 functionality
        self.store_lock = threading.RLock()

        # Raft state variables (Assignment 3)
        self.state_lock = threading.RLock()
        self.currentTerm = 0
        self.votedFor = None
        self.role = "follower"  # "follower", "candidate", "leader"
        self.leaderId = None
        self.votes_received = 0

        # Load peer info from config.ini
        self.peers = {}
        self._load_peers()
        self.total_servers = len(self.peers) + 1  # peers + self

        # Timers
        self.election_timer = None
        self.heartbeat_timer = None
        self.reset_election_timer()

    def _load_peers(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        base_port = int(config.get('Servers', 'base_port', fallback='9001'))
        active_str = config.get('Servers', 'active', fallback='')
        active_ids = [int(x.strip()) for x in active_str.split(',') if x.strip()]
        for peer_id in active_ids:
            if peer_id != self.server_id:
                self.peers[peer_id] = f'localhost:{base_port + peer_id}'

    def ping(self, request, context):
        return raft_pb2.GenericResponse(success=True)

    def reset_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()
        timeout = random.uniform(0.15, 0.30)
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.daemon = True
        self.election_timer.start()

    def start_election(self):
        with self.state_lock:
            if self.role == "leader":
                return
            self.role = "candidate"
            self.currentTerm += 1
            self.votedFor = self.server_id
            self.votes_received = 1  # self-vote
            election_term = self.currentTerm
            self.reset_election_timer()

        # Send RequestVote to all peers concurrently
        for peer_id, addr in self.peers.items():
            t = threading.Thread(
                target=self._request_vote_from_peer,
                args=(peer_id, addr, election_term)
            )
            t.daemon = True
            t.start()

    def _request_vote_from_peer(self, peer_id, addr, election_term):
        try:
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            request = raft_pb2.RequestVoteArgs(
                term=election_term,
                candidateId=self.server_id,
                lastLogIndex=0,
                lastLogTerm=0
            )
            response = stub.RequestVote(request, timeout=2)
            channel.close()

            with self.state_lock:
                # Abort if no longer candidate or term changed
                if self.role != "candidate" or self.currentTerm != election_term:
                    return
                # Step down if higher term discovered
                if response.term > self.currentTerm:
                    self.become_follower(response.term)
                    return
                if response.voteGranted:
                    self.votes_received += 1
                    if self.votes_received > self.total_servers // 2:
                        self.become_leader()
        except:
            pass

    def become_follower(self, new_term):
        self.currentTerm = new_term
        self.votedFor = None
        self.role = "follower"
        self.leaderId = None
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.reset_election_timer()

    def become_leader(self):
        self.role = "leader"
        self.leaderId = self.server_id
        if self.election_timer:
            self.election_timer.cancel()
        self.send_heartbeats()

    def send_heartbeats(self):
        with self.state_lock:
            if self.role != "leader":
                return
            current_term = self.currentTerm

        for peer_id, addr in self.peers.items():
            t = threading.Thread(
                target=self._send_heartbeat_to_peer,
                args=(addr, current_term)
            )
            t.daemon = True
            t.start()

        # Schedule next heartbeat (75ms, well within 150ms min election timeout)
        self.heartbeat_timer = threading.Timer(0.075, self.send_heartbeats)
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()

    def _send_heartbeat_to_peer(self, addr, term):
        try:
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            request = raft_pb2.AppendEntriesArgs(
                term=term,
                leaderId=self.server_id,
                prevLogIndex=0,
                prevLogTerm=0,
                leaderCommit=0
            )
            response = stub.AppendEntries(request, timeout=2)
            channel.close()

            with self.state_lock:
                if response.term > self.currentTerm:
                    self.become_follower(response.term)
        except:
            pass

    def GetState(self, request, context):
        with self.state_lock:
            return raft_pb2.State(
                term=self.currentTerm,
                isLeader=(self.role == "leader")
            )

    def Get(self, request, context):
        key = request.arg
        with self.store_lock:
            value = self.store.get(key, "")
        return raft_pb2.KeyValue(key=key, value=value)

    def Put(self, request, context):
        key = request.key
        value = request.value
        with self.store_lock:
            self.store[key] = value
        return raft_pb2.GenericResponse(success=True)

    def RequestVote(self, request, context):
        with self.state_lock:
            candidate_term = request.term
            candidate_id = request.candidateId

            if candidate_term > self.currentTerm:
                self.become_follower(candidate_term)

            vote_granted = False
            if (candidate_term >= self.currentTerm and
                (self.votedFor is None or self.votedFor == candidate_id)):
                vote_granted = True
                self.votedFor = candidate_id
                self.reset_election_timer()

            return raft_pb2.RequestVoteReply(
                term=self.currentTerm,
                voteGranted=vote_granted
            )

    def AppendEntries(self, request, context):
        with self.state_lock:
            leader_term = request.term
            leader_id = request.leaderId

            # Reject stale term
            if leader_term < self.currentTerm:
                return raft_pb2.AppendEntriesReply(
                    term=self.currentTerm,
                    success=False
                )

            # Higher term: update and reset vote
            if leader_term > self.currentTerm:
                self.currentTerm = leader_term
                self.votedFor = None

            # Step down from candidate/leader, accept this leader
            self.role = "follower"
            self.leaderId = leader_id
            if self.heartbeat_timer:
                self.heartbeat_timer.cancel()
            self.reset_election_timer()

            return raft_pb2.AppendEntriesReply(
                term=self.currentTerm,
                success=True
            )

def serve(server_id):
    port = 9001 + server_id
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStoreServicer(server_id), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    server_id = int(sys.argv[1])
    serve(server_id)
