import configparser
import random
import sys
import threading
from concurrent import futures

import grpc

import raft_pb2
import raft_pb2_grpc


class KeyValueStoreServicer(raft_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, server_id):
        self.server_id = server_id
        self.store = {}
        self.log = [None]  # index 0 is sentinel; real entries start at 1
        self.commitIndex = 0
        self.lastApplied = 0
        self.state_machine = {}

        # Raft state variables
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

        # Leader-specific state — initialized in become_leader()
        self.next_index = {}
        self.match_index = {}

    def _load_peers(self):
        config = configparser.ConfigParser()
        config.read("config.ini")
        base_port = int(config.get("Servers", "base_port", fallback="9001"))
        active_str = config.get("Servers", "active", fallback="")
        active_ids = [int(x.strip()) for x in active_str.split(",") if x.strip()]
        for peer_id in active_ids:
            if peer_id != self.server_id:
                self.peers[peer_id] = f"localhost:{base_port + peer_id}"

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

        for peer_id, addr in self.peers.items():
            t = threading.Thread(
                target=self._request_vote_from_peer, args=(peer_id, addr, election_term)
            )
            t.daemon = True
            t.start()

    def _request_vote_from_peer(self, peer_id, addr, election_term):
        try:
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            with self.state_lock:
                last_log_index = len(self.log) - 1
                last_log_term = self.log[-1].term if last_log_index > 0 else 0
            request = raft_pb2.RequestVoteArgs(
                term=election_term,
                candidateId=self.server_id,
                lastLogIndex=last_log_index,
                lastLogTerm=last_log_term,
            )
            response = stub.RequestVote(request, timeout=2)
            channel.close()

            with self.state_lock:
                if self.role != "candidate" or self.currentTerm != election_term:
                    return
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
        # Reinitialize per Raft spec: nextIndex = last log index + 1, matchIndex = 0
        self.next_index = {pid: len(self.log) for pid in self.peers}
        self.match_index = {pid: 0 for pid in self.peers}
        if self.election_timer:
            self.election_timer.cancel()
        self.send_heartbeats()

    def send_heartbeats(self):
        with self.state_lock:
            if self.role != "leader":
                return
            current_term = self.currentTerm

        for peer_id, addr in self.peers.items():
            t = threading.Thread(target=self._replicate_to_peer, args=(peer_id, addr))
            t.daemon = True
            t.start()

        self.heartbeat_timer = threading.Timer(0.075, self.send_heartbeats)
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()

    def GetState(self, request, context):
        with self.state_lock:
            return raft_pb2.State(
                term=self.currentTerm,
                isLeader=(self.role == "leader"),
                commitIndex=self.commitIndex,
                lastApplied=self.lastApplied,
            )

    def Get(self, request, context):
        with self.state_lock:
            key = request.arg
            value = self.state_machine.get(key, "")
            return raft_pb2.KeyValue(key=key, value=value)

    def Put(self, request, context):
        with self.state_lock:
            if self.role != "leader":
                return raft_pb2.GenericResponse(success=False, error="Not leader")

            entry = raft_pb2.LogEntry(
                term=self.currentTerm,
                key=request.key,
                value=request.value,
                clientId=request.clientId,
                requestId=request.requestId,
            )
            self.log.append(entry)
            self.replicate_to_followers()

            return raft_pb2.GenericResponse(success=True)

    def replicate_to_followers(self):
        for peer_id, addr in self.peers.items():
            t = threading.Thread(target=self._replicate_to_peer, args=(peer_id, addr))
            t.start()

    def _replicate_to_peer(self, peer_id, addr):
        try:
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            while True:
                with self.state_lock:
                    # need this because leader might step down mid replication
                    if self.role != "leader":
                        return

                    ni = self.next_index[peer_id]
                    prev_index = ni - 1
                    prev_term = self.log[prev_index].term if prev_index > 0 else 0
                    entries = list(self.log[ni:])
                    request = raft_pb2.AppendEntriesArgs(
                        term=self.currentTerm,
                        leaderId=self.server_id,
                        prevLogIndex=prev_index,
                        prevLogTerm=prev_term,
                        entries=entries,
                        leaderCommit=self.commitIndex,
                    )

                response = stub.AppendEntries(request, timeout=2)

                with self.state_lock:
                    if response.term > self.currentTerm:
                        self.become_follower(response.term)
                        return
                    if response.success:
                        self.next_index[peer_id] = ni + len(entries)
                        self.match_index[peer_id] = self.next_index[peer_id] - 1
                        self.update_commit_index()
                        return
                    else:
                        # Log inconsistency: decrement and retry
                        self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)

        except:
            pass

    def update_commit_index(self):
        # Called under state_lock by leader after successful replication
        for n in range(self.commitIndex + 1, len(self.log)):
            if self.log[n].term != self.currentTerm:
                continue
            replicated = 1 + sum(1 for mi in self.match_index.values() if mi >= n)
            if replicated > self.total_servers // 2:
                self.commitIndex = n
        self.apply_committed_entries()

    def apply_committed_entries(self):
        while self.lastApplied < self.commitIndex:
            self.lastApplied += 1
            entry = self.log[self.lastApplied]
            self.state_machine[entry.key] = entry.value

    def is_candidate_log_up_to_date(self, lastLogIndex, lastLogTerm):
        if len(self.log) == 1:  # no real entries
            return True
        last_entry = self.log[-1]
        if lastLogTerm != last_entry.term:
            return lastLogTerm > last_entry.term
        return lastLogIndex >= len(self.log) - 1

    def RequestVote(self, request, context):
        with self.state_lock:
            candidate_term = request.term
            candidate_id = request.candidateId

            if candidate_term > self.currentTerm:
                self.become_follower(candidate_term)

            vote_granted = False
            if (
                candidate_term >= self.currentTerm
                and (self.votedFor is None or self.votedFor == candidate_id)
                and self.is_candidate_log_up_to_date(
                    request.lastLogIndex, request.lastLogTerm
                )
            ):
                vote_granted = True
                self.votedFor = candidate_id
                self.reset_election_timer()

            return raft_pb2.RequestVoteReply(
                term=self.currentTerm, voteGranted=vote_granted
            )

    def AppendEntries(self, request, context):
        with self.state_lock:
            leader_term = request.term
            leader_id = request.leaderId

            # Reject stale term
            if leader_term < self.currentTerm:
                return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=False)

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

            # Check log consistency, if this returns false, leader decrements nextIndex and retries
            if request.prevLogIndex > 0:
                if (
                    request.prevLogIndex >= len(self.log)
                    or self.log[request.prevLogIndex].term != request.prevLogTerm
                ):
                    return raft_pb2.AppendEntriesReply(
                        term=self.currentTerm, success=False
                    )

            # Truncate once, then append any new entries (if exists)
            log_index = request.prevLogIndex + 1
            if request.entries:
                if log_index < len(self.log):
                    self.log = self.log[:log_index]
                self.log.extend(request.entries)

            # Update commit index
            if request.leaderCommit > self.commitIndex:
                self.commitIndex = min(request.leaderCommit, len(self.log) - 1)
                self.apply_committed_entries()

            return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=True)


def serve(server_id):
    port = 9001 + server_id
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KeyValueStoreServicer(server_id), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    server_id = int(sys.argv[1])
    serve(server_id)
