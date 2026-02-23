import configparser
import random
import sys
import threading
import time
from concurrent import futures

import grpc

import raft_pb2
import raft_pb2_grpc


class KeyValueStoreServicer(raft_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, server_id):
        self.server_id = server_id

        # Raft state variables (Assignment 3)
        self.lock = threading.RLock()
        self.currentTerm = 0
        self.votedFor = None
        self.role = "follower"  # "follower", "candidate", "leader"
        self.leaderId = None
        self.votes_received = 0

        # Raft state variables (Assignment 4)
        self.commit_index = 0
        self.last_applied = 0
        self.log = [None]  # index 0 unused, log starts at index 1
        self.state_machine = {}  # Key-value store
        self.leader_event = threading.Event()

        # Leader-specific state (Assignment 4)
        self.next_index = {}
        self.match_index = {}

        # Load peer info from config.ini
        self.peers = {}
        self._load_peers()
        self.total_servers = len(self.peers) + 1  # peers + self
        self.majority = self.total_servers // 2 + 1

        # Timers
        self.election_timer = None
        self.heartbeat_timer = None
        self.reset_election_timer()

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
        with self.lock:
            if self.role == "leader":
                return
            self.role = "candidate"
            self.currentTerm += 1
            self.votedFor = self.server_id
            self.votes_received = 1  # self-vote
            election_term = self.currentTerm
            self.reset_election_timer()

            last_log_term = 0
            if self.last_applied > 0:
                last_log_term = self.log[self.last_applied].term

        # Send RequestVote to all peers concurrently
        for peer_id, addr in self.peers.items():
            t = threading.Thread(
                target=self._request_vote_from_peer,
                args=(peer_id, addr, election_term, last_log_term),
            )
            t.daemon = True
            t.start()

    def _request_vote_from_peer(self, peer_id, addr, election_term, last_log_term):
        try:
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)

            request = raft_pb2.RequestVoteArgs(
                term=election_term,
                candidateId=self.server_id,
                lastLogIndex=self.last_applied,
                lastLogTerm=last_log_term,
            )
            response = stub.RequestVote(request, timeout=2)
            channel.close()

            with self.lock:
                # Abort if no longer candidate or term changed
                if self.role != "candidate" or self.currentTerm != election_term:
                    return
                # Step down if higher term discovered
                if response.term > self.currentTerm:
                    self.become_follower(response.term, peer_id)
                    return
                if response.voteGranted:
                    self.votes_received += 1
                    if self.votes_received >= self.majority:
                        self.become_leader()
        except:
            pass

    def become_follower(self, new_term, leader_id):
        if self.currentTerm != new_term:
            self.votedFor = None
        self.currentTerm = new_term
        self.role = "follower"
        self.leaderId = leader_id
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.reset_election_timer()
        self.leader_event.clear()

    def become_leader(self):
        self.role = "leader"
        self.leaderId = self.server_id

        # initialize next_index (last log index + 1) and match_index (init 0)
        self.next_index = {peer_id: self.last_applied + 1 for peer_id in self.peers}
        self.match_index = {peer_id: 0 for peer_id in self.peers}
        self.leader_event.set()

        if self.election_timer:
            self.election_timer.cancel()
        self.send_heartbeats()

    def send_heartbeats(self):
        with self.lock:
            if self.role != "leader":
                return
            current_term = self.currentTerm

        for peer_id, addr in self.peers.items():
            t = threading.Thread(
                target=self._send_heartbeat_to_peer, args=(peer_id, addr, current_term)
            )
            t.daemon = True
            t.start()

        # Schedule next heartbeat (75ms, well within 150ms min election timeout)
        self.heartbeat_timer = threading.Timer(0.075, self.send_heartbeats)
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()

    def _send_heartbeat_to_peer(self, peer_id, addr, term):
        try:
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            request = raft_pb2.AppendEntriesArgs(
                term=term,
                leaderId=self.server_id,
                prevLogIndex=0,
                prevLogTerm=0,
                leaderCommit=self.commit_index,
            )
            response = stub.AppendEntries(request, timeout=2)
            channel.close()

            with self.lock:
                if response.term > self.currentTerm:
                    self.become_follower(response.term, peer_id)
        except:
            pass

    def GetState(self, request, context):
        with self.lock:
            return raft_pb2.State(
                term=self.currentTerm,
                isLeader=(self.role == "leader"),
                commitIndex=self.commit_index,
                lastApplied=self.last_applied,
            )

    def Get(self, request, context):
        """Handle client GET request - all servers can serve reads"""
        with self.lock:
            # Read from local state machine
            key = request.arg
            value = self.state_machine.get(key, "")
            return raft_pb2.KeyValue(key=key, value=value)

    def Put(self, request, context):
        """Handle client PUT request"""
        with self.lock:
            if self.role != "leader":
                return raft_pb2.GenericResponse(success=False, error="Not leader")

            # Create log entry with all fields from request
            entry = raft_pb2.LogEntry(
                term=self.currentTerm,
                key=request.key,
                value=request.value,
                clientId=request.clientId,
                requestId=request.requestId,
            )

            # Append to log
            self.log.append(entry)
            log_index = len(self.log) - 1

        # replicate logs with followers
        for peer_id, addr in self.peers.items():
            t = threading.Thread(
                target=self._replicate_log_to_peer,
                args=(peer_id, addr, log_index, self.currentTerm),
            )
            t.daemon = True
            t.start()

        # Wait for commitment (simplified - actual implementation
        # should wait for replication AND check leadership status)
        # In practice:
        # - Wait until commit_index >= log_index
        # - Continuously check if still leader
        # - Return error if stepped down before commit
        while self.commit_index < log_index and self.leader_event.is_set():
            time.sleep(0.01)  # avoid spinning forever

        with self.lock:
            if self.role != "leader":
                return raft_pb2.GenericResponse(success=False, error="Not leader")

        return raft_pb2.GenericResponse(success=True)

    def _replicate_log_to_peer(self, peer_id, addr, log_index, term):
        """Replicate a log entry to a peer server"""

        # ask for entries to be appended.
        # if log consistency fails, send a deeper log
        with grpc.insecure_channel(addr) as channel:
            stub = raft_pb2_grpc.KeyValueStoreStub(channel)
            while self.next_index[peer_id] >= 0 and self.leader_event.is_set():
                next_index = self.next_index[peer_id]
                prev_log_term = self.log[next_index - 1].term if next_index > 1 else 0
                request = raft_pb2.AppendEntriesArgs(
                    term=term,
                    leaderId=self.server_id,
                    prevLogIndex=next_index - 1,
                    prevLogTerm=prev_log_term,
                    leaderCommit=self.commit_index,
                    entries=self.log[next_index:],
                )
                response = stub.AppendEntries(request, timeout=2)

                with self.lock:
                    # Abort if no longer leader
                    if self.role != "leader":
                        return
                    # Step down if higher term discovered
                    elif response.term > self.currentTerm:
                        self.become_follower(response.term, peer_id)
                        return
                    elif response.success:
                        # update correspondng match_index
                        self.match_index[peer_id] = log_index
                        self.next_index[peer_id] = log_index + 1

                        # want to check how many others have updated their log
                        replicated_count = 1 + sum(
                            1
                            for m_index in self.match_index.values()
                            if m_index >= log_index
                        )

                        # if majority, then we update commit index
                        if replicated_count >= self.majority:
                            # commit this index
                            self.commit_index = max(self.commit_index, log_index)
                            self.apply_committed_entries()  # commit on leader
                            self.send_heartbeats()  # signal nodes to commit
                        return
                    else:
                        # lower the next_index and try again
                        self.next_index[peer_id] -= 1

    def RequestVote(self, request, context):
        with self.lock:
            candidate_term = request.term
            candidate_id = request.candidateId
            last_log_index = request.lastLogIndex
            last_log_term = request.lastLogTerm

            # check term
            if candidate_term > self.currentTerm:
                self.become_follower(candidate_term, candidate_id)

            # Check the log
            if last_log_index > 0:
                if last_log_index < len(self.log) or (
                    last_log_index == self.last_applied
                    and self.log[last_log_index].term > last_log_term
                ):
                    return raft_pb2.RequestVoteReply(
                        term=self.currentTerm, voteGranted=False
                    )

            vote_granted = False
            if candidate_term >= self.currentTerm and (
                self.votedFor is None or self.votedFor == candidate_id
            ):
                vote_granted = True
                self.votedFor = candidate_id
                self.reset_election_timer()

            return raft_pb2.RequestVoteReply(
                term=self.currentTerm, voteGranted=vote_granted
            )

    def AppendEntries(self, request, context):
        with self.lock:
            leader_term = request.term
            leader_id = request.leaderId
            prev_log_index = request.prevLogIndex
            prev_log_term = request.prevLogTerm

            # Reject stale term
            if leader_term < self.currentTerm:
                return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=False)

            # Higher term: update and reset vote
            if leader_term > self.currentTerm:
                self.currentTerm = leader_term
                self.votedFor = None

            # Step down from candidate/leader, accept this leader !! wrap with become_follower ? !!
            if self.role != "follower":
                self.become_follower(leader_term, leader_id)
            else:
                self.leaderId = leader_id
                self.reset_election_timer()

            # Check log consistency
            if prev_log_index > 0:
                if (
                    prev_log_index >= len(self.log)
                    or self.log[prev_log_index].term != prev_log_term
                ):
                    return raft_pb2.AppendEntriesReply(
                        term=self.currentTerm, success=False
                    )

            # Append new entries
            for i, entry in enumerate(request.entries):
                log_index = request.prevLogIndex + i + 1
                if log_index < len(self.log):
                    # Remove conflicting entry and all that follows
                    if self.log[log_index].term != entry.term:
                        self.log = self.log[:log_index]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            # Update commit index
            if request.leaderCommit > self.commit_index:
                self.commit_index = min(request.leaderCommit, len(self.log) - 1)
                self.apply_committed_entries()

            return raft_pb2.AppendEntriesReply(term=self.currentTerm, success=True)

    def apply_committed_entries(self):
        """Apply committed entries to state machine"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            # Apply to state machine using key and value from LogEntry
            self.state_machine[entry.key] = entry.value


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
