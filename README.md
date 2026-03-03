# Assignment One

1. raft.proto defines the contracts for message and service types --> langauge agnostic
2. use raft.proto to generate the raft_pb2.py and raft_pb2_grpc.py; these files
have the base classes for client/server and client stubs --> can be generated in variety of supported languages
3. frontend.py implements the service base classes defined in generated base protobuf files (also uses the base classes for the message types)
4. server.py does the same for its base classes
5. a1_tests.py is the client, frontend/server are gRPC servers... RPC calls made from client to server,
frontend is used to spawn the servers... that is all


How does a1_tests.py make RPC calls?
- It creates a client stub then creates a channel to the port on which frontend/server is running,
then makes calls based on the APIs defined by, again, the proto file


# Assignment Two

1. Updated raft.proto to modify Get/Put definitions for KeyValueStore service
2. Modified frontend to:
- read config.ini, get active servers as list
- be able to call ping using KeyValueStore stub
- check if specific server id is in list of active servers AND in frontend's dict of processes
- use KeyValueStore stub to call Get and Put operations from frontend to specifc server_id
3. Modified server to:
- Have server_id, lock, and key-value store as member vars
- implement get and store operations with thread saftey


# Assignment Three

1. Updated raft.proto to add Raft RPCs: AppendEntries, RequestVote, and supporting message types (LogEntry, State, etc.)
2. Modified server to:
- Load peer addresses from config.ini on startup
- Maintain Raft state: currentTerm, votedFor, role (follower/candidate/leader), leaderId
- Randomized election timeout (150-300ms) using threading.Timer
- start_election: increment term, vote for self, send RequestVote RPCs to all peers concurrently via threads
- become_leader: on receiving majority votes, transition to leader and begin sending heartbeats
- send_heartbeats: periodic AppendEntries (75ms interval) to all peers to maintain leadership
- AppendEntries handler: reset election timer, step down if valid leader exists, reject stale terms
- RequestVote handler: grant vote if candidate term >= current and haven't voted for someone else this term
3. Modified frontend to:
- find_leader_server: query GetState on all active servers to find the leader
- Route Get/Put requests to leader instead of any available server


# Assignment Four

1. Implemented log replication:
- Put: leader appends entry to log, spawns replication threads per peer
- _replicate_to_peer: sends AppendEntries with entries from nextIndex onwards, retries with decremented nextIndex on log inconsistency
- update_commit_index: after majority ack, advances commitIndex and applies committed entries to state machine
- Followers learn committed index via leaderCommit in heartbeats, then apply entries locally
2. State variables added:
- log: list of LogEntry (index 0 is sentinel), commitIndex, lastApplied, state_machine
- next_index, match_index: leader-only, reinitialized on election


## Example Workflow: PUT("x", "42") on a 3-server cluster (S0, S1, S2)

### Phase 1: Startup
All three servers boot as followers with empty logs and randomized election timers (150-300ms).

### Phase 2: Leader Election
S1 times out first. `start_election()` fires:
- role → candidate, currentTerm → 1, votes for self
- Sends RequestVote to S0 and S2 concurrently via threads

S0 and S2 grant votes (haven't voted this term, logs are up-to-date).
S1 reaches majority → `become_leader()`:
- next_index = {S0: 1, S1: 1}, match_index = {S0: 0, S2: 0}
- Cancels election timer, starts sending heartbeats every 75ms

### Phase 3: Heartbeats (steady state)
S1 sends empty AppendEntries to S0 and S2 every 75ms. Followers reset their election timers on receipt, preventing new elections.

### Phase 4: Client PUT
Frontend queries GetState on all servers → finds S1 is leader → forwards PUT to S1.

`S1.Put()`:
- Creates LogEntry(term=1, key="x", value="42")
- Appends to log: `log = [None, e1]`
- Spawns replication threads for S0 and S2
- Returns success immediately

### Phase 5: Replication
`_replicate_to_peer(S0)` thread runs:
- ni=1, prev_index=0, entries=[e1]
- Sends AppendEntries(prevLogIndex=0, prevLogTerm=0, entries=[e1], leaderCommit=0)

S0's AppendEntries handler:
- Accepts (leader term valid)
- prevLogIndex=0 → skip consistency check
- Appends e1: `S0.log = [None, e1]`
- leaderCommit(0) == commitIndex(0) → nothing applied yet
- Returns success

### Phase 6: Leader Commits
Back on S1 after both S0 and S2 acknowledge:
- match_index = {S0: 1, S2: 1}
- `update_commit_index()`: n=1, replicated = 1(self) + 2(peers) = 3 > 3//2=1 → commitIndex=1
- `apply_committed_entries()`: lastApplied 0→1, state_machine["x"] = "42"

### Phase 7: Followers Apply
Next heartbeat from S1 carries leaderCommit=1.

S0 and S2's AppendEntries handler:
- leaderCommit(1) > commitIndex(0) → commitIndex=1
- `apply_committed_entries()`: state_machine["x"] = "42"

### Phase 8: GET
Client GET("x") → frontend routes to leader S1 → `Get()` reads state_machine["x"] → returns "42".
Any follower would also return "42" (within one heartbeat interval of the commit).

```
Client PUT("x","42")
    → Frontend finds leader via GetState
    → S1.Put(): append to log, spawn replication threads
        → _replicate_to_peer: AppendEntries with entries to S0, S2
            → S0/S2: append to log, return success
        → Leader: update next_index/match_index → update_commit_index → apply to state_machine
    → Next heartbeat carries leaderCommit=1
        → S0/S2: advance commitIndex → apply to state_machine
Client GET("x") → "42"
```
