# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CS380D Distributed Systems course project: a Raft consensus protocol implementation in Python using gRPC. The system is a distributed key-value store with a Frontend coordinator and multiple Server (Raft node) processes.

## Commands

```bash
# Activate virtual environment (Python 3.13)
source venv/bin/activate

# Regenerate protobuf/gRPC stubs after modifying raft.proto
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto

# Run tests (each assignment has its own test file)
python3 a1_tests.py
python3 a2_tests.py
python3 a3_tests.py

# Run individual services manually
python3 frontend.py
python3 server.py <server_id>
```

Tests manage their own process lifecycle: they start the frontend, spawn servers via `StartRaft` RPC, run test cases, then clean up all processes. No manual setup is needed.

## Architecture

**Two gRPC services** defined in `raft.proto`:

- **FrontEnd** (`frontend.py`, port 8001) — Coordinator that spawns server subprocesses, routes client Get/Put requests to available servers, manages server lifecycle. Reads `config.ini` for active server list and port configuration.

- **KeyValueStore** (`server.py`, port 9001 + server_id) — Individual Raft nodes. Each maintains an in-memory key-value store with thread-safe access (RLock). Implements Raft RPCs: `AppendEntries` and `RequestVote` for consensus, plus `Get`, `Put`, `GetState`, and `ping`.

**Request flow:** Test client → Frontend (8001) → Server (9001+N) → response back

**Generated files** (`raft_pb2.py`, `raft_pb2_grpc.py`) are auto-generated from `raft.proto` — do not edit directly.

## Configuration

- `config.ini` — Runtime config: base address (127.0.0.1), server base port (9001), active server IDs, max gRPC workers
- `test_config.json` — Test harness config: startup commands, ports, timeouts (RPC: 8s, leader election: 15s)

## Assignment Progression

- **A1**: Basic gRPC infrastructure (complete)
- **A2**: Key-value store Get/Put operations (complete, tests passing)
- **A3**: Raft consensus — leader election, log replication, term management (in progress on `feat/milan`)
