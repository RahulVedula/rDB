# Scalable Distributed Database

A high-performance, distributed transactional key-value store built in Rust with the following features:

- ACID Transactions: Full ACID compliance with MVCC (Multi-Version Concurrency Control)
- Distributed Consensus: Raft consensus protocol for cluster coordination
- High Availability: Automatic failover and leader election
- Consistency Levels: Configurable read and write consistency levels
- Write-Ahead Logging: Durability guarantees with WAL

## Architecture

The system consists of several key components:

- Storage Engine: MVCC-based storage with WAL for durability
- Consensus Layer: Raft protocol for distributed coordination
- Network Layer: Inter-node communication and RPC
- API Server: HTTP REST API for client interactions

## Sample (Demo coming soon)
<img width="761" height="282" alt="Screenshot 2025-09-15 at 11 15 16 PM" src="https://github.com/user-attachments/assets/80d683d4-c696-48ed-b42d-df55bed0ec60" />
