# Scalable Distributed Database

A high-performance, distributed transactional key-value store built in Rust with the following features:

- **ACID Transactions**: Full ACID compliance with MVCC (Multi-Version Concurrency Control)
- **Distributed Consensus**: Raft consensus protocol for cluster coordination
- **High Availability**: Automatic failover and leader election
- **Consistency Levels**: Configurable read and write consistency levels
- **Write-Ahead Logging**: Durability guarantees with WAL
- **HTTP API**: RESTful API for easy integration
- **Client Library**: Easy-to-use Rust client library

## Architecture

The system consists of several key components:

- **Storage Engine**: MVCC-based storage with WAL for durability
- **Consensus Layer**: Raft protocol for distributed coordination
- **Network Layer**: Inter-node communication and RPC
- **API Server**: HTTP REST API for client interactions
- **Client Library**: High-level client for application integration

