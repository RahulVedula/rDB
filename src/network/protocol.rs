use crate::core::{Key, Value, TransactionId, NodeId, Result, KvError};
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// Network protocol messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    // Raft protocol messages
    RequestVote {
        term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        next_index: u64,
    },
    
    // Key-value store operations
    GetRequest {
        key: Key,
        transaction_id: TransactionId,
        consistency_level: ConsistencyLevel,
    },
    GetResponse {
        value: Option<Value>,
        success: bool,
        error: Option<String>,
    },
    PutRequest {
        key: Key,
        value: Value,
        transaction_id: TransactionId,
        consistency_level: WriteConsistency,
    },
    PutResponse {
        success: bool,
        error: Option<String>,
    },
    DeleteRequest {
        key: Key,
        transaction_id: TransactionId,
        consistency_level: WriteConsistency,
    },
    DeleteResponse {
        success: bool,
        error: Option<String>,
    },
    ScanRequest {
        start_key: Key,
        end_key: Option<Key>,
        limit: Option<u64>,
        transaction_id: TransactionId,
    },
    ScanResponse {
        results: Vec<KeyValue>,
        success: bool,
        error: Option<String>,
    },
    
    // Transaction management
    BeginTransactionRequest,
    BeginTransactionResponse {
        transaction_id: TransactionId,
        success: bool,
        error: Option<String>,
    },
    CommitTransactionRequest {
        transaction_id: TransactionId,
    },
    CommitTransactionResponse {
        success: bool,
        error: Option<String>,
    },
    AbortTransactionRequest {
        transaction_id: TransactionId,
        reason: String,
    },
    AbortTransactionResponse {
        success: bool,
        error: Option<String>,
    },
    
    // Cluster management
    JoinClusterRequest {
        node_id: NodeId,
        address: NodeAddress,
    },
    JoinClusterResponse {
        success: bool,
        cluster_config: Option<ClusterConfig>,
        error: Option<String>,
    },
    LeaveClusterRequest {
        node_id: NodeId,
    },
    LeaveClusterResponse {
        success: bool,
        error: Option<String>,
    },
    
    // Health check
    PingRequest,
    PingResponse {
        node_id: NodeId,
        timestamp: DateTime<Utc>,
    },
}

/// Log entry for Raft protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Command,
    pub timestamp: DateTime<Utc>,
}

/// Commands that can be applied to the state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Put { key: Key, value: Value },
    Delete { key: Key },
    NoOp,
}

/// Consistency levels
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    Eventual,
    Majority,
    Strong,
}

/// Write consistency levels
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum WriteConsistency {
    Any,
    Majority,
    All,
}

/// Key-value pair
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: Key,
    pub value: Value,
}

/// Node address
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeAddress {
    pub host: String,
    pub port: u16,
}

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub nodes: std::collections::HashMap<NodeId, NodeAddress>,
    pub leader: Option<NodeId>,
}

/// Message wrapper with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageWrapper {
    pub message: Message,
    pub from: NodeId,
    pub to: NodeId,
    pub timestamp: DateTime<Utc>,
    pub message_id: u64,
}

impl MessageWrapper {
    pub fn new(message: Message, from: NodeId, to: NodeId) -> Self {
        Self {
            message,
            from,
            to,
            timestamp: Utc::now(),
            message_id: 0, 
        }
    }
}

/// Network protocol handler trait
pub trait ProtocolHandler {
    async fn handle_message(&self, message: MessageWrapper) -> Result<Option<Message>>;
}

pub struct MessageSerializer;

impl MessageSerializer {
    pub fn serialize(message: &MessageWrapper) -> Result<Vec<u8>> {
        bincode::serialize(message).map_err(|e| KvError::SerializationError(e.to_string()))
    }
    
    pub fn deserialize(data: &[u8]) -> Result<MessageWrapper> {
        bincode::deserialize(data).map_err(|e| KvError::SerializationError(e.to_string()))
    }
    
    pub fn serialize_message(message: &Message) -> Result<Vec<u8>> {
        bincode::serialize(message).map_err(|e| KvError::SerializationError(e.to_string()))
    }
    
    pub fn deserialize_message(data: &[u8]) -> Result<Message> {
        bincode::deserialize(data).map_err(|e| KvError::SerializationError(e.to_string()))
    }
}
