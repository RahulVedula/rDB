use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key(pub Vec<u8>);

impl Key {
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn from_string(s: String) -> Self {
        Self(s.into_bytes())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<String> for Key {
    fn from(s: String) -> Self {
        Self::from_string(s)
    }
}

impl From<&str> for Key {
    fn from(s: &str) -> Self {
        Self::from_string(s.to_string())
    }
}

/// A value in the key-value store
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Value {
    pub data: Vec<u8>,
    pub version: u64,
    pub timestamp: DateTime<Utc>,
}

impl Value {
    pub fn new(data: Vec<u8>, version: u64) -> Self {
        Self {
            data,
            version,
            timestamp: Utc::now(),
        }
    }

    pub fn from_string(s: String, version: u64) -> Self {
        Self::new(s.into_bytes(), version)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self::from_string(s, 0)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::from_string(s.to_string(), 0)
    }
}

/// Transaction ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId(pub Uuid);

impl TransactionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        Self::new()
    }
}

/// Node ID in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl NodeId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

/// Read consistency levels
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