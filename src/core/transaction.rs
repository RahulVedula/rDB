use crate::core::{Key, Value, TransactionId, Result, KvError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};

// Transaction status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TransactionStatus {
    Pending,
    Committed,
    Aborted,
    Failed(String),
}
#[derive(Debug, Clone)]
pub struct Transaction {
    // Transaction Class
    pub id: TransactionId,
    pub status: TransactionStatus,
    pub created_at: DateTime<Utc>,
    pub read_set: HashMap<Key, Value>, //
    pub write_set: HashMap<Key, Value>,
}

impl Transaction {
    // Construct the transaction; default constructor
    pub fn new() -> Self {
        Self {
            id: TransactionId::new(),
            status: TransactionStatus::Pending,
            created_at: Utc::now(),
            read_set: HashMap::new(),
            write_set: HashMap::new(),
        }
    }

    pub fn add_read(&mut self, key: Key, value: Value) {
        self.read_set.insert(key, value);
    }

    pub fn add_write(&mut self, key: Key, value: Value) {
        self.write_set.insert(key, value);
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.status != TransactionStatus::Pending {
            return Err(KvError::InvalidOperation(
                "Transaction is not in pending state".to_string()
            ));
        }
        self.status = TransactionStatus::Committed;
        Ok(())
    }

    pub fn abort(&mut self, reason: String) -> Result<()> {
        if self.status != TransactionStatus::Pending {
            return Err(KvError::InvalidOperation(
                "Transaction is not in pending state".to_string()
            ));
        }
        self.status = TransactionStatus::Aborted;
        Ok(())
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}