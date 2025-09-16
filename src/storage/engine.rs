use crate::core::{Key, Value, TransactionId, Result, KvError, ConsistencyLevel, WriteConsistency};
use crate::storage::{WriteAheadLog, MvccStorage, WalEntry};
use std::path::Path;
use std::sync::{Arc, RwLock};
use sled::Db;

/// Main storage engine that combines WAL and MVCC
pub struct StorageEngine {
    mvcc: Arc<MvccStorage>,
    wal: Arc<RwLock<WriteAheadLog>>,
    persistent_db: Option<Db>,
}

impl StorageEngine {
    pub fn new<P: AsRef<Path>>(wal_path: P, persistent_path: Option<P>) -> Result<Self> {
        let wal = WriteAheadLog::new(wal_path)?;
        let mvcc = Arc::new(MvccStorage::new());
        
        let persistent_db = if let Some(path) = persistent_path {
            Some(sled::open(path)?)
        } else {
            None
        };
        
        Ok(Self {
            mvcc,
            wal: Arc::new(RwLock::new(wal)),
            persistent_db,
        })
    }

    /// Initialize the storage engine by replaying WAL entries
    pub fn initialize(&self) -> Result<()> {
        let wal = self.wal.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        let entries = wal.replay()?;
        
        // Replay all WAL entries to reconstruct the state
        for entry in entries {
            self.replay_entry(entry)?;
        }
        
        Ok(())
    }

    /// Begin a new transaction
    pub fn begin_transaction(&self) -> Result<TransactionId> {
        let transaction_id = TransactionId::new();
        let _timestamp = self.mvcc.begin_transaction(transaction_id)?;
        
        // Log transaction begin
        let entry = WalEntry::BeginTransaction {
            transaction_id,
            timestamp: chrono::Utc::now(),
        };
        self.log_entry(entry)?;
        
        Ok(transaction_id)
    }

    /// Read a value
    pub fn read(&self, key: &Key, transaction_id: TransactionId, consistency: ConsistencyLevel) -> Result<Option<Value>> {
        match consistency {
            ConsistencyLevel::Eventual => {
                // Read from any available replica (in a real implementation)
                self.mvcc.read(key, transaction_id)
            }
            ConsistencyLevel::Majority => {
                // Read from majority of replicas (in a real implementation)
                self.mvcc.read(key, transaction_id)
            }
            ConsistencyLevel::Strong => {
                // Read from all replicas (in a real implementation)
                self.mvcc.read(key, transaction_id)
            }
        }
    }

    /// Write a value
    pub fn write(&self, key: Key, value: Value, transaction_id: TransactionId, consistency: WriteConsistency) -> Result<()> {
        // Check for write conflicts
        if self.mvcc.check_write_conflict(&key, transaction_id)? {
            return Err(KvError::TransactionConflict("Write conflict detected".to_string()));
        }
        
        // Write to MVCC storage
        self.mvcc.write(key.clone(), value.clone(), transaction_id)?;
        
        // Log the write operation
        let entry = WalEntry::Put {
            transaction_id,
            key: key.clone(),
            value: value.clone(),
            timestamp: chrono::Utc::now(),
        };
        self.log_entry(entry)?;
        
        // Write to persistent storage if available
        if let Some(ref db) = self.persistent_db {
            let key_bytes = bincode::serialize(&key)?;
            let value_bytes = bincode::serialize(&value)?;
            db.insert(key_bytes, value_bytes)?;
        }
        
        Ok(())
    }

    /// Delete a key
    pub fn delete(&self, key: &Key, transaction_id: TransactionId, consistency: WriteConsistency) -> Result<()> {
        // Check for write conflicts
        if self.mvcc.check_write_conflict(key, transaction_id)? {
            return Err(KvError::TransactionConflict("Write conflict detected".to_string()));
        }
        
        // Delete from MVCC storage
        self.mvcc.delete(key, transaction_id)?;
        
        // Log the delete operation
        let entry = WalEntry::Delete {
            transaction_id,
            key: key.clone(),
            timestamp: chrono::Utc::now(),
        };
        self.log_entry(entry)?;
        
        // Delete from persistent storage if available
        if let Some(ref db) = self.persistent_db {
            let key_bytes = bincode::serialize(key)?;
            db.remove(key_bytes)?;
        }
        
        Ok(())
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        // Commit in MVCC storage
        self.mvcc.commit_transaction(transaction_id)?;
        
        // Log the commit
        let entry = WalEntry::Commit {
            transaction_id,
            timestamp: chrono::Utc::now(),
        };
        self.log_entry(entry)?;
        
        // Sync WAL to ensure durability
        self.sync_wal()?;
        
        Ok(())
    }

    /// Abort a transaction
    pub fn abort_transaction(&self, transaction_id: TransactionId, reason: String) -> Result<()> {
        // Abort in MVCC storage
        self.mvcc.abort_transaction(transaction_id)?;
        
        // Log the abort
        let entry = WalEntry::Abort {
            transaction_id,
            timestamp: chrono::Utc::now(),
        };
        self.log_entry(entry)?;
        
        Ok(())
    }

    /// Scan keys in a range
    pub fn scan(&self, start_key: &Key, end_key: Option<&Key>, limit: Option<u64>) -> Result<Vec<Key>> {
        let all_keys = self.mvcc.scan_keys()?;
        let mut result = Vec::new();
        
        for key in all_keys {
            // Check if key is in range
            if key.as_bytes() >= start_key.as_bytes() {
                if let Some(ref end) = end_key {
                    if key.as_bytes() >= end.as_bytes() {
                        break;
                    }
                }
                
                result.push(key);
                
                if let Some(limit) = limit {
                    if result.len() >= limit as usize {
                        break;
                    }
                }
            }
        }
        
        Ok(result)
    }

    /// Get storage statistics
    pub fn get_stats(&self) -> Result<crate::storage::StorageStats> {
        self.mvcc.get_stats()
    }

    /// Clean up old versions
    pub fn cleanup(&self, cutoff_time: chrono::DateTime<chrono::Utc>) -> Result<usize> {
        self.mvcc.cleanup_old_versions(cutoff_time)
    }

    /// Sync WAL to disk
    pub fn sync_wal(&self) -> Result<()> {
        let wal = self.wal.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        wal.sync()
    }

    /// Replay a WAL entry during initialization
    fn replay_entry(&self, entry: WalEntry) -> Result<()> {
        match entry {
            WalEntry::BeginTransaction { transaction_id, .. } => {
                self.mvcc.begin_transaction(transaction_id)?;
            }
            WalEntry::Put { transaction_id, key, value, .. } => {
                self.mvcc.write(key, value, transaction_id)?;
            }
            WalEntry::Delete { transaction_id, key, .. } => {
                self.mvcc.delete(&key, transaction_id)?;
            }
            WalEntry::Commit { transaction_id, .. } => {
                self.mvcc.commit_transaction(transaction_id)?;
            }
            WalEntry::Abort { transaction_id, .. } => {
                self.mvcc.abort_transaction(transaction_id)?;
            }
        }
        Ok(())
    }

    /// Log an entry to WAL
    fn log_entry(&self, entry: WalEntry) -> Result<()> {
        let wal = self.wal.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        wal.append(entry)
    }
}