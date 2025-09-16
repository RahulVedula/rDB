use crate::core::{Key, Value, TransactionId, Result, KvError};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use chrono::{DateTime, Utc};

/// Multi-Version Concurrency Control implementation
/// Each key can have multiple versions with different timestamps and transaction IDs

/// A versioned value with metadata
#[derive(Debug, Clone)]
pub struct VersionedValue {
    pub value: Value,
    pub transaction_id: TransactionId,
    pub created_at: DateTime<Utc>,
    pub is_committed: bool,
    pub is_deleted: bool,
}

impl VersionedValue {
    pub fn new(value: Value, transaction_id: TransactionId) -> Self {
        Self {
            value,
            transaction_id,
            created_at: Utc::now(),
            is_committed: false,
            is_deleted: false,
        }
    }

    pub fn commit(&mut self) {
        self.is_committed = true;
    }

    pub fn mark_deleted(&mut self) {
        self.is_deleted = true;
    }
}

/// MVCC storage engine
pub struct MvccStorage {
    /// Map from key to list of versions (newest first)
    data: Arc<RwLock<HashMap<Key, Vec<VersionedValue>>>>,
    /// Active transactions and their read timestamps
    active_transactions: Arc<RwLock<HashMap<TransactionId, DateTime<Utc>>>>,
}

impl MvccStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Begin a new transaction and return its read timestamp
    pub fn begin_transaction(&self, transaction_id: TransactionId) -> Result<DateTime<Utc>> {
        let read_timestamp = Utc::now();
        let mut active = self.active_transactions.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        active.insert(transaction_id, read_timestamp);
        Ok(read_timestamp)
    }

    /// Read a value for a given transaction
    pub fn read(&self, key: &Key, transaction_id: TransactionId) -> Result<Option<Value>> {
        let active = self.active_transactions.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        let read_timestamp = active.get(&transaction_id)
            .ok_or_else(|| KvError::InvalidOperation("Transaction not found".to_string()))?;

        let data = self.data.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        if let Some(versions) = data.get(key) {
            // Find the latest committed version that was created before our read timestamp
            for version in versions {
                if version.is_committed && 
                   !version.is_deleted && 
                   version.created_at <= *read_timestamp {
                    return Ok(Some(version.value.clone()));
                }
            }
        }
        
        Ok(None)
    }

    /// Write a value for a given transaction
    pub fn write(&self, key: Key, value: Value, transaction_id: TransactionId) -> Result<()> {
        let mut data = self.data.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        let versioned_value = VersionedValue::new(value, transaction_id);
        
        data.entry(key)
            .or_insert_with(Vec::new)
            .insert(0, versioned_value); // Insert at the beginning (newest first)
        
        Ok(())
    }

    /// Delete a key for a given transaction
    pub fn delete(&self, key: &Key, transaction_id: TransactionId) -> Result<()> {
        let mut data = self.data.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        if let Some(versions) = data.get_mut(key) {
            // Create a tombstone version
            let tombstone = VersionedValue {
                value: Value::new(vec![], 0), // Empty value for tombstone
                transaction_id,
                created_at: Utc::now(),
                is_committed: false,
                is_deleted: true,
            };
            versions.insert(0, tombstone);
        }
        
        Ok(())
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let mut data = self.data.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        let mut active = self.active_transactions.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        // Mark all versions for this transaction as committed
        for versions in data.values_mut() {
            for version in versions.iter_mut() {
                if version.transaction_id == transaction_id {
                    version.commit();
                }
            }
        }
        
        // Remove from active transactions
        active.remove(&transaction_id);
        
        Ok(())
    }

    /// Abort a transaction
    pub fn abort_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let mut data = self.data.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        let mut active = self.active_transactions.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        // Remove all uncommitted versions for this transaction
        for versions in data.values_mut() {
            versions.retain(|version| version.transaction_id != transaction_id);
        }
        
        // Remove empty entries
        data.retain(|_, versions| !versions.is_empty());
        
        // Remove from active transactions
        active.remove(&transaction_id);
        
        Ok(())
    }

    /// Check for write-write conflicts
    pub fn check_write_conflict(&self, key: &Key, transaction_id: TransactionId) -> Result<bool> {
        let data = self.data.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        if let Some(versions) = data.get(key) {
            // Check if there are any uncommitted versions from other transactions
            for version in versions {
                if version.transaction_id != transaction_id && !version.is_committed {
                    return Ok(true);
                }
            }
        }
        
        Ok(false)
    }

    /// Get all keys (for scanning)
    pub fn scan_keys(&self) -> Result<Vec<Key>> {
        let data = self.data.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        Ok(data.keys().cloned().collect())
    }

    /// Clean up old versions (garbage collection)
    pub fn cleanup_old_versions(&self, cutoff_time: DateTime<Utc>) -> Result<usize> {
        let mut data = self.data.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        let mut cleaned = 0;
        
        for versions in data.values_mut() {
            let original_len = versions.len();
            versions.retain(|version| {
                // Keep the latest committed version and any uncommitted versions
                version.is_committed && version.created_at > cutoff_time || !version.is_committed
            });
            cleaned += original_len - versions.len();
        }
        
        // Remove empty entries
        data.retain(|_, versions| !versions.is_empty());
        
        Ok(cleaned)
    }

    /// Get statistics about the storage
    pub fn get_stats(&self) -> Result<StorageStats> {
        let data = self.data.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        let active = self.active_transactions.read().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        let mut total_versions = 0;
        let mut committed_versions = 0;
        let mut uncommitted_versions = 0;
        
        for versions in data.values() {
            total_versions += versions.len();
            for version in versions {
                if version.is_committed {
                    committed_versions += 1;
                } else {
                    uncommitted_versions += 1;
                }
            }
        }
        
        Ok(StorageStats {
            total_keys: data.len(),
            total_versions,
            committed_versions,
            uncommitted_versions,
            active_transactions: active.len(),
        })
    }
}

/// Storage statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct StorageStats {
    pub total_keys: usize,
    pub total_versions: usize,
    pub committed_versions: usize,
    pub uncommitted_versions: usize,
    pub active_transactions: usize,
}

impl Default for MvccStorage {
    fn default() -> Self {
        Self::new()
    }
}