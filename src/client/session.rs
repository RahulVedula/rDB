use crate::core::{Key, Value, TransactionId, Result, KvError, ConsistencyLevel, WriteConsistency};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

/// Session for managing connections to the key-value store
#[derive(Clone)]
pub struct Session {
    pub base_url: String,
    pub client: reqwest::Client,
    pub timeout: Duration,
}

impl Session {
    pub fn new(base_url: String, client: reqwest::Client) -> Self {
        Self {
            base_url,
            client,
            timeout: Duration::from_secs(30),
        }
    }

    pub fn with_timeout(base_url: String, client: reqwest::Client, timeout: Duration) -> Self {
        Self {
            base_url,
            client,
            timeout,
        }
    }

    /// Begin a new transaction
    pub async fn begin_transaction(&self) -> Result<TransactionId> {
        let response = timeout(
            self.timeout,
            self.client.post(&format!("{}/transaction/begin", self.base_url)).send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let result: BeginTransactionResponse = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            
            if result.success {
                Ok(result.transaction_id)
            } else {
                Err(KvError::TransactionAborted("Failed to begin transaction".to_string()))
            }
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Get a value
    pub async fn get(&self, key: &str, transaction_id: TransactionId, consistency: Option<ConsistencyLevel>) -> Result<Option<String>> {
        let mut url = format!("{}/key/{}?transaction_id={}", self.base_url, key, transaction_id.0);
        
        if let Some(consistency) = consistency {
            url.push_str(&format!("&consistency={:?}", consistency));
        }

        let response = timeout(
            self.timeout,
            self.client.get(&url).send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let result: GetResponse = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            
            if result.success {
                Ok(result.value.map(|v| String::from_utf8_lossy(&v.data).to_string()))
            } else {
                Err(KvError::KeyNotFound(key.to_string()))
            }
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Put a value
    pub async fn put(&self, key: &str, value: &str, transaction_id: TransactionId, consistency: Option<WriteConsistency>) -> Result<()> {
        let request = PutRequest {
            value: value.to_string(),
            transaction_id,
            consistency,
        };

        let response = timeout(
            self.timeout,
            self.client.put(&format!("{}/key/{}", self.base_url, key))
                .json(&request)
                .send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let result: PutResponse = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            
            if result.success {
                Ok(())
            } else {
                Err(KvError::TransactionAborted("Failed to put value".to_string()))
            }
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Delete a key
    pub async fn delete(&self, key: &str, transaction_id: TransactionId, consistency: Option<WriteConsistency>) -> Result<()> {
        let mut url = format!("{}/key/{}?transaction_id={}", self.base_url, key, transaction_id.0);
        
        if let Some(consistency) = consistency {
            url.push_str(&format!("&consistency={:?}", consistency));
        }

        let response = timeout(
            self.timeout,
            self.client.delete(&url).send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let result: DeleteResponse = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            
            if result.success {
                Ok(())
            } else {
                Err(KvError::TransactionAborted("Failed to delete key".to_string()))
            }
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Scan keys
    pub async fn scan(&self, start_key: &str, end_key: Option<&str>, limit: Option<u64>, transaction_id: TransactionId) -> Result<Vec<String>> {
        let mut url = format!("{}/scan?start_key={}&transaction_id={}", self.base_url, start_key, transaction_id.0);
        
        if let Some(end_key) = end_key {
            url.push_str(&format!("&end_key={}", end_key));
        }
        
        if let Some(limit) = limit {
            url.push_str(&format!("&limit={}", limit));
        }

        let response = timeout(
            self.timeout,
            self.client.get(&url).send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let result: ScanResponse = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            
            if result.success {
                Ok(result.keys.into_iter().map(|k| String::from_utf8_lossy(&k.0).to_string()).collect())
            } else {
                Err(KvError::InternalError("Failed to scan keys".to_string()))
            }
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Commit a transaction
    pub async fn commit(&self, transaction_id: TransactionId) -> Result<()> {
        let request = CommitTransactionRequest { transaction_id };

        let response = timeout(
            self.timeout,
            self.client.post(&format!("{}/transaction/commit", self.base_url))
                .json(&request)
                .send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let result: TransactionResponse = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            
            if result.success {
                Ok(())
            } else {
                Err(KvError::TransactionAborted("Failed to commit transaction".to_string()))
            }
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Abort a transaction
    pub async fn abort(&self, transaction_id: TransactionId, reason: String) -> Result<()> {
        let request = AbortTransactionRequest { transaction_id, reason };

        let response = timeout(
            self.timeout,
            self.client.post(&format!("{}/transaction/abort", self.base_url))
                .json(&request)
                .send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let result: TransactionResponse = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            
            if result.success {
                Ok(())
            } else {
                Err(KvError::TransactionAborted("Failed to abort transaction".to_string()))
            }
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Get cluster health
    pub async fn get_cluster_health(&self) -> Result<crate::client::ClusterHealth> {
        let response = timeout(
            self.timeout,
            self.client.get(&format!("{}/health", self.base_url)).send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let health: crate::client::ClusterHealth = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            Ok(health)
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }

    /// Get storage statistics
    pub async fn get_stats(&self) -> Result<crate::client::StorageStats> {
        let response = timeout(
            self.timeout,
            self.client.get(&format!("{}/stats", self.base_url)).send()
        ).await
            .map_err(|_| KvError::TimeoutError("Request timeout".to_string()))?
            .map_err(|e| KvError::NetworkError(e.to_string()))?;

        if response.status().is_success() {
            let stats: crate::client::StorageStats = response.json().await
                .map_err(|e| KvError::SerializationError(e.to_string()))?;
            Ok(stats)
        } else {
            Err(KvError::NetworkError("Server error".to_string()))
        }
    }
}

/// Request/Response types for the session

#[derive(Debug, Serialize)]
struct PutRequest {
    value: String,
    transaction_id: TransactionId,
    consistency: Option<WriteConsistency>,
}

#[derive(Debug, Deserialize)]
struct BeginTransactionResponse {
    transaction_id: TransactionId,
    success: bool,
}

#[derive(Debug, Deserialize)]
struct GetResponse {
    value: Option<Value>,
    success: bool,
}

#[derive(Debug, Deserialize)]
struct PutResponse {
    success: bool,
}

#[derive(Debug, Deserialize)]
struct DeleteResponse {
    success: bool,
}

#[derive(Debug, Deserialize)]
struct ScanResponse {
    keys: Vec<Key>,
    success: bool,
}

#[derive(Debug, Serialize)]
struct CommitTransactionRequest {
    transaction_id: TransactionId,
}

#[derive(Debug, Serialize)]
struct AbortTransactionRequest {
    transaction_id: TransactionId,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct TransactionResponse {
    success: bool,
}
