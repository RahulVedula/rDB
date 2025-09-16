use crate::core::{Key, Value, TransactionId, NodeId, Result, KvError, ConsistencyLevel, WriteConsistency};
use crate::network::{Message, MessageWrapper, MessageSerializer};
use crate::storage::StorageEngine;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::oneshot;
use uuid::Uuid;

/// RPC client for making remote procedure calls
pub struct RpcClient {
    pub node_id: NodeId,
    pub transport: Arc<NetworkTransport>,
    pub pending_requests: Arc<RwLock<HashMap<u64, oneshot::Sender<Message>>>>,
    pub next_request_id: Arc<RwLock<u64>>,
}

impl RpcClient {
    pub fn new(node_id: NodeId, transport: Arc<NetworkTransport>) -> Self {
        Self {
            node_id,
            transport,
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            next_request_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Make a request to a remote node and wait for response
    pub async fn call(&self, to: NodeId, request: Message) -> Result<Message> {
        let request_id = self.get_next_request_id();
        let (sender, receiver) = oneshot::channel();
        
        // Store the pending request
        {
            let mut pending = self.pending_requests.write().map_err(|e| KvError::InternalError(e.to_string()))?;
            pending.insert(request_id, sender);
        }
        
        // Send the request
        self.transport.send_message(to, request).await?;
        
        // Wait for response
        match receiver.await {
            Ok(response) => Ok(response),
            Err(_) => Err(KvError::NetworkError("Request timeout".to_string())),
        }
    }

    /// Get the next request ID
    fn get_next_request_id(&self) -> u64 {
        let mut id = self.next_request_id.write().map_err(|_| KvError::InternalError("Lock error".to_string())).unwrap_or_default();
        *id += 1;
        *id
    }

    /// Handle a response message
    pub fn handle_response(&self, request_id: u64, response: Message) -> Result<()> {
        let mut pending = self.pending_requests.write().map_err(|e| KvError::InternalError(e.to_string()))?;
        
        if let Some(sender) = pending.remove(&request_id) {
            let _ = sender.send(response);
        }
        
        Ok(())
    }
}

/// RPC server for handling incoming requests
pub struct RpcServer {
    pub node_id: NodeId,
    pub storage_engine: Arc<StorageEngine>,
    pub transport: Arc<NetworkTransport>,
}

impl RpcServer {
    pub fn new(node_id: NodeId, storage_engine: Arc<StorageEngine>, transport: Arc<NetworkTransport>) -> Self {
        Self {
            node_id,
            storage_engine,
            transport,
        }
    }

    /// Start the RPC server
    pub async fn start(&self) -> Result<()> {
        loop {
            if let Some(message_wrapper) = self.transport.receive_message().await? {
                let response = self.handle_message(message_wrapper.message).await?;
                
                if let Some(response_message) = response {
                    self.transport.send_message(message_wrapper.from, response_message).await?;
                }
            }
        }
    }

    /// Handle an incoming message
    async fn handle_message(&self, message: Message) -> Result<Option<Message>> {
        match message {
            // Key-value operations
            Message::GetRequest { key, transaction_id, consistency_level } => {
                let value = self.storage_engine.read(&key, transaction_id, consistency_level).await?;
                Ok(Some(Message::GetResponse {
                    value,
                    success: true,
                    error: None,
                }))
            }
            
            Message::PutRequest { key, value, transaction_id, consistency_level } => {
                match self.storage_engine.write(key, value, transaction_id, consistency_level).await {
                    Ok(_) => Ok(Some(Message::PutResponse {
                        success: true,
                        error: None,
                    })),
                    Err(e) => Ok(Some(Message::PutResponse {
                        success: false,
                        error: Some(e.to_string()),
                    })),
                }
            }
            
            Message::DeleteRequest { key, transaction_id, consistency_level } => {
                match self.storage_engine.delete(&key, transaction_id, consistency_level).await {
                    Ok(_) => Ok(Some(Message::DeleteResponse {
                        success: true,
                        error: None,
                    })),
                    Err(e) => Ok(Some(Message::DeleteResponse {
                        success: false,
                        error: Some(e.to_string()),
                    })),
                }
            }
            
            Message::ScanRequest { start_key, end_key, limit, transaction_id } => {
                match self.storage_engine.scan(&start_key, end_key.as_ref(), limit).await {
                    Ok(keys) => {
                        let mut results = Vec::new();
                        for key in keys {
                            if let Ok(Some(value)) = self.storage_engine.read(&key, transaction_id, ConsistencyLevel::Eventual).await {
                                results.push(KeyValue { key, value });
                            }
                        }
                        Ok(Some(Message::ScanResponse {
                            results,
                            success: true,
                            error: None,
                        }))
                    }
                    Err(e) => Ok(Some(Message::ScanResponse {
                        results: Vec::new(),
                        success: false,
                        error: Some(e.to_string()),
                    })),
                }
            }
            
            // Transaction management
            Message::BeginTransactionRequest => {
                match self.storage_engine.begin_transaction().await {
                    Ok(transaction_id) => Ok(Some(Message::BeginTransactionResponse {
                        transaction_id,
                        success: true,
                        error: None,
                    })),
                    Err(e) => Ok(Some(Message::BeginTransactionResponse {
                        transaction_id: TransactionId::new(),
                        success: false,
                        error: Some(e.to_string()),
                    })),
                }
            }
            
            Message::CommitTransactionRequest { transaction_id } => {
                match self.storage_engine.commit_transaction(transaction_id).await {
                    Ok(_) => Ok(Some(Message::CommitTransactionResponse {
                        success: true,
                        error: None,
                    })),
                    Err(e) => Ok(Some(Message::CommitTransactionResponse {
                        success: false,
                        error: Some(e.to_string()),
                    })),
                }
            }
            
            Message::AbortTransactionRequest { transaction_id, reason } => {
                match self.storage_engine.abort_transaction(transaction_id, reason).await {
                    Ok(_) => Ok(Some(Message::AbortTransactionResponse {
                        success: true,
                        error: None,
                    })),
                    Err(e) => Ok(Some(Message::AbortTransactionResponse {
                        success: false,
                        error: Some(e.to_string()),
                    })),
                }
            }
            
            // Health check
            Message::PingRequest => {
                Ok(Some(Message::PingResponse {
                    node_id: self.node_id,
                    timestamp: chrono::Utc::now(),
                }))
            }
            
            // Other messages are handled by the consensus layer
            _ => Ok(None),
        }
    }
}

/// Key-value pair for network messages
#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: Key,
    pub value: Value,
}
