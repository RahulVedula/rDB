use crate::core::{Key, Value, TransactionId, Result, KvError, ConsistencyLevel, WriteConsistency};
use crate::storage::StorageEngine;
use serde::{Deserialize, Serialize};
use warp::Filter;
use std::sync::Arc;
use std::convert::Infallible;

/// HTTP API server for the key-value store
pub struct HttpApiServer {
    pub storage_engine: Arc<StorageEngine>,
    pub port: u16,
}

impl HttpApiServer {
    pub fn new(storage_engine: Arc<StorageEngine>, port: u16) -> Self {
        Self {
            storage_engine,
            port,
        }
    }

    /// Start the HTTP API server
    pub async fn start(&self) -> Result<()> {
        let storage = self.storage_engine.clone();
        
        // GET /health
        let health = warp::path("health")
            .and(warp::get())
            .and(with_storage(storage.clone()))
            .and_then(health_handler);
        
        // GET /stats
        let stats = warp::path("stats")
            .and(warp::get())
            .and(with_storage(storage.clone()))
            .and_then(stats_handler);
        
        // POST /transaction/begin
        let begin_transaction = warp::path("transaction")
            .and(warp::path("begin"))
            .and(warp::post())
            .and(with_storage(storage.clone()))
            .and_then(begin_transaction_handler);
        
        // POST /transaction/commit
        let commit_transaction = warp::path("transaction")
            .and(warp::path("commit"))
            .and(warp::post())
            .and(warp::body::json())
            .and(with_storage(storage.clone()))
            .and_then(commit_transaction_handler);
        
        // GET /key/{key}
        let get_key = warp::path("key")
            .and(warp::path::param::<String>())
            .and(warp::get())
            .and(warp::query::<GetQuery>())
            .and(with_storage(storage.clone()))
            .and_then(get_key_handler);
        
        // PUT /key/{key}
        let put_key = warp::path("key")
            .and(warp::path::param::<String>())
            .and(warp::put())
            .and(warp::body::json())
            .and(with_storage(storage.clone()))
            .and_then(put_key_handler);
        
        // DELETE /key/{key}
        let delete_key = warp::path("key")
            .and(warp::path::param::<String>())
            .and(warp::delete())
            .and(warp::query::<DeleteQuery>())
            .and(with_storage(storage.clone()))
            .and_then(delete_key_handler);
        
        let routes = health
            .or(stats)
            .or(begin_transaction)
            .or(commit_transaction)
            .or(get_key)
            .or(put_key)
            .or(delete_key)
            .with(warp::cors().allow_any_origin().allow_headers(vec!["content-type"]).allow_methods(vec!["GET", "POST", "PUT", "DELETE"]));
        
        println!("HTTP API server starting on port {}", self.port);
        warp::serve(routes)
            .run(([0, 0, 0, 0], self.port))
            .await;
        
        Ok(())
    }
}

/// Helper function to inject storage engine into handlers
fn with_storage(storage: Arc<StorageEngine>) -> impl Filter<Extract = (Arc<StorageEngine>,), Error = Infallible> + Clone {
    warp::any().map(move || storage.clone())
}

/// Health check handler
async fn health_handler(storage: Arc<StorageEngine>) -> std::result::Result<Box<dyn warp::Reply>, Infallible> {
    match storage.get_stats() {
        Ok(stats) => Ok(Box::new(warp::reply::json(&HealthResponse {
            status: "healthy".to_string(),
            total_keys: stats.total_keys,
        }))),
        Err(e) => Ok(Box::new(warp::reply::with_status(
            warp::reply::json(&format!("Error: {}", e)),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ))),
    }
}

/// Stats handler
async fn stats_handler(storage: Arc<StorageEngine>) -> std::result::Result<Box<dyn warp::Reply>, Infallible> {
    match storage.get_stats() {
        Ok(stats) => Ok(Box::new(warp::reply::json(&stats))),
        Err(e) => Ok(Box::new(warp::reply::with_status(
            warp::reply::json(&format!("Error: {}", e)),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ))),
    }
}

/// Begin transaction handler
async fn begin_transaction_handler(storage: Arc<StorageEngine>) -> std::result::Result<Box<dyn warp::Reply>, Infallible> {
    match storage.begin_transaction() {
        Ok(transaction_id) => Ok(Box::new(warp::reply::json(&BeginTransactionResponse {
            transaction_id,
            success: true,
        }))),
        Err(e) => Ok(Box::new(warp::reply::with_status(
            warp::reply::json(&BeginTransactionResponse {
                transaction_id: TransactionId::new(),
                success: false,
            }),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ))),
    }
}

/// Commit transaction handler
async fn commit_transaction_handler(
    request: CommitTransactionRequest,
    storage: Arc<StorageEngine>,
) -> std::result::Result<Box<dyn warp::Reply>, Infallible> {
    match storage.commit_transaction(request.transaction_id) {
        Ok(_) => Ok(Box::new(warp::reply::json(&TransactionResponse { success: true }))),
        Err(e) => Ok(Box::new(warp::reply::with_status(
            warp::reply::json(&TransactionResponse { success: false }),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ))),
    }
}

/// Get key handler
async fn get_key_handler(
    key: String,
    query: GetQuery,
    storage: Arc<StorageEngine>,
) -> std::result::Result<Box<dyn warp::Reply>, Infallible> {
    let key = Key::from_string(key);
    
    match storage.read(&key, query.transaction_id, query.consistency.unwrap_or(ConsistencyLevel::Eventual)) {
        Ok(value) => Ok(Box::new(warp::reply::json(&GetResponse {
            value,
            success: true,
        }))),
        Err(e) => Ok(Box::new(warp::reply::with_status(
            warp::reply::json(&GetResponse {
                value: None,
                success: false,
            }),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ))),
    }
}

/// Put key handler
async fn put_key_handler(
    key: String,
    request: PutRequest,
    storage: Arc<StorageEngine>,
) -> std::result::Result<Box<dyn warp::Reply>, Infallible> {
    let key = Key::from_string(key);
    let value = Value::from_string(request.value, 0);
    
    match storage.write(key, value, request.transaction_id, request.consistency.unwrap_or(WriteConsistency::Majority)) {
        Ok(_) => Ok(Box::new(warp::reply::json(&PutResponse { success: true }))),
        Err(e) => Ok(Box::new(warp::reply::with_status(
            warp::reply::json(&PutResponse { success: false }),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ))),
    }
}

/// Delete key handler
async fn delete_key_handler(
    key: String,
    query: DeleteQuery,
    storage: Arc<StorageEngine>,
) -> std::result::Result<Box<dyn warp::Reply>, Infallible> {
    let key = Key::from_string(key);
    
    match storage.delete(&key, query.transaction_id, query.consistency.unwrap_or(WriteConsistency::Majority)) {
        Ok(_) => Ok(Box::new(warp::reply::json(&DeleteResponse { success: true }))),
        Err(e) => Ok(Box::new(warp::reply::with_status(
            warp::reply::json(&DeleteResponse { success: false }),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ))),
    }
}

/// Request/Response types for HTTP API

#[derive(Debug, Deserialize)]
pub struct GetQuery {
    pub transaction_id: TransactionId,
    pub consistency: Option<ConsistencyLevel>,
}

#[derive(Debug, Serialize)]
pub struct GetResponse {
    pub value: Option<Value>,
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct PutRequest {
    pub value: String,
    pub transaction_id: TransactionId,
    pub consistency: Option<WriteConsistency>,
}

#[derive(Debug, Serialize)]
pub struct PutResponse {
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct DeleteQuery {
    pub transaction_id: TransactionId,
    pub consistency: Option<WriteConsistency>,
}

#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    pub success: bool,
}

#[derive(Debug, Serialize)]
pub struct BeginTransactionResponse {
    pub transaction_id: TransactionId,
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct CommitTransactionRequest {
    pub transaction_id: TransactionId,
}

#[derive(Debug, Serialize)]
pub struct TransactionResponse {
    pub success: bool,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub total_keys: usize,
}