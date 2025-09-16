use distributed_kv_store::client::KvClient;
use distributed_kv_store::core::{ConsistencyLevel, WriteConsistency};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Create a client
    let servers = vec!["127.0.0.1:9000".to_string()];
    let client = KvClient::new(servers);

    // Connect to the cluster
    client.connect().await?;
    println!("Connected to cluster");

    // Example 1: Simple transaction
    println!("\n=== Simple Transaction ===");
    let transaction_id = client.begin_transaction().await?;
    println!("Started transaction: {}", transaction_id.0);

    // Put some values
    client.put("user:1", "Alice", transaction_id, Some(WriteConsistency::Majority)).await?;
    client.put("user:2", "Bob", transaction_id, Some(WriteConsistency::Majority)).await?;
    client.put("user:3", "Charlie", transaction_id, Some(WriteConsistency::Majority)).await?;
    println!("Put 3 user records");

    // Get values
    let user1 = client.get("user:1", transaction_id, Some(ConsistencyLevel::Strong)).await?;
    let user2 = client.get("user:2", transaction_id, Some(ConsistencyLevel::Strong)).await?;
    let user3 = client.get("user:3", transaction_id, Some(ConsistencyLevel::Strong)).await?;
    println!("Retrieved users: {:?}, {:?}, {:?}", user1, user2, user3);

    // Commit transaction
    client.commit(transaction_id).await?;
    println!("Committed transaction");

    // Example 2: Using transaction builder
    println!("\n=== Transaction Builder ===");
    let result = client.transaction()
        .begin().await?
        .put("counter", "0", Some(WriteConsistency::Majority)).await?
        .put("status", "active", Some(WriteConsistency::Majority)).await?
        .commit().await?;
    println!("Transaction completed");

    // Example 3: Read committed data
    println!("\n=== Reading Committed Data ===");
    let read_transaction = client.begin_transaction().await?;
    let counter = client.get("counter", read_transaction, Some(ConsistencyLevel::Eventual)).await?;
    let status = client.get("status", read_transaction, Some(ConsistencyLevel::Eventual)).await?;
    println!("Counter: {:?}, Status: {:?}", counter, status);
    client.commit(read_transaction).await?;

    // Example 4: Scan operation
    println!("\n=== Scan Operation ===");
    let scan_transaction = client.begin_transaction().await?;
    let keys = client.scan("user:", None, Some(10), scan_transaction).await?;
    println!("Found keys: {:?}", keys);
    client.commit(scan_transaction).await?;

    // Example 5: Error handling
    println!("\n=== Error Handling ===");
    let error_transaction = client.begin_transaction().await?;
    
    // Try to get a non-existent key
    match client.get("non_existent", error_transaction, None).await {
        Ok(value) => println!("Unexpected value: {:?}", value),
        Err(e) => println!("Expected error: {}", e),
    }
    
    client.abort(error_transaction, "Testing error handling".to_string()).await?;
    println!("Aborted transaction");

    // Example 6: Cluster information
    println!("\n=== Cluster Information ===");
    let health = client.get_cluster_health().await?;
    println!("Cluster health: {:?}", health);
    
    let stats = client.get_stats().await?;
    println!("Storage stats: {:?}", stats);

    println!("\nAll examples completed successfully!");
    Ok(())
}
