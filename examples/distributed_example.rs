use distributed_kv_store::client::KvClient;
use distributed_kv_store::core::{ConsistencyLevel, WriteConsistency};
use std::sync::Arc;
use tokio;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Create clients for multiple servers
    let servers = vec![
        "127.0.0.1:9000".to_string(),
        "127.0.0.1:9001".to_string(),
        "127.0.0.1:9002".to_string(),
    ];
    
    let client1 = Arc::new(KvClient::new(vec![servers[0].clone()]));
    let client2 = Arc::new(KvClient::new(vec![servers[1].clone()]));
    let client3 = Arc::new(KvClient::new(vec![servers[2].clone()]));

    // Connect all clients
    client1.connect().await?;
    client2.connect().await?;
    client3.connect().await?;
    println!("All clients connected");

    // Example 1: Distributed writes with different consistency levels
    println!("\n=== Distributed Writes ===");
    
    let transaction1 = client1.begin_transaction().await?;
    client1.put("distributed:key1", "value1", transaction1, Some(WriteConsistency::Majority)).await?;
    client1.commit(transaction1).await?;
    println!("Client 1 wrote with Majority consistency");

    let transaction2 = client2.begin_transaction().await?;
    client2.put("distributed:key2", "value2", transaction2, Some(WriteConsistency::All)).await?;
    client2.commit(transaction2).await?;
    println!("Client 2 wrote with All consistency");

    let transaction3 = client3.begin_transaction().await?;
    client3.put("distributed:key3", "value3", transaction3, Some(WriteConsistency::Any)).await?;
    client3.commit(transaction3).await?;
    println!("Client 3 wrote with Any consistency");

    // Example 2: Distributed reads with different consistency levels
    println!("\n=== Distributed Reads ===");
    
    let read_transaction1 = client1.begin_transaction().await?;
    let value1 = client1.get("distributed:key1", read_transaction1, Some(ConsistencyLevel::Strong)).await?;
    client1.commit(read_transaction1).await?;
    println!("Client 1 read with Strong consistency: {:?}", value1);

    let read_transaction2 = client2.begin_transaction().await?;
    let value2 = client2.get("distributed:key2", read_transaction2, Some(ConsistencyLevel::Majority)).await?;
    client2.commit(read_transaction2).await?;
    println!("Client 2 read with Majority consistency: {:?}", value2);

    let read_transaction3 = client3.begin_transaction().await?;
    let value3 = client3.get("distributed:key3", read_transaction3, Some(ConsistencyLevel::Eventual)).await?;
    client3.commit(read_transaction3).await?;
    println!("Client 3 read with Eventual consistency: {:?}", value3);

    // Example 3: Concurrent transactions
    println!("\n=== Concurrent Transactions ===");
    
    let client1_clone = client1.clone();
    let client2_clone = client2.clone();
    let client3_clone = client3.clone();
    
    let task1 = tokio::spawn(async move {
        let transaction = client1_clone.begin_transaction().await?;
        client1_clone.put("concurrent:task1", "data1", transaction, Some(WriteConsistency::Majority)).await?;
        sleep(Duration::from_millis(100)).await; // Simulate some work
        client1_clone.commit(transaction).await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    
    let task2 = tokio::spawn(async move {
        let transaction = client2_clone.begin_transaction().await?;
        client2_clone.put("concurrent:task2", "data2", transaction, Some(WriteConsistency::Majority)).await?;
        sleep(Duration::from_millis(150)).await; // Simulate some work
        client2_clone.commit(transaction).await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    
    let task3 = tokio::spawn(async move {
        let transaction = client3_clone.begin_transaction().await?;
        client3_clone.put("concurrent:task3", "data3", transaction, Some(WriteConsistency::Majority)).await?;
        sleep(Duration::from_millis(200)).await; // Simulate some work
        client3_clone.commit(transaction).await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    
    // Wait for all tasks to complete
    let results = tokio::try_join!(task1, task2, task3)?;
    println!("All concurrent transactions completed: {:?}", results);

    // Example 4: Read from different clients to verify consistency
    println!("\n=== Consistency Verification ===");
    
    let verify_transaction1 = client1.begin_transaction().await?;
    let task1_data = client1.get("concurrent:task1", verify_transaction1, Some(ConsistencyLevel::Strong)).await?;
    client1.commit(verify_transaction1).await?;
    
    let verify_transaction2 = client2.begin_transaction().await?;
    let task2_data = client2.get("concurrent:task2", verify_transaction2, Some(ConsistencyLevel::Strong)).await?;
    client2.commit(verify_transaction2).await?;
    
    let verify_transaction3 = client3.begin_transaction().await?;
    let task3_data = client3.get("concurrent:task3", verify_transaction3, Some(ConsistencyLevel::Strong)).await?;
    client3.commit(verify_transaction3).await?;
    
    println!("Verified data from all clients:");
    println!("  Task 1 data: {:?}", task1_data);
    println!("  Task 2 data: {:?}", task2_data);
    println!("  Task 3 data: {:?}", task3_data);

    // Example 5: Scan operations from different clients
    println!("\n=== Distributed Scan Operations ===");
    
    let scan_transaction1 = client1.begin_transaction().await?;
    let distributed_keys = client1.scan("distributed:", None, Some(10), scan_transaction1).await?;
    client1.commit(scan_transaction1).await?;
    println!("Client 1 scan results: {:?}", distributed_keys);
    
    let scan_transaction2 = client2.begin_transaction().await?;
    let concurrent_keys = client2.scan("concurrent:", None, Some(10), scan_transaction2).await?;
    client2.commit(scan_transaction2).await?;
    println!("Client 2 scan results: {:?}", concurrent_keys);

    // Example 6: Cluster health monitoring
    println!("\n=== Cluster Health Monitoring ===");
    
    let health1 = client1.get_cluster_health().await?;
    let health2 = client2.get_cluster_health().await?;
    let health3 = client3.get_cluster_health().await?;
    
    println!("Cluster health from different clients:");
    println!("  Client 1: {:?}", health1);
    println!("  Client 2: {:?}", health2);
    println!("  Client 3: {:?}", health3);

    // Example 7: Storage statistics
    println!("\n=== Storage Statistics ===");
    
    let stats1 = client1.get_stats().await?;
    let stats2 = client2.get_stats().await?;
    let stats3 = client3.get_stats().await?;
    
    println!("Storage stats from different clients:");
    println!("  Client 1: {:?}", stats1);
    println!("  Client 2: {:?}", stats2);
    println!("  Client 3: {:?}", stats3);

    println!("\nDistributed example completed successfully!");
    Ok(())
}
