use pumpkin::PumpkinServer;
use pumpkin_config::{AdvancedConfiguration, BasicConfiguration};
use pumpkin::data::VanillaData;
use pumpkin_protocol::ser::NetworkWriteExt;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::packet::MultiVersionJavaPacket;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn test_headless_clients_stress() {
    // 1. Configure the server for the test
    let mut basic_config = BasicConfiguration::default();
    basic_config.online_mode = false; // Disable authentication
    basic_config.java_edition_address = "127.0.0.1:0".parse().unwrap(); // Pick a random port

    let mut advanced_config = AdvancedConfiguration::default();
    advanced_config.networking.java_compression.enabled = false; // Disable compression for simplicity
    advanced_config.networking.lan_broadcast.enabled = false;
    advanced_config.commands.use_console = false;
    
    // Enable logging to a file for this test run
    advanced_config.logging.enabled = true;
    advanced_config.logging.file = "stress_test_logs".to_string(); // Will produce stress_test_logs.log
    
    // Initialize the logger before starting the server
    pumpkin::init_logger(&advanced_config);

    let vanilla_data = VanillaData::load();

    // 2. Initialize and start the server
    let server = PumpkinServer::new(basic_config, advanced_config, vanilla_data).await;
    let addr = server.tcp_listener.as_ref().unwrap().local_addr().unwrap();
    let port = addr.port();

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();

    // Spawn server in a background task
    tokio::spawn(async move {
        server_clone.start().await;
    });

    // Give the server a moment to start up
    sleep(Duration::from_millis(500)).await;

    // 2.5 Spawn a background task to monitor TPS
    let tps_server = server_arc.clone();
    let tps_task = tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(1)).await;
            let tps = tps_server.get_tps();
            println!("Current TPS: {:.2}", tps);
        }
    });

    // 3. Spawn 1000 headless clients
    let num_clients = 1000;
    let mut tasks = Vec::new();

    for i in 0..num_clients {
        tasks.push(tokio::spawn(async move {
            // Add a small jitter to connection times to spread out the load
            sleep(Duration::from_millis(i % 1000)).await;
            
            match TcpStream::connect(format!("127.0.0.1:{port}")).await {
                Ok(mut stream) => {
                    // Send Handshake
                    // Packet ID 0x00
                    // Protocol version (767 for 1.21.x usually)
                    // Server address "127.0.0.1"
                    // Server port
                    // Next state: 2 (Login)
                    
                    let mut handshake_data = Vec::new();
                    handshake_data.write_var_int(&VarInt(0x00)).unwrap(); // Packet ID
                    handshake_data.write_var_int(&VarInt(767)).unwrap(); // Protocol version
                    handshake_data.write_string("127.0.0.1").unwrap();
                    handshake_data.write_u16_be(port).unwrap();
                    handshake_data.write_var_int(&VarInt(2)).unwrap(); // Next state = Login
                    
                    let mut handshake_packet = Vec::new();
                    handshake_packet.write_var_int(&VarInt(handshake_data.len() as i32)).unwrap();
                    handshake_packet.extend_from_slice(&handshake_data);
                    
                    if stream.write_all(&handshake_packet).await.is_err() { return; }

                    // Send LoginStart
                    // Packet ID 0x00
                    // Name
                    // UUID
                    let mut login_data = Vec::new();
                    login_data.write_var_int(&VarInt(0x00)).unwrap(); // Packet ID
                    login_data.write_string(&format!("Player{i}")).unwrap();
                    login_data.write_uuid(&uuid::Uuid::new_v4()).unwrap();
                    
                    let mut login_packet = Vec::new();
                    login_packet.write_var_int(&VarInt(login_data.len() as i32)).unwrap();
                    login_packet.extend_from_slice(&login_data);
                    
                    if stream.write_all(&login_packet).await.is_err() { return; }
                    
                    sleep(Duration::from_millis(50)).await;
                    
                    let version = &pumpkin_util::version::MinecraftVersion::V_1_21;
                    
                    // Send SLoginAcknowledged to transition to Config state
                    let mut ack_data = Vec::new();
                    use pumpkin_protocol::java::server::login::SLoginAcknowledged;
                    ack_data.write_var_int(&VarInt(SLoginAcknowledged::to_id(*version))).unwrap();
                    let mut ack_packet = Vec::new();
                    ack_packet.write_var_int(&VarInt(ack_data.len() as i32)).unwrap();
                    ack_packet.extend_from_slice(&ack_data);
                    if stream.write_all(&ack_packet).await.is_err() { return; }
                    
                    sleep(Duration::from_millis(50)).await;
                    
                    // Send SKnownPacks
                    let mut kp_data = Vec::new();
                    use pumpkin_protocol::java::server::config::SKnownPacks;
                    kp_data.write_var_int(&VarInt(SKnownPacks::to_id(*version))).unwrap();
                    kp_data.write_var_int(&VarInt(0)).unwrap(); // 0 known packs
                    let mut kp_packet = Vec::new();
                    kp_packet.write_var_int(&VarInt(kp_data.len() as i32)).unwrap();
                    kp_packet.extend_from_slice(&kp_data);
                    if stream.write_all(&kp_packet).await.is_err() { return; }
                    
                    sleep(Duration::from_millis(50)).await;
                    
                    // Send SAcknowledgeFinishConfig to transition to Play state
                    let mut fin_data = Vec::new();
                    use pumpkin_protocol::java::server::config::SAcknowledgeFinishConfig;
                    fin_data.write_var_int(&VarInt(SAcknowledgeFinishConfig::to_id(*version))).unwrap();
                    let mut fin_packet = Vec::new();
                    fin_packet.write_var_int(&VarInt(fin_data.len() as i32)).unwrap();
                    fin_packet.extend_from_slice(&fin_data);
                    if stream.write_all(&fin_packet).await.is_err() { return; }
                    
                    // Wait a bit to ensure we are in Play state
                    sleep(Duration::from_millis(50)).await;
                    
                    // Simulate some movement and interactions
                    use pumpkin_protocol::java::server::play::SPlayerPosition;
                    for j in 0..10 {
                        sleep(Duration::from_millis(100)).await;
                        let mut pos_data = Vec::new();
                        pos_data.write_var_int(&VarInt(SPlayerPosition::to_id(*version))).unwrap();
                        pos_data.write_f64_be(j as f64).unwrap(); // X
                        pos_data.write_f64_be(100.0).unwrap(); // Y
                        pos_data.write_f64_be(0.0).unwrap(); // Z
                        pos_data.push(1); // On Ground
                        let mut pos_packet = Vec::new();
                        pos_packet.write_var_int(&VarInt(pos_data.len() as i32)).unwrap();
                        pos_packet.extend_from_slice(&pos_data);
                        if stream.write_all(&pos_packet).await.is_err() { break; }
                    }
                    
                    // Keep the connection alive for a bit longer
                    sleep(Duration::from_secs(1)).await;
                }
                Err(e) => {
                    eprintln!("Client {i} failed to connect: {e}");
                    panic!("Connection failed");
                }
            }
        }));
    }

    // Wait for all clients to finish
    let results = futures::future::join_all(tasks).await;
    let mut success_count = 0;
    
    for result in results {
        if result.is_ok() {
            success_count += 1;
        }
    }
    
    println!("Successfully simulated {success_count}/{num_clients} clients.");
    assert!(success_count >= (num_clients * 8 / 10), "Not enough clients connected successfully ({} / {})", success_count, num_clients);
    
    // Stop the server
    pumpkin::stop_server();
    
    // Let the server shutdown gracefully
    sleep(Duration::from_secs(1)).await;
    
    tps_task.abort();
}
