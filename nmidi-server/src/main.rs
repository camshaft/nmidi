mod midi;
mod session;

use anyhow::Result;
use clap::Parser;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(name = "nmidi-server")]
#[command(about = "Network MIDI Server - exposes local MIDI ports over RTP-MIDI")]
struct Args {
    /// Device name to advertise
    #[arg(short, long, default_value = "nmidi-server")]
    name: String,

    /// Starting control port (each MIDI port gets a consecutive pair: control + data)
    #[arg(short = 'p', long, default_value = "5004")]
    control_port: u16,

    /// Bind address
    #[arg(short, long, default_value = "0.0.0.0")]
    bind: String,

    /// Port monitoring interval in seconds
    #[arg(long, default_value = "5")]
    monitor_interval: u64,

    /// Log level
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    let level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting nmidi-server: {}", args.name);
    info!("Starting control port: {}", args.control_port);
    info!("Port monitoring interval: {}s", args.monitor_interval);

    // Start MIDI port monitoring
    let mut port_rx = midi::start_port_monitor(Duration::from_secs(args.monitor_interval)).await;
    
    // Wait for initial port detection
    let initial_ports = port_rx.borrow_and_update().clone();
    info!(
        "Initial MIDI ports: {} inputs, {} outputs",
        initial_ports.inputs.len(),
        initial_ports.outputs.len()
    );

    // Create service advertiser
    let advertiser = nmidi_core::discovery::ServiceAdvertiser::new()?;
    
    // Track active services and sessions
    let mut active_services: HashMap<String, String> = HashMap::new(); // port_key -> service_fullname
    
    // Advertise initial ports
    let mut next_port = args.control_port;
    for port in initial_ports.all_ports() {
        let port_key = format!("{:?}_{}", port.port_type, port.index);
        let service_name = format!("{}_{}", args.name, port_key);
        
        let mut properties = HashMap::new();
        properties.insert("name".to_string(), port.name.clone());
        properties.insert("ver".to_string(), "2".to_string());
        properties.insert("type".to_string(), format!("{:?}", port.port_type));
        properties.insert("index".to_string(), port.index.to_string());
        
        match advertiser.advertise_service(&service_name, &args.name, next_port, properties) {
            Ok(fullname) => {
                info!(
                    "Advertising MIDI port '{}' ({:?} #{}) on port {}",
                    port.name, port.port_type, port.index, next_port
                );
                active_services.insert(port_key.clone(), fullname);
                
                // Create session manager for this port
                let session_manager = session::SessionManager::new(
                    service_name.clone(),
                    format!("{}:{}", args.bind, next_port),
                    format!("{}:{}", args.bind, next_port + 1),
                )
                .await?;
                
                // Spawn session handler
                let port_name = port.name.clone();
                tokio::spawn(async move {
                    if let Err(e) = session_manager.run().await {
                        warn!("Session manager error for port '{}': {}", port_name, e);
                    }
                });
            }
            Err(e) => {
                warn!("Failed to advertise port '{}': {}", port.name, e);
            }
        }
        
        next_port += 2; // Control port + data port
    }

    // Monitor for port changes
    info!("Server started, advertising {} MIDI ports via mDNS", active_services.len());
    
    loop {
        tokio::select! {
            _ = port_rx.changed() => {
                let new_ports = port_rx.borrow_and_update().clone();
                info!(
                    "MIDI ports changed: {} inputs, {} outputs",
                    new_ports.inputs.len(),
                    new_ports.outputs.len()
                );
                
                // For now, just log the change
                // TODO: Implement dynamic service add/remove
                // This would require:
                // 1. Computing diff between old and new ports
                // 2. Unregistering services for removed ports
                // 3. Advertising services for new ports
                // 4. Managing session manager lifecycle
            }
        }
    }
}
