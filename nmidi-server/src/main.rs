mod midi;
mod session;

use anyhow::Result;
use clap::Parser;
use nmidi_core::discovery::Service;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(name = "nmidi-server")]
#[command(about = "Network MIDI Server - exposes local MIDI ports over RTP-MIDI")]
struct Args {
    /// Device name to advertise
    #[arg(short, long, default_value = "nmidi-server")]
    name: String,

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

struct MidiService {
    _service: Service,
    shutdown_tx: Option<oneshot::Sender<()>>,
    session_handle: tokio::task::JoinHandle<()>,
}

impl Drop for MidiService {
    fn drop(&mut self) {
        // Send shutdown signal to gracefully close sessions
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
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
    info!("Port monitoring interval: {}s", args.monitor_interval);

    // Create service advertiser
    let advertiser = nmidi_core::discovery::ServiceAdvertiser::new()?;
    
    // Start MIDI port monitoring
    let mut port_rx = midi::start_port_monitor(Duration::from_secs(args.monitor_interval)).await;
    
    // Track active services by port key
    let mut active_services: HashMap<String, MidiService> = HashMap::new();
    
    loop {
        // Get current ports
        let current_ports = port_rx.borrow_and_update().clone();
        
        // Create set of current port keys
        let current_keys: std::collections::HashSet<String> = current_ports
            .all_ports()
            .iter()
            .map(|p| format!("{}_{}", p.port_type.as_str(), p.index))
            .collect();
        
        // Remove services for ports that no longer exist
        active_services.retain(|key, _service| {
            if !current_keys.contains(key) {
                info!("Removing service for port: {}", key);
                false
            } else {
                true
            }
        });
        
        // Add services for new ports
        for port in current_ports.all_ports() {
            let port_key = format!("{}_{}", port.port_type.as_str(), port.index);
            
            if active_services.contains_key(&port_key) {
                continue;
            }
            
            let service_name = format!("{}_{}", args.name, port_key);
            
            let mut properties = HashMap::new();
            properties.insert("name".to_string(), port.name.clone());
            properties.insert("ver".to_string(), "2".to_string());
            properties.insert("type".to_string(), port.port_type.as_str().to_string());
            properties.insert("index".to_string(), port.index.to_string());
            
            // Create session manager with consecutive ports (Apple MIDI requirement)
            let session_manager = match session::SessionManager::new(
                service_name.clone(),
                args.bind.clone(),
                port.clone(),
            )
            .await
            {
                Ok(sm) => sm,
                Err(e) => {
                    warn!("Failed to create session manager for port '{}': {}", port.name, e);
                    continue;
                }
            };
            
            let control_port = session_manager.control_port();
            let data_port = session_manager.data_port();
            
            let service = match advertiser.advertise_service(
                &service_name,
                &args.name,
                control_port,
                properties,
            ) {
                Ok(s) => s,
                Err(e) => {
                    warn!("Failed to advertise port '{}': {}", port.name, e);
                    continue;
                }
            };
            
            info!(
                "Advertising MIDI port '{}' ({} #{}) on ports {}/{}",
                port.name, port.port_type.as_str(), port.index, control_port, data_port
            );
            
            // Create shutdown channel
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            
            // Spawn session handler
            let port_name = port.name.clone();
            let session_handle = tokio::spawn(async move {
                if let Err(e) = session_manager.run(shutdown_rx).await {
                    warn!("Session manager error for port '{}': {}", port_name, e);
                }
            });
            
            active_services.insert(
                port_key,
                MidiService {
                    _service: service,
                    shutdown_tx: Some(shutdown_tx),
                    session_handle,
                },
            );
        }
        
        if active_services.is_empty() {
            info!("No MIDI ports available, waiting for ports...");
        } else {
            info!("Server advertising {} MIDI ports via mDNS", active_services.len());
        }
        
        // Wait for port changes
        if port_rx.changed().await.is_err() {
            info!("Port monitor channel closed, shutting down");
            break;
        }
    }
    
    Ok(())
}
