mod midi;
mod session;

use anyhow::Result;
use clap::Parser;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(name = "nmidi-server")]
#[command(about = "Network MIDI Server - exposes local MIDI ports over RTP-MIDI")]
struct Args {
    /// Device name to advertise
    #[arg(short, long, default_value = "nmidi-server")]
    name: String,

    /// Control port (data port will be +1)
    #[arg(short = 'p', long, default_value = "5004")]
    control_port: u16,

    /// Bind address
    #[arg(short, long, default_value = "0.0.0.0")]
    bind: String,

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
    info!("Control port: {}", args.control_port);
    info!("Data port: {}", args.control_port + 1);

    // Detect MIDI ports (may not be available in all environments)
    let _midi_ports = match midi::detect_ports() {
        Ok(ports) => {
            info!("Found {} MIDI input ports", ports.inputs.len());
            info!("Found {} MIDI output ports", ports.outputs.len());
            ports
        }
        Err(e) => {
            warn!("Could not detect MIDI ports: {}. Continuing without MIDI hardware.", e);
            midi::MidiPorts {
                inputs: Vec::new(),
                outputs: Vec::new(),
            }
        }
    };

    // Create session manager
    let session_manager = session::SessionManager::new(
        args.name.clone(),
        format!("{}:{}", args.bind, args.control_port),
        format!("{}:{}", args.bind, args.control_port + 1),
    )
    .await?;

    // Start mDNS service discovery
    let _mdns_service = nmidi_common::discovery::advertise_service(
        &args.name,
        args.control_port,
    )?;

    info!("Server started, advertising via mDNS");

    // Run session manager
    session_manager.run().await?;

    Ok(())
}
