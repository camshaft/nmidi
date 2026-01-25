mod discovery;
mod network;

use anyhow::{Context, Result};
use clap::Parser;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use nmidi_protocol::{AppleMidiPacket, APPLEMIDI_VERSION};

#[derive(Parser, Debug)]
#[command(name = "nmidi-client")]
#[command(about = "Network MIDI Client - connects to remote MIDI services")]
struct Args {
    /// Remote host to connect to (if not browsing)
    #[arg(short = 'H', long)]
    host: Option<String>,

    /// Remote control port
    #[arg(short = 'p', long, default_value = "5004")]
    port: u16,

    /// Device name
    #[arg(short, long, default_value = "nmidi-client")]
    name: String,

    /// Local bind address
    #[arg(short, long, default_value = "0.0.0.0:0")]
    bind: String,

    /// Browse for services instead of connecting directly
    #[arg(short = 'B', long)]
    browse: bool,

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

    info!("Starting nmidi-client: {}", args.name);

    if args.browse {
        // Browse for services
        info!("Browsing for MIDI services...");
        let receiver = discovery::browse_services()?;

        loop {
            match receiver.recv() {
                Ok(event) => {
                    info!("Service event: {:?}", event);
                }
                Err(e) => {
                    info!("Browse error: {}", e);
                    break;
                }
            }
        }
    } else {
        // Connect to specified host
        let host = args.host.as_ref().context("--host is required when not browsing")?;
        let remote_addr: SocketAddr = format!("{}:{}", host, args.port).parse()?;

        info!("Connecting to {}...", remote_addr);

        let sockets = network::NetworkSockets::bind(&args.bind, &format!("{}:0", args.bind.split(':').next().unwrap())).await?;

        // Generate SSRC and token
        let ssrc = generate_ssrc();
        let token = generate_token();

        // Send invitation
        let invitation = AppleMidiPacket::Invitation {
            version: APPLEMIDI_VERSION,
            token,
            ssrc,
            name: args.name.clone(),
        };

        sockets.send_control(&invitation, &remote_addr).await?;
        info!("Sent invitation to {}", remote_addr);

        // Wait for response
        loop {
            match tokio::time::timeout(
                std::time::Duration::from_secs(10),
                sockets.recv_control()
            ).await {
                Ok(Ok((packet, addr))) => {
                    info!("Received control packet from {}: {:?}", addr, packet);

                    match packet {
                        AppleMidiPacket::InvitationAccepted { .. } => {
                            info!("Connection accepted!");
                            // Continue with synchronization and data exchange
                            break;
                        }
                        AppleMidiPacket::Synchronization { .. } => {
                            info!("Synchronization received");
                        }
                        _ => {
                            info!("Unexpected packet type");
                        }
                    }
                }
                Ok(Err(e)) => {
                    info!("Error receiving control packet: {}", e);
                }
                Err(_) => {
                    info!("Timeout waiting for response");
                    break;
                }
            }
        }

        info!("Client session established. Press Ctrl+C to exit.");
        tokio::signal::ctrl_c().await?;
    }

    Ok(())
}

fn generate_ssrc() -> u32 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    (now & 0xFFFFFFFF) as u32
}

fn generate_token() -> u32 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    ((now >> 32) & 0xFFFFFFFF) as u32
}
