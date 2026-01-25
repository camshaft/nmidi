use anyhow::Result;
use clap::{Parser, Subcommand};
use nmidi_core::{
    APPLEMIDI_VERSION, AppleMidiPacket, discovery,
    network::NetworkSockets,
    util::{generate_ssrc, generate_token, get_hostname},
};
use std::net::SocketAddr;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(name = "nmidi-client")]
#[command(about = "Network MIDI Client - connects to remote MIDI services")]
struct Args {
    #[command(subcommand)]
    command: Commands,

    /// Log level
    #[arg(short, long, default_value = "info", global = true)]
    log_level: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Discover MIDI services on the network
    Discover,
    /// Connect to a remote MIDI service
    Connect {
        /// Remote host to connect to
        #[arg(short = 'H', long)]
        host: String,

        /// Remote control port
        #[arg(short = 'p', long, default_value = "5004")]
        port: u16,

        /// Device name (defaults to hostname)
        #[arg(short, long)]
        name: Option<String>,

        /// Local bind address
        #[arg(short, long, default_value = "0.0.0.0:0")]
        bind: String,
    },
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

    match args.command {
        Commands::Discover => {
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
        }
        Commands::Connect {
            host,
            port,
            name,
            bind,
        } => {
            let device_name = name.unwrap_or_else(get_hostname);
            info!("Starting nmidi-client: {}", device_name);

            let remote_addr: SocketAddr = format!("{}:{}", host, port).parse()?;
            info!("Connecting to {}...", remote_addr);

            // AppleMIDI expects the control and data sockets to be on consecutive
            // ports. Bind a paired socket set on the requested interface.
            let bind_ip = bind
                .split(':')
                .next()
                .filter(|s| !s.is_empty())
                .unwrap_or("0.0.0.0");
            let sockets = NetworkSockets::bind_consecutive(bind_ip).await?;

            let local_control = sockets.control.local_addr()?;
            let local_data = sockets.data.local_addr()?;
            info!("Local control {} data {}", local_control, local_data);

            // Generate SSRC and token
            let ssrc = generate_ssrc();
            let token = generate_token();

            // Send invitation on control and data sockets (per spec the inviter
            // repeats the invitation on the data port).
            let invitation = AppleMidiPacket::Invitation {
                version: APPLEMIDI_VERSION,
                token,
                ssrc,
                name: device_name.clone(),
            };
            let remote_data_addr = SocketAddr::new(remote_addr.ip(), remote_addr.port() + 1);

            sockets.send_control(&invitation, &remote_addr).await?;
            sockets
                .send_control_on_data(&invitation, &remote_data_addr)
                .await?;
            info!(
                "Sent invitation to control {} and data {}",
                remote_addr, remote_data_addr
            );

            // Wait for response with retry
            // TODO: Implement proper state machine for connection handling
            // (states: Connecting -> Connected -> Disconnected/Rejected)
            // to handle protocol events more robustly
            let mut attempts = 0;
            let max_attempts = 3;

            while attempts < max_attempts {
                match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    sockets.recv_control(),
                )
                .await
                {
                    Ok(Ok((packet, addr))) => {
                        info!("Received control packet from {}: {:?}", addr, packet);

                        match packet {
                            AppleMidiPacket::InvitationAccepted { .. } => {
                                info!("Connection accepted!");
                                // TODO: Implement MIDI mounting - create virtual MIDI ports
                                // and forward messages bidirectionally
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
                        attempts += 1;
                        if attempts < max_attempts {
                            info!(
                                "Timeout waiting for response, retrying... (attempt {}/{})",
                                attempts, max_attempts
                            );
                            sockets.send_control(&invitation, &remote_addr).await?;
                            sockets
                                .send_control_on_data(&invitation, &remote_data_addr)
                                .await?;
                        } else {
                            info!("Failed to connect after {} attempts", max_attempts);
                            return Ok(());
                        }
                    }
                }
            }

            info!("Client session established. Press Ctrl+C to exit.");
            tokio::signal::ctrl_c().await?;
        }
    }

    Ok(())
}
