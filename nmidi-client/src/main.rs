use anyhow::Result;
use clap::{Parser, Subcommand};
use midir::{MidiInput, MidiOutput};
#[cfg(target_os = "linux")]
use midir::os::unix::{VirtualInput, VirtualOutput};
#[cfg(target_os = "macos")]
use midir::os::unix::{VirtualInput, VirtualOutput};
use nmidi_core::{
    APPLEMIDI_SIGNATURE, APPLEMIDI_VERSION, AppleMidiPacket, RtpPacket, discovery,
    network::{MAX_UDP_PAYLOAD, NetworkSockets},
    util::{generate_ssrc, generate_token, get_hostname, get_timestamp, micros_to_rtp_timestamp},
};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::mpsc;
use tracing::{Level, debug, info, warn};
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

        /// Remote MIDI port name to connect to
        #[arg(long)]
        port_name: String,

        /// Remote MIDI port type (from server's perspective)
        #[arg(long, value_parser = ["input", "output"])]
        port_type: String,
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
            port_name,
            port_type,
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
            let mut attempts = 0;
            let max_attempts = 3;
            let mut connected = false;

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
                                connected = true;
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

            if !connected {
                info!("Failed to establish connection");
                return Ok(());
            }

            // Create virtual MIDI ports and start bidirectional forwarding
            let virtual_port_name = format!("nmidi-client: {}", port_name);
            
            let sockets = Arc::new(sockets);
            
            if port_type == "input" {
                // Server has INPUT port: client creates virtual OUTPUT port
                // Local apps send MIDI → client → network → server → physical input
                info!("Creating virtual MIDI output port: {}", virtual_port_name);
                
                let handle = spawn_virtual_output_task(
                    virtual_port_name,
                    Arc::clone(&sockets),
                    remote_data_addr,
                    ssrc,
                )?;
                
                info!("Virtual output port created. Local MIDI apps can now send to this port.");
                info!("MIDI data will be forwarded to server's input port '{}'", port_name);
                info!("Press Ctrl+C to exit.");
                
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        info!("Ctrl+C received, shutting down...");
                    }
                    _ = handle => {
                        info!("MIDI task ended");
                    }
                }
            } else {
                // Server has OUTPUT port: client creates virtual INPUT port
                // Server → physical output → network → client → local apps receive MIDI
                info!("Creating virtual MIDI input port: {}", virtual_port_name);
                
                let handle = spawn_virtual_input_task(
                    virtual_port_name,
                    Arc::clone(&sockets),
                )?;
                
                info!("Virtual input port created. Local MIDI apps can now receive from this port.");
                info!("MIDI data from server's output port '{}' will be relayed here", port_name);
                info!("Press Ctrl+C to exit.");
                
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        info!("Ctrl+C received, shutting down...");
                    }
                    _ = handle => {
                        info!("MIDI task ended");
                    }
                }
            }
        }
    }

    Ok(())
}

/// Spawn task for virtual output port (server has input)
/// Local apps → virtual output → network → server input
fn spawn_virtual_output_task(
    port_name: String,
    sockets: Arc<NetworkSockets>,
    remote_addr: SocketAddr,
    ssrc: u32,
) -> Result<tokio::task::JoinHandle<()>> {
    let midi_in = MidiInput::new("nmidi-client")?;
    
    // Create channel for MIDI input
    let (input_tx, mut input_rx) = mpsc::channel::<(u64, Vec<u8>)>(100);
    
    // Create virtual MIDI input to receive from local apps
    let _connection = midi_in
        .create_virtual(
            &port_name,
            move |timestamp, message, _| {
                if let Err(e) = input_tx.try_send((timestamp, message.to_vec())) {
                    debug!("Failed to send MIDI input to channel: {}", e);
                }
            },
            (),
        )
        .map_err(|e| anyhow::anyhow!("Failed to create virtual MIDI port: {:?}", e))?;
    
    // Spawn task to forward MIDI to network
    Ok(tokio::spawn(async move {
        let mut sequence = 0u16;
        
        loop {
            match input_rx.recv().await {
                Some((_, midi_data)) => {
                    debug!("Received MIDI from virtual output: {:?}", midi_data);
                    
                    // Convert to RTP timestamp
                    let current_micros = get_timestamp();
                    let rtp_timestamp = micros_to_rtp_timestamp(current_micros);
                    
                    // Create RTP packet
                    let mut packet = RtpPacket::new(ssrc, sequence, rtp_timestamp);
                    packet.add_command(0, midi_data);
                    
                    // Send to server
                    let packet_bytes = packet.to_bytes();
                    if let Err(e) = sockets.data.send_to(&packet_bytes, &remote_addr).await {
                        warn!("Failed to send MIDI data to server: {}", e);
                    }
                    
                    sequence = sequence.wrapping_add(1);
                }
                None => {
                    info!("MIDI input channel closed");
                    break;
                }
            }
        }
        
        // Keep connection alive
        drop(_connection);
    }))
}

/// Spawn task for virtual input port (server has output)
/// Server output → network → virtual input → local apps
fn spawn_virtual_input_task(
    port_name: String,
    sockets: Arc<NetworkSockets>,
) -> Result<tokio::task::JoinHandle<()>> {
    let midi_out = MidiOutput::new("nmidi-client")?;
    
    // Create virtual MIDI output for local apps to receive from
    let mut connection = midi_out
        .create_virtual(&port_name)
        .map_err(|e| anyhow::anyhow!("Failed to create virtual MIDI port: {:?}", e))?;
    
    // Spawn task to receive from network and forward to virtual port
    Ok(tokio::spawn(async move {
        let mut buf = [0u8; MAX_UDP_PAYLOAD];
        
        loop {
            match sockets.data.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    // Skip AppleMIDI control packets
                    if len >= 2 {
                        let sig = u16::from_be_bytes([buf[0], buf[1]]);
                        if sig == APPLEMIDI_SIGNATURE {
                            continue;
                        }
                    }
                    
                    // Parse RTP packet
                    match RtpPacket::parse(&buf[..len]) {
                        Ok(packet) => {
                            debug!("Received RTP packet from {}: {} commands", addr, packet.commands.len());
                            
                            // Forward all MIDI commands to virtual port
                            for cmd in packet.commands {
                                if let Err(e) = connection.send(&cmd.data) {
                                    warn!("Failed to send MIDI to virtual port: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse RTP packet from {}: {}", addr, e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Error receiving data: {}", e);
                }
            }
        }
    }))
}
