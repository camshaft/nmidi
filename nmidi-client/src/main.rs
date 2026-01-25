use anyhow::Result;
use clap::{Parser, Subcommand};
use midir::{MidiInput, MidiOutput};
#[cfg(unix)]
use midir::os::unix::{VirtualInput, VirtualOutput};
use nmidi_core::{
    APPLEMIDI_SIGNATURE, APPLEMIDI_VERSION, AppleMidiPacket, RtpPacket, SessionState,
    create_sync_request, discovery, handle_synchronization,
    network::{MAX_UDP_PAYLOAD, NetworkSockets},
    util::{generate_ssrc, generate_token, get_hostname, get_timestamp, micros_to_rtp_timestamp},
};
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    net::SocketAddr,
    sync::Arc,
};
use tokio::{
    sync::{Mutex, mpsc},
    time::{Duration, Instant},
};
use tracing::{Level, debug, info, warn};
use tracing_subscriber::FmtSubscriber;

/// Scheduled MIDI event for future execution
#[derive(Debug, Clone, Eq, PartialEq)]
struct ScheduledMidiEvent {
    /// Time to execute this event (Instant)
    execute_at: Instant,
    /// MIDI data to send
    data: Vec<u8>,
}

// Implement Ord for BinaryHeap (min-heap: earliest events first)
impl Ord for ScheduledMidiEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering so BinaryHeap becomes a min-heap
        other.execute_at.cmp(&self.execute_at)
    }
}

impl PartialOrd for ScheduledMidiEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

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

            // Create session state to track synchronization
            let session_state = Arc::new(Mutex::new(SessionState {
                ssrc: 0, // Will be set when we get server's SSRC
                token,
                addr: remote_addr,
                data_addr: remote_data_addr,
                sequence: 0,
                timestamp: 0,
                time_offset_ticks: 0,
                last_sync_count: 0,
                last_status: None,
            }));

            let sockets = Arc::new(sockets);

            // Spawn control handler for sync packets first
            let control_handle = spawn_control_handler(
                Arc::clone(&sockets),
                Arc::clone(&session_state),
                ssrc,
                remote_addr,
            );

            // Send initial synchronization request after handler is ready
            let sync_request = create_sync_request(ssrc);
            sockets.send_control(&sync_request, &remote_addr).await?;
            info!("Sent initial synchronization request");

            // Create virtual MIDI ports and start bidirectional forwarding
            let virtual_port_name = format!("nmidi-client: {}", port_name);
            
            if port_type == "input" {
                // Server has INPUT port: client creates virtual OUTPUT port
                // Local apps send MIDI → client → network → server → physical input
                info!("Creating virtual MIDI output port: {}", virtual_port_name);
                
                let handle = spawn_virtual_output_task(
                    virtual_port_name,
                    Arc::clone(&sockets),
                    Arc::clone(&session_state),
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
                    _ = control_handle => {
                        info!("Control handler ended");
                    }
                }
            } else {
                // Server has OUTPUT port: client creates virtual INPUT port
                // Server → physical output → network → client → local apps receive MIDI
                info!("Creating virtual MIDI input port: {}", virtual_port_name);
                
                let handle = spawn_virtual_input_task(
                    virtual_port_name,
                    Arc::clone(&sockets),
                    Arc::clone(&session_state),
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
                    _ = control_handle => {
                        info!("Control handler ended");
                    }
                }
            }
        }
    }

    Ok(())
}

/// Spawn task to handle control packets (synchronization)
fn spawn_control_handler(
    sockets: Arc<NetworkSockets>,
    session_state: Arc<Mutex<SessionState>>,
    ssrc: u32,
    remote_addr: SocketAddr,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Periodic resync every 60 seconds
        let mut resync_interval = tokio::time::interval(Duration::from_secs(60));
        resync_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        loop {
            tokio::select! {
                // Handle incoming control packets
                result = sockets.recv_control() => {
                    match result {
                        Ok((packet, addr)) => {
                            match packet {
                                AppleMidiPacket::Synchronization {
                                    ssrc: peer_ssrc,
                                    count,
                                    timestamp1,
                                    timestamp2,
                                    timestamp3,
                                } => {
                                    debug!("Received sync from SSRC {} (count {})", peer_ssrc, count);
                                    
                                    // Update peer SSRC if not set
                                    {
                                        let mut state = session_state.lock().await;
                                        if state.ssrc == 0 {
                                            state.ssrc = peer_ssrc;
                                        }
                                    }
                                    
                                    // Use shared synchronization handler
                                    let sync_result = handle_synchronization(
                                        ssrc,
                                        peer_ssrc,
                                        count,
                                        timestamp1,
                                        timestamp2,
                                        timestamp3,
                                    );
                                    
                                    // Update session state with calculated offset
                                    if let Some(time_offset) = sync_result.time_offset_ticks {
                                        let mut state = session_state.lock().await;
                                        state.time_offset_ticks = time_offset;
                                        state.last_sync_count = count;
                                        info!("Updated time offset: {} ticks", time_offset);
                                    }
                                    
                                    // Send response if needed
                                    if let Some(response) = sync_result.response {
                                        if let Err(e) = sockets.send_control(&response, &addr).await {
                                            warn!("Failed to send sync response: {}", e);
                                        }
                                    }
                                }
                                _ => {
                                    debug!("Received other control packet: {:?}", packet);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Error receiving control packet: {}", e);
                        }
                    }
                }
                
                // Periodic resync
                _ = resync_interval.tick() => {
                    let sync_request = create_sync_request(ssrc);
                    if let Err(e) = sockets.send_control(&sync_request, &remote_addr).await {
                        warn!("Failed to send periodic sync request: {}", e);
                    } else {
                        debug!("Sent periodic sync request");
                    }
                }
            }
        }
    })
}

/// Spawn task for virtual output port (server has input)
/// Local apps → virtual output → network → server input
fn spawn_virtual_output_task(
    port_name: String,
    sockets: Arc<NetworkSockets>,
    session_state: Arc<Mutex<SessionState>>,
    remote_addr: SocketAddr,
    ssrc: u32,
) -> Result<tokio::task::JoinHandle<()>> {
    let midi_in = MidiInput::new("nmidi-client")?;
    
    // Create channel for MIDI input
    let (input_tx, mut input_rx) = mpsc::channel::<(u64, Vec<u8>)>(100);
    
    // Create virtual MIDI input port that local apps can send to
    // (from the local app's perspective, this is an output destination)
    // Keep the connection alive for the duration of the task
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
    // The connection must stay alive for the duration, so we move it into the task
    Ok(tokio::spawn(async move {
        let mut sequence = 0u16;
        
        loop {
            match input_rx.recv().await {
                Some((_, midi_data)) => {
                    debug!("Received MIDI from virtual output: {:?}", midi_data);
                    
                    // Get current time in microseconds
                    let current_micros = get_timestamp();
                    let local_ticks = micros_to_rtp_timestamp(current_micros) as i64;
                    
                    // Apply time offset: peer_time = local_time + offset
                    let time_offset = session_state.lock().await.time_offset_ticks;
                    let peer_ticks = local_ticks.wrapping_add(time_offset);
                    // Cast to u32 with wrapping (RTP timestamps are u32 and wrap naturally)
                    let rtp_timestamp = (peer_ticks as u64 & 0xFFFFFFFF) as u32;
                    
                    debug!("Sending MIDI with local_ticks={}, offset={}, peer_ticks={}", 
                           local_ticks, time_offset, peer_ticks);
                    
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
        
        // Connection is kept alive by moving it into this task scope
        drop(_connection);
    }))
}

/// Spawn task for virtual input port (server has output)
/// Server output → network → virtual input → local apps
fn spawn_virtual_input_task(
    port_name: String,
    sockets: Arc<NetworkSockets>,
    session_state: Arc<Mutex<SessionState>>,
) -> Result<tokio::task::JoinHandle<()>> {
    let midi_out = MidiOutput::new("nmidi-client")?;
    
    // Create virtual MIDI output port that local apps can receive from
    // (from the local app's perspective, this is an input source)
    let mut connection = midi_out
        .create_virtual(&port_name)
        .map_err(|e| anyhow::anyhow!("Failed to create virtual MIDI port: {:?}", e))?;
    
    // Spawn task to receive from network and forward to virtual port
    Ok(tokio::spawn(async move {
        let mut buf = [0u8; MAX_UDP_PAYLOAD];
        let mut event_queue: BinaryHeap<ScheduledMidiEvent> = BinaryHeap::new();
        
        loop {
            // Calculate next event time for timeout
            let next_event_time = event_queue.peek().map(|e| e.execute_at);
            
            let receive_future = sockets.data.recv_from(&mut buf);
            
            tokio::select! {
                // Handle incoming data packets
                result = receive_future => {
                    match result {
                        Ok((len, addr)) => {
                            // Skip AppleMIDI control packets (check for signature 0xFFFF)
                            // Also ensure we have at least minimum RTP header size (12 bytes)
                            if len < 12 {
                                continue;
                            }
                            
                            let sig = u16::from_be_bytes([buf[0], buf[1]]);
                            if sig == APPLEMIDI_SIGNATURE {
                                continue;
                            }
                            
                            // Parse RTP packet
                            match RtpPacket::parse(&buf[..len]) {
                                Ok(packet) => {
                                    debug!("Received RTP packet from {}: {} commands", addr, packet.commands.len());
                                    
                                    // Get time offset for scheduling
                                    let (time_offset_ticks, mut last_status) = {
                                        let state = session_state.lock().await;
                                        (state.time_offset_ticks, state.last_status)
                                    };
                                    
                                    // RTP timestamp is in 10kHz ticks (100us per tick)
                                    let rtp_timestamp_micros = (packet.header.timestamp as u64) * 100;
                                    
                                    // Apply time offset to convert from peer time to local time
                                    // peer_time = local_time + offset
                                    // Therefore: local_time = peer_time - offset
                                    let time_offset_micros = time_offset_ticks * 100;
                                    let base_local_micros = if time_offset_micros >= 0 {
                                        rtp_timestamp_micros.saturating_sub(time_offset_micros as u64)
                                    } else {
                                        rtp_timestamp_micros.saturating_add(time_offset_micros.unsigned_abs())
                                    };
                                    
                                    let mut current_timestamp_ticks = base_local_micros / 100;
                                    
                                    // Process commands and schedule them
                                    for cmd in &packet.commands {
                                        current_timestamp_ticks = current_timestamp_ticks.wrapping_add(cmd.delta_time as u64);
                                        
                                        // Reconstruct full MIDI message with running status
                                        let mut midi_bytes = cmd.data.clone();
                                        if !midi_bytes.is_empty() && midi_bytes[0] < 0x80 {
                                            // Running status: prepend last known status if available
                                            if let Some(status) = last_status {
                                                midi_bytes.insert(0, status);
                                            } else {
                                                continue; // Can't reconstruct, drop
                                            }
                                        }
                                        
                                        // Update last_status if this is a status byte
                                        if !midi_bytes.is_empty() && midi_bytes[0] >= 0x80 && midi_bytes[0] < 0xF0 {
                                            last_status = Some(midi_bytes[0]);
                                        }
                                        
                                        // Calculate execution time
                                        let execute_micros = current_timestamp_ticks * 100;
                                        let now_micros = get_timestamp();
                                        
                                        // Schedule for future or execute immediately
                                        if execute_micros > now_micros {
                                            let delay = Duration::from_micros(execute_micros - now_micros);
                                            let execute_at = Instant::now() + delay;
                                            event_queue.push(ScheduledMidiEvent {
                                                execute_at,
                                                data: midi_bytes,
                                            });
                                            debug!("Scheduled MIDI event for {:?} from now", delay);
                                        } else {
                                            // Execute immediately (event is in the past or now)
                                            if let Err(e) = connection.send(&midi_bytes) {
                                                warn!("Failed to send MIDI to virtual port: {}", e);
                                            }
                                        }
                                    }
                                    
                                    // Update session state with last_status
                                    {
                                        let mut state = session_state.lock().await;
                                        state.last_status = last_status;
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
                
                // Execute scheduled events when their time arrives
                _ = async {
                    if let Some(event_time) = next_event_time {
                        tokio::time::sleep_until(event_time).await;
                    } else {
                        // No events, wait forever
                        std::future::pending::<()>().await;
                    }
                }, if next_event_time.is_some() => {
                    let now = Instant::now();
                    
                    // Execute all events that are due
                    while let Some(event) = event_queue.peek() {
                        if event.execute_at <= now {
                            let event = event_queue.pop().unwrap();
                            if let Err(e) = connection.send(&event.data) {
                                warn!("Failed to send scheduled MIDI to virtual port: {}", e);
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        
        // connection is dropped here when task ends
    }))
}
