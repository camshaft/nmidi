use crate::midi::{MidiPortInfo, MidiPortType};
use anyhow::Result;
use midir::{MidiInput, MidiOutput};
use nmidi_core::{
    APPLEMIDI_SIGNATURE, APPLEMIDI_VERSION, AppleMidiPacket, RtpPacket, SessionState,
    create_sync_request, handle_synchronization,
    network::{MAX_UDP_PAYLOAD, NetworkSockets},
    util::{generate_ssrc, get_timestamp, micros_to_rtp_timestamp},
};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    net::SocketAddr,
    sync::Arc,
};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time::{Duration, Instant},
};
use tracing::{debug, info, warn};

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

#[derive(Clone, Copy)]
enum SendOn {
    Control,
    Data,
}

pub struct SessionManager {
    name: String,
    ssrc: u32,
    sockets: Arc<NetworkSockets>,
    sessions: Arc<Mutex<HashMap<u32, SessionState>>>,
    port_info: MidiPortInfo,
}

impl SessionManager {
    pub async fn new(name: String, bind_addr: String, port_info: MidiPortInfo) -> Result<Self> {
        let ssrc = generate_ssrc();
        let sockets = NetworkSockets::bind_consecutive(&bind_addr).await?;

        Ok(Self {
            name,
            ssrc,
            sockets: Arc::new(sockets),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            port_info,
        })
    }

    /// Get the control port the session manager is bound to
    pub fn control_port(&self) -> u16 {
        self.sockets
            .control
            .local_addr()
            .map(|addr| addr.port())
            .unwrap_or(0)
    }

    /// Get the data port the session manager is bound to
    pub fn data_port(&self) -> u16 {
        self.sockets
            .data
            .local_addr()
            .map(|addr| addr.port())
            .unwrap_or(0)
    }

    pub async fn run(self, mut shutdown_rx: oneshot::Receiver<()>) -> Result<()> {
        // Create channel for MIDI output forwarding (used by data_handler -> output_task)
        // Use bounded channel to prevent unbounded memory growth if MIDI output is slow
        let (midi_tx, midi_rx) = mpsc::channel::<(u64, Vec<u8>)>(100);

        // Spawn MIDI task based on port type
        let midi_task = match self.port_info.port_type {
            MidiPortType::Output => self.spawn_output_task(midi_rx).await?,
            MidiPortType::Input => self.spawn_input_task().await?,
        };

        let control_handle = self.spawn_control_handler();
        let data_handle = self.spawn_data_handler(midi_tx);

        // Wait for shutdown or task completion
        let result = tokio::select! {
            _ = &mut shutdown_rx => {
                info!("Received shutdown signal");
                Ok(())
            }
            _ = control_handle => {
                warn!("Control handler task ended unexpectedly");
                Err(anyhow::anyhow!("Control handler ended"))
            }
            _ = data_handle => {
                warn!("Data handler task ended unexpectedly");
                Err(anyhow::anyhow!("Data handler ended"))
            }
            _ = midi_task => {
                warn!("MIDI handler task ended unexpectedly");
                Err(anyhow::anyhow!("MIDI handler ended"))
            }
        };

        // Always run shutdown cleanup regardless of which exit path was taken
        Self::handle_shutdown(&self.name, self.ssrc, &self.sockets, &self.sessions).await?;

        result
    }

    async fn spawn_output_task(
        &self,
        mut midi_rx: mpsc::Receiver<(u64, Vec<u8>)>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let midi_out = MidiOutput::new(&format!("nmidi-server-{}", self.name))?;
        let ports = midi_out.ports();

        if self.port_info.index >= ports.len() {
            anyhow::bail!(
                "MIDI output port index {} out of range",
                self.port_info.index
            );
        }

        let port = &ports[self.port_info.index];
        let port_name = self.port_info.name.clone();

        // Connect to MIDI output port
        let mut connection = midi_out
            .connect(port, &self.name)
            .map_err(|e| anyhow::anyhow!("Failed to connect to MIDI output: {:?}", e))?;

        info!("Connected to MIDI output port: {}", port_name);

        // Spawn task to forward MIDI output to hardware with scheduling
        let handle = tokio::spawn(async move {
            // Priority queue (min-heap) for scheduled events
            let mut scheduled_events = BinaryHeap::new();

            // Base time for the scheduler - set when we receive first event
            // Stored as (Instant, RTP timestamp) tuple to keep them synchronized
            let mut base_time: Option<(Instant, u64)> = None;

            loop {
                // Check if we have any events ready to execute
                let next_event_time: Option<Instant> = scheduled_events
                    .peek()
                    .map(|e: &ScheduledMidiEvent| e.execute_at);

                let timeout_duration: Duration = if let Some(next_time) = next_event_time {
                    // Calculate time until next event
                    let now = Instant::now();
                    if next_time <= now {
                        // Event is ready now, don't wait
                        Duration::from_micros(0)
                    } else {
                        next_time.saturating_duration_since(now)
                    }
                } else {
                    // No scheduled events, wait indefinitely for new ones
                    Duration::from_secs(30) // 30 second timeout as fallback to check for new events
                };

                // Wait for either a new event or timeout for scheduled event
                let new_event = tokio::time::timeout(timeout_duration, midi_rx.recv()).await;

                match new_event {
                    Ok(Some((rtp_timestamp, midi_data))) => {
                        // Received a new MIDI event
                        debug!(
                            "Received MIDI event for scheduling: timestamp={}, data={:?}",
                            rtp_timestamp, midi_data
                        );

                        // Initialize base timestamp on first event
                        if base_time.is_none() {
                            base_time = Some((Instant::now(), rtp_timestamp));
                            debug!("Initialized scheduler base: timestamp={}", rtp_timestamp);
                        }

                        // Calculate when to execute this event relative to base time
                        let (base_instant, base_timestamp) = base_time.unwrap();

                        // Handle timestamp wraparound: if new timestamp is significantly smaller
                        // than base, assume it wrapped around. If it's slightly behind (out of order),
                        // clamp to 0 to avoid giant wrapped deltas.
                        let delta_ticks = if rtp_timestamp < base_timestamp {
                            let back_diff = base_timestamp - rtp_timestamp;
                            if back_diff > (u32::MAX as u64 / 2) {
                                // Treat as wraparound across u32::MAX
                                let ticks_to_max = (u32::MAX as u64).wrapping_sub(base_timestamp);
                                ticks_to_max.wrapping_add(rtp_timestamp).wrapping_add(1)
                            } else {
                                // Out-of-order or jitter: execute immediately
                                0
                            }
                        } else {
                            // Normal monotonic case
                            rtp_timestamp - base_timestamp
                        };

                        // Convert RTP ticks (10kHz = 100us per tick) to microseconds safely
                        let delta_micros = delta_ticks.saturating_mul(100);

                        // Calculate absolute execution time
                        let execute_at = base_instant + Duration::from_micros(delta_micros);

                        scheduled_events.push(ScheduledMidiEvent {
                            execute_at,
                            data: midi_data,
                        });

                        debug!(
                            "Scheduled event for +{} us (delta_ticks={})",
                            delta_micros, delta_ticks
                        );
                    }
                    Ok(None) => {
                        // Channel closed, exit
                        debug!("MIDI output channel closed");
                        break;
                    }
                    Err(_) => {
                        // Timeout - check for scheduled events to execute
                    }
                }

                // Execute all events that are due
                let now = Instant::now();
                while let Some(event) = scheduled_events.peek() {
                    if event.execute_at <= now {
                        let event = scheduled_events.pop().unwrap();
                        debug!("Executing scheduled MIDI event: {:?}", event.data);

                        if let Err(e) = connection.send(&event.data) {
                            warn!("Failed to send MIDI data to output: {}", e);
                        }
                    } else {
                        // Next event is in the future
                        break;
                    }
                }
            }

            // Connection is dropped here when the task ends, closing the MIDI output
            drop(connection);
        });

        Ok(handle)
    }

    async fn spawn_input_task(&self) -> Result<tokio::task::JoinHandle<()>> {
        let midi_in = MidiInput::new(&format!("nmidi-server-{}", self.name))?;
        let ports = midi_in.ports();

        if self.port_info.index >= ports.len() {
            anyhow::bail!(
                "MIDI input port index {} out of range",
                self.port_info.index
            );
        }

        let port = &ports[self.port_info.index];
        let port_name = self.port_info.name.clone();

        // Create channel for MIDI input (timestamp, data)
        // Use bounded channel to prevent unbounded memory growth if network sending is slow
        let (input_tx, mut input_rx) = mpsc::channel::<(u64, Vec<u8>)>(100);

        // Connect to MIDI input port with callback
        let connection = midi_in
            .connect(
                port,
                &self.name,
                move |timestamp, message, _| {
                    // Capture timestamp immediately when event is received
                    // Send MIDI message with timestamp through channel
                    // Use try_send to avoid blocking the MIDI callback thread
                    if let Err(e) = input_tx.try_send((timestamp, message.to_vec())) {
                        debug!("Failed to send MIDI input to channel: {}", e);
                    }
                },
                (),
            )
            .map_err(|e| anyhow::anyhow!("Failed to connect to MIDI input: {:?}", e))?;

        info!("Connected to MIDI input port: {}", port_name);

        // Spawn task to forward MIDI input to network
        let sockets = Arc::clone(&self.sockets);
        let sessions = Arc::clone(&self.sessions);
        let ssrc = self.ssrc;

        Ok(tokio::spawn(async move {
            let mut sequence = 0u16;

            // Overflow buffer for events that didn't fit in the previous packet
            let mut overflow: Option<(u64, Vec<u8>)> = None;

            loop {
                // Get the first event: either from overflow or wait for new event
                let (first_midir_timestamp, first_midi_data) = if let Some(event) = overflow.take()
                {
                    event
                } else {
                    match input_rx.recv().await {
                        Some(event) => event,
                        None => break, // Channel closed
                    }
                };

                debug!(
                    "Received MIDI input: {:?} at midir timestamp {}",
                    first_midi_data, first_midir_timestamp
                );

                // Get all connected peers
                let peers: Vec<SocketAddr> = {
                    let sessions_lock = sessions.lock().await;
                    sessions_lock.values().map(|s| s.data_addr).collect()
                };

                if peers.is_empty() {
                    continue;
                }

                // Convert midir timestamp to our timebase
                // midir timestamp is in platform-specific units, not directly usable
                // Get current time in microseconds and convert to RTP MIDI timestamp (10kHz clock rate)
                let current_micros = get_timestamp();
                let rtp_timestamp = micros_to_rtp_timestamp(current_micros);

                // Create RTP packet with MIDI data
                let mut packet = RtpPacket::new(ssrc, sequence, rtp_timestamp);

                // Add first event with delta_time of 0
                packet.add_command(0, first_midi_data);
                let mut last_timestamp = first_midir_timestamp;

                // Calculate initial packet size (RTP header + payload header + first command)
                // RTP header is 12 bytes, payload flags are 1-2 bytes, plus command data
                let mut estimated_size = 12 + 2; // RTP header + payload header
                estimated_size +=
                    SessionManager::estimate_command_size(0, &packet.commands[0].data);

                let mut event_count = 1;

                // Peek the channel for additional events to batch into the same packet
                // This optimization reduces network overhead by sending multiple MIDI
                // commands in a single RTP packet when events are already queued
                while let Ok((event_timestamp, midi_data)) = input_rx.try_recv() {
                    // Calculate delta time for this event
                    let delta_time = event_timestamp
                        .saturating_sub(last_timestamp)
                        .min(u32::MAX as u64) as u32;

                    // Estimate size this command would add to the packet
                    let command_size =
                        SessionManager::estimate_command_size(delta_time, &midi_data);

                    // Check if adding this command would exceed MTU
                    if estimated_size + command_size > MAX_UDP_PAYLOAD {
                        // Save this event for the next packet
                        overflow = Some((event_timestamp, midi_data));
                        break;
                    }

                    // Add command to packet
                    packet.add_command(delta_time, midi_data);
                    estimated_size += command_size;
                    last_timestamp = event_timestamp;
                    event_count += 1;
                }

                debug!(
                    "Batching {} MIDI events into single packet (estimated {} bytes)",
                    event_count, estimated_size
                );

                // Serialize packet once outside the loop
                let packet_bytes = packet.to_bytes();

                // TODO: Use multicast instead of iterating through every peer
                // for better performance with many connected clients

                // Send to all connected peers
                for peer_addr in peers {
                    if let Err(e) = sockets.data.send_to(&packet_bytes, &peer_addr).await {
                        warn!("Failed to send MIDI data to {}: {}", peer_addr, e);
                    }
                }

                sequence = sequence.wrapping_add(1);
            }

            // Connection is dropped here when the task ends, closing the MIDI input
            drop(connection);
        }))
    }

    /// Estimate the byte size a MIDI command will add to an RTP packet
    /// This includes the variable-length delta_time encoding and the MIDI data
    /// Variable-length encoding uses 7 bits per byte with the high bit as continuation flag
    fn estimate_command_size(delta_time: u32, midi_data: &[u8]) -> usize {
        // Estimate variable-length encoding of delta_time
        // Each byte encodes 7 bits, with the high bit indicating continuation
        let delta_size = if delta_time == 0 {
            1
        } else {
            let bits = 32 - delta_time.leading_zeros();
            bits.div_ceil(7).max(1) as usize // Round up to nearest 7-bit group
        };

        delta_size + midi_data.len()
    }

    fn spawn_control_handler(&self) -> tokio::task::JoinHandle<()> {
        let sockets = Arc::clone(&self.sockets);
        let sessions = Arc::clone(&self.sessions);
        let name = self.name.clone();
        let ssrc = self.ssrc;

        tokio::spawn(async move {
            loop {
                match sockets.recv_control().await {
                    Ok((packet, addr)) => {
                        if let Err(e) = Self::handle_control_packet(
                            &sockets,
                            &sessions,
                            &name,
                            ssrc,
                            packet,
                            addr,
                            SendOn::Control,
                        )
                        .await
                        {
                            warn!("Error handling control packet: {}", e);
                        }
                    }
                    Err(e) => warn!("Error receiving control packet: {}", e),
                }
            }
        })
    }

    fn spawn_data_handler(
        &self,
        midi_tx: mpsc::Sender<(u64, Vec<u8>)>,
    ) -> tokio::task::JoinHandle<()> {
        let sockets = Arc::clone(&self.sockets);
        let sessions = Arc::clone(&self.sessions);
        let port_type = self.port_info.port_type;
        let name = self.name.clone();
        let ssrc = self.ssrc;

        tokio::spawn(async move {
            loop {
                let mut buf = [0u8; MAX_UDP_PAYLOAD];
                match sockets.data.recv_from(&mut buf).await {
                    Ok((len, addr)) => {
                        // If this looks like an AppleMIDI control packet (signature 0xFFFF),
                        // handle it as control on the data socket. This is required because
                        // AppleMIDI sends a second invitation on the data port.
                        if len >= 2 {
                            let sig = u16::from_be_bytes([buf[0], buf[1]]);
                            if sig == APPLEMIDI_SIGNATURE {
                                match AppleMidiPacket::parse(&buf[..len]) {
                                    Ok(control_pkt) => {
                                        if let Err(e) = Self::handle_control_packet(
                                            &sockets,
                                            &sessions,
                                            &name,
                                            ssrc,
                                            control_pkt,
                                            addr,
                                            SendOn::Data,
                                        )
                                        .await
                                        {
                                            warn!("Error handling data-port control packet: {}", e);
                                        }
                                        continue;
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to parse control packet on data socket from {}: {}",
                                            addr, e
                                        );
                                        continue;
                                    }
                                }
                            }
                        }

                        // Otherwise treat as RTP data
                        match RtpPacket::parse(&buf[..len]) {
                            Ok(packet) => {
                                if let Err(e) = Self::handle_data_packet(
                                    &sessions, port_type, &midi_tx, packet, addr,
                                )
                                .await
                                {
                                    warn!("Error handling data packet: {}", e);
                                }
                            }
                            Err(e) => {
                                warn!("Failed to parse data packet from {}: {}", addr, e);
                            }
                        }
                    }
                    Err(e) => warn!("Error receiving data packet: {}", e),
                }
            }
        })
    }

    async fn handle_shutdown(
        name: &str,
        ssrc: u32,
        sockets: &NetworkSockets,
        sessions: &Arc<Mutex<HashMap<u32, SessionState>>>,
    ) -> Result<()> {
        info!("Shutting down session manager for {}", name);

        // Send End packets to all peers
        let sessions_snapshot = {
            let sessions_lock = sessions.lock().await;
            sessions_lock.clone()
        };

        for (peer_ssrc, session_state) in sessions_snapshot {
            let end_packet = AppleMidiPacket::End {
                version: APPLEMIDI_VERSION,
                token: session_state.token,
                ssrc,
            };

            if let Err(e) = sockets.send_control(&end_packet, &session_state.addr).await {
                warn!("Failed to send End packet to peer {}: {}", peer_ssrc, e);
            } else {
                info!("Sent End packet to peer {}", peer_ssrc);
            }
        }

        // Clear all sessions
        let mut sessions_lock = sessions.lock().await;
        sessions_lock.clear();

        Ok(())
    }

    async fn handle_control_packet(
        sockets: &NetworkSockets,
        sessions: &Arc<Mutex<HashMap<u32, SessionState>>>,
        name: &str,
        ssrc: u32,
        packet: AppleMidiPacket,
        addr: SocketAddr,
        send_on: SendOn,
    ) -> Result<()> {
        match packet {
            AppleMidiPacket::Invitation {
                version: _,
                token,
                ssrc: peer_ssrc,
                name: peer_name,
            } => {
                info!("Received invitation from {} ({})", peer_name, addr);

                // Send acceptance (protocol version is always 2 per spec)
                let response = AppleMidiPacket::InvitationAccepted {
                    version: APPLEMIDI_VERSION,
                    token,
                    ssrc,
                    name: name.to_string(),
                };
                match send_on {
                    SendOn::Control => sockets.send_control(&response, &addr).await?,
                    SendOn::Data => sockets.send_control_on_data(&response, &addr).await?,
                }

                // Store session - derive peer data port. If this invitation arrived
                // on the control socket, assume data is control+1. If it arrived on
                // the data socket (Apple’s second-stage invite), use the same port.
                let data_port = match send_on {
                    SendOn::Control => addr.port().checked_add(1).unwrap_or(addr.port()),
                    SendOn::Data => addr.port(),
                };
                let data_addr = SocketAddr::new(addr.ip(), data_port);
                let mut sessions_lock = sessions.lock().await;
                let entry = sessions_lock.entry(peer_ssrc).or_insert(SessionState {
                    ssrc: peer_ssrc,
                    token,
                    addr,
                    data_addr,
                    sequence: 0,
                    timestamp: 0,
                    time_offset_ticks: 0,
                    last_sync_count: 0,
                    last_status: None,
                });

                // Update address/token if they changed (peer may reinvite with same SSRC)
                entry.token = token;
                entry.addr = addr;
                entry.data_addr = data_addr;

                info!("Session established with {}", peer_name);

                // Send initial synchronization using shared function
                let sync = create_sync_request(ssrc);
                match send_on {
                    SendOn::Control => sockets.send_control(&sync, &addr).await?,
                    SendOn::Data => sockets.send_control_on_data(&sync, &addr).await?,
                }
            }
            AppleMidiPacket::InvitationAccepted {
                ssrc: peer_ssrc, ..
            } => {
                info!("Invitation accepted by peer SSRC {}", peer_ssrc);
            }
            AppleMidiPacket::End {
                ssrc: peer_ssrc, ..
            } => {
                info!("Session ended by peer SSRC {}", peer_ssrc);
                let mut sessions_lock = sessions.lock().await;
                sessions_lock.remove(&peer_ssrc);
            }
            AppleMidiPacket::Synchronization {
                ssrc: peer_ssrc,
                count,
                timestamp1,
                timestamp2,
                timestamp3,
            } => {
                info!("Received sync from SSRC {} (count {})", peer_ssrc, count);

                // Ensure we have a session entry for this SSRC so we can track time offset
                {
                    let mut sessions_lock = sessions.lock().await;
                    sessions_lock.entry(peer_ssrc).or_insert_with(|| {
                        // If we don't know the data port, prefer the port we received on
                        // when handled via data socket; otherwise control+1.
                        let data_port = match send_on {
                            SendOn::Control => addr.port().checked_add(1).unwrap_or(addr.port()),
                            SendOn::Data => addr.port(),
                        };
                        let data_addr = SocketAddr::new(addr.ip(), data_port);
                        SessionState {
                            ssrc: peer_ssrc,
                            token: 0,
                            addr,
                            data_addr,
                            sequence: 0,
                            timestamp: 0,
                            time_offset_ticks: 0,
                            last_sync_count: count,
                            last_status: None,
                        }
                    });
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
                    let mut sessions_lock = sessions.lock().await;
                    if let Some(session) = sessions_lock.get_mut(&peer_ssrc) {
                        session.time_offset_ticks = time_offset;
                        session.last_sync_count = count;
                    }
                } else {
                    // No offset calculated (initial request), just update count
                    let mut sessions_lock = sessions.lock().await;
                    if let Some(session) = sessions_lock.get_mut(&peer_ssrc) {
                        session.last_sync_count = count;
                    }
                }

                // Send response if needed
                if let Some(response) = sync_result.response {
                    match send_on {
                        SendOn::Control => sockets.send_control(&response, &addr).await?,
                        SendOn::Data => sockets.send_control_on_data(&response, &addr).await?,
                    }
                }
            }
            AppleMidiPacket::ReceiverFeedback {
                ssrc: peer_ssrc,
                sequence,
            } => {
                debug!(
                    "Received receiver feedback from SSRC {} (sequence {})",
                    peer_ssrc, sequence
                );
                // Currently no recovery journal support; keep session alive but no-op.
            }
        }

        Ok(())
    }

    async fn handle_data_packet(
        sessions: &Arc<Mutex<HashMap<u32, SessionState>>>,
        port_type: MidiPortType,
        midi_tx: &mpsc::Sender<(u64, Vec<u8>)>,
        packet: RtpPacket,
        addr: SocketAddr,
    ) -> Result<()> {
        // Only forward to MIDI output ports
        if port_type == MidiPortType::Output {
            // TODO: Verify all commands should be executed immediately and are not part of
            // the journal/recovery mechanism. Need to handle recovery information correctly
            // to get things on track in case of packet loss (RFC 6295 Section 5)

            // Look up the session to get time synchronization info and running status
            let (time_offset_ticks, mut last_status) = {
                let sessions_lock = sessions.lock().await;
                sessions_lock
                    .get(&packet.header.ssrc)
                    .map(|s| (s.time_offset_ticks, s.last_status))
                    .unwrap_or((0, None))
            };

            debug!(
                "Processing RTP packet {packet:?} from {}, time_offset={}",
                addr, time_offset_ticks
            );

            // Start from the RTP packet timestamp and add each command's delta_time to
            // compute a scheduled timestamp for that command.
            // The RTP timestamp is in the peer's timebase (10kHz clock).
            // Convert it to our local timebase using the time offset.

            // RTP timestamp is in 10kHz ticks (100us per tick)
            let rtp_timestamp_micros = (packet.header.timestamp as u64) * 100;

            // Apply time offset to convert from peer time to local time
            // time_offset_ticks is in 10kHz ticks: peer_time = local_time + offset
            // Therefore: local_time = peer_time - offset
            let time_offset_micros = time_offset_ticks * 100;
            let base_local_micros = if time_offset_micros >= 0 {
                rtp_timestamp_micros.saturating_sub(time_offset_micros as u64)
            } else {
                rtp_timestamp_micros.saturating_add((-time_offset_micros) as u64)
            };

            // Convert back to RTP ticks for internal consistency
            let mut current_timestamp = base_local_micros / 100;

            for cmd in &packet.commands {
                // Reconstruct full MIDI message with running status handling
                fn required_data_bytes(status: u8) -> Option<usize> {
                    match status & 0xF0 {
                        0x80 | 0x90 | 0xA0 | 0xB0 | 0xE0 => Some(2),
                        0xC0 | 0xD0 => Some(1),
                        _ => None,
                    }
                }

                // Skip real-time single-byte messages that can appear anywhere
                if cmd.data.len() == 1 {
                    let b = cmd.data[0];
                    if b >= 0xF8 || b == 0xFE {
                        continue;
                    }
                }

                let mut midi_bytes = cmd.data.clone();
                if midi_bytes.is_empty() {
                    continue;
                }

                if midi_bytes[0] < 0x80 {
                    // Running status: prepend last known status if available
                    if let Some(status) = last_status {
                        midi_bytes.insert(0, status);
                    } else {
                        // Can't reconstruct, drop
                        continue;
                    }
                }

                // Update running status on valid channel status bytes (exclude system real-time)
                let status = midi_bytes[0];
                if status < 0xF0 {
                    last_status = Some(status);
                }

                // Validate data length
                if let Some(needed) = required_data_bytes(status) {
                    if midi_bytes.len() < 1 + needed {
                        // Incomplete message, drop
                        continue;
                    }
                    midi_bytes.truncate(1 + needed);
                }

                // Accumulate delta times to get an absolute scheduled timestamp
                current_timestamp = current_timestamp.wrapping_add(cmd.delta_time as u64);

                debug!(
                    "Forwarding MIDI command: delta={}, scheduled_ts={}, data={:?}",
                    cmd.delta_time, current_timestamp, midi_bytes
                );

                // Send MIDI data with the computed scheduled timestamp to the output channel.
                // The output task will schedule playback based on this timestamp.
                // Use try_send with timeout to avoid blocking if channel is full
                match midi_tx.try_send((current_timestamp, midi_bytes)) {
                    Ok(_) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!("MIDI output channel full, dropping event (backpressure)");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        warn!("MIDI output channel closed");
                        break;
                    }
                }
            }

            // Persist updated running status back into the session
            let mut sessions_lock = sessions.lock().await;
            if let Some(session) = sessions_lock.get_mut(&packet.header.ssrc) {
                session.last_status = last_status;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    #[test]
    fn test_scheduled_midi_event_ordering() {
        // Test that ScheduledMidiEvent implements correct ordering for min-heap
        let now = Instant::now();
        let event1 = ScheduledMidiEvent {
            execute_at: now + Duration::from_millis(100),
            data: vec![0x90, 0x3C, 0x64],
        };
        let event2 = ScheduledMidiEvent {
            execute_at: now + Duration::from_millis(50),
            data: vec![0x80, 0x3C, 0x00],
        };
        let event3 = ScheduledMidiEvent {
            execute_at: now + Duration::from_millis(200),
            data: vec![0xB0, 0x07, 0x7F],
        };

        let mut heap = BinaryHeap::new();
        heap.push(event1.clone());
        heap.push(event2.clone());
        heap.push(event3.clone());

        // In a min-heap, earliest event should come first
        assert_eq!(heap.pop().unwrap().execute_at, event2.execute_at);
        assert_eq!(heap.pop().unwrap().execute_at, event1.execute_at);
        assert_eq!(heap.pop().unwrap().execute_at, event3.execute_at);
    }

    #[test]
    fn test_time_offset_calculation() {
        // Test time offset calculation logic with realistic RTT values
        // Simulate a peer that is 1000 microseconds ahead
        let peer_send_time = 10000u64; // timestamp1
        let our_receive_time = 9000u64; // timestamp2 (we're behind)
        let rtt = 200u64;
        let latency = rtt / 2;

        let peer_time_estimate = peer_send_time.saturating_add(latency);
        let our_current_time = our_receive_time + rtt;
        let time_offset = (peer_time_estimate as i64) - (our_current_time as i64);

        // peer_time_estimate = 10000 + 100 = 10100
        // our_current_time = 9000 + 200 = 9200
        // time_offset = 10100 - 9200 = 900
        assert_eq!(time_offset, 900);
    }

    #[test]
    fn test_timestamp_wraparound_detection() {
        // Test wraparound detection logic for u32 RTP timestamps
        let base_timestamp: u64 = 4294967200; // Near u32::MAX
        let new_timestamp: u64 = 100; // After wraparound

        // Check if this looks like a wraparound
        let is_wraparound = new_timestamp < base_timestamp
            && base_timestamp.wrapping_sub(new_timestamp) > (u32::MAX as u64 / 2);
        assert!(is_wraparound);

        // Calculate correct delta for wraparound case
        let ticks_to_max = (u32::MAX as u64).wrapping_sub(base_timestamp);
        let delta_ticks = ticks_to_max.wrapping_add(new_timestamp).wrapping_add(1);

        // Should be: (u32::MAX - 4294967200) + 100 + 1 = 95 + 100 + 1 = 196
        // u32::MAX = 4294967295
        // ticks_to_max = 4294967295 - 4294967200 = 95
        assert_eq!(delta_ticks, 196);
    }
}
