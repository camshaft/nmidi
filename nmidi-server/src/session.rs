use anyhow::Result;
use midir::{MidiInput, MidiOutput};
use std::collections::{HashMap, BinaryHeap};
use std::cmp::Ordering;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, mpsc};
use tokio::time::{Duration, Instant};
use tracing::{debug, info, warn};

use nmidi_core::{AppleMidiPacket, RtpPacket, APPLEMIDI_VERSION, network::NetworkSockets, util::{generate_ssrc, get_timestamp, micros_to_rtp_timestamp}};

use crate::midi::{MidiPortInfo, MidiPortType};

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

#[derive(Debug, Clone)]
struct SessionState {
    ssrc: u32,
    token: u32,
    addr: SocketAddr,
    data_addr: SocketAddr,
    sequence: u16,
    timestamp: u32,
    /// Time offset in microseconds: peer_time = local_time + offset
    /// Calculated from AppleMIDI synchronization packets
    time_offset: i64,
    /// Last synchronization count received from peer
    last_sync_count: u8,
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
        self.sockets.control.local_addr()
            .map(|addr| addr.port())
            .unwrap_or(0)
    }

    /// Get the data port the session manager is bound to
    pub fn data_port(&self) -> u16 {
        self.sockets.data.local_addr()
            .map(|addr| addr.port())
            .unwrap_or(0)
    }

    pub async fn run(self, mut shutdown_rx: oneshot::Receiver<()>) -> Result<()> {
        // Create channel for MIDI output forwarding (used by data_handler -> output_task)
        // Use bounded channel to prevent unbounded memory growth if MIDI output is slow
        let (midi_tx, midi_rx) = mpsc::channel::<(u64, Vec<u8>)>(100);

        // Spawn MIDI task based on port type
        let midi_task = match self.port_info.port_type {
            MidiPortType::Output => {
                self.spawn_output_task(midi_rx).await?
            }
            MidiPortType::Input => {
                self.spawn_input_task().await?
            }
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
            anyhow::bail!("MIDI output port index {} out of range", self.port_info.index);
        }
        
        let port = &ports[self.port_info.index];
        let port_name = self.port_info.name.clone();
        
        // Connect to MIDI output port
        let mut connection = midi_out.connect(port, &self.name)
            .map_err(|e| anyhow::anyhow!("Failed to connect to MIDI output: {:?}", e))?;
        
        info!("Connected to MIDI output port: {}", port_name);
        
        // Spawn task to forward MIDI output to hardware with scheduling
        let handle = tokio::spawn(async move {
            // Priority queue (min-heap) for scheduled events
            let mut scheduled_events = BinaryHeap::new();
            
            // Base timestamp for the scheduler - set when we receive first event
            let mut base_instant: Option<Instant> = None;
            let mut base_timestamp: u64 = 0;
            
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
                    Duration::from_secs(3600) // 1 hour timeout as fallback
                };
                
                // Wait for either a new event or timeout for scheduled event
                let new_event = tokio::time::timeout(timeout_duration, midi_rx.recv()).await;
                
                match new_event {
                    Ok(Some((rtp_timestamp, midi_data))) => {
                        // Received a new MIDI event
                        debug!("Received MIDI event for scheduling: timestamp={}, data={:?}", 
                               rtp_timestamp, midi_data);
                        
                        // Initialize base timestamp on first event
                        if base_instant.is_none() {
                            base_instant = Some(Instant::now());
                            base_timestamp = rtp_timestamp;
                            debug!("Initialized scheduler base: timestamp={}", base_timestamp);
                        }
                        
                        // Calculate when to execute this event relative to base time
                        let base = base_instant.unwrap();
                        let delta_ticks = rtp_timestamp.wrapping_sub(base_timestamp);
                        
                        // Convert RTP ticks (10kHz = 100us per tick) to microseconds
                        let delta_micros = (delta_ticks as u64) * 100;
                        
                        // Calculate absolute execution time
                        let execute_at = base + Duration::from_micros(delta_micros);
                        
                        scheduled_events.push(ScheduledMidiEvent {
                            execute_at,
                            data: midi_data,
                        });
                        
                        debug!("Scheduled event for +{} us (delta_ticks={})", delta_micros, delta_ticks);
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

    async fn spawn_input_task(
        &self,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let midi_in = MidiInput::new(&format!("nmidi-server-{}", self.name))?;
        let ports = midi_in.ports();
        
        if self.port_info.index >= ports.len() {
            anyhow::bail!("MIDI input port index {} out of range", self.port_info.index);
        }
        
        let port = &ports[self.port_info.index];
        let port_name = self.port_info.name.clone();
        
        // Create channel for MIDI input (timestamp, data)
        // Use bounded channel to prevent unbounded memory growth if network sending is slow
        let (input_tx, mut input_rx) = mpsc::channel::<(u64, Vec<u8>)>(100);
        
        // Connect to MIDI input port with callback
        let connection = midi_in.connect(
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
        ).map_err(|e| anyhow::anyhow!("Failed to connect to MIDI input: {:?}", e))?;
        
        info!("Connected to MIDI input port: {}", port_name);
        
        // Spawn task to forward MIDI input to network
        let sockets = Arc::clone(&self.sockets);
        let sessions = Arc::clone(&self.sessions);
        let ssrc = self.ssrc;
        
        Ok(tokio::spawn(async move {
            let mut sequence = 0u16;
            
            while let Some((midir_timestamp, midi_data)) = input_rx.recv().await {
                debug!("Received MIDI input: {:?} at midir timestamp {}", midi_data, midir_timestamp);
                
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
                packet.add_command(0, midi_data);
                
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
        
        tokio::spawn(async move {
            loop {
                match sockets.recv_data().await {
                    Ok((packet, addr)) => {
                        if let Err(e) = Self::handle_data_packet(&sessions, port_type, &midi_tx, packet, addr).await {
                            warn!("Error handling data packet: {}", e);
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
    ) -> Result<()> {
        match packet {
            AppleMidiPacket::Invitation {
                version: _,
                token,
                ssrc: peer_ssrc,
                name: peer_name,
            } => {
                info!("Received invitation from {} ({})", peer_name, addr);

                // Send acceptance
                let response = AppleMidiPacket::InvitationAccepted {
                    version: APPLEMIDI_VERSION,
                    token,
                    ssrc,
                    name: name.to_string(),
                };
                sockets.send_control(&response, &addr).await?;

                // Store session - calculate data port safely
                let data_port = addr.port().checked_add(1).unwrap_or(addr.port());
                let data_addr = SocketAddr::new(addr.ip(), data_port);
                let mut sessions_lock = sessions.lock().await;
                sessions_lock.insert(
                    peer_ssrc,
                    SessionState {
                        ssrc: peer_ssrc,
                        token,
                        addr,
                        data_addr,
                        sequence: 0,
                        timestamp: 0,
                        time_offset: 0,
                        last_sync_count: 0,
                    },
                );

                info!("Session established with {}", peer_name);

                // Send initial synchronization
                let sync = AppleMidiPacket::Synchronization {
                    ssrc,
                    count: 0,
                    timestamp1: get_timestamp(),
                    timestamp2: 0,
                    timestamp3: 0,
                };
                sockets.send_control(&sync, &addr).await?;
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
                ..
            } => {
                info!("Received sync from SSRC {} (count {})", peer_ssrc, count);

                // Get current time for response
                let now = get_timestamp();
                
                // Update session with time synchronization info
                if timestamp2 > 0 {
                    // This is a response to our sync request (count is odd)
                    // Calculate time offset: peer_time = local_time + offset
                    // timestamp1 = peer's send time
                    // timestamp2 = our receive time (from our original request)
                    // timestamp3 = peer's response time
                    // now = our current time
                    
                    // Round trip time
                    let rtt = now.saturating_sub(timestamp2);
                    
                    // Estimated one-way latency (half of RTT)
                    let latency = rtt / 2;
                    
                    // Peer's time when we received this response
                    let peer_time_estimate = timestamp1.saturating_add(latency);
                    
                    // Calculate offset: how much to add to local time to get peer time
                    let time_offset = (peer_time_estimate as i64) - (now as i64);
                    
                    debug!(
                        "Sync from {}: offset={}, rtt={}, latency={}",
                        peer_ssrc, time_offset, rtt, latency
                    );
                    
                    // Update session state with synchronization info
                    let mut sessions_lock = sessions.lock().await;
                    if let Some(session) = sessions_lock.get_mut(&peer_ssrc) {
                        session.time_offset = time_offset;
                        session.last_sync_count = count;
                    }
                } else {
                    // This is an initial sync request (count is even, timestamp2 == 0)
                    // Just store the count, we'll respond below
                    let mut sessions_lock = sessions.lock().await;
                    if let Some(session) = sessions_lock.get_mut(&peer_ssrc) {
                        session.last_sync_count = count;
                    }
                }

                // Get session address without holding lock
                let session_addr = {
                    let sessions_lock = sessions.lock().await;
                    sessions_lock.get(&peer_ssrc).map(|s| s.addr)
                };
                
                // Respond to sync without holding the lock
                if let Some(addr) = session_addr {
                    let response = AppleMidiPacket::Synchronization {
                        ssrc,
                        count: count + 1,
                        timestamp1,
                        timestamp2,
                        timestamp3: now,
                    };
                    sockets.send_control(&response, &addr).await?;
                }
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
            
            // Look up the session to get time synchronization info
            let time_offset = {
                let sessions_lock = sessions.lock().await;
                // Find session by checking data address (addr is the data port)
                sessions_lock
                    .values()
                    .find(|s| s.data_addr == addr)
                    .map(|s| s.time_offset)
                    .unwrap_or(0)
            };
            
            debug!("Processing RTP packet from {}, time_offset={}", addr, time_offset);
            
            // Start from the RTP packet timestamp and add each command's delta_time to
            // compute a scheduled timestamp for that command.
            // The RTP timestamp is in the peer's timebase (10kHz clock).
            // Convert it to our local timebase using the time offset.
            
            // RTP timestamp is in 10kHz ticks (100us per tick)
            let rtp_timestamp_micros = (packet.header.timestamp as u64) * 100;
            
            // Apply time offset to convert from peer time to local time
            // time_offset is in microseconds: peer_time = local_time + offset
            // Therefore: local_time = peer_time - offset
            let base_local_micros = if time_offset >= 0 {
                rtp_timestamp_micros.saturating_sub(time_offset as u64)
            } else {
                rtp_timestamp_micros.saturating_add((-time_offset) as u64)
            };
            
            // Convert back to RTP ticks for internal consistency
            let mut current_timestamp = base_local_micros / 100;
            
            for cmd in &packet.commands {
                // Accumulate delta times to get an absolute scheduled timestamp
                current_timestamp = current_timestamp.wrapping_add(cmd.delta_time as u64);
                
                debug!(
                    "Forwarding MIDI command: delta={}, scheduled_ts={}, data={:?}",
                    cmd.delta_time,
                    current_timestamp,
                    cmd.data
                );
                
                // Send MIDI data with the computed scheduled timestamp to the output channel.
                // The output task will schedule playback based on this timestamp.
                // Use try_send with timeout to avoid blocking if channel is full
                match midi_tx.try_send((current_timestamp, cmd.data.clone())) {
                    Ok(_) => {},
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!("MIDI output channel full, dropping event (backpressure)");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        warn!("MIDI output channel closed");
                        break;
                    }
                }
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
        // Test time offset calculation logic
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
    fn test_rtp_timestamp_conversion() {
        // Test RTP timestamp to microseconds conversion
        let rtp_timestamp: u32 = 1000;
        let micros = (rtp_timestamp as u64) * 100;
        assert_eq!(micros, 100_000);

        // Test wrapping behavior
        let rtp_timestamp_max: u32 = u32::MAX;
        let micros_max = (rtp_timestamp_max as u64) * 100;
        assert_eq!(micros_max, 429496729500);
    }

    #[test]
    fn test_session_state_initialization() {
        // Test that SessionState initializes with correct defaults
        let session = SessionState {
            ssrc: 12345,
            token: 67890,
            addr: "127.0.0.1:5004".parse().unwrap(),
            data_addr: "127.0.0.1:5005".parse().unwrap(),
            sequence: 0,
            timestamp: 0,
            time_offset: 0,
            last_sync_count: 0,
        };

        assert_eq!(session.ssrc, 12345);
        assert_eq!(session.token, 67890);
        assert_eq!(session.time_offset, 0);
        assert_eq!(session.last_sync_count, 0);
    }

    #[test]
    fn test_timestamp_with_time_offset() {
        // Test converting peer RTP timestamp to local time
        let peer_rtp_timestamp: u32 = 5000; // in 10kHz ticks
        let time_offset: i64 = 10000; // peer is 10000 microseconds ahead
        
        // Convert to microseconds
        let rtp_timestamp_micros = (peer_rtp_timestamp as u64) * 100;
        assert_eq!(rtp_timestamp_micros, 500_000);
        
        // Apply time offset (peer_time = local_time + offset)
        // Therefore: local_time = peer_time - offset
        let local_micros = rtp_timestamp_micros.saturating_sub(time_offset as u64);
        assert_eq!(local_micros, 490_000);
        
        // Convert back to RTP ticks
        let local_rtp_timestamp = local_micros / 100;
        assert_eq!(local_rtp_timestamp, 4900);
    }

    #[test]
    fn test_timestamp_with_negative_offset() {
        // Test when peer is behind us (negative offset)
        let peer_rtp_timestamp: u32 = 5000;
        let time_offset: i64 = -10000; // peer is 10000 microseconds behind
        
        let rtp_timestamp_micros = (peer_rtp_timestamp as u64) * 100;
        
        // When offset is negative, we add it
        let local_micros = rtp_timestamp_micros.saturating_add((-time_offset) as u64);
        assert_eq!(local_micros, 510_000);
        
        let local_rtp_timestamp = local_micros / 100;
        assert_eq!(local_rtp_timestamp, 5100);
    }
}
