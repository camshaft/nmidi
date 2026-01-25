use anyhow::Result;
use midir::{MidiInput, MidiOutput};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, mpsc};
use tracing::{debug, info, warn};

use nmidi_core::{AppleMidiPacket, RtpPacket, APPLEMIDI_VERSION, network::NetworkSockets, util::{generate_ssrc, get_timestamp, micros_to_rtp_timestamp}};

use crate::midi::{MidiPortInfo, MidiPortType};

/// Maximum RTP packet size to stay within typical MTU bounds
/// Standard Ethernet MTU is 1500 bytes, minus IP (20) and UDP (8) headers = 1472 bytes
/// We use 1400 to leave some margin for headers and fragmentation avoidance
const MAX_PACKET_SIZE: usize = 1400;

#[derive(Debug, Clone)]
struct SessionState {
    ssrc: u32,
    token: u32,
    addr: SocketAddr,
    data_addr: SocketAddr,
    sequence: u16,
    timestamp: u32,
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
        
        // Spawn task to forward MIDI output to hardware
        let handle = tokio::spawn(async move {
            // TODO: Implement MIDI scheduler to handle delta_time correctly.
            // The scheduler should consume timestamped events from the channel
            // and schedule them for execution at the appropriate time based on delta_time.
            
            while let Some((timestamp, midi_data)) = midi_rx.recv().await {
                debug!("Forwarding MIDI output: {:?} at timestamp {}", midi_data, timestamp);
                
                if let Err(e) = connection.send(&midi_data) {
                    warn!("Failed to send MIDI data to output: {}", e);
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
            
            // Overflow buffer for events that didn't fit in the previous packet
            let mut overflow: Option<(u64, Vec<u8>)> = None;
            
            loop {
                // Get the first event: either from overflow or wait for new event
                let (first_midir_timestamp, first_midi_data) = if let Some(event) = overflow.take() {
                    event
                } else {
                    match input_rx.recv().await {
                        Some(event) => event,
                        None => break, // Channel closed
                    }
                };
                
                debug!("Received MIDI input: {:?} at midir timestamp {}", first_midi_data, first_midir_timestamp);
                
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
                estimated_size += Self::estimate_command_size(0, &packet.commands[0].data);
                
                let mut event_count = 1;
                
                // Peek the channel for additional events to batch into the same packet
                // This optimization reduces network overhead by sending multiple MIDI
                // commands in a single RTP packet when events are already queued
                loop {
                    match input_rx.try_recv() {
                        Ok((event_timestamp, midi_data)) => {
                            // Calculate delta time for this event
                            let delta_time = event_timestamp.saturating_sub(last_timestamp)
                                .min(u32::MAX as u64) as u32;
                            
                            // Estimate size this command would add to the packet
                            let command_size = Self::estimate_command_size(delta_time, &midi_data);
                            
                            // Check if adding this command would exceed MTU
                            if estimated_size + command_size > MAX_PACKET_SIZE {
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
                        Err(_) => {
                            // No more events queued, send what we have
                            break;
                        }
                    }
                }
                
                debug!("Batching {} MIDI events into single packet (estimated {} bytes)", event_count, estimated_size);
                
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
    fn estimate_command_size(delta_time: u32, midi_data: &[u8]) -> usize {
        // Estimate variable-length encoding of delta_time
        // Each byte encodes 7 bits, with the high bit indicating continuation
        let delta_size = if delta_time == 0 {
            1
        } else {
            let mut bits = 32 - delta_time.leading_zeros();
            ((bits + 6) / 7).max(1) as usize // Round up to nearest 7-bit group
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
                        timestamp3: get_timestamp(),
                    };
                    sockets.send_control(&response, &addr).await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_data_packet(
        _sessions: &Arc<Mutex<HashMap<u32, SessionState>>>,
        port_type: MidiPortType,
        midi_tx: &mpsc::Sender<(u64, Vec<u8>)>,
        packet: RtpPacket,
        _addr: SocketAddr,
    ) -> Result<()> {
        // Only forward to MIDI output ports
        if port_type == MidiPortType::Output {
            // TODO: Verify all commands should be executed immediately and are not part of
            // the journal/recovery mechanism. Need to handle recovery information correctly
            // to get things on track in case of packet loss (RFC 6295 Section 5)
            
            // Start from the RTP packet timestamp and add each command's delta_time to
            // compute a scheduled timestamp for that command.
            let mut current_timestamp = packet.header.timestamp as u64;
            
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
                // The output task can now schedule playback based on this timestamp.
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
