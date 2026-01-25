use anyhow::Result;
use midir::{MidiInput, MidiOutput};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, mpsc};
use tracing::{debug, info, warn};

use nmidi_core::{AppleMidiPacket, RtpPacket, APPLEMIDI_VERSION, network::NetworkSockets, util::{generate_ssrc, get_timestamp}};

use crate::midi::{MidiPortInfo, MidiPortType};

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
        let sockets = Arc::clone(&self.sockets);
        let sessions = Arc::clone(&self.sessions);
        let name = self.name.clone();
        let ssrc = self.ssrc;
        let port_type = self.port_info.port_type;

        // Create channel for MIDI output forwarding (used by data_handler -> output_task)
        let (midi_tx, midi_rx) = mpsc::unbounded_channel::<(u64, Vec<u8>)>();

        // Spawn MIDI task based on port type
        let midi_task = match port_type {
            MidiPortType::Output => {
                Self::spawn_output_task(
                    &name,
                    &self.port_info,
                    midi_rx,
                ).await?
            }
            MidiPortType::Input => {
                Self::spawn_input_task(
                    &name,
                    &self.port_info,
                    ssrc,
                    Arc::clone(&sessions),
                    Arc::clone(&sockets),
                ).await?
            }
        };

        let control_handle = Self::spawn_control_handler(
            Arc::clone(&sockets),
            Arc::clone(&sessions),
            name.clone(),
            ssrc,
        );

        let data_handle = Self::spawn_data_handler(
            Arc::clone(&sockets),
            Arc::clone(&sessions),
            port_type,
            midi_tx,
        );

        // Wait for shutdown or task completion
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    Self::handle_shutdown(&name, ssrc, &sockets, &sessions).await?;
                    break;
                }
                _ = control_handle => {
                    warn!("Control handler task ended unexpectedly");
                    break;
                }
                _ = data_handle => {
                    warn!("Data handler task ended unexpectedly");
                    break;
                }
                _ = midi_task => {
                    warn!("MIDI handler task ended unexpectedly");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn spawn_output_task(
        name: &str,
        port_info: &MidiPortInfo,
        mut midi_rx: mpsc::UnboundedReceiver<(u64, Vec<u8>)>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let midi_out = MidiOutput::new(&format!("nmidi-server-{}", name))?;
        let ports = midi_out.ports();
        
        if port_info.index >= ports.len() {
            anyhow::bail!("MIDI output port index {} out of range", port_info.index);
        }
        
        let port = &ports[port_info.index];
        let port_name = port_info.name.clone();
        
        // Connect to MIDI output port
        let mut connection = midi_out.connect(port, name)
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
        name: &str,
        port_info: &MidiPortInfo,
        ssrc: u32,
        sessions: Arc<Mutex<HashMap<u32, SessionState>>>,
        sockets: Arc<NetworkSockets>,
    ) -> Result<tokio::task::JoinHandle<()>> {
        let midi_in = MidiInput::new(&format!("nmidi-server-{}", name))?;
        let ports = midi_in.ports();
        
        if port_info.index >= ports.len() {
            anyhow::bail!("MIDI input port index {} out of range", port_info.index);
        }
        
        let port = &ports[port_info.index];
        let port_name = port_info.name.clone();
        
        // Create channel for MIDI input (timestamp, data)
        let (input_tx, mut input_rx) = mpsc::unbounded_channel::<(u64, Vec<u8>)>();
        
        // Connect to MIDI input port with callback
        let connection = midi_in.connect(
            port,
            name,
            move |timestamp, message, _| {
                // Capture timestamp immediately when event is received
                // Send MIDI message with timestamp through channel
                if let Err(e) = input_tx.send((timestamp, message.to_vec())) {
                    debug!("Failed to send MIDI input to channel: {}", e);
                }
            },
            (),
        ).map_err(|e| anyhow::anyhow!("Failed to connect to MIDI input: {:?}", e))?;
        
        info!("Connected to MIDI input port: {}", port_name);
        
        // Spawn task to forward MIDI input to network
        Ok(tokio::spawn(async move {
            let mut sequence = 0u16;
            
            while let Some((timestamp, midi_data)) = input_rx.recv().await {
                debug!("Received MIDI input: {:?} at timestamp {}", midi_data, timestamp);
                
                // Get all connected peers
                let peers: Vec<SocketAddr> = {
                    let sessions_lock = sessions.lock().await;
                    sessions_lock.values().map(|s| s.data_addr).collect()
                };
                
                if peers.is_empty() {
                    continue;
                }
                
                // Create RTP packet with MIDI data
                // TODO: Encode timestamp in RTP MIDI's special delta format per RFC 6295
                let mut packet = RtpPacket::new(ssrc, sequence, timestamp as u32);
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

    fn spawn_control_handler(
        sockets: Arc<NetworkSockets>,
        sessions: Arc<Mutex<HashMap<u32, SessionState>>>,
        name: String,
        ssrc: u32,
    ) -> tokio::task::JoinHandle<()> {
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
        sockets: Arc<NetworkSockets>,
        sessions: Arc<Mutex<HashMap<u32, SessionState>>>,
        port_type: MidiPortType,
        midi_tx: mpsc::UnboundedSender<(u64, Vec<u8>)>,
    ) -> tokio::task::JoinHandle<()> {
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
        midi_tx: &mpsc::UnboundedSender<(u64, Vec<u8>)>,
        packet: RtpPacket,
        _addr: SocketAddr,
    ) -> Result<()> {
        // Only forward to MIDI output ports
        if port_type == MidiPortType::Output {
            // TODO: Verify all commands should be executed immediately and are not part of
            // the journal/recovery mechanism. Need to handle recovery information correctly
            // to get things on track in case of packet loss (RFC 6295 Section 5)
            for cmd in &packet.commands {
                debug!(
                    "Forwarding MIDI command: delta={}, data={:?}",
                    cmd.delta_time, cmd.data
                );
                
                // Send MIDI data with timestamp to the output channel
                // The output task will handle scheduling based on delta_time
                let timestamp = get_timestamp();
                if let Err(e) = midi_tx.send((timestamp, cmd.data.clone())) {
                    warn!("Failed to send MIDI data to output channel: {}", e);
                }
            }
        }

        Ok(())
    }
}
