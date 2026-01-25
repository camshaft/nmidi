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
    midi_output_tx: Option<mpsc::UnboundedSender<Vec<u8>>>,
}

impl SessionManager {
    pub async fn new(name: String, bind_addr: String, port_info: MidiPortInfo) -> Result<Self> {
        let ssrc = generate_ssrc();
        let sockets = NetworkSockets::bind_consecutive(&bind_addr).await?;
        
        // For output ports, we'll set up the connection and channel in run()
        // This allows us to spawn the output task alongside input task
        Ok(Self {
            name,
            ssrc,
            sockets: Arc::new(sockets),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            port_info,
            midi_output_tx: None,
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

    pub async fn run(mut self, mut shutdown_rx: oneshot::Receiver<()>) -> Result<()> {
        let sockets = Arc::clone(&self.sockets);
        let sessions = Arc::clone(&self.sessions);
        let name = self.name.clone();
        let ssrc = self.ssrc;
        let port_type = self.port_info.port_type;

        // Set up MIDI output forwarding if this is an output port
        let midi_output_handle = if port_type == MidiPortType::Output {
            let midi_out = MidiOutput::new(&format!("nmidi-server-{}", name))?;
            let ports = midi_out.ports();
            
            if self.port_info.index >= ports.len() {
                anyhow::bail!("MIDI output port index {} out of range", self.port_info.index);
            }
            
            let port = &ports[self.port_info.index];
            let port_name = self.port_info.name.clone();
            
            // Create channel for MIDI output
            let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Vec<u8>>();
            self.midi_output_tx = Some(output_tx.clone());
            
            // Connect to MIDI output port
            let mut connection = midi_out.connect(port, &name)
                .map_err(|e| anyhow::anyhow!("Failed to connect to MIDI output: {:?}", e))?;
            
            info!("Connected to MIDI output port: {}", port_name);
            
            // Spawn task to forward MIDI output to hardware
            Some(tokio::spawn(async move {
                while let Some(midi_data) = output_rx.recv().await {
                    debug!("Forwarding MIDI output: {:?}", midi_data);
                    
                    if let Err(e) = connection.send(&midi_data) {
                        warn!("Failed to send MIDI data to output: {}", e);
                    }
                }
                
                // Connection is dropped here when the task ends, closing the MIDI output
                drop(connection);
            }))
        } else {
            None
        };

        // Set up MIDI input forwarding if this is an input port
        let midi_input_handle = if port_type == MidiPortType::Input {
            let midi_in = MidiInput::new(&format!("nmidi-server-{}", name))?;
            let ports = midi_in.ports();
            
            if self.port_info.index >= ports.len() {
                anyhow::bail!("MIDI input port index {} out of range", self.port_info.index);
            }
            
            let port = &ports[self.port_info.index];
            let port_name = self.port_info.name.clone();
            
            // Create channel for MIDI input (timestamp, data)
            let (input_tx, mut input_rx) = mpsc::unbounded_channel::<(u64, Vec<u8>)>();
            
            // Connect to MIDI input port with callback
            let connection = midi_in.connect(
                port,
                &name,
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
            let sockets_clone = Arc::clone(&sockets);
            let sessions_clone = Arc::clone(&sessions);
            
            Some(tokio::spawn(async move {
                let mut sequence = 0u16;
                
                while let Some((timestamp, midi_data)) = input_rx.recv().await {
                    debug!("Received MIDI input: {:?} at timestamp {}", midi_data, timestamp);
                    
                    // Get all connected peers
                    let peers: Vec<SocketAddr> = {
                        let sessions_lock = sessions_clone.lock().await;
                        sessions_lock.values().map(|s| s.data_addr).collect()
                    };
                    
                    if peers.is_empty() {
                        continue;
                    }
                    
                    // Create RTP packet with MIDI data, using captured timestamp
                    let mut packet = RtpPacket::new(ssrc, sequence, timestamp as u32);
                    packet.add_command(0, midi_data);
                    
                    // Serialize packet once outside the loop
                    let packet_bytes = packet.to_bytes();
                    
                    // Send to all connected peers
                    for peer_addr in peers {
                        if let Err(e) = sockets_clone.data.send_to(&packet_bytes, &peer_addr).await {
                            warn!("Failed to send MIDI data to {}: {}", peer_addr, e);
                        }
                    }
                    
                    sequence = sequence.wrapping_add(1);
                }
                
                // Connection is dropped here when the task ends, closing the MIDI input
                drop(connection);
            }))
        } else {
            None
        };

        let control_handle = {
            let sockets = Arc::clone(&sockets);
            let sessions = Arc::clone(&sessions);
            let name = name.clone();

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
        };

        let data_handle = {
            let sockets = Arc::clone(&sockets);
            let sessions = Arc::clone(&sessions);
            let midi_output_tx = self.midi_output_tx.clone();

            tokio::spawn(async move {
                loop {
                    match sockets.recv_data().await {
                        Ok((packet, addr)) => {
                            if let Err(e) =
                                Self::handle_data_packet(&sessions, &midi_output_tx, packet, addr).await
                            {
                                warn!("Error handling data packet: {}", e);
                            }
                        }
                        Err(e) => warn!("Error receiving data packet: {}", e),
                    }
                }
            })
        };

        // Wait for shutdown or task completion
        // Create optional futures for MIDI handlers
        let mut midi_output_fut = midi_output_handle;
        let mut midi_input_fut = midi_input_handle;
        
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
                Some(_) = async {
                    match &mut midi_output_fut {
                        Some(handle) => handle.await.ok(),
                        None => None,
                    }
                }, if midi_output_fut.is_some() => {
                    warn!("MIDI output handler task ended unexpectedly");
                    break;
                }
                Some(_) = async {
                    match &mut midi_input_fut {
                        Some(handle) => handle.await.ok(),
                        None => None,
                    }
                }, if midi_input_fut.is_some() => {
                    warn!("MIDI input handler task ended unexpectedly");
                    break;
                }
            }
        }

        Ok(())
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
        midi_output_tx: &Option<mpsc::UnboundedSender<Vec<u8>>>,
        packet: RtpPacket,
        _addr: SocketAddr,
    ) -> Result<()> {
        // Forward MIDI commands to local MIDI output port via channel
        if let Some(tx) = midi_output_tx {
            // TODO: Verify all commands should be executed immediately and are not part of
            // the journal/recovery mechanism. Need to handle recovery information correctly
            // to get things on track in case of packet loss (RFC 6295 Section 5)
            for cmd in &packet.commands {
                debug!(
                    "Forwarding MIDI command: delta={}, data={:?}",
                    cmd.delta_time, cmd.data
                );
                
                // TODO: Implement MIDI scheduler to handle delta_time correctly.
                // Currently all commands are executed immediately, but delta_time
                // should be used to schedule commands at the correct time.
                
                // Send MIDI data to the output channel
                if let Err(e) = tx.send(cmd.data.clone()) {
                    warn!("Failed to send MIDI data to output channel: {}", e);
                }
            }
        } else {
            // If no MIDI output channel, just log the received data
            for cmd in &packet.commands {
                info!(
                    "MIDI command (no output): delta={}, data={:?}",
                    cmd.delta_time, cmd.data
                );
            }
        }

        Ok(())
    }
}
